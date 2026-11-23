//! Schedule one ping with `call_soon_threadsafe` and wait for the callback.
//!
//! `asyncio.sleep` lets the loop run the callback. A stuck loop does not.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyCFunction;

static PING_BUSY: AtomicBool = AtomicBool::new(false);

/// `true` when this call owns the single in-flight ping.
pub fn begin_ping() -> bool {
    !PING_BUSY.swap(true, Ordering::SeqCst)
}

pub fn end_ping() {
    PING_BUSY.store(false, Ordering::SeqCst);
}

pub fn ping_busy() -> bool {
    PING_BUSY.load(Ordering::SeqCst)
}

/// Run one stage. `schedule` must queue a callback that sets the flag.
pub fn run_stage<S, W>(schedule: S, wait: W) -> bool
where
    S: FnOnce() -> bool,
    W: FnOnce() -> bool,
{
    if !begin_ping() {
        return false;
    }
    let scheduled = schedule();
    let responded = scheduled && wait();
    end_ping();
    responded
}

pub fn wait_flag(flag: &AtomicBool, timeout: Duration) -> bool {
    let started = std::time::Instant::now();
    while started.elapsed() < timeout {
        if flag.load(Ordering::SeqCst) {
            return true;
        }
        thread::sleep(Duration::from_millis(5));
    }
    flag.load(Ordering::SeqCst)
}

/// Ping `loop_obj` from another thread. A miss returns `false` and does not raise.
pub fn ping_loop(loop_obj: Py<PyAny>, timeout: Duration) -> bool {
    let flag = Arc::new(AtomicBool::new(false));
    let flag_for_wait = Arc::clone(&flag);
    run_stage(
        || schedule_threadsafe(loop_obj, flag, timeout),
        || wait_flag(&flag_for_wait, timeout),
    )
}

fn schedule_threadsafe(loop_obj: Py<PyAny>, flag: Arc<AtomicBool>, timeout: Duration) -> bool {
    let (sender, receiver) = mpsc::channel();
    let worker = thread::Builder::new().name("arq-ping".into()).spawn(move || {
        let result = Python::attach(|py| schedule_on(py, loop_obj.bind(py), &flag));
        let _ = sender.send(result);
    });
    if worker.is_err() {
        return false;
    }
    match receiver.recv_timeout(timeout) {
        Ok(Ok(())) => true,
        Ok(Err(_)) | Err(_) => false,
    }
}

fn schedule_on(py: Python<'_>, event_loop: &Bound<'_, PyAny>, flag: &Arc<AtomicBool>) -> PyResult<()> {
    let flag = Arc::clone(flag);
    let callback = PyCFunction::new_closure(py, None, None, move |_args, _kwargs| {
        flag.store(true, Ordering::SeqCst);
        Ok::<_, PyErr>(())
    })?;
    event_loop.call_method1("call_soon_threadsafe", (callback,))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_second_ping_is_rejected_until_the_first_finishes() {
        assert!(begin_ping());
        assert!(ping_busy());
        assert!(!begin_ping());
        assert!(!run_stage(|| true, || true));
        end_ping();
        assert!(!ping_busy());
        assert!(run_stage(|| true, || true));
        assert!(!run_stage(|| false, || true));
        assert!(!run_stage(|| true, || false));
    }

    #[test]
    fn wait_flag_sees_a_flag_that_is_already_set() {
        let flag = AtomicBool::new(true);
        assert!(wait_flag(&flag, Duration::from_millis(1)));
        let flag = AtomicBool::new(false);
        assert!(!wait_flag(&flag, Duration::from_millis(1)));
    }
}
