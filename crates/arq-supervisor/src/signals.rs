//! `SIGTERM` and `SIGINT` are armed before any slot starts.

use std::io;

use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio::sync::watch;

#[derive(Clone)]
pub struct Shutdown {
    flag: watch::Sender<bool>,
    /// `watch::send` drops the value when no receiver is alive.
    _keep: watch::Receiver<bool>,
}

impl Shutdown {
    pub fn new() -> Self {
        let (flag, keep) = watch::channel(false);
        Self { flag, _keep: keep }
    }

    pub fn request(&self) {
        let _ = self.flag.send(true);
    }

    pub fn is_requested(&self) -> bool {
        *self.flag.borrow()
    }

    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.flag.subscribe()
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

pub struct StopSignals {
    terminate: Signal,
    interrupt: Signal,
}

impl StopSignals {
    pub fn arm() -> io::Result<Self> {
        Ok(Self { terminate: signal(SignalKind::terminate())?, interrupt: signal(SignalKind::interrupt())? })
    }
}

pub async fn stop_requested(shutdown: &Shutdown, signals: &mut StopSignals) -> bool {
    if shutdown.is_requested() {
        return true;
    }
    let mut flag = shutdown.subscribe();
    tokio::select! {
        biased;
        _ = signals.terminate.recv() => true,
        _ = signals.interrupt.recv() => true,
        changed = flag.changed() => changed.is_ok() && *flag.borrow(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{block_on, test_lock};
    use std::time::Duration;

    #[test]
    fn sigterm_and_sigint_are_armed_before_slots_and_request_stop() {
        let _guard = test_lock();
        block_on(async {
            let shutdown = Shutdown::new();
            let mut signals = StopSignals::arm().unwrap();
            let early = tokio::time::timeout(Duration::from_millis(30), stop_requested(&shutdown, &mut signals)).await;
            assert!(early.is_err(), "nothing has asked the supervisor to stop");
            assert!(!shutdown.is_requested());
            assert!(crate::osutil::signal_pid(std::process::id(), libc::SIGTERM));
            assert!(stop_requested(&shutdown, &mut signals).await);
            assert!(crate::osutil::signal_pid(std::process::id(), libc::SIGINT));
            assert!(stop_requested(&shutdown, &mut signals).await);
            shutdown.request();
            assert!(shutdown.is_requested());
            assert!(stop_requested(&shutdown, &mut signals).await);
            let shutdown = Shutdown::default();
            assert!(!shutdown.is_requested());
        });
    }
}
