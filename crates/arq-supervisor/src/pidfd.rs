//! The child learns that the supervisor is gone by waiting on a pidfd.
//!
//! Linux without `pidfd_open` does not start slots. The child claims the inherited
//! descriptor after the runtime is up and before Python is imported. Parent exit,
//! including `SIGKILL`, starts the same drain as `SIGUSR1`. A live parent, even a
//! stuck one, is not an exit. A drain that does not return is cut off by
//! `ARQ_SHUTDOWN_TIMEOUT_MS`.

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{Interest, unix::AsyncFd};
use tokio::sync::Notify;

use crate::fd::{set_cloexec, set_nonblock};

pub const PARENT_FD_ENV: &str = "ARQRS_PARENT_PIDFD";

#[derive(Debug)]
pub struct PidfdError {
    pub message: String,
}

impl std::fmt::Display for PidfdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for PidfdError {}

impl PidfdError {
    fn new(message: impl Into<String>) -> Self {
        Self { message: message.into() }
    }
}

#[derive(Clone)]
pub struct ParentWatch {
    inner: Arc<Inner>,
}

struct Inner {
    exited: AtomicBool,
    watching: AtomicBool,
    detached: AtomicBool,
    failed: Mutex<Option<String>>,
    notify: Notify,
}

impl ParentWatch {
    pub fn detached() -> Self {
        let watch = Self::empty();
        watch.inner.detached.store(true, Ordering::SeqCst);
        watch
    }

    pub fn broken(message: impl Into<String>) -> Self {
        let watch = Self::empty();
        watch.mark_failed(io::Error::other(message.into()));
        watch
    }

    fn empty() -> Self {
        Self {
            inner: Arc::new(Inner {
                exited: AtomicBool::new(false),
                watching: AtomicBool::new(false),
                detached: AtomicBool::new(false),
                failed: Mutex::new(None),
                notify: Notify::new(),
            }),
        }
    }

    pub fn exited(&self) -> bool {
        self.inner.exited.load(Ordering::SeqCst)
    }

    pub fn is_watching(&self) -> bool {
        self.inner.watching.load(Ordering::SeqCst)
    }

    pub fn is_detached(&self) -> bool {
        self.inner.detached.load(Ordering::SeqCst)
    }

    pub fn failed(&self) -> Option<String> {
        self.inner.failed.lock().unwrap_or_else(|err| err.into_inner()).clone()
    }

    /// The watch is armed, already observed an exit, or this process has no parent pidfd.
    pub fn ready_before_python(&self) -> bool {
        self.is_detached() || self.exited() || self.is_watching()
    }

    fn mark_exited(&self) {
        self.inner.exited.store(true, Ordering::SeqCst);
        self.wake();
    }

    fn mark_watching(&self) {
        self.inner.watching.store(true, Ordering::SeqCst);
        self.wake();
    }

    fn mark_failed(&self, error: io::Error) {
        *self.inner.failed.lock().unwrap_or_else(|err| err.into_inner()) = Some(error.to_string());
        self.wake();
    }

    fn wake(&self) {
        self.inner.notify.notify_waiters();
        self.inner.notify.notify_one();
    }

    async fn until_watching(&self) {
        loop {
            if self.is_watching() || self.failed().is_some() || self.exited() {
                return;
            }
            self.inner.notify.notified().await;
        }
    }

    pub async fn serve(fd: OwnedFd) -> io::Result<Self> {
        let watch = Self::empty();
        set_nonblock(fd.as_raw_fd())?;
        if pidfd_terminated(fd.as_raw_fd())? {
            watch.mark_exited();
            return Ok(watch);
        }
        // SAFETY: `fd` is an owned pidfd. It stays inside this `AsyncFd` until the
        // watch task drops it, and `as_raw_fd` keeps returning that descriptor.
        let async_fd = unsafe { AsyncFd::register_with_interest(fd, Interest::READABLE) }.map_err(std::io::Error::from)?;
        if pidfd_terminated(async_fd.as_raw_fd())? {
            watch.mark_exited();
            return Ok(watch);
        }
        let background = watch.clone();
        tokio::spawn(async move {
            background.mark_watching();
            match wait_pidfd(async_fd).await {
                Ok(()) => background.mark_exited(),
                Err(error) => background.mark_failed(error),
            }
        });
        watch.until_watching().await;
        if let Some(error) = watch.failed() {
            return Err(io::Error::other(error));
        }
        Ok(watch)
    }
}

pub fn open_supervisor_pidfd() -> io::Result<OwnedFd> {
    open_pidfd(std::process::id())
}

pub fn open_pidfd(pid: u32) -> io::Result<OwnedFd> {
    let pid = libc::pid_t::try_from(pid).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "pid"))?;
    let fd = match pidfd_syscall(pid, libc::O_NONBLOCK as libc::c_uint) {
        Ok(fd) => fd,
        Err(error) if error.raw_os_error() == Some(libc::EINVAL) => pidfd_syscall(pid, 0)?,
        Err(error) => return Err(error),
    };
    set_nonblock(fd.as_raw_fd())?;
    set_cloexec(fd.as_raw_fd())?;
    Ok(fd)
}

fn pidfd_syscall(pid: libc::pid_t, flags: libc::c_uint) -> io::Result<OwnedFd> {
    let rc = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, flags) };
    if rc < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { OwnedFd::from_raw_fd(rc as i32) })
    }
}

/// Take the inherited supervisor pidfd once. The variable is removed so a later
/// `Config::from_env` does not see an unknown `ARQ_*` name. This name is `ARQRS_`.
pub fn claim_parent_pidfd() -> Result<Option<OwnedFd>, PidfdError> {
    let Some(raw) = std::env::var(PARENT_FD_ENV).ok() else {
        return Ok(None);
    };
    unsafe { std::env::remove_var(PARENT_FD_ENV) };
    let fd: i32 = raw.parse().map_err(|_| PidfdError::new(format!("{PARENT_FD_ENV} is {raw:?}, not a descriptor")))?;
    if let Err(error) = probe(fd) {
        return Err(PidfdError::new(format!("{PARENT_FD_ENV}={fd} is not a pidfd ({error})")));
    }
    let owned = unsafe { OwnedFd::from_raw_fd(fd) };
    set_cloexec(owned.as_raw_fd()).map_err(|error| PidfdError::new(error.to_string()))?;
    Ok(Some(owned))
}

fn probe(fd: i32) -> io::Result<()> {
    loop {
        let rc = unsafe { libc::syscall(libc::SYS_pidfd_send_signal, fd, 0, 0, 0) };
        if rc == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return match error.raw_os_error() {
            Some(libc::ESRCH | libc::EPERM) => Ok(()),
            _ => Err(error),
        };
    }
}

/// Claim the pidfd and start the watch before `import_python` runs.
pub async fn prepare_child_runtime(import_python: impl FnOnce(&ParentWatch)) -> Result<ParentWatch, PidfdError> {
    let watch = match claim_parent_pidfd()? {
        None => ParentWatch::detached(),
        Some(fd) => ParentWatch::serve(fd).await.map_err(|error| PidfdError::new(error.to_string()))?,
    };
    if let Some(error) = watch.failed() {
        return Err(PidfdError::new(error));
    }
    import_python(&watch);
    Ok(watch)
}

pub async fn cut_off_drain<T>(work: impl std::future::Future<Output = T>, timeout: Duration) -> Result<T, DrainExpired> {
    match tokio::time::timeout(timeout, work).await {
        Ok(value) => Ok(value),
        Err(_) => Err(DrainExpired),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrainExpired;

impl std::fmt::Display for DrainExpired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("drain did not return before ARQ_SHUTDOWN_TIMEOUT_MS")
    }
}

impl std::error::Error for DrainExpired {}

fn pidfd_terminated(fd: i32) -> io::Result<bool> {
    loop {
        let mut pollfd = libc::pollfd { fd, events: libc::POLLIN, revents: 0 };
        let rc = unsafe { libc::poll(&mut pollfd, 1, 0) };
        if rc < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if rc == 0 {
            return Ok(false);
        }
        let revents = pollfd.revents;
        if revents & libc::POLLNVAL != 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "supervisor pidfd is not pollable"));
        }
        if revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) == 0 {
            return Ok(false);
        }
        return Ok(true);
    }
}

async fn wait_pidfd(fd: AsyncFd<OwnedFd>) -> io::Result<()> {
    loop {
        let mut guard = fd.ready(Interest::READABLE).await?;
        match guard.try_io(|inner| match pidfd_terminated(inner.as_raw_fd())? {
            true => Ok(()),
            false => Err(io::Error::new(io::ErrorKind::WouldBlock, "supervisor pidfd is not terminated")),
        }) {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(error)) => return Err(error),
            Err(_blocked) => continue,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        crate::testutil::env_lock()
    }

    fn sleep_child() -> std::process::Child {
        Command::new("/bin/sleep").arg("30").spawn().unwrap()
    }

    #[test]
    fn open_self_succeeds_and_a_missing_pid_fails() {
        let fd = open_supervisor_pidfd().unwrap();
        assert!(crate::fd::is_cloexec(fd.as_raw_fd()).unwrap());
        assert!(open_pidfd(u32::MAX).is_err());
        assert!(pidfd_terminated(2_000).is_err());
    }

    #[tokio::test]
    async fn parent_exit_is_visible_and_a_live_parent_is_not() {
        let mut child = sleep_child();
        let fd = open_pidfd(child.id()).unwrap();
        assert!(!pidfd_terminated(fd.as_raw_fd()).unwrap());
        let watch = ParentWatch::serve(fd).await.unwrap();
        assert!(watch.is_watching());
        assert!(!watch.exited());
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(!watch.exited(), "a live parent is not an exit");
        assert!(crate::osutil::signal_pid(child.id(), libc::SIGKILL));
        let _ = child.wait();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !watch.exited() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(watch.exited());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_already_reaped_parent_is_an_exit_before_python_import() {
        let _guard = env_lock();
        let mut child = sleep_child();
        let fd = open_pidfd(child.id()).unwrap();
        assert!(crate::osutil::signal_pid(child.id(), libc::SIGKILL));
        let _ = child.wait();
        assert!(pidfd_terminated(fd.as_raw_fd()).unwrap());
        let mut imported = false;
        unsafe { std::env::set_var(PARENT_FD_ENV, fd.as_raw_fd().to_string()) };
        std::mem::forget(fd);
        let watch = prepare_child_runtime(|watch| {
            assert!(watch.ready_before_python());
            assert!(watch.exited());
            imported = true;
        })
        .await
        .unwrap();
        assert!(imported);
        assert!(watch.exited());
        assert!(std::env::var(PARENT_FD_ENV).is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepare_imports_python_only_after_the_watch_is_armed() {
        let _guard = env_lock();
        let mut child = sleep_child();
        let fd = open_pidfd(child.id()).unwrap();
        let raw = fd.as_raw_fd();
        unsafe { std::env::set_var(PARENT_FD_ENV, raw.to_string()) };
        std::mem::forget(fd);
        let mut imported = false;
        let watch = prepare_child_runtime(|watch| {
            assert!(watch.is_watching());
            assert!(!watch.exited());
            assert!(watch.ready_before_python());
            imported = true;
        })
        .await
        .unwrap();
        assert!(imported);
        assert!(crate::osutil::signal_pid(child.id(), libc::SIGKILL));
        let _ = child.wait();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !watch.exited() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(watch.exited());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_hand_started_child_has_no_pidfd_and_a_bad_one_does_not_import() {
        let _guard = env_lock();
        unsafe { std::env::remove_var(PARENT_FD_ENV) };
        let mut imported = false;
        let watch = prepare_child_runtime(|watch| {
            assert!(watch.is_detached());
            assert!(watch.ready_before_python());
            imported = true;
        })
        .await
        .unwrap();
        assert!(imported);
        assert!(watch.is_detached());
        assert!(watch.failed().is_none());

        unsafe { std::env::set_var(PARENT_FD_ENV, "nope") };
        imported = false;
        let Err(error) = prepare_child_runtime(|_| imported = true).await else {
            panic!("a bad descriptor must fail before python import");
        };
        assert!(!imported);
        assert!(!error.to_string().is_empty());
        let mem = unsafe { libc::memfd_create(c"not-a-pidfd".as_ptr(), 0) };
        assert!(mem >= 0);
        unsafe { std::env::set_var(PARENT_FD_ENV, mem.to_string()) };
        let Err(error) = prepare_child_runtime(|_| imported = true).await else {
            panic!("a memfd is not a pidfd");
        };
        assert!(!imported);
        assert!(error.to_string().contains("not a pidfd"));
        unsafe { libc::close(mem) };
        unsafe { std::env::remove_var(PARENT_FD_ENV) };
        let broken = ParentWatch::broken("watch failed");
        assert!(broken.failed().is_some());
        assert!(!broken.ready_before_python());
    }

    #[test]
    fn claim_twice_returns_none_the_second_time() {
        let _guard = env_lock();
        let fd = open_supervisor_pidfd().unwrap();
        let raw = fd.as_raw_fd();
        unsafe { std::env::set_var(PARENT_FD_ENV, raw.to_string()) };
        std::mem::forget(fd);
        let claimed = claim_parent_pidfd().unwrap().unwrap();
        assert!(std::env::var(PARENT_FD_ENV).is_err());
        assert!(claim_parent_pidfd().unwrap().is_none());
        drop(claimed);
    }

    #[tokio::test]
    async fn a_drain_that_does_not_return_is_cut_off() {
        let expired = cut_off_drain(std::future::pending::<u8>(), Duration::from_millis(20)).await;
        assert_eq!(expired, Err(DrainExpired));
        assert!(!DrainExpired.to_string().is_empty());
        let ready = cut_off_drain(async { 7 }, Duration::from_secs(1)).await.unwrap();
        assert_eq!(ready, 7);
    }
}
