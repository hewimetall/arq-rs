//! Slot spawn goes through watchexec-supervisor 6.0.0.
//!
//! `start_job` is the spawn mechanism. The process group and signal delivery are the
//! ones that crate applies (`SpawnOptions.grouped`). A metrics helper is not a job.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use watchexec_supervisor::command::{Command, Program, SpawnOptions};
use watchexec_supervisor::job::{start_job, CommandState, Job};
use watchexec_supervisor::ProcessEnd;

use crate::fd::clear_cloexec;
use crate::osutil::signal_group;
use crate::pidfd::PARENT_FD_ENV;

pub const SPAWN_CRATE: &str = "watchexec-supervisor";

#[derive(Debug, Clone)]
pub struct CommandSpec {
    pub program: String,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
}

impl CommandSpec {
    pub fn new(program: impl Into<String>) -> Self {
        Self { program: program.into(), args: Vec::new(), env: HashMap::new() }
    }

    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }

    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(key.into(), value.into());
        self
    }
}

pub struct SlotChild {
    job: Job,
    task: Option<tokio::task::JoinHandle<()>>,
    pid: u32,
    reaped: bool,
}

impl SlotChild {
    pub fn id(&self) -> u32 {
        self.pid
    }

    pub async fn wait(mut self) -> io::Result<Option<i32>> {
        self.job.to_wait().await;
        self.reaped = true;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.job
            .run(move |ctx| {
                if let CommandState::Finished { status, .. } = ctx.current {
                    let _ = sender.send(status.clone());
                }
            })
            .await;
        let status = receiver.await.map_err(io::Error::other)?;
        if let Some(task) = self.task.take() {
            task.abort();
        }
        Ok(exit_code(status))
    }

    pub fn drain_signal(&self) -> bool {
        signal_group(self.pid, libc::SIGUSR1)
    }
}

impl Drop for SlotChild {
    fn drop(&mut self) {
        if !self.reaped {
            unsafe {
                libc::kill(self.pid as i32, libc::SIGKILL);
            }
            let job = self.job.clone();
            if let Some(task) = self.task.take() {
                tokio::spawn(async move {
                    job.stop().await;
                    let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
                });
            }
        } else if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Start one grouped slot. `parent_fd` is inherited after `FD_CLOEXEC` is cleared in
/// the child, immediately before exec. `slot_fd` is the busy-digit page.
pub async fn spawn_slot(
    spec: CommandSpec,
    parent_fd: Option<i32>,
    slot_fd: Option<i32>,
    slot: u32,
) -> io::Result<SlotChild> {
    let (job, task) = start_job(Arc::new(Command {
        program: Program::Exec { prog: PathBuf::from(spec.program), args: spec.args },
        options: SpawnOptions { grouped: true, reset_sigmask: true, ..SpawnOptions::default() },
    }));
    let env = spec.env;
    job.set_spawn_hook(move |wrap, _ctx| {
        let command = wrap.command_mut();
        command.stdin(std::process::Stdio::null());
        command.stdout(std::process::Stdio::null());
        command.stderr(std::process::Stdio::null());
        command.env(crate::busy::SLOT_INDEX_ENV, slot.to_string());
        for (key, value) in &env {
            command.env(key, value);
        }
        if let Some(fd) = parent_fd {
            command.env(PARENT_FD_ENV, fd.to_string());
        }
        if let Some(fd) = slot_fd {
            command.env(crate::busy::SLOT_FD_ENV, fd.to_string());
        }
        // SAFETY: this runs in the forked child, before exec, and only calls fcntl.
        // The parent descriptor stays cloexec for every other process.
        unsafe {
            command.pre_exec(move || {
                if let Some(fd) = parent_fd {
                    clear_cloexec(fd)?;
                }
                if let Some(fd) = slot_fd {
                    clear_cloexec(fd)?;
                }
                Ok(())
            });
        }
    })
    .await;
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let slot_sender = Arc::new(Mutex::new(Some(sender)));
    let spawn_sender = Arc::clone(&slot_sender);
    job.set_spawn_fn(move |command| {
        let child = command.spawn()?;
        if let Some(sender) = spawn_sender.lock().unwrap_or_else(|err| err.into_inner()).take() {
            let _ = sender.send(child.id());
        }
        Ok(child)
    })
    .await;
    let error_sender = Arc::clone(&slot_sender);
    job.set_error_handler(move |error| {
        let _ = error.get();
        if let Some(sender) = error_sender.lock().unwrap_or_else(|err| err.into_inner()).take() {
            let _ = sender.send(None);
        }
    })
    .await;
    job.start().await;
    let reported = match tokio::time::timeout(Duration::from_secs(3), receiver).await {
        Ok(Ok(pid)) => pid,
        Ok(Err(_)) | Err(_) => None,
    };
    let Some(pid) = reported else {
        job.stop().await;
        task.abort();
        return Err(io::Error::new(io::ErrorKind::Other, "slot did not report a pid"));
    };
    Ok(SlotChild { job, task: Some(task), pid, reaped: false })
}

pub(crate) fn exit_code(status: ProcessEnd) -> Option<i32> {
    match status {
        ProcessEnd::Success | ProcessEnd::Continued => Some(0),
        ProcessEnd::ExitError(code) => i32::try_from(code.get()).ok(),
        ProcessEnd::ExitSignal(_) | ProcessEnd::ExitStop(_) | ProcessEnd::Exception(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::{NonZeroI32, NonZeroI64};
    use std::os::fd::AsRawFd;
    use std::time::Duration;

    #[test]
    fn every_process_end_maps_to_a_slot_code() {
        assert_eq!(exit_code(ProcessEnd::Success), Some(0));
        assert_eq!(exit_code(ProcessEnd::Continued), Some(0));
        assert_eq!(exit_code(ProcessEnd::ExitError(NonZeroI64::new(4).unwrap())), Some(4));
        assert_eq!(exit_code(ProcessEnd::ExitSignal(watchexec_supervisor::Signal::ForceStop)), None);
        assert_eq!(exit_code(ProcessEnd::ExitStop(NonZeroI32::new(19).unwrap())), None);
        assert_eq!(exit_code(ProcessEnd::Exception(NonZeroI32::new(1).unwrap())), None);
    }

    #[tokio::test]
    async fn watchexec_runs_the_slot_and_returns_its_exit_code() {
        assert_eq!(SPAWN_CRATE, "watchexec-supervisor");
        let child = spawn_slot(CommandSpec::new("/bin/sh").args(["-c", "exit 4"]), None, None, 0).await.unwrap();
        assert!(child.id() > 1);
        assert_eq!(child.wait().await.unwrap(), Some(4));
    }

    #[tokio::test]
    async fn sigusr1_does_not_kill_the_slot_and_drop_does() {
        let ready = std::env::temp_dir().join(format!("arq-slot-usr1-{}", std::process::id()));
        let _ = std::fs::remove_file(&ready);
        let child = spawn_slot(
            CommandSpec::new("/bin/sh")
                .args(["-c", "trap '' USR1; echo ready > \"$ARQ_READY\"; while true; do sleep 30; done"])
                .env("ARQ_READY", ready.to_string_lossy()),
            None,
            None,
            1,
        )
        .await
        .unwrap();
        let pid = child.id();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !ready.exists() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(ready.exists(), "the shell did not install the SIGUSR1 ignore");
        assert!(child.drain_signal());
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(crate::osutil::process_alive(pid));
        let _ = std::fs::remove_file(&ready);
        drop(child);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while crate::osutil::process_alive(pid) && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(!crate::osutil::process_alive(pid));
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    #[tokio::test]
    async fn a_missing_program_is_a_spawn_error() {
        let error = spawn_slot(CommandSpec::new("/no/such/arq-slot"), None, None, 0).await;
        assert!(error.is_err());
    }

    #[tokio::test]
    async fn the_child_inherits_the_parent_pidfd_and_its_process_group() {
        let parent = crate::pidfd::open_supervisor_pidfd().unwrap();
        let digits = crate::busy::SlotDigits::create(1).unwrap();
        let dir = std::env::temp_dir();
        let nonce = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos();
        let status = dir.join(format!("arq-slot-status-{}-{nonce}", std::process::id()));
        let grand = dir.join(format!("arq-slot-grand-{}-{nonce}", std::process::id()));
        let _ = std::fs::remove_file(&status);
        let _ = std::fs::remove_file(&grand);
        let script = r#"
            if [ -e "/proc/self/fd/$ARQRS_PARENT_PIDFD" ]; then echo open; else echo closed; fi > "$ARQ_STATUS"
            echo "$ARQRS_SLOT" >> "$ARQ_STATUS"
            if [ -e "/proc/self/fd/$ARQRS_SLOT_FD" ]; then echo digit; else echo nodigit; fi >> "$ARQ_STATUS"
            sleep 30 &
            echo $! > "$ARQ_GRAND"
            wait
        "#;
        let child = spawn_slot(
            CommandSpec::new("/bin/sh")
                .args(["-c", script])
                .env("ARQ_STATUS", status.to_string_lossy())
                .env("ARQ_GRAND", grand.to_string_lossy()),
            Some(parent.as_raw_fd()),
            Some(digits.raw_fd()),
            3,
        )
        .await
        .unwrap();
        let pid = child.id();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let (text, grand_pid) = loop {
            let text = std::fs::read_to_string(&status).unwrap_or_default();
            let parsed = std::fs::read_to_string(&grand).ok().and_then(|raw| raw.trim().parse::<u32>().ok());
            if text.contains("open") && text.contains("digit") && text.lines().any(|line| line == "3") {
                if let Some(grand_pid) = parsed {
                    if stat_field(grand_pid, 1) == Some(u64::from(pid)) && crate::osutil::process_alive(grand_pid) {
                        break (text, grand_pid);
                    }
                }
            }
            if tokio::time::Instant::now() > deadline {
                panic!("slot did not publish its inherited descriptors and grandchild: {text:?}");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        assert!(text.contains("open"), "{text}");
        assert!(text.contains("digit"), "{text}");
        assert!(text.lines().any(|line| line == "3"), "{text}");
        let grand_born = stat_field(grand_pid, 19).expect("grandchild starttime");
        let shell_born = stat_field(pid, 19).expect("shell starttime");
        assert_eq!(stat_field(grand_pid, 2), Some(u64::from(pid)), "grandchild must be in the slot process group");
        assert!(crate::osutil::signal_group(pid, libc::SIGKILL));
        let _ = child.wait().await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while (same_process(pid, shell_born) || same_process(grand_pid, grand_born)) && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // `process_alive` is only a pid. After wait reaps the grandchild, Linux can
        // reuse that pid for a process another test thread just spawned.
        assert!(!same_process(pid, shell_born), "slot shell was not reaped");
        assert!(!same_process(grand_pid, grand_born), "grandchild was not reaped");
        let _ = std::fs::remove_file(&status);
        let _ = std::fs::remove_file(&grand);
    }

    /// Field `n` of `/proc/pid/stat` after the comm field. `1` is ppid, `2` is pgrp, `19` is starttime.
    fn stat_field(pid: u32, index: usize) -> Option<u64> {
        let text = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let (_, rest) = text.rsplit_once(')')?;
        rest.split_whitespace().nth(index)?.parse().ok()
    }

    fn same_process(pid: u32, starttime: u64) -> bool {
        crate::osutil::process_alive(pid) && stat_field(pid, 19) == Some(starttime)
    }
}
