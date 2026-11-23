//! Process-group signals. Soft memory uses `SIGUSR1`. Hard memory uses `SIGKILL`.

use std::io;

pub fn signal_pid(pid: u32, signal: i32) -> bool {
    if pid == 0 || pid > i32::MAX as u32 {
        return false;
    }
    unsafe { libc::kill(pid as i32, signal) == 0 }
}

/// Signal the process group when `pid` is a group leader, otherwise the pid.
pub fn signal_group(pid: u32, signal: i32) -> bool {
    if pid == 0 || pid > i32::MAX as u32 {
        return false;
    }
    let grouped = unsafe { libc::kill(-(pid as i32), signal) == 0 };
    grouped || signal_pid(pid, signal)
}

pub fn process_alive(pid: u32) -> bool {
    if pid == 0 || pid > i32::MAX as u32 {
        return false;
    }
    // A zombie still answers `kill(pid, 0)` until it is reaped. The supervisor
    // treats that as gone: the process cannot run another task.
    match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(text) => {
            let Some((_, rest)) = text.rsplit_once(')') else {
                return false;
            };
            let state = rest.split_whitespace().next().unwrap_or("");
            !matches!(state, "" | "Z" | "X")
        }
        Err(_) => false,
    }
}

pub fn last_os_error() -> io::Error {
    io::Error::last_os_error()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::process::CommandExt;
    use std::process::Command;
    use std::time::Duration;

    #[test]
    fn zero_and_overflow_pids_are_not_signaled() {
        assert!(!signal_pid(0, libc::SIGUSR1));
        assert!(!signal_group(0, libc::SIGUSR1));
        assert!(!signal_pid(u32::MAX, libc::SIGKILL));
        assert!(!signal_group(u32::MAX, libc::SIGKILL));
        assert!(!process_alive(0));
        assert!(!process_alive(u32::MAX));
        let _ = last_os_error();
    }

    #[test]
    fn soft_signal_leaves_a_child_alive_and_kill_reaps_it() {
        // Default SIGUSR1 terminates. A drain handler ignores it; this shell does too.
        let ready = std::env::temp_dir().join(format!("arq-usr1-ready-{}", std::process::id()));
        let _ = std::fs::remove_file(&ready);
        let mut child = Command::new("/bin/sh");
        child
            .args(["-c", "trap '' USR1; echo ready > \"$ARQ_READY\"; while true; do sleep 30; done"])
            .env("ARQ_READY", &ready)
            .process_group(0);
        let mut child = child.spawn().expect("shell");
        let pid = child.id();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !ready.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(ready.exists(), "the shell did not ignore SIGUSR1 before the signal");
        assert!(process_alive(pid));
        assert!(signal_group(pid, libc::SIGUSR1));
        std::thread::sleep(Duration::from_millis(40));
        assert!(process_alive(pid), "SIGUSR1 must not kill the child");
        assert!(signal_group(pid, libc::SIGKILL));
        let status = child.wait().expect("wait");
        assert!(!status.success());
        assert!(!process_alive(pid));
        let _ = std::fs::remove_file(&ready);
    }

    #[test]
    fn group_kill_stops_a_grandchild() {
        let mut child = Command::new("/bin/sh");
        child.arg("-c").arg("sleep 30 & echo $! > \"$ARQ_NOTE\"; wait").process_group(0);
        let note = std::env::temp_dir().join(format!("arq-grandchild-{}", std::process::id()));
        let _ = std::fs::remove_file(&note);
        child.env("ARQ_NOTE", &note);
        let mut child = child.spawn().expect("shell");
        let pid = child.id();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let grand = loop {
            if let Ok(text) = std::fs::read_to_string(&note) {
                if let Ok(value) = text.trim().parse::<u32>() {
                    break value;
                }
            }
            if std::time::Instant::now() > deadline {
                let _ = signal_group(pid, libc::SIGKILL);
                let _ = child.wait();
                panic!("grandchild pid was not written");
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        assert!(process_alive(grand));
        assert!(signal_group(pid, libc::SIGKILL));
        let _ = child.wait();
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while process_alive(grand) && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(!process_alive(grand));
        assert!(!process_alive(pid));
        let _ = std::fs::remove_file(note);
    }
}
