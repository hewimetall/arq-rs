//! Metrics helper. This is a `tokio::process::Command`, not a watchexec slot.
//!
//! `watchexec_supervisor::job::Job` is not constructed here. The helper is reaped
//! with `Child::kill` so its pid does not stay a zombie after the supervisor exits.

use std::io;
use std::process::Stdio;

use tokio::process::Command;

pub struct MetricsProcess {
    child: tokio::process::Child,
    pid: u32,
}

impl MetricsProcess {
    pub fn id(&self) -> u32 {
        self.pid
    }

    pub async fn spawn(program: &str, args: &[String]) -> io::Result<Self> {
        let mut command = Command::new(program);
        command.args(args).kill_on_drop(true).stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());
        let mut child = command.spawn()?;
        let pid = child.id().ok_or_else(|| io::Error::new(io::ErrorKind::Other, "metrics helper has no pid"))?;
        // Touch the child once so a failed spawn is distinct from a live pid.
        let _ = child.try_wait()?;
        Ok(Self { child, pid })
    }

    pub async fn stop(mut self) -> io::Result<()> {
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn the_helper_is_a_process_and_stop_reaps_it() {
        let metrics = MetricsProcess::spawn("/bin/sleep", &["30".into()]).await.unwrap();
        let pid = metrics.id();
        assert!(crate::osutil::process_alive(pid));
        tokio::time::sleep(Duration::from_millis(30)).await;
        metrics.stop().await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while crate::osutil::process_alive(pid) && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(!crate::osutil::process_alive(pid));
    }

    #[tokio::test]
    async fn a_missing_helper_does_not_start() {
        let error = MetricsProcess::spawn("/no/such/arq-metrics", &[]).await;
        assert!(error.is_err());
    }
}
