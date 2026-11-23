//! One terminal task per transaction. Indexes are not watched. Uniq is not deleted.

use super::{inconsistency, RedisBackend};
use crate::error::BackendError;
use crate::observe::load_for_task;
use crate::session::{Fail, Session};
use crate::types::MaintenanceReport;
use arq_core::{ExecClass, PlannedCommand, QueueKeys, TaskId, TaskState, WatchBudget};

#[derive(Clone, Copy)]
enum Index {
    Done,
    Dead,
}

impl RedisBackend {
    pub async fn maintain(&self, queue: &str) -> Result<MaintenanceReport, BackendError> {
        let this = self.clone();
        let queue = queue.to_string();
        super::block_on(move || this.maintain_blocking(&queue)).await
    }

    fn maintain_blocking(&self, queue: &str) -> Result<MaintenanceReport, BackendError> {
        self.keys(queue)?;
        let config = self.config();
        let batch = config.maintenance_batch as usize;
        let candidates = self.with_session(queue, |session| {
            let keys = QueueKeys::new(queue).map_err(|error| Fail::Backend(BackendError::Input(error)))?;
            let now = session.time_ms()?;
            let mut found = Vec::new();
            let done_cut = now.saturating_sub(config.done_ttl_secs.saturating_mul(1000));
            let dead_cut = now.saturating_sub(config.dead_ttl_secs.saturating_mul(1000));
            for (index, key, cutoff) in [
                (Index::Done, keys.done(), done_cut),
                (Index::Dead, keys.dead(), dead_cut),
            ] {
                if found.len() >= batch {
                    break;
                }
                let left = batch - found.len();
                let members = session.zrange_by_score(&key, "-inf", &format!("({cutoff}"), left)?;
                for member in members {
                    found.push((index, member));
                }
            }
            Ok(found)
        })?;
        let mut report = MaintenanceReport { examined: 0, removed: 0 };
        for (index, member) in candidates {
            report.examined += 1;
            let Ok(task_id) = TaskId::parse(&member) else {
                continue;
            };
            if self.delete_terminal(queue, index, task_id)? {
                report.removed += 1;
            }
        }
        Ok(report)
    }

    fn delete_terminal(&self, queue: &str, index: Index, task_id: TaskId) -> Result<bool, BackendError> {
        let mut budget = WatchBudget::new(self.config().watch_retries);
        let mut aborts = 0u64;
        let limit = self.config().watch_retries;
        loop {
            if self.is_halted(queue) {
                return Err(BackendError::Halted { queue: queue.to_string() });
            }
            let outcome = self.with_session(queue, |session| self.delete_once(session, queue, &index, task_id))?;
            match outcome {
                DeleteOnce::Removed => return Ok(true),
                DeleteOnce::Skip => return Ok(false),
                DeleteOnce::Conflict => {
                    budget.on_conflict().map_err(BackendError::Transaction)?;
                }
                DeleteOnce::Abort | DeleteOnce::Recheck => {
                    if matches!(outcome, DeleteOnce::Recheck) {
                        let gone = self.with_session(queue, |session| {
                            let keys = QueueKeys::new(queue).map_err(|error| Fail::Backend(BackendError::Input(error)))?;
                            Ok(session.key_type(&keys.job(&task_id))? == "none")
                        })?;
                        if gone {
                            return Ok(true);
                        }
                    }
                    aborts += 1;
                    if aborts > limit {
                        return Err(crate::error::transport("EXECABORT persisted"));
                    }
                }
            }
        }
    }

    fn delete_once(&self, session: &mut Session, queue: &str, index: &Index, task_id: TaskId) -> Result<DeleteOnce, Fail> {
        let keys = QueueKeys::new(queue).map_err(|error| Fail::Backend(BackendError::Input(error)))?;
        let job = keys.job(&task_id);
        session.watch(&[job.clone()])?;
        let loaded = load_for_task(session, &keys, task_id, None, None)?;
        let mut watched = vec![job.clone()];
        if let Some(unique) = loaded.observed.unique_key.as_deref() {
            let uniq = keys.uniq(unique).map_err(|error| Fail::Backend(BackendError::Input(error)))?;
            session.watch(&[uniq.clone()])?;
            watched.push(uniq);
            let owner = session.get(watched.last().expect("uniq key"))?;
            if owner.as_deref() == Some(task_id.to_string().as_str()) {
                return Err(Fail::Backend(inconsistency(
                    queue,
                    None,
                    "terminal uniq still names this task",
                )));
            }
        }
        let terminal = match index {
            Index::Done => loaded.observed.state == Some(TaskState::Done) && loaded.observed.in_done,
            Index::Dead => loaded.observed.state == Some(TaskState::Dead) && loaded.observed.in_dead,
        };
        if !loaded.observed.present || !terminal {
            let _ = session.unwatch();
            return Ok(DeleteOnce::Skip);
        }
        let member = task_id.to_string();
        let mut commands = vec![PlannedCommand::Zrem {
            key: match index {
                Index::Done => keys.done(),
                Index::Dead => keys.dead(),
            },
            member: member.clone(),
        }];
        if matches!(index, Index::Dead) {
            if let Some(tag) = loaded.observed.tag.as_deref() {
                commands.push(PlannedCommand::Zrem {
                    key: keys.dead_tag(tag).map_err(|error| Fail::Backend(BackendError::Input(error)))?,
                    member: member.clone(),
                });
            }
        }
        commands.push(PlannedCommand::Del { key: job });
        for command in &commands {
            session.expect_type(command.key())?;
        }
        let executed = session.exec_plan(&commands, &watched, self.take_exec_fault(), self.before_hook().as_deref())?;
        match executed.class {
            ExecClass::RetryCycle { charge_watch: true } => Ok(DeleteOnce::Conflict),
            ExecClass::RetryCycle { charge_watch: false } => Ok(DeleteOnce::Abort),
            ExecClass::CheckPostconditions => {
                let after = session.key_type(&keys.job(&task_id))?;
                if after == "none" {
                    Ok(DeleteOnce::Removed)
                } else {
                    Err(Fail::Backend(inconsistency(queue, None, "maintenance delete left the job hash")))
                }
            }
            // The EXEC reply was not read. Drop this connection and recheck on a new one.
            ExecClass::UnknownOutcome => Ok(DeleteOnce::Recheck),
            ExecClass::StorageInconsistency => Err(Fail::Backend(inconsistency(
                queue,
                None,
                "maintenance EXEC array was the wrong length or contained a command error",
            ))),
        }
    }
}

enum DeleteOnce {
    Removed,
    Skip,
    Conflict,
    Abort,
    Recheck,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arq_core::{CanonicalUuid, Config, StorageInconsistency, TaskId};

    #[test]
    fn halted_queue_stops_before_another_delete() {
        let backend = RedisBackend::connect("redis://127.0.0.1:1", Config::parse(std::iter::empty::<(&str, &str)>()).unwrap()).unwrap();
        let _ = backend.note_inconsistent(StorageInconsistency {
            queue: "default".into(),
            operation_id: None,
            detail: "coverage".into(),
        });
        let task = TaskId::from_uuid(CanonicalUuid::from_random_v4([9; 16]));
        let error = backend.delete_terminal("default", Index::Dead, task).unwrap_err();
        assert!(matches!(error, BackendError::Halted { .. }));
    }
}
