//! Concrete `RedisBackend`. One operation holds one connection.

use crate::error::{transport, BackendError};
use crate::session::{self, Fail, Session};
use crate::types::{ExecFault, Fault};
use arq_core::{Config, OperationId, QueueKeys, StorageInconsistency, WatchBudget};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

mod admin;
mod maintain;
mod ops;
mod registry;

pub use admin::QueueAdmin;

#[derive(Debug)]
pub(crate) enum Step<T> {
    Done(T),
    Watch,
    Abort,
    Lost,
}

#[derive(Debug)]
pub(crate) enum Drive<T> {
    Done(T),
    Lost,
}

struct Route {
    url: String,
    /// `ASK` redirects stay on this node until a later redirect replaces the route.
    /// `ASKING` is harmless on a stable primary and required on an importing replica of the slot.
    ask: bool,
}

struct Inner {
    url: String,
    resp3: bool,
    config: Config,
    halted: Mutex<std::collections::BTreeSet<String>>,
    /// `(queue, reason)` notes from quarantine moves. The child drains them into metrics.
    quarantine: Mutex<Vec<(String, String)>>,
    inconsistent: AtomicU64,
    fault: Mutex<Fault>,
    routes: Mutex<HashMap<String, Route>>,
}

/// Executes arq queue operations against Redis. This is a struct, not a trait.
#[derive(Clone)]
pub struct RedisBackend {
    inner: Arc<Inner>,
}

impl RedisBackend {
    pub fn connect(url: &str, config: Config) -> Result<Self, BackendError> {
        let _ = redis::Client::open(url).map_err(|error| transport(error.to_string()))?;
        Ok(Self {
            inner: Arc::new(Inner {
                url: url.to_string(),
                resp3: url.contains("protocol=resp3") || url.contains("protocol=3"),
                config,
                halted: Mutex::new(std::collections::BTreeSet::new()),
                quarantine: Mutex::new(Vec::new()),
                inconsistent: AtomicU64::new(0),
                fault: Mutex::new(Fault::default()),
                routes: Mutex::new(HashMap::new()),
            }),
        })
    }

    pub fn config(&self) -> Config {
        self.inner.config
    }

    pub fn set_fault(&self, fault: Fault) {
        *self
            .inner
            .fault
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = fault;
    }

    pub fn is_halted(&self, queue: &str) -> bool {
        if self.halt_cached(queue) {
            return true;
        }
        self.load_persisted_halt(queue)
    }

    /// Quarantine moves since the previous drain. Each item is `(queue, reason)`.
    pub fn drain_quarantine(&self) -> Vec<(String, String)> {
        std::mem::take(
            &mut *self
                .inner
                .quarantine
                .lock()
                .unwrap_or_else(|error| error.into_inner()),
        )
    }

    fn halt_cached(&self, queue: &str) -> bool {
        self.inner
            .halted
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .contains(queue)
    }

    fn cache_halt(&self, queue: &str) {
        self.inner
            .halted
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(queue.to_string());
    }

    /// `q:{queue}:halted` survives a child restart. A missing key or a connection error is not a halt.
    fn load_persisted_halt(&self, queue: &str) -> bool {
        let Ok(keys) = QueueKeys::new(queue) else {
            return false;
        };
        let Ok(mut session) = self.open_session(queue) else {
            return false;
        };
        match session.get(&keys.halted()) {
            Ok(Some(_)) => {
                self.cache_halt(queue);
                true
            }
            Ok(None) => false,
            Err(Fail::Io(detail)) if detail.contains("WRONGTYPE") => {
                self.cache_halt(queue);
                true
            }
            Err(_) => false,
        }
    }

    fn persist_halt(&self, queue: &str, detail: &str) {
        let Ok(keys) = QueueKeys::new(queue) else {
            return;
        };
        let Ok(mut session) = self.open_session(queue) else {
            return;
        };
        let _ = session.set(&keys.halted(), detail);
    }

    pub(crate) fn note_quarantine(&self, queue: &str, reason: &str) {
        eprintln!("arq_quarantine_total queue={queue} reason={reason}");
        self.inner
            .quarantine
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push((queue.to_string(), reason.to_string()));
    }

    /// Process-local count of `arq_storage_inconsistent` observations.
    pub fn storage_inconsistent_count(&self) -> u64 {
        self.inner.inconsistent.load(Ordering::Relaxed)
    }

    /// Names the driver types this crate is built against.
    pub fn driver() -> (&'static str, &'static str) {
        (
            std::any::type_name::<redis::Client>(),
            std::any::type_name::<tokio::runtime::Runtime>(),
        )
    }

    pub(crate) fn keys(&self, queue: &str) -> Result<QueueKeys, BackendError> {
        if self.is_halted(queue) {
            return Err(BackendError::Halted {
                queue: queue.to_string(),
            });
        }
        QueueKeys::new(queue).map_err(BackendError::Input)
    }

    pub(crate) fn note_inconsistent(&self, error: StorageInconsistency) -> BackendError {
        self.inner.inconsistent.fetch_add(1, Ordering::Relaxed);
        eprintln!(
            "arq_storage_inconsistent queue={} detail={}",
            error.queue, error.detail
        );
        if halts_the_queue(&error.detail) && !self.halt_cached(&error.queue) {
            self.persist_halt(&error.queue, &error.detail);
            self.cache_halt(&error.queue);
        }
        BackendError::Inconsistent(error)
    }

    fn note_cross(&self, queue: &str) -> BackendError {
        self.note_inconsistent(StorageInconsistency {
            queue: queue.to_string(),
            operation_id: None,
            detail: "CROSSSLOT on queue keys".to_string(),
        })
    }

    pub(crate) fn absorb(&self, error: BackendError) -> BackendError {
        match error {
            BackendError::Inconsistent(error) => self.note_inconsistent(error),
            other => other,
        }
    }

    pub(crate) fn fail_to_error(&self, queue: &str, fail: Fail) -> BackendError {
        match fail {
            Fail::Backend(error) => self.absorb(error),
            Fail::CrossSlot => self.note_cross(queue),
            Fail::Io(detail) => transport(detail),
            Fail::Redirect { addr, ask } => transport(format!("redirect {addr} ask={ask}")),
        }
    }

    fn cross_slot(&self) -> Option<String> {
        self.inner
            .fault
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .cross_slot_watch
            .clone()
    }

    fn before_hook(&self) -> Option<Arc<dyn Fn(&[String]) + Send + Sync>> {
        self.inner
            .fault
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .before_exec
            .clone()
    }

    fn after_zcard(&self) {
        let hook = self
            .inner
            .fault
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .after_ready_zcard
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    fn take_exec_fault(&self) -> ExecFault {
        let mut fault = self
            .inner
            .fault
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let exec = fault.exec;
        if exec != ExecFault::None && exec != ExecFault::AbortAlways {
            fault.exec = ExecFault::None;
        }
        exec
    }

    fn remember(&self, queue: &str, addr: &str, ask: bool) {
        let url = if self.inner.resp3 {
            format!("redis://{addr}?protocol=resp3")
        } else {
            format!("redis://{addr}")
        };
        self.inner
            .routes
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(queue.to_string(), Route { url, ask });
    }

    fn open_session(&self, queue: &str) -> Result<Session, Fail> {
        let (url, ask) = {
            let routes = self
                .inner
                .routes
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            match routes.get(queue) {
                Some(route) => (route.url.clone(), route.ask),
                None => (self.inner.url.clone(), false),
            }
        };
        Session::open(&url, ask)
    }

    pub(crate) fn with_session<T>(
        &self,
        queue: &str,
        mut body: impl FnMut(&mut Session) -> Result<T, Fail>,
    ) -> Result<T, BackendError> {
        let mut redirects = 0u64;
        loop {
            let mut session = match self.open_session(queue) {
                Ok(session) => session,
                Err(Fail::Redirect { addr, ask }) => {
                    self.remember(queue, &addr, ask);
                    redirects += 1;
                    if redirects > 16 {
                        return Err(transport("too many cluster redirects"));
                    }
                    continue;
                }
                Err(Fail::CrossSlot) => return Err(self.note_cross(queue)),
                Err(Fail::Backend(error)) => return Err(self.absorb(error)),
                Err(Fail::Io(detail)) => return Err(transport(detail)),
            };
            match body(&mut session) {
                Ok(value) => return Ok(value),
                Err(Fail::Redirect { addr, ask }) => {
                    self.remember(queue, &addr, ask);
                    redirects += 1;
                    if redirects > 16 {
                        return Err(transport("too many cluster redirects"));
                    }
                }
                Err(Fail::CrossSlot) => return Err(self.note_cross(queue)),
                Err(Fail::Backend(error)) => return Err(self.absorb(error)),
                Err(Fail::Io(detail)) => return Err(transport(detail)),
            }
        }
    }

    pub(crate) fn drive<T>(
        &self,
        queue: &str,
        once: impl FnMut(&mut Session) -> Result<Step<T>, Fail>,
    ) -> Result<Drive<T>, BackendError> {
        self.drive_inner(queue, false, once)
    }

    /// Renew and finish of a delivery the child already holds. A queue halt does not refuse them.
    pub(crate) fn drive_held<T>(
        &self,
        queue: &str,
        once: impl FnMut(&mut Session) -> Result<Step<T>, Fail>,
    ) -> Result<Drive<T>, BackendError> {
        self.drive_inner(queue, true, once)
    }

    fn drive_inner<T>(
        &self,
        queue: &str,
        allow_halted: bool,
        mut once: impl FnMut(&mut Session) -> Result<Step<T>, Fail>,
    ) -> Result<Drive<T>, BackendError> {
        let mut budget = WatchBudget::new(self.inner.config.watch_retries);
        let mut aborts = 0u64;
        let mut transport_errors = 0u64;
        let mut redirects = 0u64;
        let limit = self.inner.config.watch_retries;
        let mut session = None;
        loop {
            if !allow_halted && self.is_halted(queue) {
                return Err(BackendError::Halted {
                    queue: queue.to_string(),
                });
            }
            if session.is_none() {
                match self.open_session(queue) {
                    Ok(opened) => session = Some(opened),
                    Err(Fail::Redirect { addr, ask }) => {
                        self.remember(queue, &addr, ask);
                        redirects += 1;
                        if redirects > 16 {
                            return Err(transport("too many cluster redirects"));
                        }
                        continue;
                    }
                    Err(Fail::CrossSlot) => return Err(self.note_cross(queue)),
                    Err(Fail::Backend(error)) => return Err(self.absorb(error)),
                    Err(Fail::Io(detail)) => {
                        transport_errors += 1;
                        if transport_errors > limit {
                            return Err(transport(detail));
                        }
                        continue;
                    }
                }
            }
            let current = session.as_mut().expect("session is opened above");
            current.discard_and_unwatch();
            match once(current) {
                Ok(Step::Done(value)) => return Ok(Drive::Done(value)),
                Ok(Step::Lost) => return Ok(Drive::Lost),
                Ok(Step::Watch) => {
                    budget.on_conflict().map_err(BackendError::Transaction)?;
                }
                Ok(Step::Abort) => {
                    aborts += 1;
                    if aborts > limit {
                        return Err(transport("EXECABORT persisted"));
                    }
                }
                Err(Fail::Redirect { addr, ask }) => {
                    session = None;
                    self.remember(queue, &addr, ask);
                    redirects += 1;
                    if redirects > 16 {
                        return Err(transport("too many cluster redirects"));
                    }
                }
                Err(Fail::Io(detail)) => {
                    session = None;
                    transport_errors += 1;
                    if transport_errors > limit {
                        return Err(transport(detail));
                    }
                }
                Err(Fail::CrossSlot) => return Err(self.note_cross(queue)),
                Err(Fail::Backend(error)) => return Err(self.absorb(error)),
            }
        }
    }
}

pub(crate) async fn block_on<T>(
    work: impl FnOnce() -> Result<T, BackendError> + Send + 'static,
) -> Result<T, BackendError>
where
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(work).await {
        Ok(result) => result,
        Err(error) => Err(transport(error.to_string())),
    }
}

/// Whole-queue halt is only for a broken shared index, a bad `EXEC`, or `CROSSSLOT`.
fn halts_the_queue(detail: &str) -> bool {
    if detail == "CROSSSLOT on queue keys"
        || detail.contains("EXEC array was the wrong length or contained a command error")
    {
        return true;
    }
    let Some(rest) = detail.strip_prefix("key ") else {
        return false;
    };
    let Some((key, after)) = rest.split_once(" has type ") else {
        return false;
    };
    after.contains(", expected ") && arq_core::is_shared_index(key)
}

pub(crate) fn inconsistency(
    queue: &str,
    operation_id: Option<OperationId>,
    detail: impl Into<String>,
) -> BackendError {
    session::inconsistency(queue, operation_id, detail)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::Fail;

    fn config(pairs: &[(&str, &str)]) -> Config {
        Config::parse(pairs.iter().copied()).unwrap()
    }

    fn open(pairs: &[(&str, &str)]) -> RedisBackend {
        RedisBackend::connect("redis://127.0.0.1:6379/1", config(pairs)).unwrap()
    }

    #[test]
    fn failures_redirects_and_halt_map_without_repair() {
        clear_halt("default");
        clear_halt("haltmap");
        clear_halt("sloto");
        clear_halt("slotd");
        let backend = open(&[]);
        let inconsistent = StorageInconsistency {
            queue: "default".into(),
            operation_id: None,
            detail: "from-fail".into(),
        };
        assert!(matches!(
            backend.fail_to_error(
                "default",
                Fail::Backend(BackendError::Inconsistent(inconsistent))
            ),
            BackendError::Inconsistent(_)
        ));
        assert!(matches!(
            backend.fail_to_error("sloto", Fail::CrossSlot),
            BackendError::Inconsistent(_)
        ));
        assert!(matches!(
            backend.fail_to_error("default", Fail::Io("down".into())),
            BackendError::Transport { .. }
        ));
        assert!(matches!(
            backend.fail_to_error(
                "default",
                Fail::Redirect {
                    addr: "127.0.0.1:9".into(),
                    ask: true
                }
            ),
            BackendError::Transport { .. }
        ));
        assert!(matches!(
            backend.absorb(BackendError::Page {
                detail: "page".into()
            }),
            BackendError::Page { .. }
        ));

        let resp3 =
            RedisBackend::connect("redis://127.0.0.1:6379?protocol=resp3", config(&[])).unwrap();
        resp3.remember("default", "127.0.0.1:6379", true);
        let routes = resp3.inner.routes.lock().unwrap();
        let route = routes.get("default").unwrap();
        assert!(route.url.contains("protocol=resp3"));
        assert!(route.ask);
        drop(routes);

        let halted = open(&[]);
        let _ = halted.note_inconsistent(StorageInconsistency {
            queue: "haltmap".into(),
            operation_id: None,
            detail: "EXEC array was the wrong length or contained a command error".into(),
        });
        let error = halted.drive("haltmap", |_| Ok(Step::Done(()))).unwrap_err();
        assert!(matches!(error, BackendError::Halted { .. }));
        let restarted = open(&[]);
        assert!(restarted.is_halted("haltmap"));
        clear_halt("haltmap");
        let cleared = open(&[]);
        assert!(!cleared.is_halted("haltmap"));

        let limited = open(&[("ARQ_WATCH_RETRIES", "1")]);
        let error = limited
            .drive("default", |_| Err::<Step<()>, _>(Fail::Io("again".into())))
            .unwrap_err();
        assert!(matches!(error, BackendError::Transport { .. }));
        let error = limited
            .drive("default", |_| Ok(Step::<()>::Abort))
            .unwrap_err();
        assert!(error.to_string().contains("EXECABORT"));
        let error = limited
            .drive("default", |_| Ok(Step::<()>::Watch))
            .unwrap_err();
        assert!(matches!(error, BackendError::Transaction(_)));
        assert!(matches!(
            limited.drive::<()>("default", |_| Ok(Step::Lost)),
            Ok(Drive::Lost)
        ));
        assert!(matches!(
            limited.drive("default", |_| Err::<Step<()>, _>(Fail::Backend(
                BackendError::Page { detail: "p".into() }
            ))),
            Err(BackendError::Page { .. })
        ));
        let error = limited
            .drive("default", |_| {
                Err::<Step<()>, _>(Fail::Redirect {
                    addr: "127.0.0.1:6379".into(),
                    ask: false,
                })
            })
            .unwrap_err();
        assert!(error.to_string().contains("too many cluster redirects"));
        let crossing = open(&[]);
        assert!(matches!(
            crossing.drive("slotd", |_| Err::<Step<()>, _>(Fail::CrossSlot)),
            Err(BackendError::Inconsistent(_))
        ));

        let reading = open(&[]);
        let error = reading
            .with_session("default", |_| {
                Err::<(), _>(Fail::Redirect {
                    addr: "127.0.0.1:1".into(),
                    ask: true,
                })
            })
            .unwrap_err();
        assert!(matches!(error, BackendError::Transport { .. }));
        reading.remember("default", "127.0.0.1:6379", false);
        let error = reading
            .with_session("slotd", |_| Err::<(), _>(Fail::CrossSlot))
            .unwrap_err();
        assert!(matches!(error, BackendError::Inconsistent(_)));
        let error = reading
            .with_session("default", |_| {
                Err::<(), _>(Fail::Backend(BackendError::Page { detail: "p".into() }))
            })
            .unwrap_err();
        assert!(matches!(error, BackendError::Page { .. }));
        reading.remember("cover", "127.0.0.1:1", false);
        let error = reading.with_session("cover", |_| Ok(())).unwrap_err();
        assert!(matches!(error, BackendError::Transport { .. }));

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let error = runtime
            .block_on(block_on(|| -> Result<(), BackendError> {
                panic!("blocked")
            }))
            .unwrap_err();
        assert!(matches!(error, BackendError::Transport { .. }));
        clear_halt("sloto");
        clear_halt("slotd");
        clear_halt("haltmap");
    }

    fn clear_halt(queue: &str) {
        let Ok(client) = redis::Client::open("redis://127.0.0.1:6379/1") else {
            return;
        };
        let Ok(mut con) = client.get_connection() else {
            return;
        };
        let _: redis::RedisResult<()> = redis::cmd("DEL")
            .arg(format!("q:{{{queue}}}:halted"))
            .query(&mut con);
    }
}
