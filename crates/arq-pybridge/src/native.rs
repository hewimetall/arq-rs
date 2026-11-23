//! The `arq_rs._native` module. Maturin builds it; the worker does not call it.

use pyo3::prelude::*;
use pyo3::types::PyModule;

use crate::decide::DispatchRaw;
use crate::encode::error_response;
use crate::load::BridgeError;
use crate::service::{self, DeliveryRef};

#[pyclass]
struct Backend {
    inner: arq_redis::RedisBackend,
}

#[pymethods]
impl Backend {
    #[new]
    fn new(url: &str, config_json: &str) -> PyResult<Self> {
        let inner = service::open(url, config_json).map_err(raise)?;
        Ok(Self { inner })
    }

    fn submit(&self, spec: &str) -> String {
        service::submit(&self.inner, spec)
    }

    #[pyo3(signature = (queue, host=""))]
    fn snapshot(&self, queue: &str, host: &str) -> String {
        service::snapshot(&self.inner, queue, blank(host))
    }

    fn get(&self, queue: &str, task_id: &str) -> String {
        service::get(&self.inner, queue, task_id)
    }

    #[pyo3(signature = (queue, state, limit, cursor=""))]
    fn peek(&self, queue: &str, state: &str, limit: u64, cursor: &str) -> String {
        service::peek(&self.inner, queue, state, limit, blank(cursor))
    }

    #[pyo3(signature = (queue, limit, cursor=""))]
    fn dead(&self, queue: &str, limit: u64, cursor: &str) -> String {
        service::dead(&self.inner, queue, limit, blank(cursor))
    }

    #[pyo3(signature = (queue, tag, limit, cursor=""))]
    fn dead_for_tag(&self, queue: &str, tag: &str, limit: u64, cursor: &str) -> String {
        service::dead_for_tag(&self.inner, queue, tag, limit, blank(cursor))
    }

    fn command(&self, queue: &str, operation_id: &str) -> String {
        service::command(&self.inner, queue, operation_id)
    }

    fn control(&self, kind: &str, queue: &str, task_id: &str, operation_id: &str) -> String {
        service::control(&self.inner, kind, queue, task_id, operation_id)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (queue, task_id, message_id, cycle_id, delivery_token, operation_id, kind, body=""))]
    fn commit(
        &self,
        queue: &str,
        task_id: &str,
        message_id: &str,
        cycle_id: &str,
        delivery_token: &str,
        operation_id: &str,
        kind: &str,
        body: &str,
    ) -> String {
        let raw = match kind {
            "raised" => DispatchRaw::Raised,
            "grace" => DispatchRaw::GraceExceeded,
            _ => DispatchRaw::Returned(body.to_string()),
        };
        service::commit(
            &self.inner,
            &DeliveryRef {
                queue: queue.to_string(),
                task_id: task_id.to_string(),
                message_id: message_id.to_string(),
                cycle_id: cycle_id.to_string(),
                delivery_token: delivery_token.to_string(),
                operation_id: operation_id.to_string(),
            },
            &raw,
        )
    }
}

#[pyfunction]
fn parse_config(text: &str) -> String {
    service::describe_config(text)
}

#[pyfunction]
fn redis_connects() -> u64 {
    service::connects()
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(parse_config, module)?)?;
    module.add_function(wrap_pyfunction!(redis_connects, module)?)?;
    module.add_class::<Backend>()?;
    Ok(())
}

fn blank(value: &str) -> Option<&str> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

fn raise(error: BridgeError) -> PyErr {
    let message = error_response(&error);
    match error.kind() {
        "config" | "input" | "page" => pyo3::exceptions::PyValueError::new_err(message),
        _ => pyo3::exceptions::PyRuntimeError::new_err(message),
    }
}
