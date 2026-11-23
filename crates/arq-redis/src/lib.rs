//! Concrete Redis adapter. The `WATCH / MULTI / EXEC` cycle is not implemented yet.
//!
//! `RedisBackend` is a struct, not a trait. There is no `QueuePort`.

/// Placeholder for the Redis execution adapter.
#[derive(Debug, Default, Clone, Copy)]
pub struct RedisBackend;

impl RedisBackend {
    pub const fn new() -> Self {
        Self
    }

    /// Names the driver types this crate is built against. The protocol is not called.
    pub fn driver() -> (&'static str, &'static str) {
        (
            std::any::type_name::<redis::Client>(),
            std::any::type_name::<tokio::runtime::Runtime>(),
        )
    }
}
