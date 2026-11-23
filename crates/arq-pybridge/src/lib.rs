//! PyO3 bridge placeholder.
//!
//! Maturin enables the `extension-module` feature. The worker links this crate as an
//! rlib and does not enable that feature. `Client`, `dispatch`, and the child
//! interpreter are not implemented yet.

/// `true` only when maturin (or another caller) builds with `extension-module`.
pub const fn extension_module_enabled() -> bool {
    cfg!(feature = "extension-module")
}

/// Links the PyO3 crate without initializing an interpreter.
pub fn pyo3_linked() -> &'static str {
    std::any::type_name::<pyo3::PyErr>()
}
