//! Worker binary. This crate is the only one that depends on all four libraries.
//! It does not enable `arq-pybridge`'s `extension-module` feature.

fn main() {
    if std::env::args().any(|arg| arg == "--version" || arg == "-V") {
        println!("arq-worker {}", env!("CARGO_PKG_VERSION"));
        return;
    }
    let _linked = (
        std::any::type_name::<arq_core::Config>(),
        arq_redis::RedisBackend::driver(),
        arq_pybridge::extension_module_enabled(),
        arq_pybridge::pyo3_linked(),
        arq_supervisor::supervisor_job(),
    );
    println!("arq-worker {}", env!("CARGO_PKG_VERSION"));
}
