//! Start one child interpreter and run a Python script. Used by the bridge tests.

fn main() {
    let mut args = std::env::args().skip(1);
    let Some(path) = args.next() else {
        eprintln!("usage: child_probe SCRIPT");
        std::process::exit(2);
    };
    let code = arq_pybridge::run_child_script(std::path::Path::new(&path));
    std::process::exit(code);
}
