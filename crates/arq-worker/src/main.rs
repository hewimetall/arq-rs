//! Process entry. Configuration and the supervisor live in the library.

fn main() {
    let code = arq_worker::execute(std::env::args().skip(1));
    std::process::exit(code);
}
