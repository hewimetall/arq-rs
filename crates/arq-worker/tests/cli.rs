//! The binary prints its version, rejects bad configuration with exit 3, and
//! runs `--factory` in the child process rather than the parent.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use arq_worker::package_version;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_arq-worker")
}

fn command(args: &[&str]) -> Command {
    let mut cmd = Command::new(bin());
    cmd.args(args);
    for (key, _) in std::env::vars() {
        if key.starts_with("ARQ_")
            || key.starts_with("SIMPLE_Q_")
            || key == "REDIS_URL"
            || key == "ARQRS_PARENT_PIDFD"
        {
            cmd.env_remove(key);
        }
    }
    cmd
}

fn output(args: &[&str], env: &[(&str, &str)]) -> std::process::Output {
    let mut cmd = command(args);
    for (key, value) in env {
        cmd.env(key, value);
    }
    cmd.output().expect("spawn arq-worker")
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn version_prints_the_workspace_package_version() {
    let printed = output(
        &["--version"],
        &[
            ("SIMPLE_Q_APP", "1"),
            ("REDIS_URL", "nope"),
            ("ARQ_NOT_A_SETTING", "1"),
        ],
    );
    assert_eq!(
        printed.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&printed.stderr)
    );
    assert_eq!(
        String::from_utf8(printed.stdout).unwrap(),
        format!("arq-worker {}\n", package_version())
    );
    let short = output(&["-V"], &[]);
    assert_eq!(short.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&short.stdout).contains(package_version()));
}

#[test]
fn bad_config_exits_3_without_starting_python() {
    let url = [("REDIS_URL", "redis://redis:6379/0")];
    let cases: &[&[&str]] = &[
        &[],
        &["--app"],
        &["--app", "module"],
        &["--app", ":app"],
        &["--app", "module:"],
        &["--factory"],
        &["--app", "--factory"],
        &["--bogus"],
        &["--app", "module:app", "extra"],
        &["--app", "module:app", "--processes", "0"],
        &["--app", "module:app", "--processes", "no"],
        &["--app", "module:app", "--concurrency", "0"],
        &["--app", "module:app", "--queues", "Bad"],
        &["--app", "module:app", "--queues", "default,Bad"],
    ];
    for args in cases {
        let outcome = output(args, &url);
        assert_eq!(
            outcome.status.code(),
            Some(3),
            "{args:?}\n{}",
            String::from_utf8_lossy(&outcome.stderr)
        );
    }
    let simple = output(
        &["--app", "module:app"],
        &[
            ("SIMPLE_Q_APP", "module:app"),
            ("REDIS_URL", "redis://redis:6379/0"),
        ],
    );
    assert_eq!(simple.status.code(), Some(3));
    assert!(String::from_utf8_lossy(&simple.stderr).contains("SIMPLE_Q_APP"));
    let unknown = output(
        &["--app", "module:app"],
        &[
            ("ARQ_NOT_A_SETTING", "1"),
            ("REDIS_URL", "redis://redis:6379/0"),
        ],
    );
    assert_eq!(unknown.status.code(), Some(3));
    assert!(String::from_utf8_lossy(&unknown.stderr).contains("ARQ_NOT_A_SETTING"));
    let missing = output(&["--app", "module:app"], &[]);
    assert_eq!(missing.status.code(), Some(3));
    assert!(String::from_utf8_lossy(&missing.stderr).contains("REDIS_URL"));
    let invalid = output(
        &["--app", "module:app"],
        &[("REDIS_URL", "http://redis:6379/0")],
    );
    assert_eq!(invalid.status.code(), Some(3));
    assert!(String::from_utf8_lossy(&invalid.stderr).contains("invalid"));
}

#[test]
fn factory_runs_in_the_child_and_not_in_the_parent() {
    let dir = std::env::temp_dir().join(format!("arq-worker-factory-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let marker = dir.join("marker");
    fs::write(
        dir.join("appmod.py"),
        "\
import os
from pathlib import Path

path = Path(os.environ['WORKER_MARKER'])

def note(label):
    with path.open('a') as handle:
        handle.write(f'{label} {os.getpid()}\\n')

class App:
    def __init__(self, label):
        note(label)
        self.client = object()

    def dispatch(self, message):
        return {'outcome': 'ok'}

def create_app():
    return App('factory')

app = App('attribute')
",
    )
    .unwrap();

    let mut child = spawn_worker(
        &["--child", "--app", "appmod:create_app", "--factory"],
        &dir,
        &marker,
    );
    let text = wait_for_marker(&mut child, &marker, "factory");
    assert!(
        text.lines()
            .any(|line| line == format!("factory {}", child.id())),
        "{text}"
    );
    signal(child.id(), "-TERM");
    let status = wait_exit(child, Duration::from_secs(10));
    assert_eq!(
        status.code(),
        Some(0),
        "child stderr:\n{}",
        read_stderr(&dir.join("child.err"))
    );

    let _ = fs::remove_file(&marker);
    let mut plain = spawn_worker(&["--child", "--app", "appmod:app"], &dir, &marker);
    let text = wait_for_marker(&mut plain, &marker, "attribute");
    assert!(!text.contains("factory"), "{text}");
    signal(plain.id(), "-INT");
    let status = wait_exit(plain, Duration::from_secs(10));
    assert_eq!(
        status.code(),
        Some(0),
        "{}",
        read_stderr(&dir.join("child.err"))
    );

    let _ = fs::remove_file(&marker);
    let broken = output_in(
        &["--child", "--app", "appmod:missing"],
        &dir,
        &[
            ("REDIS_URL", "redis://redis:6379/0"),
            ("WORKER_MARKER", marker.to_str().unwrap()),
        ],
    );
    assert_eq!(
        broken.status.code(),
        Some(3),
        "{}",
        String::from_utf8_lossy(&broken.stderr)
    );
    let bad_fd = output_in(
        &["--child", "--app", "appmod:app"],
        &dir,
        &[
            ("REDIS_URL", "redis://redis:6379/0"),
            ("WORKER_MARKER", marker.to_str().unwrap()),
            ("ARQRS_PARENT_PIDFD", "nope"),
        ],
    );
    assert_eq!(
        bad_fd.status.code(),
        Some(3),
        "{}",
        String::from_utf8_lossy(&bad_fd.stderr)
    );

    let mut parent = spawn_worker(
        &[
            "--app",
            "appmod:create_app",
            "--factory",
            "--processes",
            "1",
        ],
        &dir,
        &marker,
    );
    let text = wait_for_marker(&mut parent, &marker, "factory");
    assert!(
        text.lines().any(|line| line.starts_with("factory ")),
        "{text}"
    );
    for line in text.lines() {
        let pid = line.split_whitespace().nth(1).unwrap();
        assert_ne!(
            pid,
            parent.id().to_string(),
            "parent imported or called the factory:\n{text}"
        );
    }
    signal(parent.id(), "-TERM");
    let status = wait_exit(parent, Duration::from_secs(20));
    assert_eq!(
        status.code(),
        Some(0),
        "parent stderr:\n{}",
        read_stderr(&dir.join("child.err"))
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn only_the_worker_crate_depends_on_all_four() {
    let root = workspace_root();
    let deps = ["arq-core", "arq-redis", "arq-pybridge", "arq-supervisor"];
    for name in [
        "arq-core",
        "arq-redis",
        "arq-pybridge",
        "arq-supervisor",
        "arq-worker",
    ] {
        let path = root.join("crates").join(name).join("Cargo.toml");
        let has_all = deps.iter().all(|dep| manifest_depends(&path, dep));
        if name == "arq-worker" {
            assert!(has_all, "{}", path.display());
            let text = fs::read_to_string(&path).unwrap();
            let dependencies = text.split("[dependencies]").nth(1).unwrap_or("");
            assert!(
                !dependencies.contains("extension-module"),
                "arq-worker must not enable extension-module"
            );
        } else {
            assert!(!has_all, "{name} must not depend on all four");
        }
    }
}

fn manifest_depends(path: &Path, dep: &str) -> bool {
    let text = fs::read_to_string(path).unwrap();
    let mut in_deps = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_deps = trimmed == "[dependencies]";
            continue;
        }
        if in_deps
            && (trimmed.starts_with(&format!("{dep} ")) || trimmed.starts_with(&format!("{dep}=")))
        {
            return true;
        }
    }
    false
}

fn spawn_worker(args: &[&str], dir: &Path, marker: &Path) -> std::process::Child {
    let err_path = dir.join("child.err");
    let stderr = fs::File::create(&err_path).unwrap();
    let mut cmd = command(args);
    cmd.current_dir(dir)
        .env("PYTHONPATH", dir)
        .env("REDIS_URL", "redis://redis:6379/0")
        .env("WORKER_MARKER", marker)
        .env("PYTHONUNBUFFERED", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr));
    cmd.spawn().expect("spawn worker")
}

fn output_in(args: &[&str], dir: &Path, env: &[(&str, &str)]) -> std::process::Output {
    let mut cmd = command(args);
    cmd.current_dir(dir).env("PYTHONPATH", dir);
    for (key, value) in env {
        cmd.env(key, value);
    }
    cmd.output().expect("spawn worker")
}

fn wait_for_marker(child: &mut std::process::Child, marker: &Path, needle: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            panic!(
                "worker {} exited {status} before {needle}\n{}",
                child.id(),
                read_stderr(&marker.parent().unwrap().join("child.err"))
            );
        }
        if marker.is_file() {
            let text = fs::read_to_string(marker).unwrap_or_default();
            if text.contains(needle) {
                return text;
            }
        }
        if Instant::now() > deadline {
            panic!(
                "timed out waiting for {needle} in {} from pid {}\n{}",
                marker.display(),
                child.id(),
                read_stderr(&marker.parent().unwrap().join("child.err"))
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn signal(pid: u32, flag: &str) {
    let status = Command::new("kill")
        .args([flag, &pid.to_string()])
        .status()
        .expect("kill");
    assert!(status.success());
}

fn wait_exit(mut child: std::process::Child, limit: Duration) -> std::process::ExitStatus {
    let deadline = Instant::now() + limit;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if Instant::now() > deadline {
            let _ = Command::new("kill")
                .args(["-KILL", &child.id().to_string()])
                .status();
            panic!("worker {} did not exit", child.id());
        }
        std::thread::sleep(Duration::from_millis(30));
    }
}

fn read_stderr(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_default()
}
