//! `--app module:app` and `--app module:create_app --factory`.
//!
//! A configuration error is exit 3. `--version` prints the workspace package
//! version and does not read `ARQ_*` or `REDIS_URL`.

use arq_core::{validate_queue, Config, CONFIG_EXIT_CODE};
use arq_redis::RedisBackend;
use arq_supervisor::Shutdown;

use crate::child::run_child;
use crate::parent::run_parent;

pub fn package_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

pub fn version_line() -> String {
    format!("arq-worker {}", package_version())
}

pub fn usage() -> &'static str {
    "\
arq-worker --app module:app
arq-worker --app module:create_app --factory

--app module:attribute     object used as the App, or a factory when --factory is set
--factory                  call the attribute inside the child; it must return an App
--queues default           comma-separated queue names ([a-z0-9]+)
--processes 1              child processes supervised by this process
--concurrency 1            reserved in-flight tasks per child
--version                  print the workspace package version and exit
"
}

#[derive(Clone)]
pub struct ParentPlan {
    pub app: String,
    pub factory: bool,
    pub queues: Vec<String>,
    pub processes: u32,
    pub concurrency: u32,
    pub redis_url: String,
    pub config: Config,
    pub shutdown: Shutdown,
}

#[derive(Debug, Clone)]
pub struct ChildPlan {
    pub app: String,
    pub factory: bool,
    pub queues: Vec<String>,
    pub concurrency: u32,
    pub redis_url: String,
    pub config: Config,
}

impl std::fmt::Debug for ParentPlan {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ParentPlan")
            .field("app", &self.app)
            .field("factory", &self.factory)
            .field("queues", &self.queues)
            .field("processes", &self.processes)
            .field("concurrency", &self.concurrency)
            .field("redis_url", &self.redis_url)
            .field("shutdown", &self.shutdown.is_requested())
            .finish()
    }
}

#[derive(Debug)]
pub enum Action {
    Version,
    Help,
    Parent(ParentPlan),
    Child(ChildPlan),
}

struct RunArgs {
    app: String,
    factory: bool,
    child: bool,
    queues: Vec<String>,
    processes: u32,
    concurrency: u32,
}

enum Parsed {
    Version,
    Help,
    Run(RunArgs),
}

pub fn execute<A, S>(args: A) -> i32
where
    A: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    dispatch(prepare(args, std::env::vars()))
}

pub fn prepare<A, S, I, K, V>(args: A, env: I) -> Result<Action, String>
where
    A: IntoIterator<Item = S>,
    S: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: AsRef<str>,
{
    let args: Vec<String> = args
        .into_iter()
        .map(|arg| arg.as_ref().to_string())
        .collect();
    match parse_args(&args)? {
        Parsed::Version => Ok(Action::Version),
        Parsed::Help => Ok(Action::Help),
        Parsed::Run(run) => {
            let (config, redis_url) = load_settings(env)?;
            if run.child {
                Ok(Action::Child(ChildPlan {
                    app: run.app,
                    factory: run.factory,
                    queues: run.queues,
                    concurrency: run.concurrency,
                    redis_url,
                    config,
                }))
            } else {
                Ok(Action::Parent(ParentPlan {
                    app: run.app,
                    factory: run.factory,
                    queues: run.queues,
                    processes: run.processes,
                    concurrency: run.concurrency,
                    redis_url,
                    config,
                    shutdown: Shutdown::new(),
                }))
            }
        }
    }
}

fn dispatch(parsed: Result<Action, String>) -> i32 {
    match parsed {
        Err(error) => {
            eprintln!("arq-worker: {error}");
            CONFIG_EXIT_CODE
        }
        Ok(Action::Version) => {
            println!("{}", version_line());
            0
        }
        Ok(Action::Help) => {
            println!("{}", usage());
            0
        }
        Ok(Action::Parent(plan)) => run_parent(plan),
        Ok(Action::Child(plan)) => run_child(plan),
    }
}

fn parse_args(args: &[String]) -> Result<Parsed, String> {
    if args.iter().any(|arg| arg == "--version" || arg == "-V") {
        return Ok(Parsed::Version);
    }
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        return Ok(Parsed::Help);
    }
    let mut app = None;
    let mut factory = false;
    let mut child = false;
    let mut queues = None;
    let mut processes = None;
    let mut concurrency = None;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--factory" => factory = true,
            "--child" => child = true,
            "--app" => app = Some(require_value("--app", iter.next())?),
            "--queues" => queues = Some(split_queues(&require_value("--queues", iter.next())?)?),
            "--processes" => processes = Some(require_count("--processes", iter.next())?),
            "--concurrency" => concurrency = Some(require_count("--concurrency", iter.next())?),
            other if let Some(value) = other.strip_prefix("--app=") => {
                app = Some(non_empty("--app", value)?);
            }
            other if let Some(value) = other.strip_prefix("--queues=") => {
                queues = Some(split_queues(&non_empty("--queues", value)?)?);
            }
            other if let Some(value) = other.strip_prefix("--processes=") => {
                processes = Some(parse_count("--processes", value)?);
            }
            other if let Some(value) = other.strip_prefix("--concurrency=") => {
                concurrency = Some(parse_count("--concurrency", value)?);
            }
            other => return Err(format!("unknown argument {other}")),
        }
    }
    let app = app.ok_or_else(|| "missing --app module:attribute".to_string())?;
    split_app_path(&app)?;
    let queues = queues.unwrap_or_else(|| vec!["default".to_string()]);
    if queues.is_empty() {
        return Err("at least one queue is required".into());
    }
    for queue in &queues {
        validate_queue(queue).map_err(|_| format!("invalid queue {queue}"))?;
    }
    Ok(Parsed::Run(RunArgs {
        app,
        factory,
        child,
        queues,
        processes: processes.unwrap_or(1),
        concurrency: concurrency.unwrap_or(1),
    }))
}

fn require_value(flag: &str, value: Option<&String>) -> Result<String, String> {
    match value {
        Some(value) => non_empty(flag, value),
        None => Err(format!("missing value for {flag}")),
    }
}

fn non_empty(flag: &str, value: &str) -> Result<String, String> {
    if value.is_empty() || value.starts_with('-') {
        Err(format!("missing value for {flag}"))
    } else {
        Ok(value.to_string())
    }
}

fn require_count(flag: &str, value: Option<&String>) -> Result<u32, String> {
    match value {
        Some(value) => parse_count(flag, value),
        None => Err(format!("missing value for {flag}")),
    }
}

fn parse_count(flag: &str, value: &str) -> Result<u32, String> {
    if value.is_empty() || value.starts_with('-') {
        return Err(format!("missing value for {flag}"));
    }
    let parsed: u32 = value
        .parse()
        .map_err(|_| format!("invalid {flag} {value}"))?;
    if parsed == 0 {
        return Err(format!("{flag} must be greater than 0"));
    }
    Ok(parsed)
}

fn split_app_path(path: &str) -> Result<(), String> {
    match path.split_once(':') {
        Some((module, attribute)) if !module.is_empty() && !attribute.is_empty() => Ok(()),
        _ => Err("Expected application path: module:attribute".into()),
    }
}

fn split_queues(value: &str) -> Result<Vec<String>, String> {
    if value.is_empty() {
        return Err("missing value for --queues".into());
    }
    Ok(value.split(',').map(|queue| queue.to_string()).collect())
}

fn load_settings<I, K, V>(env: I) -> Result<(Config, String), String>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: AsRef<str>,
{
    let pairs: Vec<(String, String)> = env
        .into_iter()
        .map(|(key, value)| (key.as_ref().to_string(), value.as_ref().to_string()))
        .collect();
    let config = Config::parse(
        pairs
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    )
    .map_err(|error| error.to_string())?;
    let redis_url = pairs
        .iter()
        .rev()
        .find(|(key, _)| key == "REDIS_URL")
        .map(|(_, value)| value.clone())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "REDIS_URL is required".to_string())?;
    if let Err(error) = RedisBackend::connect(&redis_url, config) {
        return Err(format!("REDIS_URL is invalid: {error}"));
    }
    Ok((config, redis_url))
}

/// Host and port (or socket path) with userinfo removed. Passwords are not logged.
pub fn redis_endpoint_label(url: &str) -> String {
    let rest = url.split_once("://").map(|(_, rest)| rest).unwrap_or(url);
    let host = rest.rsplit_once('@').map(|(_, host)| host).unwrap_or(rest);
    let host = host.split(['/', '?']).next().unwrap_or(host);
    if host.is_empty() {
        "redis".to_string()
    } else {
        host.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const URL: &str = "redis://redis:6379/0";

    fn parent(args: &[&str]) -> ParentPlan {
        match prepare(args, [("REDIS_URL", URL)]).unwrap() {
            Action::Parent(plan) => plan,
            other => panic!("expected parent, got {other:?}"),
        }
    }

    #[test]
    fn version_and_help_ignore_bad_config() {
        assert!(matches!(
            prepare(["--version", "--app"], [("SIMPLE_Q_APP", "1")]).unwrap(),
            Action::Version
        ));
        assert!(matches!(
            prepare(["-V"], [("REDIS_URL", "nope")]).unwrap(),
            Action::Version
        ));
        assert!(matches!(
            prepare(["--help", "--factory"], std::iter::empty::<(&str, &str)>()).unwrap(),
            Action::Help
        ));
        assert!(matches!(
            prepare(["-h"], std::iter::empty::<(&str, &str)>()).unwrap(),
            Action::Help
        ));
        assert_eq!(version_line(), format!("arq-worker {}", package_version()));
        assert!(usage().contains("--app module:app"));
        assert!(usage().contains("--app module:create_app --factory"));
        assert_eq!(execute(["--version"]), 0);
        assert_eq!(execute(["--help"]), 0);
        assert_eq!(execute(Vec::<String>::new()), CONFIG_EXIT_CODE);
        assert_eq!(execute(["--app", "not-an-app"]), CONFIG_EXIT_CODE);
    }

    #[test]
    fn accepts_attribute_and_factory_forms() {
        let direct = parent(&["--app", "module:app"]);
        assert_eq!(direct.app, "module:app");
        assert!(!direct.factory);
        assert_eq!(direct.queues, vec!["default".to_string()]);
        assert_eq!(direct.processes, 1);
        assert_eq!(direct.concurrency, 1);
        assert_eq!(direct.redis_url, URL);
        let factory = parent(&["--app", "module:create_app", "--factory"]);
        assert_eq!(factory.app, "module:create_app");
        assert!(factory.factory);
        let equals = parent(&[
            "--app=pkg.mod:create_app",
            "--factory",
            "--queues=default",
            "--processes=2",
            "--concurrency=1",
        ]);
        assert_eq!(equals.app, "pkg.mod:create_app");
        assert_eq!(equals.processes, 2);
        assert!(equals.factory);
        let child = match prepare(
            [
                "--child",
                "--app",
                "module:create_app",
                "--factory",
                "--queues",
                "default",
                "--concurrency",
                "1",
            ],
            [("REDIS_URL", URL)],
        )
        .unwrap()
        {
            Action::Child(plan) => plan,
            other => panic!("expected child, got {other:?}"),
        };
        assert!(child.factory);
        assert_eq!(child.app, "module:create_app");
        assert_eq!(child.queues, vec!["default".to_string()]);
        assert!(!crate::python_initialized());
    }

    #[test]
    fn bad_config_is_rejected_before_python() {
        let env = [("REDIS_URL", URL)];
        let errors = [
            prepare(["--app"], env),
            prepare(["--app", "module"], env),
            prepare(["--app", ":app"], env),
            prepare(["--app", "module:"], env),
            prepare(["--factory"], env),
            prepare(["--app", "--factory"], env),
            prepare(["--bogus"], env),
            prepare(["--app=module:app", "extra"], env),
            prepare(["--processes"], env),
            prepare(["--app", "module:app", "--processes", "0"], env),
            prepare(["--app", "module:app", "--processes", "no"], env),
            prepare(["--app", "module:app", "--concurrency", "0"], env),
            prepare(["--app", "module:app", "--concurrency="], env),
            prepare(["--app", "module:app", "--queues"], env),
            prepare(["--app", "module:app", "--queues", ""], env),
            prepare(["--app", "module:app", "--queues", "Bad"], env),
            prepare(["--app", "module:app", "--queues", "default,Bad"], env),
            prepare(["--app", "module:app", "--queues", "default,"], env),
            prepare(["--app=", "module:app"], env),
            prepare(Vec::<&str>::new(), env),
        ];
        for error in errors {
            let message = error.expect_err("bad config must fail");
            assert!(!message.is_empty(), "{message}");
        }
        let simple = prepare(
            ["--app", "module:app"],
            [("SIMPLE_Q_APP", "module:app"), ("REDIS_URL", URL)],
        );
        assert!(simple.unwrap_err().contains("SIMPLE_Q_APP"));
        let unknown = prepare(
            ["--app", "module:app"],
            [("ARQ_NOT_A_SETTING", "1"), ("REDIS_URL", URL)],
        );
        assert!(unknown.unwrap_err().contains("ARQ_NOT_A_SETTING"));
        let invalid = prepare(
            ["--app", "module:app"],
            [("ARQ_LEASE_MS", "0"), ("REDIS_URL", URL)],
        );
        assert!(invalid.unwrap_err().contains("ARQ_LEASE_MS"));
        assert!(
            prepare(["--app", "module:app"], std::iter::empty::<(&str, &str)>())
                .unwrap_err()
                .contains("REDIS_URL")
        );
        assert!(prepare(["--app", "module:app"], [("REDIS_URL", "")])
            .unwrap_err()
            .contains("REDIS_URL"));
        assert!(prepare(
            ["--app", "module:app"],
            [("REDIS_URL", "http://redis:6379/0")]
        )
        .unwrap_err()
        .contains("invalid"));
        assert!(!crate::python_initialized());
    }

    #[test]
    fn endpoint_label_drops_userinfo() {
        assert_eq!(redis_endpoint_label("redis://redis:6379/0"), "redis:6379");
        assert_eq!(
            redis_endpoint_label("rediss://user:secret@redis:6379/0"),
            "redis:6379"
        );
        assert_eq!(
            redis_endpoint_label("redis://:secret@redis:6379/0?password=secret"),
            "redis:6379"
        );
        assert_eq!(redis_endpoint_label("unix:///tmp/redis.sock"), "redis");
        assert!(!redis_endpoint_label("redis://:secret@redis:6379/0").contains("secret"));
    }

    #[test]
    fn an_empty_queue_string_is_rejected() {
        assert_eq!(split_queues("").unwrap_err(), "missing value for --queues");
    }
}
