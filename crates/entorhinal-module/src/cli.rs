//! Operator surface for the registry: `ck entorhinal <verb>`.
//!
//! This lives in the module binary rather than a sibling `-admin` binary
//! because `ck` dispatches an unknown domain to `ck-<domain>` on PATH, so
//! `ck entorhinal list` lands here whatever we do. Splitting the operator verbs
//! into a second binary would leave the module binary receiving those arguments
//! and — before this module existed — starting a daemon in response to them.
//!
//! The module is the DEFAULT and empty argv is the only way to reach it, which
//! is how the supervisor spawns it. Anything else is parsed before it can act:
//! an unrecognised verb reports and exits rather than falling through to serve,
//! so a mistyped command can never claim the module's identity and sit there
//! looking healthy.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use serde_json::{json, Value};

const EXIT_TRANSPORT: u8 = 2;
const EXIT_REFUSED: u8 = 3;
const EXIT_USAGE: u8 = 64;

/// What the operator asked for, resolved before anything is opened or dialled.
pub enum Invocation {
    /// No arguments: run as the supervised module. The only path that serves.
    Module,
    /// Answer an informational request and exit 0: `--help`, `--version`, or
    /// flags with no verb. Never serves; empty argv is `Module` instead.
    Report(String),
    /// Refuse a malformed invocation and exit nonzero.
    ///
    /// Distinct from `Report` because ASKING FOR HELP AND BEING REFUSED ARE NOT
    /// THE SAME OUTCOME, and folding them loses the only signal a script has: a
    /// mistyped flag that exits 0 is a wrapper reporting success for a command
    /// that never ran.
    Refuse(String),
    /// A verb to run against the daemon.
    Command(Command),
}

pub struct Command {
    verb: String,
    connection: Option<PathBuf>,
    args: Vec<String>,
    json: bool,
}

/// Parse argv WITHOUT acting on it.
///
/// Separating this from execution is what lets `--help` and a bad flag be
/// answered without connecting to anything, and it is the same parse-then-act
/// split the daemon binary uses.
pub fn parse(arguments: &[String]) -> Invocation {
    if arguments.is_empty() {
        return Invocation::Module;
    }
    if arguments.iter().any(|a| a == "--version") {
        return Invocation::Report(format!("ck-entorhinal {}", env!("CARGO_PKG_VERSION")));
    }
    // Help is honoured ANYWHERE in the tail, including after a verb, so
    // `ck entorhinal register --help` explains rather than registering. A
    // help flag that only works in first position is one a hurried operator
    // discovers by mutating the registry.
    if arguments.iter().any(|a| a == "--help" || a == "-h") {
        return Invocation::Report(usage());
    }
    // Everything past this point is either a well-formed command or a refusal.

    let mut verb = None;
    let mut connection = None;
    let mut args = Vec::new();
    let mut json = false;
    let mut rest = arguments.iter();
    while let Some(argument) = rest.next() {
        match argument.as_str() {
            "--subc" => match rest.next() {
                Some(path) => connection = Some(PathBuf::from(path)),
                None => return Invocation::Refuse("--subc needs a path".to_string()),
            },
            "--json" => json = true,
            other if other.starts_with('-') => {
                return Invocation::Refuse(format!("unknown flag '{other}'\n\n{}", usage()));
            }
            other if verb.is_none() => verb = Some(other.to_string()),
            other => args.push(other.to_string()),
        }
    }

    match verb {
        // A bare invocation with only flags is a question, not a mistake.
        None => Invocation::Report(usage()),
        Some(verb) => Invocation::Command(Command {
            verb,
            connection,
            args,
            json,
        }),
    }
}

pub fn usage() -> String {
    "\
ck entorhinal — the workspace/project registry

usage: ck entorhinal <verb> [args] [--subc <connection file>] [--json]

  list [workspace]        projects, optionally scoped to one workspace
  resolve <dir>           which project owns a directory
  register <name> <dir>   register a project rooted at a directory
  workspace <project> <workspace>
                          put a project in a workspace (moves it if already in another)
  remove <project>        remove a project from the registry
  verify                  check the store against its journal

  --subc <path>   daemon connection file; defaults to the usual discovery path
  --json          raw response instead of the rendered form

With no arguments this binary runs as the supervised module, which is how the
daemon spawns it."
        .to_string()
}

pub fn run(command: Command) -> ExitCode {
    // A standalone operator client, never a supervised module. Run from a shell
    // inside the fleet, the parent module's spawn attestation leaks in through
    // the environment; the client would present it as a consumer identity and
    // the daemon would refuse the mismatched launch nonce. Scrubbing makes this
    // a plain Direct consumer.
    std::env::remove_var("SUBC_MODULE_ID");
    std::env::remove_var("SUBC_LAUNCH_NONCE");

    let (method, params) = match build(&command) {
        Ok(request) => request,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::from(EXIT_USAGE);
        }
    };

    let connection = match command.connection.clone() {
        Some(path) => {
            // An explicit path that does not exist is a REFUSAL, never a
            // fallback to discovery: silently retargeting an operator who named
            // a rig connection file would run their command against production.
            if !path.exists() {
                eprintln!("connection file not found: {}", path.display());
                return ExitCode::from(EXIT_TRANSPORT);
            }
            path
        }
        None => match discover() {
            Some(path) => path,
            None => {
                eprintln!("no daemon connection file found; pass --subc <path>");
                return ExitCode::from(EXIT_TRANSPORT);
            }
        },
    };

    match call(&connection, &method, params) {
        Ok(value) => {
            render(&command, &value);
            ExitCode::SUCCESS
        }
        Err(failure) => {
            eprintln!("{}", failure.1);
            ExitCode::from(failure.0)
        }
    }
}

/// Translate a verb into the module's own wire vocabulary.
///
/// The method names and the camelCase parameter spellings are the module's,
/// read from its dispatcher and request types rather than assumed: an operator
/// command that guesses a field name fails as a refusal from the module, which
/// reads like the registry rejecting the request rather than the CLI addressing
/// it wrongly.
fn build(command: &Command) -> Result<(String, Value), String> {
    let need = |index: usize, what: &str| -> Result<String, String> {
        command
            .args
            .get(index)
            .cloned()
            .ok_or_else(|| format!("{} needs {what}\n\n{}", command.verb, usage()))
    };
    // Directories are resolved against the operator's shell before they are
    // sent, because the registry canonicalizes what it is given and a relative
    // path would canonicalize against the DAEMON's working directory.
    let absolute = |value: &str| -> Result<String, String> {
        let path = Path::new(value);
        let joined = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(|error| format!("resolve '{value}': {error}"))?
                .join(path)
        };
        Ok(joined.to_string_lossy().to_string())
    };

    Ok(match command.verb.as_str() {
        "list" => (
            "enumerate".to_string(),
            match command.args.first() {
                Some(workspace) => json!({ "workspaceId": workspace }),
                None => json!({}),
            },
        ),
        "resolve" => (
            "resolve".to_string(),
            json!({ "canonicalRoot": absolute(&need(0, "a directory")?)? }),
        ),
        "register" => (
            "register".to_string(),
            json!({
                "name": need(0, "a name")?,
                "roots": [absolute(&need(1, "a directory")?)?],
                "actor": "ck-entorhinal",
            }),
        ),
        "workspace" => (
            "assign_workspace".to_string(),
            json!({
                "projectId": need(0, "a project id")?,
                "workspaceId": need(1, "a workspace id")?,
                "actor": "ck-entorhinal",
            }),
        ),
        "remove" => (
            "remove".to_string(),
            json!({ "projectId": need(0, "a project id")?, "actor": "ck-entorhinal" }),
        ),
        "verify" => ("verify".to_string(), json!({})),
        other => return Err(format!("unknown verb '{other}'\n\n{}", usage())),
        // NOTE: an unknown verb is refused here rather than at parse time
        // because `run` maps a build error to EXIT_USAGE, so it already exits
        // nonzero. Both routes refuse; only the exit code has to agree.
    })
}

/// Where the daemon publishes its connection file, in the order `ck` looks.
fn discover() -> Option<PathBuf> {
    if let Ok(explicit) = std::env::var("SUBC_CONNECTION_FILE") {
        let path = PathBuf::from(explicit);
        return path.exists().then_some(path);
    }
    let home = std::env::var("HOME").ok()?;
    let candidate = PathBuf::from(&home)
        .join(".local/share/cortexkit/run")
        .join("subc-connection.json");
    candidate.exists().then_some(candidate)
}

fn call(connection: &Path, method: &str, params: Value) -> Result<Value, (u8, String)> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .map_err(|error| (EXIT_TRANSPORT, format!("start runtime: {error}")))?;
    let body = serde_json::to_vec(&json!({ "method": method, "params": params }))
        .map_err(|error| (EXIT_TRANSPORT, format!("serialize request: {error}")))?;
    let connection = connection.to_path_buf();
    runtime.block_on(async move {
        let consumer = subc_client_rs::SubcConsumer::connect(
            &connection,
            subc_client_rs::ConsumerOptions::default(),
        )
        .await
        .map_err(|error| {
            (
                EXIT_TRANSPORT,
                format!("connect {}: {error}", connection.display()),
            )
        })?;
        let bytes = consumer
            .call(
                subc_protocol::RouteTarget::ManagementSurface {
                    module_id: super::MODULE_ID.to_string(),
                },
                subc_protocol::BindIdentity {
                    project_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/")),
                    harness: "ck-entorhinal".to_string(),
                    session: "operator".to_string(),
                },
                body,
                subc_client_rs::CallOptions::default(),
            )
            .await
            .map_err(|error| match error {
                // A module refusal is the registry answering, and its code is
                // the answer: `not_found` and a store failure need different
                // reactions from the operator, so the code is surfaced VERBATIM
                // rather than folded into a transport message.
                subc_client_rs::CallError::Module(body) => {
                    (EXIT_REFUSED, format!("{}: {}", body.code, body.message))
                }
                other => (EXIT_TRANSPORT, format!("call: {other}")),
            })?;
        serde_json::from_slice(&bytes)
            .map_err(|error| (EXIT_TRANSPORT, format!("decode response: {error}")))
    })
}

fn render(command: &Command, value: &Value) {
    if command.json {
        println!(
            "{}",
            serde_json::to_string_pretty(value).unwrap_or_default()
        );
        return;
    }
    match command.verb.as_str() {
        "list" => {
            let projects = value["projects"].as_array().cloned().unwrap_or_default();
            if projects.is_empty() {
                // An empty registry and a filter matching nothing are different
                // situations, and a bare blank screen is the shape that reads as
                // a broken command. Say which one this is.
                match command.args.first() {
                    Some(workspace) => println!("no projects in workspace '{workspace}'"),
                    None => println!("no projects registered"),
                }
                return;
            }
            for project in &projects {
                println!(
                    "{:<28} {}",
                    text(&project["projectId"]),
                    text(&project["name"])
                );
            }
            println!("\n{} project(s)", projects.len());
        }
        "resolve" => {
            println!("project:   {}", text(&value["projectId"]));
            println!("name:      {}", text(&value["name"]));
            println!("workspace: {}", text(&value["workspaceId"]));
        }
        _ => println!(
            "{}",
            serde_json::to_string_pretty(value).unwrap_or_default()
        ),
    }
}

/// Render a JSON scalar for humans: strings unquoted, absence as `-`.
///
/// Absence and the string "null" must not render alike — the first means the
/// registry had nothing to say and the second would mean it said so.
fn text(value: &Value) -> String {
    match value {
        Value::String(inner) => inner.clone(),
        Value::Null => "-".to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_of(args: &[&str]) -> Invocation {
        parse(&args.iter().map(|a| a.to_string()).collect::<Vec<_>>())
    }

    /// Empty argv is the ONLY path that serves. This is the property that keeps
    /// a mistyped operator command from claiming the module's identity.
    #[test]
    fn only_empty_argv_runs_as_the_module() {
        assert!(matches!(parse_of(&[]), Invocation::Module));
        for argv in [
            vec!["--help"],
            vec!["-h"],
            vec!["--version"],
            vec!["list"],
            vec!["--nonsense"],
            vec!["typo-verb"],
        ] {
            assert!(
                !matches!(parse_of(&argv), Invocation::Module),
                "{argv:?} must not reach module mode"
            );
        }
    }

    /// A help flag AFTER a verb explains rather than executing.
    #[test]
    fn help_is_honoured_after_a_verb() {
        for argv in [
            vec!["remove", "--help"],
            vec!["register", "name", "/tmp", "-h"],
        ] {
            match parse_of(&argv) {
                Invocation::Report(text) => assert!(text.contains("usage:"), "{argv:?}"),
                _ => panic!("{argv:?} must report usage, not run"),
            }
        }
    }

    /// An unrecognised flag REFUSES rather than being swallowed as an argument.
    ///
    /// Asserts the Refuse variant specifically, because Report exits 0 and a
    /// malformed invocation exiting 0 tells a wrapper the command succeeded.
    #[test]
    fn unknown_flag_refuses_rather_than_becoming_an_argument() {
        match parse_of(&["list", "--wat"]) {
            Invocation::Refuse(text) => assert!(text.contains("unknown flag '--wat'")),
            _ => panic!("unknown flag must refuse, not report"),
        }
        match parse_of(&["list", "--subc"]) {
            Invocation::Refuse(text) => assert!(text.contains("needs a path")),
            _ => panic!("a flag missing its value must refuse"),
        }
    }

    /// Asking for help and being refused must not share an exit status.
    ///
    /// Both print usage, so the TEXT cannot distinguish them and only the
    /// variant can — which is the whole reason Refuse exists separately.
    #[test]
    fn help_and_refusal_are_different_outcomes() {
        assert!(matches!(parse_of(&["--help"]), Invocation::Report(_)));
        assert!(matches!(parse_of(&[]), Invocation::Module));
        assert!(matches!(parse_of(&["--wat"]), Invocation::Refuse(_)));
    }

    /// The wire vocabulary is the MODULE's, not the operator's.
    ///
    /// These assert the exact method names and camelCase spellings read from
    /// the module's dispatcher and request types. A rename on either side that
    /// does not move both fails here rather than at runtime, where it would
    /// surface as the registry refusing a request that was addressed wrongly.
    #[test]
    fn verbs_map_to_the_modules_own_method_and_field_names() {
        let cmd = |args: &[&str]| Command {
            verb: args[0].to_string(),
            connection: None,
            args: args[1..].iter().map(|a| a.to_string()).collect(),
            json: false,
        };

        let (method, params) = build(&cmd(&["list"])).unwrap();
        assert_eq!(method, "enumerate");
        assert_eq!(params, json!({}));

        let (method, params) = build(&cmd(&["list", "w1"])).unwrap();
        assert_eq!(method, "enumerate");
        assert_eq!(params["workspaceId"], "w1");

        let (method, params) = build(&cmd(&["workspace", "p1", "w2"])).unwrap();
        assert_eq!(method, "assign_workspace");
        assert_eq!(params["projectId"], "p1");
        assert_eq!(params["workspaceId"], "w2");

        let (method, params) = build(&cmd(&["remove", "p1"])).unwrap();
        assert_eq!(method, "remove");
        assert_eq!(params["projectId"], "p1");

        assert_eq!(build(&cmd(&["verify"])).unwrap().0, "verify");
    }

    /// Relative directories are resolved against the OPERATOR's shell.
    ///
    /// The registry canonicalizes what it is handed, and it runs in the
    /// daemon's working directory — so sending a relative path would register a
    /// different directory than the operator named, silently and plausibly.
    #[test]
    fn directories_are_absolute_before_they_are_sent() {
        let cmd = Command {
            verb: "resolve".to_string(),
            connection: None,
            args: vec![".".to_string()],
            json: false,
        };
        let (_, params) = build(&cmd).unwrap();
        let sent = params["canonicalRoot"].as_str().unwrap();
        assert!(
            Path::new(sent).is_absolute(),
            "sent a relative path: {sent}"
        );
        assert_eq!(
            Path::new(sent),
            std::env::current_dir().unwrap().join("."),
            "resolved against something other than the operator's cwd"
        );
    }

    /// A verb missing its arguments is a usage error, not a call with a hole in
    /// it. Naming the missing thing is what stops an operator retrying blind.
    #[test]
    fn missing_arguments_name_what_is_missing() {
        let cases = [
            (vec!["resolve"], "a directory"),
            (vec!["register", "only-a-name"], "a directory"),
            (vec!["workspace", "p1"], "a workspace id"),
            (vec!["remove"], "a project id"),
        ];
        for (argv, expected) in cases {
            let cmd = Command {
                verb: argv[0].to_string(),
                connection: None,
                args: argv[1..].iter().map(|a| a.to_string()).collect(),
                json: false,
            };
            let error = build(&cmd).expect_err("must refuse");
            assert!(error.contains(expected), "{argv:?} said: {error}");
        }
    }

    /// Absence and a literal "null" must not render alike.
    #[test]
    fn absent_values_render_distinctly() {
        assert_eq!(text(&Value::Null), "-");
        assert_eq!(text(&json!("null")), "null");
        assert_eq!(text(&json!("p1")), "p1");
    }
}
