//! Operator surface for the registry: `ck projects <verb>` and
//! `ck workspaces <verb>`.
//!
//! One binary, three names. `ck` dispatches unknown domains to `ck-<domain>`
//! on PATH, and `ck-projects` / `ck-workspaces` are symlinks to this module
//! binary; argv[0] selects the face. The module id (entorhinal) is
//! infrastructure and never part of the operator vocabulary -- a user thinks
//! in projects and workspaces, not in the anatomy of the module serving them.
//!
//! The verbs live in the module binary rather than a sibling `-admin` binary
//! because a second binary would leave this one receiving misdirected
//! arguments and — before this module existed — starting a daemon in response
//! to them.
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

/// Which name this binary was invoked under. The face decides the verb
/// vocabulary and the help text; the wire behind them is shared.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Face {
    /// `ck-projects`: the project registry verbs.
    Projects,
    /// `ck-workspaces`: workspace listing and project placement.
    Workspaces,
    /// `ck-entorhinal`: the supervised module itself. Serves; its only
    /// operator surface is `--version`/`--help`, which point at the faces.
    Entorhinal,
}

/// Resolve the face from argv[0]'s file stem. Unrecognised or absent argv[0]
/// defaults to the module face: the daemon spawns this binary by its real
/// path, and a wrapper that renames it should get the serving default, not a
/// CLI that refuses to serve.
pub fn face_from_argv0(argv0: Option<&str>) -> Face {
    let stem = argv0
        .map(Path::new)
        .and_then(Path::file_stem)
        .and_then(|stem| stem.to_str())
        .unwrap_or("");
    match stem {
        "ck-projects" => Face::Projects,
        "ck-workspaces" => Face::Workspaces,
        _ => Face::Entorhinal,
    }
}

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
    face: Face,
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
pub fn parse(face: Face, arguments: &[String]) -> Invocation {
    // A supervised spawn is NOT empty argv. subc launches module-mode children as
    // `<program> [configured args] --subc <connection-file-path>` (supervise.rs
    // SUBC_ARG), so the shape that reaches the module is a bare `--subc <path>`
    // with no verb. Treating empty argv as the only serving path sent every
    // supervised spawn into the usage branch, which printed help and EXITED 0 --
    // and the daemon logged "supervised module exited cleanly", because from its
    // side that is exactly what happened.
    //
    // The discriminator is therefore "no verb, and a connection file was given":
    // the daemon always supplies one, and an operator invoking a verb always has
    // one. Empty argv is kept as Module too so a hand-run child behaves the same,
    // but it is no longer the condition being relied on.
    if arguments.is_empty() {
        // Bare `ck projects` / `ck workspaces` is a question about the domain,
        // answered with its help (the ck convention: verbless domains explain).
        // Only the module face serves from empty argv.
        return match face {
            Face::Entorhinal => Invocation::Module,
            _ => Invocation::Report(usage(face)),
        };
    }
    // `ck` hands `ck projects ...` to this binary only after `ck-projects
    // --ck-domain` exits 0 with exactly one headline line within 2 seconds;
    // without that handshake the dispatcher refuses the command. The answer is
    // the first line of the face's own help, so the headline cannot drift from
    // what `ck projects` explains. The module face is not an operator domain,
    // so it keeps refusing the flag.
    if arguments.len() == 1 && arguments[0] == "--ck-domain" {
        return match face {
            Face::Entorhinal => Invocation::Refuse(
                "ck-entorhinal is a module, not a ck domain; the domains are ck projects and ck workspaces"
                    .to_string(),
            ),
            _ => Invocation::Report(usage(face).lines().next().unwrap_or_default().to_string()),
        };
    }
    if arguments.iter().any(|a| a == "--version") {
        return Invocation::Report(format!(
            "{} {}",
            binary_name(face),
            env!("CARGO_PKG_VERSION")
        ));
    }
    // Help is honoured ANYWHERE in the tail, including after a verb, so
    // `ck entorhinal register --help` explains rather than registering. A
    // help flag that only works in first position is one a hurried operator
    // discovers by mutating the registry.
    if arguments.iter().any(|a| a == "--help" || a == "-h") {
        return Invocation::Report(usage(face));
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
                return Invocation::Refuse(format!("unknown flag '{other}'\n\n{}", usage(face)));
            }
            other if verb.is_none() => verb = Some(other.to_string()),
            other => args.push(other.to_string()),
        }
    }

    match verb {
        // No verb WITH a connection file is the supervisor's spawn shape:
        // serve -- but only under the module's own name. On a CLI face the
        // same shape is an operator handing a connection file to a verbless
        // command, which is a question.
        None if connection.is_some() && face == Face::Entorhinal => Invocation::Module,
        // No verb otherwise is a question, not a mistake.
        None => Invocation::Report(usage(face)),
        Some(verb) => Invocation::Command(Command {
            face,
            verb,
            connection,
            args,
            json,
        }),
    }
}

fn binary_name(face: Face) -> &'static str {
    match face {
        Face::Projects => "ck-projects",
        Face::Workspaces => "ck-workspaces",
        Face::Entorhinal => "ck-entorhinal",
    }
}

pub fn usage(face: Face) -> String {
    match face {
        Face::Projects => "\
ck projects — the project registry

usage: ck projects <verb> [args] [--subc <connection file>] [--json]

  list [workspace]        projects, optionally scoped to one workspace
  resolve <dir>           which project owns a directory
  register <name> <dir>   register a project rooted at a directory
  remove <project>        remove a project from the registry
  verify                  check the store against its journal

  --subc <path>   daemon connection file; defaults to the usual discovery path
  --json          raw response instead of the rendered form

workspace placement lives under 'ck workspaces'"
            .to_string(),
        Face::Workspaces => "\
ck workspaces — group projects into workspaces

usage: ck workspaces <verb> [args] [--subc <connection file>] [--json]

  list                    workspaces known to the registry
  assign <project> <workspace>
                          put a project in a workspace (moves it if already in another)

  --subc <path>   daemon connection file; defaults to the usual discovery path
  --json          raw response instead of the rendered form

projects themselves live under 'ck projects'"
            .to_string(),
        Face::Entorhinal => "\
ck-entorhinal — the registry module (supervised by the subc daemon)

This binary is the module itself; it is not an operator command. The operator
surface is:

  ck projects      list, register, resolve, remove, verify
  ck workspaces    list, assign

With no arguments it runs as the supervised module, which is how the daemon
spawns it."
            .to_string(),
    }
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
            .ok_or_else(|| format!("{} needs {what}\n\n{}", command.verb, usage(command.face)))
    };
    // Directories are CANONICALIZED against the operator's shell before they are
    // sent. Two separate reasons, and only the first was obvious:
    //
    // 1. A relative path has to be resolved here, because the registry resolves
    //    what it is given against the DAEMON's working directory, not the
    //    operator's.
    // 2. The registry REFUSES a non-canonical path outright (`not_canonical`).
    //    On macOS /tmp is a symlink to /private/tmp, so an absolute path that
    //    the operator can `cd` into is still rejected -- making absolute
    //    necessary but not sufficient. Sending it verbatim turns an ordinary
    //    path into a refusal the operator cannot act on, since nothing about
    //    the path they typed looks wrong.
    //
    // Falls back to the lexically-joined path when the directory does not exist
    // yet: canonicalize() requires existence, and a clear refusal from the
    // registry about a missing directory is better than one from here about a
    // failed syscall.
    let absolute = |value: &str| -> Result<String, String> {
        let path = Path::new(value);
        let joined = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(|error| format!("resolve '{value}': {error}"))?
                .join(path)
        };
        let resolved = std::fs::canonicalize(&joined).unwrap_or(joined);
        Ok(resolved.to_string_lossy().to_string())
    };

    // The actor recorded in the registry journal is the FACE the operator
    // used, so journal forensics read the command that ran, not the module
    // that served it.
    let actor = binary_name(command.face);

    Ok(match (command.face, command.verb.as_str()) {
        (Face::Projects, "list") => (
            "enumerate".to_string(),
            match command.args.first() {
                Some(workspace) => json!({ "workspaceId": workspace }),
                None => json!({}),
            },
        ),
        (Face::Projects, "resolve") => (
            "resolve".to_string(),
            json!({ "canonicalRoot": absolute(&need(0, "a directory")?)? }),
        ),
        (Face::Projects, "register") => (
            "register".to_string(),
            json!({
                "name": need(0, "a name")?,
                "roots": [absolute(&need(1, "a directory")?)?],
                "actor": actor,
            }),
        ),
        (Face::Projects, "remove") => (
            "remove".to_string(),
            json!({ "projectId": need(0, "a project id")?, "actor": actor }),
        ),
        (Face::Projects, "verify") => ("verify".to_string(), json!({})),
        // `list` on the workspaces face is the same enumerate op; the renderer
        // shows the workspace column of the reply instead of the projects.
        (Face::Workspaces, "list") => ("enumerate".to_string(), json!({})),
        (Face::Workspaces, "assign") => (
            "assign_workspace".to_string(),
            json!({
                "projectId": need(0, "a project id")?,
                "workspaceId": need(1, "a workspace id")?,
                "actor": actor,
            }),
        ),
        // The old spelling teaches its replacement instead of guessing at it:
        // `workspace` as a projects-verb was the pre-rename surface.
        (Face::Projects, "workspace") => {
            return Err(format!(
                "'workspace' moved: use 'ck workspaces assign <project> <workspace>'\n\n{}",
                usage(Face::Workspaces)
            ))
        }
        (_, other) => return Err(format!("unknown verb '{other}'\n\n{}", usage(command.face))),
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
                // `project_id` is deliberately left unset: this is the operator
                // CLI, which has no registered project to resolve and must not
                // invent one. Absent means "key on the triple", which is the
                // honest answer for a caller that binds from whatever directory
                // the operator happened to be standing in.
                subc_protocol::BindIdentity::new(
                    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/")),
                    "ck-entorhinal".to_string(),
                    "operator".to_string(),
                ),
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
        let envelope: Value = serde_json::from_slice(&bytes)
            .map_err(|error| (EXIT_TRANSPORT, format!("decode response: {error}")))?;
        unwrap_result(envelope)
    })
}

/// Strip the `{"result": ...}` wrapper the registry puts on every reply.
///
/// FAILS LOUD when the wrapper is absent rather than passing the envelope
/// through. Returning it unwrapped would make every field lookup miss by one
/// level, and a miss renders as a legitimate empty answer -- `list` printed
/// "no projects registered" against a registry holding a project, which is a
/// confident wrong answer rather than a visible error. A contract violation
/// should look like one.
fn unwrap_result(envelope: Value) -> Result<Value, (u8, String)> {
    match envelope {
        Value::Object(mut map) => map.remove("result").ok_or_else(|| {
            let keys: Vec<&str> = map.keys().map(String::as_str).collect();
            (
                EXIT_TRANSPORT,
                format!(
                    "registry reply has no 'result' envelope (keys: {})",
                    if keys.is_empty() {
                        "none".to_string()
                    } else {
                        keys.join(", ")
                    }
                ),
            )
        }),
        other => Err((
            EXIT_TRANSPORT,
            format!("registry reply is not an object: {other}"),
        )),
    }
}

fn render(command: &Command, value: &Value) {
    if command.json {
        println!(
            "{}",
            serde_json::to_string_pretty(value).unwrap_or_default()
        );
        return;
    }
    match (command.face, command.verb.as_str()) {
        (Face::Workspaces, "list") => {
            let workspaces = value["workspaces"].as_array().cloned().unwrap_or_default();
            if workspaces.is_empty() {
                println!("no workspaces");
                return;
            }
            for workspace in &workspaces {
                println!(
                    "{:<28} {}",
                    text(&workspace["workspaceId"]),
                    text(&workspace["name"])
                );
            }
            println!("\n{} workspace(s)", workspaces.len());
        }
        (Face::Projects, "list") => {
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
        (Face::Projects, "resolve") => {
            // Field names are the REGISTRY's, not this renderer's guesses.
            // `projectName` was read as `name` here and rendered as "-" against
            // a project that has one -- absence and a wrong key look identical
            // on screen, which is why the verb-to-field mapping is pinned by a
            // test against the module's own schema rather than by memory.
            println!("project:   {}", text(&value["projectId"]));
            println!("name:      {}", text(&value["projectName"]));
            println!("workspace: {}", text(&value["workspaceId"]));
            // AN IMPLICIT PROJECT IS NOT A REGISTERED ONE. The registry always
            // answers `resolve` -- an unregistered directory gets a derived id
            // with `via: "implicit"` and nothing stored behind it. Rendered
            // without this line, that is indistinguishable from a registered
            // project whose name happens to be unset, so an operator checking
            // whether a directory is registered would read "yes" from a reply
            // that means "no".
            if value["via"].as_str() == Some("implicit") {
                println!("status:    NOT REGISTERED (derived id; nothing stored)");
            }
            // `gone` marks a project whose root no longer exists. Rendering it
            // only when true keeps the common case quiet, and silence here
            // means the root was present rather than that nothing was checked.
            if value["gone"].as_bool() == Some(true) {
                println!("root:      GONE (registered directory no longer exists)");
            }
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
        parse(
            Face::Entorhinal,
            &args.iter().map(|a| a.to_string()).collect::<Vec<_>>(),
        )
    }

    fn parse_as(face: Face, args: &[&str]) -> Invocation {
        parse(
            face,
            &args.iter().map(|a| a.to_string()).collect::<Vec<_>>(),
        )
    }

    /// argv[0]'s file stem selects the face; anything unrecognised (including
    /// a renamed or absent argv[0]) serves as the module, which is the shape a
    /// supervisor spawn must always reach.
    #[test]
    fn face_resolution_follows_argv0() {
        assert_eq!(
            face_from_argv0(Some("/usr/local/bin/ck-projects")),
            Face::Projects
        );
        assert_eq!(face_from_argv0(Some("ck-workspaces")), Face::Workspaces);
        assert_eq!(
            face_from_argv0(Some("/opt/cortexkit/bin/ck-entorhinal")),
            Face::Entorhinal
        );
        assert_eq!(face_from_argv0(Some("weird-wrapper")), Face::Entorhinal);
        assert_eq!(face_from_argv0(None), Face::Entorhinal);
    }

    /// Bare CLI faces explain; only the module face serves from empty argv.
    /// This is the `ck` convention (verbless domains print their help), and it
    /// is also what stops `ck projects` from booting a daemon.
    #[test]
    fn bare_cli_faces_explain_rather_than_serve() {
        for face in [Face::Projects, Face::Workspaces] {
            match parse_as(face, &[]) {
                Invocation::Report(text) => {
                    assert!(text.contains("usage:"), "{face:?} bare must print usage")
                }
                _ => panic!("{face:?} with empty argv must report, never serve"),
            }
        }
        assert!(matches!(
            parse_as(Face::Entorhinal, &[]),
            Invocation::Module
        ));
    }

    /// The supervisor's spawn shape (`--subc <path>`, no verb) serves ONLY
    /// under the module's own name. On a CLI face the same shape is a verbless
    /// operator question and must not claim the module identity.
    #[test]
    fn spawn_shape_serves_only_on_the_module_face() {
        assert!(matches!(
            parse_as(Face::Entorhinal, &["--subc", "/tmp/conn.json"]),
            Invocation::Module
        ));
        for face in [Face::Projects, Face::Workspaces] {
            assert!(
                !matches!(
                    parse_as(face, &["--subc", "/tmp/conn.json"]),
                    Invocation::Module
                ),
                "{face:?} must never serve"
            );
        }
    }

    /// The result envelope is stripped, and its ABSENCE fails loud.
    ///
    /// Found by driving a real registry: every renderer read one level too
    /// high, so `list` printed "no projects registered" against a store holding
    /// a project. That is the dangerous shape -- a missed lookup renders as a
    /// legitimate empty answer rather than as an error, so nothing about the
    /// output says the reply was misread.
    #[test]
    fn result_envelope_is_stripped_and_its_absence_is_loud() {
        let wrapped = json!({ "result": { "projects": [ { "projectId": "pj-1" } ] } });
        let inner = unwrap_result(wrapped).expect("wrapped reply must unwrap");
        assert_eq!(inner["projects"][0]["projectId"], "pj-1");

        // Absence must not degrade into passing the envelope through: that is
        // exactly how the original defect rendered as an empty registry.
        let bare = json!({ "projects": [] });
        let error = unwrap_result(bare).expect_err("a reply without the envelope must refuse");
        assert_eq!(error.0, EXIT_TRANSPORT);
        assert!(
            error.1.contains("no 'result' envelope"),
            "refusal must name the missing envelope, got: {}",
            error.1
        );
        // It also names what WAS there, so an operator can tell a contract
        // change from a transport fault.
        assert!(
            error.1.contains("projects"),
            "refusal must name the keys it saw, got: {}",
            error.1
        );
    }

    /// NO OPERATOR INVOCATION REACHES MODULE MODE.
    ///
    /// This is the property that keeps a mistyped command from claiming the
    /// module's identity against the daemon and sitting there looking healthy.
    /// It was originally written as "only empty argv serves", which was a
    /// stronger claim than the property needed AND was wrong -- the supervisor
    /// spawns with `--subc <path>`, so that rule sent every real spawn into the
    /// usage branch. The property that matters is about VERBS AND FLAGS, not
    /// about argv being empty.
    #[test]
    fn no_operator_invocation_reaches_module_mode() {
        for argv in [
            vec!["--help"],
            vec!["-h"],
            vec!["--version"],
            vec!["list"],
            vec!["--nonsense"],
            vec!["typo-verb"],
            // With a connection file too: a verb still never serves, which is
            // what stops `ck entorhinal list --subc <path>` from starting a
            // second module instance.
            vec!["list", "--subc", "/tmp/conn.json"],
            vec!["typo-verb", "--subc", "/tmp/conn.json"],
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
            match parse_as(Face::Projects, &argv) {
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

    /// The supervisor's spawn shape must reach the serving path.
    ///
    /// subc spawns module children as `--subc <connection-file>` with no verb.
    /// An earlier version treated ONLY empty argv as Module, so every supervised
    /// spawn fell into the usage branch, printed help and exited 0 -- and the
    /// daemon reported "exited cleanly", which is true and useless. Asserts the
    /// real shape rather than the one that was easy to imagine.
    #[test]
    fn supervisor_spawn_shape_reaches_the_module_path() {
        assert!(matches!(
            parse_of(&["--subc", "/tmp/conn.json"]),
            Invocation::Module
        ));
        // Empty argv stays module mode too, so a hand-run child behaves the
        // same as a supervised one. Flags with no verb are a question instead.
        assert!(matches!(parse_of(&[]), Invocation::Module));
        assert!(matches!(parse_of(&["--json"]), Invocation::Report(_)));
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
            face: Face::Projects,
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

        // Workspace placement lives on the workspaces face.
        let ws = |args: &[&str]| Command {
            face: Face::Workspaces,
            verb: args[0].to_string(),
            connection: None,
            args: args[1..].iter().map(|a| a.to_string()).collect(),
            json: false,
        };
        let (method, params) = build(&ws(&["assign", "p1", "w2"])).unwrap();
        assert_eq!(method, "assign_workspace");
        assert_eq!(params["projectId"], "p1");
        assert_eq!(params["workspaceId"], "w2");
        // The actor records the face the operator used, not the module name.
        assert_eq!(params["actor"], "ck-workspaces");

        let (method, _) = build(&ws(&["list"])).unwrap();
        assert_eq!(method, "enumerate");

        // The retired projects-face spelling teaches its replacement.
        let error = build(&cmd(&["workspace", "p1", "w2"])).expect_err("moved verb must refuse");
        assert!(
            error.contains("ck workspaces assign"),
            "the refusal must name the new spelling, got: {error}"
        );

        let (method, params) = build(&cmd(&["remove", "p1"])).unwrap();
        assert_eq!(method, "remove");
        assert_eq!(params["projectId"], "p1");

        assert_eq!(build(&cmd(&["verify"])).unwrap().0, "verify");
    }

    /// The RENDERER's field names come from the REGISTRY'S OWN REPLY TYPE.
    ///
    /// The request half was pinned from the start; the response half was not,
    /// and a renderer reading `name` where the reply carries `projectName`
    /// printed "-" for a project that has one. THAT IS THE DANGEROUS DIRECTION:
    /// a wrong key and a genuinely absent value render identically, so the
    /// screen cannot distinguish "the registry had nothing to say" from "I
    /// asked the wrong question".
    ///
    /// The fixture is SERIALIZED FROM `entorhinal_core::ResolveReply` rather
    /// than hand-written, so it carries whatever that type actually emits. A
    /// hand-written one would encode my belief about the shape -- which is the
    /// belief that was wrong in the first place -- and would keep passing after
    /// a field rename on the producer side.
    #[test]
    fn resolve_renders_the_reply_types_own_field_names() {
        let reply = serde_json::to_value(entorhinal_core::ResolveReply {
            project_id: "pj-1".to_string(),
            workspace_id: Some("ws-1".to_string()),
            project_name: Some("the-name".to_string()),
            via: "root".to_string(),
            gone: false,
            generation: 2,
            canonical_root: Some("/tmp/x".to_string()),
        })
        .unwrap();

        // Every key the renderer reads must exist on the real reply.
        for key in ["projectId", "projectName", "workspaceId", "gone"] {
            assert!(
                !reply[key].is_null(),
                "renderer reads '{key}', which ResolveReply does not emit"
            );
        }
        // And the values must be the ones a reader would see, not merely present.
        assert_eq!(reply["projectName"], "the-name");
        assert_eq!(reply["projectId"], "pj-1");

        // The registry ALWAYS answers resolve: an unregistered directory comes
        // back with a derived id and `via: "implicit"`. The renderer keys on
        // that field, so pin it from the same producer type -- rendered without
        // it, "not registered" is indistinguishable from a registered project
        // with no name, and an operator asking "is this directory registered?"
        // reads yes from a reply that means no.
        let implicit = serde_json::to_value(entorhinal_core::ResolveReply {
            project_id: "pj-implicit1-deadbeef".to_string(),
            workspace_id: None,
            project_name: None,
            via: "implicit".to_string(),
            gone: false,
            generation: 2,
            canonical_root: Some("/tmp/x".to_string()),
        })
        .unwrap();
        assert_eq!(implicit["via"], "implicit");
        assert!(
            implicit["projectName"].is_null(),
            "an implicit reply must carry no name; otherwise the renderer's \
             registered-vs-implicit distinction is not the one being tested"
        );
    }

    /// Directories are CANONICAL, not merely absolute, before they are sent.
    ///
    /// Two separate failures are covered, and the second is why the earlier
    /// version of this test was not enough:
    ///
    /// 1. A relative path must resolve against the OPERATOR's shell. The
    ///    registry resolves what it is handed in the DAEMON's working
    ///    directory, so sending it relative would register a different
    ///    directory than the operator named, silently and plausibly.
    /// 2. The registry REFUSES a non-canonical root outright. On macOS the
    ///    temp dir reaches through a symlink, so a perfectly ordinary absolute
    ///    path an operator can `cd` into is still rejected -- absolute was
    ///    necessary and not sufficient, and asserting only absoluteness could
    ///    not see it.
    #[test]
    fn directories_are_canonical_before_they_are_sent() {
        let sent_for = |value: &str| -> String {
            let command = Command {
                face: Face::Projects,
                verb: "resolve".to_string(),
                connection: None,
                args: vec![value.to_string()],
                json: false,
            };
            build(&command).unwrap().1["canonicalRoot"]
                .as_str()
                .unwrap()
                .to_string()
        };

        // (1) relative resolves against the operator's cwd, canonically.
        let dot = sent_for(".");
        assert!(Path::new(&dot).is_absolute(), "sent a relative path: {dot}");
        assert_eq!(
            Path::new(&dot),
            std::fs::canonicalize(std::env::current_dir().unwrap()).unwrap(),
            "resolved against something other than the operator's cwd"
        );

        // (2) an absolute path through a symlinked ancestor is canonicalized.
        let dir = std::env::temp_dir().join(format!("ent-canon-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let given = dir.to_string_lossy().to_string();
        let canonical = std::fs::canonicalize(&dir).unwrap();
        assert_eq!(
            Path::new(&sent_for(&given)),
            canonical,
            "sent path must be canonical, not just absolute"
        );
        // Non-vacuity: leg (2) only discriminates where the temp dir actually
        // goes through a symlink. Where it does not, say so rather than
        // passing as though the stronger property had been tested.
        if Path::new(&given) == canonical {
            eprintln!("note: temp dir is already canonical on this host; leg (2) ran weak");
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A verb missing its arguments is a usage error, not a call with a hole in
    /// it. Naming the missing thing is what stops an operator retrying blind.
    #[test]
    fn missing_arguments_name_what_is_missing() {
        let cases = [
            (Face::Projects, vec!["resolve"], "a directory"),
            (
                Face::Projects,
                vec!["register", "only-a-name"],
                "a directory",
            ),
            (Face::Workspaces, vec!["assign", "p1"], "a workspace id"),
            (Face::Projects, vec!["remove"], "a project id"),
        ];
        for (face, argv, expected) in cases {
            let cmd = Command {
                face,
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
