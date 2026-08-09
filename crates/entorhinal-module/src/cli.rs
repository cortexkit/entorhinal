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
        // No verb WITH a connection file is the supervisor's spawn shape: serve.
        None if connection.is_some() => Invocation::Module,
        // No verb and nothing to connect to is a question, not a mistake.
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
        parse(&args.iter().map(|a| a.to_string()).collect::<Vec<_>>())
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
