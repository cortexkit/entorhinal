//! Operator surface for the registry: `ck projects <verb>` and
//! `ck workspaces <verb>` and `ck agents <verb>`.
//!
//! One binary, four names. `ck` dispatches unknown domains to `ck-<domain>`
//! on PATH, and `ck-projects` / `ck-workspaces` / `ck-agents` are symlinks to this module
//! binary; argv[0] selects the face. The module id (entorhinal) is
//! infrastructure and never part of the operator vocabulary -- a user thinks
//! in projects, workspaces and agents, not in the anatomy of the module serving them.
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
use std::time::Duration;

use serde_json::{json, Value};

#[path = "retry_quote.rs"]
mod retry_quote;

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(not(any(windows, test)), allow(dead_code))]
enum PathClass {
    DriveRelative,
    RootedNoDrive,
    Ordinary,
}

#[cfg_attr(not(any(windows, test)), allow(dead_code))]
fn path_class(value: &str) -> PathClass {
    let bytes = value.as_bytes();
    let separator = |byte: &u8| matches!(byte, b'/' | b'\\');
    if bytes.len() >= 2
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && bytes.get(2).is_none_or(|byte| !separator(byte))
    {
        PathClass::DriveRelative
    } else if bytes.first().is_some_and(separator)
        && bytes.get(1).is_none_or(|byte| !separator(byte))
    {
        PathClass::RootedNoDrive
    } else {
        PathClass::Ordinary
    }
}

fn cli_path(value: &str, cwd: &str, mutation: bool) -> Result<String, String> {
    #[cfg(windows)]
    match path_class(value) {
        PathClass::DriveRelative => {
            return Err(format!("drive-relative path is ambiguous: {value}"))
        }
        PathClass::RootedNoDrive => {
            return Err(format!("rooted-no-drive path is ambiguous: {value}"))
        }
        PathClass::Ordinary => {}
    }
    let path = Path::new(value);
    let joined = if path.is_absolute() {
        path.to_path_buf()
    } else {
        Path::new(cwd).join(path)
    };
    let result = if mutation {
        let raw = joined
            .to_str()
            .ok_or_else(|| format!("path_refused: path_not_unicode: {value}"))?;
        entorhinal_core::RegistryStore::canonical_mutation_root(raw)
    } else {
        entorhinal_core::RegistryStore::canonical_query_path(&joined).map(|(path, _)| path)
    };
    result.map_err(|error| format!("path_refused: {value}: {error}"))
}

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
    /// `ck-agents`: reads agents, and creates, renames, retires, retags and
    /// relabels them; each write lands only after the operator approves it at
    /// a Touch ID prompt.
    Agents,
    /// `ck-entorhinal`: the supervised module itself. Serves; its only
    /// operator surface is `--version`/`--help`, which point at the faces.
    Entorhinal,
}

/// Select a face by the final component, accepting production and development
/// names and one optional executable suffix. Unknown names keep module mode.
pub fn face_from_argv0(argv0: Option<&str>) -> Face {
    let name = argv0.unwrap_or("").rsplit(['/', '\\']).next().unwrap_or("");
    let lower = name.to_ascii_lowercase();
    let stem = lower.strip_suffix(".exe").unwrap_or(&lower);
    match stem {
        "ck-projects" | "ckdev-projects" => Face::Projects,
        "ck-workspaces" | "ckdev-workspaces" => Face::Workspaces,
        "ck-agents" | "ckdev-agents" => Face::Agents,
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
    /// Print the module manifest as JSON and exit, for offline fleet
    /// inspection (`ck fleet lint`). Module face only; it connects to nothing.
    Manifest,
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
                "ck-entorhinal is a module, not a ck domain; the domains are ck projects, ck workspaces and ck agents"
                    .to_string(),
            ),
            _ => Invocation::Report(usage(face).lines().next().unwrap_or_default().to_string()),
        };
    }
    // `ck fleet lint` reads every module's manifest offline through
    // `--manifest`, which is how it checks that a capability another module
    // requires has a provider. Without it, lint cannot see what entorhinal
    // provides. Module face only: the operator faces are not modules.
    if arguments.len() == 1 && arguments[0] == "--manifest" && face == Face::Entorhinal {
        return Invocation::Manifest;
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
            "--role" | "--tag" | "--project" | "--workspace" | "--supervisor" | "--request-key"
                if face == Face::Agents =>
            {
                let Some(value) = rest.next().filter(|value| !value.is_empty()) else {
                    return Invocation::Refuse(format!("{argument} needs a value"));
                };
                args.push(argument.clone());
                args.push(value.clone());
            }
            "--none" if face == Face::Agents => args.push(argument.clone()),
            "--project" | "--label"
                if face == Face::Projects && verb.as_deref() == Some("attach") =>
            {
                let Some(value) = rest
                    .next()
                    .filter(|value| !value.starts_with('-') && !value.is_empty())
                else {
                    return Invocation::Refuse(format!("{argument} needs a value"));
                };
                args.push(argument.clone());
                args.push(value.clone());
            }
            "--yes" if face == Face::Projects && verb.as_deref() == Some("attach") => {
                args.push(argument.clone());
            }
            "--without-agents"
                if face == Face::Projects
                    && verb.as_deref() == Some("log")
                    && args.first().map(String::as_str) == Some("enable") =>
            {
                args.push(argument.clone());
            }
            "--default" | "--none"
                if face == Face::Projects && verb.as_deref() == Some("owned-remotes") =>
            {
                args.push(argument.clone());
            }
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
        Face::Agents => "ck-agents",
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
  add-root <project> <dir>
                          add another root to a project (starts unapproved)
  attach <path> [--project ID] [--label L] [--yes]
                          preview the matched project/key; --yes attaches unapproved
  remove-root <project> <dir>
                          remove one root from a project
  owned-remotes <root> <name>...
                          own exactly these remote names
  owned-remotes <root> --none
                          own no remote
  owned-remotes <root> --default
                          reset ownership to origin
  approve <dir>           approve every root of the project owning <dir>
  unapprove <dir>         withdraw that approval
  trust <dir>             show each root's identity and approval
  verify                  check the store against its journal
  log status              show local identity log state
  log enable [--without-agents]
                          enable or join the shared log (operator only);
                          skip agent import only when the fleet has no agents

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
  set-root <workspace> <directory>
                          record the workspace's root directory
  clear-root <workspace>  remove the recorded root

  --subc <path>   daemon connection file; defaults to the usual discovery path
  --json          raw response instead of the rendered form

projects themselves live under 'ck projects'"
            .to_string(),
        Face::Agents => "\
ck agents — operator-confirmed agent identities

usage: ck agents <verb> [args] [--subc <connection file>] [--json]

  create <name> --role <assistant|workspace-head|head|hiree> --tag <text>
                 [--project <id>] [--workspace <id>] [--supervisor <agent>]
  rename <agent> <new-name>
  retire <agent>
  tag <agent> <text>
  labels <agent> <label>...   replace the whole label set
  labels <agent> --none      clear the label set
  list [--project <id>] [--workspace <id>]
  show <agent>               a live digest or a retired/merged identity

  <agent> is an id or a live name, optionally workspace/name
  --request-key <key>        reuse a write's key for a safe retry
  --subc <path>              daemon connection file; defaults to discovery
  --json                     raw response instead of the rendered form"
            .to_string(),
        Face::Entorhinal => "\
ck-entorhinal — the registry module (supervised by the subc daemon)

This binary is the module itself; it is not an operator command. The operator
surface is:

  ck projects      list, register, resolve, remove, add-root, remove-root,
                   attach, owned-remotes, approve, unapprove, trust, verify, log
  ck workspaces    list, assign, set-root, clear-root
  ck agents        create, rename, retire, tag, labels, list, show

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
    // SUBC_LAUNCH_NONCE_FD names the file descriptor on which the daemon hands a
    // supervised module its launch nonce. A shell started under a module
    // inherits the variable but not the descriptor behind it. Removing
    // SUBC_MODULE_ID above already stops this client presenting an identity;
    // removing this too means nothing in the process tries to read that
    // missing descriptor and fails.
    std::env::remove_var("SUBC_LAUNCH_NONCE_FD");

    run_with_directory(command, std::env::current_dir, &mut std::io::stderr())
}

fn run_with_directory(
    command: Command,
    read: super::current_directory::CwdReader,
    stderr: &mut impl std::io::Write,
) -> ExitCode {
    let cwd = match super::current_directory::read_current_directory(read) {
        Ok(cwd) => cwd,
        Err(error) => {
            let _ = writeln!(stderr, "{}: {}", error.code, error.message);
            return ExitCode::from(EXIT_REFUSED);
        }
    };
    let (method, params) = match build_at(&command, &cwd) {
        Ok(request) => request,
        Err(message) => {
            let _ = writeln!(stderr, "{message}");
            return ExitCode::from(build_failure_exit(&message));
        }
    };

    match call(
        command.connection.as_deref(),
        &command,
        &method,
        params,
        &cwd,
    ) {
        Ok(value) => {
            render(&command, &value);
            ExitCode::SUCCESS
        }
        Err(failure) => {
            let _ = writeln!(stderr, "{}", failure.1);
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
#[cfg(test)]
fn build(command: &Command) -> Result<(String, Value), String> {
    let cwd = super::current_directory::read_current_directory(std::env::current_dir)
        .map_err(|error| format!("{}: {}", error.code, error.message))?;
    build_at(command, &cwd)
}

fn build_failure_exit(message: &str) -> u8 {
    if message.starts_with("path_refused:") {
        EXIT_REFUSED
    } else {
        EXIT_USAGE
    }
}

fn build_at(command: &Command, cwd: &str) -> Result<(String, Value), String> {
    if command.face == Face::Agents {
        return build_agents(command);
    }
    let need = |index: usize, what: &str| -> Result<String, String> {
        command
            .args
            .get(index)
            .cloned()
            .ok_or_else(|| format!("{} needs {what}\n\n{}", command.verb, usage(command.face)))
    };
    // Use the core's path policy so the client sends the exact registry identity.
    let mutation = |value: &str| cli_path(value, cwd, true);
    let query = |value: &str| cli_path(value, cwd, false);

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
            json!({ "canonicalRoot": query(&need(0, "a directory")?)? }),
        ),
        (Face::Projects, "register") => (
            "register".to_string(),
            json!({
                "name": need(0, "a name")?,
                "roots": [mutation(&need(1, "a directory")?)?],
                "actor": actor,
            }),
        ),
        (Face::Projects, "attach") => {
            let mut path = None;
            let mut project = None;
            let mut label = None;
            let mut yes = false;
            let mut args = command.args.iter();
            while let Some(arg) = args.next() {
                match arg.as_str() {
                    "--project" | "--label" => {
                        let target = if arg == "--project" {
                            &mut project
                        } else {
                            &mut label
                        };
                        if target.is_some() {
                            return Err(format!("{arg} may only be given once"));
                        }
                        let value = args
                            .next()
                            .filter(|value| !value.starts_with('-') && !value.is_empty())
                            .ok_or_else(|| format!("{arg} needs a value"))?;
                        *target = Some(value.clone());
                    }
                    "--yes" if !yes => yes = true,
                    "--yes" => return Err("--yes may only be given once".into()),
                    other if other.starts_with('-') => {
                        return Err(format!("unknown attach flag '{other}'"))
                    }
                    _ if path.is_none() => path = Some(mutation(arg)?),
                    _ => return Err("attach needs exactly one checkout path".into()),
                }
            }
            let path = path.ok_or_else(|| "attach needs a checkout path".to_string())?;
            let mut params = json!({"path": path, "actor": actor});
            if let Some(project) = project {
                params["projectId"] = json!(project);
            }
            if let Some(label) = label {
                params["label"] = json!(label);
            }
            (
                if yes {
                    "attach_root"
                } else {
                    "preview_attach_root"
                }
                .into(),
                params,
            )
        }
        (Face::Projects, "log") => {
            match command
                .args
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .as_slice()
            {
                ["status"] => ("identity_log.status".into(), json!({})),
                ["enable"] => ("identity_log.enable".into(), json!({})),
                ["enable", "--without-agents"] => {
                    ("identity_log.enable".into(), json!({"without_agents":true}))
                }
                _ => return Err("log needs exactly one of: status, enable".into()),
            }
        }
        (Face::Projects, "remove") => (
            "remove".to_string(),
            json!({ "projectId": need(0, "a project id")?, "actor": actor }),
        ),
        (Face::Projects, "verify") => ("verify".to_string(), json!({})),
        (Face::Projects, "owned-remotes") => {
            if command
                .args
                .first()
                .is_some_and(|arg| arg == "--default" || arg == "--none")
            {
                return Err("owned-remotes needs a root directory before its flag".into());
            }
            let root = query(&need(0, "a root directory")?)?;
            let names = &command.args[1..];
            let flag = names.iter().find(|name| name.starts_with("--"));
            // Owning nothing makes Plexus stop watching the root's repo, so it
            // must be asked for by name: a bare `owned-remotes <root>`, easy to
            // type while checking what a root owns, is refused, not read as an
            // empty set.
            let remotes = match flag.map(String::as_str) {
                Some(flag) if names.len() != 1 => {
                    return Err(format!("{flag} cannot be combined with remote names"))
                }
                Some("--default") => Value::Null,
                Some(_) => json!([]),
                None if names.is_empty() => {
                    return Err(
                        "name the remotes this root owns, or pass --default (origin) or --none"
                            .into(),
                    )
                }
                None => json!(names),
            };
            (
                "set_owned_remotes".to_string(),
                json!({"root":root, "remotes":remotes, "actor":actor}),
            )
        }
        (Face::Projects, "add-root") => (
            "add_root".to_string(),
            json!({
                "projectId": need(0, "a project id")?,
                "root": mutation(&need(1, "a directory")?)?,
                "actor": actor,
            }),
        ),
        (Face::Projects, "remove-root") => (
            "remove_root".to_string(),
            json!({
                "projectId": need(0, "a project id")?,
                "root": query(&need(1, "a directory")?)?,
                "actor": actor,
            }),
        ),
        (Face::Projects, "approve") => (
            "approve_project".to_string(),
            json!({ "canonicalRoot": query(&need(0, "a directory")?)?, "actor": actor }),
        ),
        (Face::Projects, "unapprove") => (
            "unapprove_project".to_string(),
            json!({ "canonicalRoot": query(&need(0, "a directory")?)?, "actor": actor }),
        ),
        (Face::Projects, "trust") => (
            "trust".to_string(),
            json!({ "canonicalRoot": query(&need(0, "a directory")?)? }),
        ),
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
        (Face::Workspaces, "set-root") => (
            "set_workspace_root".to_string(),
            json!({
                "workspaceId": need(0, "a workspace id")?,
                "root": mutation(&need(1, "a directory")?)?,
                "actor": actor,
            }),
        ),
        (Face::Workspaces, "clear-root") => (
            "set_workspace_root".to_string(),
            json!({
                "workspaceId": need(0, "a workspace id")?,
                "root": null,
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

fn agent_write(method: &str) -> bool {
    matches!(
        method,
        "agent.create" | "agent.rename" | "agent.dispose" | "agent.update_tag" | "agent.set_labels"
    )
}

fn build_agents(command: &Command) -> Result<(String, Value), String> {
    let method = match command.verb.as_str() {
        "create" => "agent.create",
        "rename" => "agent.rename",
        "retire" => "agent.dispose",
        "tag" => "agent.update_tag",
        "labels" => "agent.set_labels",
        "list" => "agent.list",
        "show" => "agent.resolve",
        other => return Err(format!("unknown verb '{other}'\n\n{}", usage(Face::Agents))),
    };
    let mut flags = std::collections::BTreeMap::new();
    let mut positional = Vec::new();
    let mut rest = command.args.iter();
    while let Some(arg) = rest.next() {
        if !arg.starts_with("--") {
            positional.push(arg.clone());
            continue;
        }
        let allowed = match arg.as_str() {
            "--request-key" => agent_write(method),
            "--role" | "--tag" | "--supervisor" => command.verb == "create",
            "--project" | "--workspace" => matches!(command.verb.as_str(), "create" | "list"),
            "--none" => command.verb == "labels",
            _ => false,
        };
        if !allowed {
            return Err(format!("{} does not accept {arg}", command.verb));
        }
        let value = if arg == "--none" {
            String::new()
        } else {
            rest.next()
                .filter(|value| !value.is_empty())
                .cloned()
                .ok_or_else(|| format!("{arg} needs a value"))?
        };
        if flags.insert(arg.as_str(), value).is_some() {
            return Err(format!("{arg} may only be given once"));
        }
    }
    let count_ok = match command.verb.as_str() {
        "list" => positional.is_empty(),
        "rename" | "tag" => positional.len() == 2,
        "labels" if flags.contains_key("--none") => positional.len() == 1,
        "labels" => positional.len() >= 2,
        _ => positional.len() == 1,
    };
    if !count_ok {
        return Err(format!(
            "{} needs the arguments shown below (labels --none cannot include labels)\n\n{}",
            command.verb,
            usage(Face::Agents)
        ));
    }
    let mut params = json!({});
    for (flag, field) in [
        ("--project", "project_id"),
        ("--workspace", "workspace_id"),
        ("--supervisor", "supervisor_agent_id"),
    ] {
        if let Some(value) = flags.get(flag) {
            params[field] = json!(value);
        }
    }
    match command.verb.as_str() {
        "create" => {
            let role = flags.get("--role").ok_or("create requires --role")?;
            if !matches!(
                role.as_str(),
                "assistant" | "workspace-head" | "head" | "hiree"
            ) {
                return Err("--role must be assistant, workspace-head, head or hiree".into());
            }
            let tag = flags.get("--tag").ok_or("create requires --tag")?;
            params["name"] = json!(positional[0]);
            params["role"] = json!(role.replace('-', "_"));
            params["tag"] = json!(tag);
        }
        "list" => (),
        verb => {
            params["agent_id"] = json!(positional[0]);
            match verb {
                "rename" => params["name"] = json!(positional[1]),
                "tag" => params["tag"] = json!(positional[1]),
                "labels" => params["labels"] = json!(&positional[1..]),
                _ => (),
            }
        }
    }
    if agent_write(method) {
        let key = match flags.get("--request-key") {
            Some(key) => key.clone(),
            None => {
                let mut bytes = [0u8; 16];
                getrandom::getrandom(&mut bytes)
                    .map_err(|error| format!("draw request key: {error}"))?;
                bytes.iter().map(|byte| format!("{byte:02x}")).collect()
            }
        };
        params["request_key"] = json!(key);
        params["actor"] = json!("ck-agents");
    }
    Ok((method.into(), params))
}

fn is_agent_id(token: &str) -> bool {
    token.strip_prefix("agent_").is_some_and(|hex| {
        matches!(hex.len(), 8 | 16)
            && hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

#[derive(Debug)]
enum CallFailure {
    Refused { code: String, message: String },
    Transport(String),
    Unknown(String),
}

impl CallFailure {
    fn display(self) -> (u8, String) {
        match self {
            Self::Refused { code, message } => {
                let explanation = match code.as_str() {
                    "operator_declined" => "the operator declined this write at the prompt",
                    "operator_presence_unavailable" => "operator confirmation is unavailable; use a daemon and machine that support it",
                    "operator_confirmation_busy" => "another operator confirmation is outstanding; wait for it to finish",
                    "operator_approval_stale" => "the identity changed after approval; review it and try again",
                    "operator_summary_too_long" => "shorten the tag, name or label set",
                    "authority_not_cut_over" => "agent identity authority has not been cut over to this registry",
                    _ => "",
                };
                let message = if message.is_empty() {
                    explanation
                } else {
                    &message
                };
                (EXIT_REFUSED, format!("{code}: {message}"))
            }
            Self::Transport(message) | Self::Unknown(message) => (EXIT_TRANSPORT, message),
        }
    }
}

fn call_failure(error: subc_client_rs::CallError) -> CallFailure {
    if error.outcome_cause() == Some(subc_client_rs::OutcomeUnknownCause::Deadline) {
        return CallFailure::Unknown("transport timeout: the write outcome is unknown".into());
    }
    if let Some(reason) = error.close_reason() {
        return CallFailure::Transport(format!(
            "daemon reported route closure: {reason:?}; {error}"
        ));
    }
    match error {
        subc_client_rs::CallError::Module(body) => CallFailure::Refused {
            code: body.code,
            message: body.message,
        },
        subc_client_rs::CallError::OutcomeUnknown(_) => {
            CallFailure::Unknown(format!("call: {error}"))
        }
        _ => CallFailure::Transport(format!("call: {error}")),
    }
}

/// This seam takes the actual SDK options, so fixtures observe the timeout
/// passed to transport rather than a second copy of the timeout policy.
#[async_trait::async_trait]
trait CliCaller: Sync {
    async fn call(
        &self,
        method: &str,
        params: Value,
        options: subc_client_rs::CallOptions,
    ) -> Result<Value, CallFailure>;
}

struct ConsumerCaller<'a> {
    consumer: &'a subc_client_rs::SubcConsumer,
    cwd: String,
}

fn connect_failure(connection: Option<&Path>, error: impl std::fmt::Display) -> String {
    match connection {
        Some(path) => format!("connect {}: {error}", path.display()),
        None => format!("connect: {error}"),
    }
}

#[async_trait::async_trait]
impl CliCaller for ConsumerCaller<'_> {
    async fn call(
        &self,
        method: &str,
        params: Value,
        options: subc_client_rs::CallOptions,
    ) -> Result<Value, CallFailure> {
        let body = serde_json::to_vec(&json!({"method":method,"params":params}))
            .map_err(|error| CallFailure::Transport(format!("serialize request: {error}")))?;
        let bytes = self
            .consumer
            .call(
                subc_protocol::RouteTarget::ManagementSurface {
                    module_id: super::MODULE_ID.into(),
                },
                // No project is invented for an operator calling from an arbitrary directory.
                subc_protocol::BindIdentity::new(&self.cwd, "ck-entorhinal", "operator"),
                body,
                options,
            )
            .await
            .map_err(call_failure)?;
        let envelope = serde_json::from_slice(&bytes)
            .map_err(|error| CallFailure::Transport(format!("decode response: {error}")))?;
        unwrap_result(envelope).map_err(|(_, message)| CallFailure::Transport(message))
    }
}

async fn agent_read(
    caller: &impl CliCaller,
    method: &str,
    params: Value,
) -> Result<Value, CallFailure> {
    let value = caller
        .call(method, params, subc_client_rs::CallOptions::default())
        .await?;
    if let Some(code) = value["refused"]["code"].as_str() {
        return Err(CallFailure::Refused {
            code: code.into(),
            message: value["refused"]["details"].to_string(),
        });
    }
    Ok(value)
}

async fn resolve_agent(caller: &impl CliCaller, token: &str) -> Result<String, CallFailure> {
    if is_agent_id(token) {
        return Ok(token.into());
    }
    let value = agent_read(caller, "agent.resolve_name", json!({"name":token})).await?;
    value["agent_id"]
        .as_str()
        .map(Into::into)
        .ok_or_else(|| CallFailure::Transport("resolve_name reply has no agent_id".into()))
}

async fn list_agents(caller: &impl CliCaller, mut params: Value) -> Result<Value, CallFailure> {
    let mut agents = Vec::new();
    loop {
        let page = agent_read(caller, "agent.list", params.clone()).await?;
        agents.extend(
            page["agents"]
                .as_array()
                .ok_or_else(|| {
                    CallFailure::Transport("agent.list reply has no agents array".into())
                })?
                .iter()
                .cloned(),
        );
        let Some(cursor) = page["next_cursor"].as_str() else {
            break;
        };
        if cursor.is_empty()
            || params["cursor"]
                .as_str()
                .is_some_and(|previous| cursor <= previous)
        {
            return Err(CallFailure::Transport(
                "agent.list cursor did not advance".into(),
            ));
        }
        params["cursor"] = json!(cursor);
    }
    Ok(json!({"agents":agents}))
}

fn retry_args(command: &Command, params: &Value) -> Vec<String> {
    let mut args = vec![command.verb.clone()];
    if command.verb == "create" {
        args.extend([
            text(&params["name"]),
            "--role".into(),
            text(&params["role"]).replace('_', "-"),
            "--tag".into(),
            text(&params["tag"]),
        ]);
        for (field, flag) in [
            ("project_id", "--project"),
            ("workspace_id", "--workspace"),
            ("supervisor_agent_id", "--supervisor"),
        ] {
            if let Some(value) = params[field].as_str() {
                args.extend([flag.into(), value.into()]);
            }
        }
    } else {
        args.push(text(&params["agent_id"]));
        match command.verb.as_str() {
            "rename" => args.push(text(&params["name"])),
            "tag" => args.push(text(&params["tag"])),
            "labels" => {
                let labels = params["labels"].as_array().expect("built label array");
                if labels.is_empty() {
                    args.push("--none".into());
                } else {
                    args.extend(labels.iter().map(text));
                }
            }
            _ => (),
        }
    }
    args.extend(["--request-key".into(), text(&params["request_key"])]);
    if let Some(path) = &command.connection {
        args.extend(["--subc".into(), path.to_string_lossy().into_owned()]);
    }
    if command.json {
        args.push("--json".into());
    }
    args
}

async fn execute_agents(
    caller: &impl CliCaller,
    command: &Command,
    method: &str,
    mut params: Value,
) -> Result<Value, (u8, String)> {
    if !agent_write(method) {
        let result = async {
            if method == "agent.list" {
                return list_agents(caller, params).await;
            }
            let token = params["agent_id"].as_str().expect("built show target");
            if !is_agent_id(token) {
                let value = agent_read(caller, "agent.resolve_name", json!({"name":token})).await?;
                return value.get("digest").cloned().ok_or_else(|| {
                    CallFailure::Transport("resolve_name reply has no digest".into())
                });
            }
            let resolved = agent_read(caller, "agent.resolve", params.clone()).await?;
            match resolved["status"].as_str() {
                Some("unknown") => Err(CallFailure::Refused {
                    code: "unknown_agent".into(),
                    message: token.into(),
                }),
                Some("live") => {
                    let listed = list_agents(caller, json!({})).await?;
                    listed["agents"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|row| row["agent_id"] == token)
                        .cloned()
                        .ok_or_else(|| CallFailure::Refused {
                            code: "unknown_agent".into(),
                            message: token.into(),
                        })
                }
                Some("retired" | "merged") => Ok(resolved),
                _ => Err(CallFailure::Transport(
                    "agent.resolve reply has no valid status".into(),
                )),
            }
        }
        .await;
        return result.map_err(CallFailure::display);
    }
    for field in ["agent_id", "supervisor_agent_id"] {
        if let Some(token) = params[field].as_str() {
            let id = resolve_agent(caller, token)
                .await
                .map_err(CallFailure::display)?;
            params[field] = json!(id);
        }
    }
    let args = retry_args(command, &params);
    let words: Vec<_> = args.iter().map(String::as_str).collect();
    let retry = retry_quote::render_retry(&words, cfg!(windows));
    eprintln!("approval is waiting at the operator prompt");
    caller.call(method, params, subc_client_rs::CallOptions {
        timeout: Duration::from_secs(300),
        ..subc_client_rs::CallOptions::default()
    }).await.map_err(|failure| {
        let uncertain = matches!(&failure, CallFailure::Unknown(_)) || matches!(&failure, CallFailure::Refused { code, .. } if code == "engram_outcome_unknown");
        let (exit, mut message) = failure.display();
        if uncertain { message.push_str(&format!("\n{retry}")); }
        (exit, message)
    })
}

fn call(
    connection: Option<&Path>,
    command: &Command,
    method: &str,
    params: Value,
    cwd: &str,
) -> Result<Value, (u8, String)> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .map_err(|error| (EXIT_TRANSPORT, format!("start runtime: {error}")))?;
    runtime.block_on(async {
        let consumer = match connection {
            Some(path) => {
                subc_client_rs::SubcConsumer::connect(
                    path,
                    subc_client_rs::ConsumerOptions::default(),
                )
                .await
            }
            None => {
                subc_client_rs::SubcConsumer::connect_default(
                    subc_client_rs::ConsumerOptions::default(),
                )
                .await
            }
        }
        .map_err(|error| (EXIT_TRANSPORT, connect_failure(connection, error)))?;
        if command.face == Face::Agents {
            execute_agents(
                &ConsumerCaller {
                    consumer: &consumer,
                    cwd: cwd.to_owned(),
                },
                command,
                method,
                params,
            )
            .await
        } else {
            // Keep the established project/workspace transport messages unchanged.
            let body = serde_json::to_vec(&json!({"method":method,"params":params}))
                .map_err(|error| (EXIT_TRANSPORT, format!("serialize request: {error}")))?;
            let bytes = consumer
                .call(
                    subc_protocol::RouteTarget::ManagementSurface {
                        module_id: super::MODULE_ID.into(),
                    },
                    // `project_id` is deliberately left unset: this is the operator
                    // CLI, which has no registered project to resolve and must not
                    // invent one. Absent means "key on the triple", which is the
                    // honest answer for a caller that binds from whatever directory
                    // the operator happened to be standing in.
                    subc_protocol::BindIdentity::new(cwd, "ck-entorhinal", "operator"),
                    body,
                    subc_client_rs::CallOptions::default(),
                )
                .await
                .map_err(|error| match error {
                    // A module refusal is the registry answering, and its code is
                    // the answer: `not_found` and a store failure need different
                    // reactions from the operator, so the code is surfaced verbatim
                    // rather than folded into a transport message.
                    subc_client_rs::CallError::Module(body) => {
                        (EXIT_REFUSED, format!("{}: {}", body.code, body.message))
                    }
                    other => (EXIT_TRANSPORT, format!("call: {other}")),
                })?;
            let envelope = serde_json::from_slice(&bytes)
                .map_err(|error| (EXIT_TRANSPORT, format!("decode response: {error}")))?;
            unwrap_result(envelope)
        }
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
        (Face::Agents, "list" | "show") => println!("{}", format_agents(&command.verb, value)),
        (Face::Workspaces, "list") => {
            let workspaces = value["workspaces"].as_array().cloned().unwrap_or_default();
            if workspaces.is_empty() {
                println!("no workspaces");
                return;
            }
            for workspace in &workspaces {
                println!(
                    "{:<28} {:<20} {}",
                    text(&workspace["workspaceId"]),
                    text(&workspace["name"]),
                    text(&workspace["root"])
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
            // Labelled apart from the `root: GONE` line below, which is about
            // the project's own directory, not its workspace's.
            println!("ws root:   {}", text(&value["workspaceRoot"]));
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
        (Face::Projects, "trust") => {
            println!("project:   {}", text(&value["projectId"]));
            println!("via:       {}", text(&value["via"]));
            let records = value["rootRecords"].as_array().cloned().unwrap_or_default();
            if records.is_empty() {
                println!("roots:     none (this directory authorizes nothing)");
            }
            for record in &records {
                println!(
                    "  {}  {}  {}",
                    text(&record["approval"]["state"]),
                    text(&record["identity"]),
                    text(&record["root"])
                );
            }
        }
        (Face::Projects, "approve" | "unapprove") => {
            for root in value["roots"].as_array().cloned().unwrap_or_default() {
                let already = root["noop"].as_bool() == Some(true);
                println!(
                    "{}{}",
                    text(&root["root"]),
                    if already { "  (unchanged)" } else { "" }
                );
            }
        }
        (Face::Projects, "verify") => println!("{}", format_verify(value)),
        (Face::Projects, "attach") => println!(
            "{}",
            format_attach(value, command.args.iter().any(|arg| arg == "--yes"))
        ),
        (Face::Projects, "owned-remotes") => println!("{}", format_owned_remotes(value)),
        _ => println!(
            "{}",
            serde_json::to_string_pretty(value).unwrap_or_default()
        ),
    }
}

fn format_agents(verb: &str, value: &Value) -> String {
    if verb == "list" {
        let agents = value["agents"].as_array().expect("combined agent list");
        if agents.is_empty() {
            return "no agents".into();
        }
        let rows = agents
            .iter()
            .map(|agent| {
                format!(
                    "{}  {}  {}  {}",
                    text(&agent["agent_id"]),
                    text(&agent["name"]),
                    text(&agent["role"]),
                    text(&agent["tag"])
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        return format!("{rows}\n\n{} agent(s)", agents.len());
    }
    if value.get("status").is_some() {
        return format!(
            "agent:       {}\nstatus:      {}\ngone:        {}\nmerged_into: {}",
            text(&value["agent_id"]),
            text(&value["status"]),
            value["gone"],
            text(&value["merged_into"])
        );
    }
    format!("agent:     {}\nname:      {}\nrole:      {}\ntag:       {}\nproject:   {}\nworkspace: {}\nlabels:    {}", text(&value["agent_id"]), text(&value["name"]), text(&value["role"]), text(&value["tag"]), text(&value["project_id"]), text(&value["workspace_id"]), value["labels"])
}

fn format_attach(value: &Value, confirmed: bool) -> String {
    format!(
        "project:   {}\nroot key:  {}:{}\nroot:      {}\n{}",
        text(&value["projectId"]),
        text(&value["rootKey"]["kind"]),
        text(&value["rootKey"]["rootKey"]),
        text(&value["root"]),
        if confirmed {
            "attached (unapproved)"
        } else {
            "preview only; pass --yes to attach (unapproved)"
        },
    )
}

fn format_owned_remotes(value: &Value) -> String {
    let names = value["remotes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    format!(
        "{}: owned remotes: {}",
        text(&value["root"]),
        if names.is_empty() {
            "none".into()
        } else {
            names.join(", ")
        }
    )
}

fn format_verify(value: &Value) -> String {
    // Preserve the existing membership summary. A clean replay adds no noise;
    // differing tables get one line each, with counts relative to the journal.
    let mut summary = value.clone();
    if let Some(fields) = summary.as_object_mut() {
        fields.remove("replay");
    }
    let mut output = serde_json::to_string_pretty(&summary).unwrap_or_default();
    if let Some(tables) = value["replay"]["tables"].as_array() {
        for table in tables {
            output.push_str(&format!(
                "\nreplay {}: missing={} unexpected={}",
                text(&table["table"]),
                text(&table["missing"]),
                text(&table["unexpected"]),
            ));
        }
    }
    output
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
        assert_eq!(face_from_argv0(Some("ck-agents")), Face::Agents);
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

    #[test]
    fn ckdev_face_names_match_their_ck_faces() {
        for (production_name, dev_name, expected) in [
            ("ck-projects", "ckdev-projects", Face::Projects),
            ("ck-workspaces", "ckdev-workspaces", Face::Workspaces),
            ("ck-agents", "ckdev-agents", Face::Agents),
            ("ck-entorhinal", "ckdev-entorhinal", Face::Entorhinal),
        ] {
            assert_eq!(face_from_argv0(Some(production_name)), expected);
            assert_eq!(face_from_argv0(Some(dev_name)), expected);
        }
    }

    #[test]
    fn unknown_ckdev_face_names_keep_the_module_default() {
        assert_eq!(face_from_argv0(Some("ckdev-")), Face::Entorhinal);
        assert_eq!(face_from_argv0(Some("ckdev-foo")), Face::Entorhinal);
        assert_eq!(face_from_argv0(Some("ckdev-ck-projects")), Face::Entorhinal);
        // Unprefixed names never selected a CLI face, and still don't.
        assert_eq!(face_from_argv0(Some("projects")), Face::Entorhinal);
        assert_eq!(face_from_argv0(Some("workspaces")), Face::Entorhinal);
    }

    /// Bare CLI faces explain; only the module face serves from empty argv.
    /// This is the `ck` convention (verbless domains print their help), and it
    /// is also what stops `ck projects` from booting a daemon.
    #[test]
    fn bare_cli_faces_explain_rather_than_serve() {
        for face in [Face::Projects, Face::Workspaces, Face::Agents] {
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
        for face in [Face::Projects, Face::Workspaces, Face::Agents] {
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
        let dir = std::env::temp_dir();
        let canonical = std::fs::canonicalize(&dir).unwrap();
        let (method, params) = build(&ws(&["set-root", "w1", dir.to_str().unwrap()])).unwrap();
        assert_eq!(method, "set_workspace_root");
        assert_eq!(params["workspaceId"], "w1");
        assert_eq!(params["root"], canonical.to_string_lossy().as_ref());
        let (method, params) = build(&ws(&["clear-root", "w1"])).unwrap();
        assert_eq!(method, "set_workspace_root");
        assert!(
            params["root"].is_null(),
            "clear-root must send an explicit null: {params}"
        );

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

    #[test]
    fn ownership_cli_maps_names_default_and_empty_set_and_renders() {
        let root = std::fs::canonicalize(std::env::temp_dir())
            .unwrap()
            .to_string_lossy()
            .into_owned();
        for (tail, expected) in [
            (vec!["origin", "mirror"], json!(["origin", "mirror"])),
            (vec!["--default"], Value::Null),
            (vec!["--none"], json!([])),
        ] {
            let mut arguments = vec!["owned-remotes".to_string(), root.clone()];
            arguments.extend(tail.iter().map(|arg| arg.to_string()));
            let Invocation::Command(command) = parse(Face::Projects, &arguments) else {
                panic!("owned-remotes must parse")
            };
            assert_eq!(
                build(&command).unwrap(),
                (
                    "set_owned_remotes".into(),
                    json!({"root":root,"remotes":expected,"actor":"ck-projects"})
                )
            );
        }
        let Invocation::Command(command) = parse(
            Face::Projects,
            &[
                "owned-remotes".into(),
                root.clone(),
                "origin".into(),
                "--default".into(),
            ],
        ) else {
            panic!("parse")
        };
        assert!(build(&command).unwrap_err().contains("cannot be combined"));
        // A bare root must not silently disown it: owning nothing is `--none`.
        let Invocation::Command(command) =
            parse(Face::Projects, &["owned-remotes".into(), root.clone()])
        else {
            panic!("parse")
        };
        assert!(build(&command)
            .unwrap_err()
            .contains("name the remotes this root owns"));
        let Invocation::Command(command) = parse(
            Face::Projects,
            &["owned-remotes".into(), "--default".into()],
        ) else {
            panic!("parse")
        };
        assert!(build(&command)
            .unwrap_err()
            .contains("needs a root directory"));
        assert_eq!(
            format_owned_remotes(&json!({"root":"/repo","remotes":["mirror","origin"]})),
            "/repo: owned remotes: mirror, origin"
        );
        assert_eq!(
            format_owned_remotes(&json!({"root":"/repo","remotes":[]})),
            "/repo: owned remotes: none"
        );
    }

    #[test]
    fn attach_cli_previews_then_writes_only_with_yes_and_prints_the_same_match() {
        use crate::{tests::log_surface_handler, Principal, RouteAdmission, WireRequest};
        let (dir, handler, _) = log_surface_handler("attach-cli");
        let path = dir.join("label-checkout");
        std::fs::create_dir_all(&path).unwrap();
        let root = std::fs::canonicalize(&path)
            .unwrap()
            .to_string_lossy()
            .into_owned();
        handler.route_admissions().insert(
            (120, 1),
            RouteAdmission::from_bind(Some(Principal::Direct), None),
        );
        let mut args = vec![
            "attach".into(),
            path.to_string_lossy().into_owned(),
            "--project".into(),
            "p".into(),
            "--label".into(),
            "local".into(),
        ];
        let head = handler
            .with_store(entorhinal_core::RegistryStore::generation)
            .unwrap();
        let mut preview = Value::Null;
        for confirmed in [false, true] {
            if confirmed {
                args.push("--yes".into());
            }
            let Invocation::Command(command) = parse(Face::Projects, &args) else {
                panic!("attach must parse")
            };
            let (method, params) = build(&command).unwrap();
            assert_eq!(
                method,
                if confirmed {
                    "attach_root"
                } else {
                    "preview_attach_root"
                }
            );
            assert_eq!(
                params,
                json!({"path":root,"projectId":"p","label":"local","actor":"ck-projects"})
            );
            let value: Value = serde_json::from_slice(
                &handler
                    .execute(WireRequest { method, params }, (120, 1))
                    .unwrap(),
            )
            .unwrap();
            let value = unwrap_result(value).unwrap();
            let rendered = format_attach(&value, confirmed);
            assert!(
                rendered.contains("project:   p\nroot key:  label:local"),
                "{rendered}"
            );
            assert!(
                rendered.contains(&format!("root:      {root}")),
                "{rendered}"
            );
            if confirmed {
                assert_eq!(value["projectId"], preview["projectId"]);
                assert_eq!(value["rootKey"], preview["rootKey"]);
                assert!(rendered.ends_with("attached (unapproved)"));
                assert!(
                    handler
                        .with_store(entorhinal_core::RegistryStore::generation)
                        .unwrap()
                        > head
                );
                let roots = handler
                    .with_store(|s| {
                        s.resolve_root_key(entorhinal_core::ResolveRootKeyRequest {
                            project_id: "p".into(),
                            kind: "label".into(),
                            root_key: "local".into(),
                        })
                    })
                    .unwrap();
                assert_eq!(roots.roots, vec![root.clone()]);
            } else {
                preview = value;
                assert!(rendered.contains("preview only; pass --yes"));
                assert_eq!(
                    handler
                        .with_store(entorhinal_core::RegistryStore::generation)
                        .unwrap(),
                    head
                );
                assert!(handler
                    .with_store(
                        |s| s.resolve_root_key(entorhinal_core::ResolveRootKeyRequest {
                            project_id: "p".into(),
                            kind: "label".into(),
                            root_key: "local".into()
                        })
                    )
                    .unwrap()
                    .roots
                    .is_empty());
            }
        }
        drop(handler);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn attach_cli_refuses_malformed_flags_and_maps_log_commands() {
        for args in [
            vec!["attach", "--project"],
            vec!["attach", "/checkout", "--label", "--yes"],
            vec!["attach", "/checkout", "--project", "p", "--project", "q"],
            vec!["attach", "/checkout", "--label", "l", "--label", "m"],
            vec!["attach", "/checkout", "--yes", "--yes"],
            vec!["attach", "/one", "/two"],
            vec!["attach", "--yes"],
            vec!["register", "n", "/checkout", "--yes"],
        ] {
            match parse_as(Face::Projects, &args) {
                Invocation::Refuse(_) => {}
                Invocation::Command(command) => assert!(build(&command).is_err(), "{args:?}"),
                _ => panic!("malformed attach invocation must refuse: {args:?}"),
            }
        }
        for (verb, method) in [
            ("enable", "identity_log.enable"),
            ("status", "identity_log.status"),
        ] {
            let Invocation::Command(command) = parse_as(Face::Projects, &["log", verb]) else {
                panic!("log must parse")
            };
            assert_eq!(build(&command).unwrap(), (method.into(), json!({})));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn log_enable_cli_request_enables_through_operator_route() {
        use crate::{fake_log::FakeLog, tests::scratch_descriptor};
        let (dir, descriptor) = scratch_descriptor("enable-cli");
        let log = std::sync::Arc::new(FakeLog::default());
        let handler =
            crate::ProjectsHandler::with_log_connector("cli-enable".into(), || 700, log.clone());
        *handler.store.lock().unwrap() =
            Some(entorhinal_core::RegistryStore::open(&descriptor).unwrap());
        let Invocation::Command(command) =
            parse_as(Face::Projects, &["log", "enable", "--without-agents"])
        else {
            panic!("log enable must parse")
        };
        let (method, params) = build(&command).unwrap();
        let body = serde_json::to_vec(&json!({"method":method,"params":params})).unwrap();
        handler.route_admissions().insert(
            (141, 1),
            crate::RouteAdmission {
                principal: Some(crate::Principal::Direct),
                flow_id: None,
                handle: None,
            },
        );
        let crate::HandlerOutcome::Response(bytes) =
            handler.handle_served_request(&body, (141, 1)).await
        else {
            panic!("CLI enable was refused")
        };
        assert_eq!(
            serde_json::from_slice::<Value>(&bytes).unwrap()["result"]["state"],
            "enabled"
        );
        assert_eq!(log.head(), 1);
        drop(handler);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn import_consent_cli_without_agents_is_enable_only_and_builds_boolean() {
        let Invocation::Command(command) =
            parse_as(Face::Projects, &["log", "enable", "--without-agents"])
        else {
            panic!("log enable --without-agents must parse")
        };
        assert_eq!(
            build(&command).unwrap(),
            ("identity_log.enable".into(), json!({"without_agents":true}))
        );
        assert!(usage(Face::Projects).contains("--without-agents"));
        for args in [
            vec!["log", "status", "--without-agents"],
            vec!["register", "--without-agents"],
            vec!["log", "--without-agents", "enable"],
        ] {
            assert!(
                matches!(parse_as(Face::Projects, &args), Invocation::Refuse(_)),
                "{args:?}"
            );
        }
        for face in [Face::Workspaces, Face::Entorhinal] {
            assert!(matches!(
                parse_as(face, &["log", "enable", "--without-agents"]),
                Invocation::Refuse(_)
            ));
        }
        for args in [
            vec!["log", "enable", "--without-agents", "--without-agents"],
            vec!["log", "enable", "--without-agents", "extra"],
        ] {
            let Invocation::Command(command) = parse_as(Face::Projects, &args) else {
                panic!("log must parse")
            };
            assert!(build(&command).is_err(), "{args:?}");
        }
        // Build must also refuse a status flag if handed a constructed command.
        let mut status = command;
        status.args[0] = "status".into();
        assert!(build(&status).is_err());
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
            workspace_root: Some("/tmp/ws".to_string()),
            project_name: Some("the-name".to_string()),
            via: "root".to_string(),
            gone: false,
            generation: 2,
            canonical_root: Some("/tmp/x".to_string()),
            root_fields: None,
        })
        .unwrap();

        // Every key the renderer reads must exist on the real reply.
        for key in [
            "projectId",
            "projectName",
            "workspaceId",
            "workspaceRoot",
            "gone",
        ] {
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
            workspace_root: None,
            project_name: None,
            via: "implicit".to_string(),
            gone: false,
            generation: 2,
            canonical_root: Some("/tmp/x".to_string()),
            root_fields: None,
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
            dot,
            entorhinal_core::RegistryStore::canonical_mutation_root(".").unwrap(),
            "resolved against something other than the operator's cwd"
        );

        // (2) an absolute path through a symlinked ancestor is canonicalized.
        let dir = std::env::temp_dir().join(format!("ent-canon-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let given = dir.to_string_lossy().to_string();
        let canonical = entorhinal_core::RegistryStore::canonical_mutation_root(&given).unwrap();
        assert_eq!(
            sent_for(&given),
            canonical,
            "sent path must be canonical, not just absolute"
        );
        // Non-vacuity: leg (2) only discriminates where the temp dir actually
        // goes through a symlink. Where it does not, say so rather than
        // passing as though the stronger property had been tested.
        if given == canonical {
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

    #[test]
    fn verify_renders_only_differing_replay_tables() {
        use entorhinal_core::{ReplayReport, ReplayTableDifference, VerifyReply};

        let mode = std::env::var("ENTORHINAL_TEST_VERIFY_RENDER_MODE").ok();
        let mut reply = VerifyReply {
            ok: true,
            local_members: 2,
            project_workspaces: 2,
            mismatches: Vec::new(),
            generation: 10,
            replay: ReplayReport {
                ok: true,
                tables: Vec::new(),
            },
        };
        if mode.as_deref() != Some("clean") {
            reply.ok = false;
            reply.replay = ReplayReport {
                ok: false,
                tables: vec![
                    ReplayTableDifference {
                        table: "project".into(),
                        missing: 0,
                        unexpected: 1,
                        missing_keys: Vec::new(),
                        unexpected_keys: Vec::new(),
                    },
                    ReplayTableDifference {
                        table: "agent_name_claim".into(),
                        missing: 1,
                        unexpected: 0,
                        missing_keys: Vec::new(),
                        unexpected_keys: Vec::new(),
                    },
                ],
            };
        }
        if let Some(mode) = mode {
            render(
                &Command {
                    face: Face::Projects,
                    verb: "verify".into(),
                    connection: None,
                    args: Vec::new(),
                    json: mode == "json",
                },
                &serde_json::to_value(reply).unwrap(),
            );
            return;
        }

        // Capture the real renderer in a child test process, so removing its
        // verify dispatch cannot pass a test of only the formatting helper.
        let capture = |mode: &str| {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "cli::tests::verify_renders_only_differing_replay_tables",
                    "--nocapture",
                ])
                .env("ENTORHINAL_TEST_VERIFY_RENDER_MODE", mode)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8(output.stdout).unwrap()
        };
        let clean = capture("clean");
        assert!(clean.contains(
            &serde_json::to_string_pretty(&json!({
                "ok":true, "localMembers":2, "projectWorkspaces":2,
                "mismatches":[], "generation":10,
            }))
            .unwrap()
        ));
        assert!(!clean.contains("\"replay\""));
        assert!(!clean.lines().any(|line| line.starts_with("replay ")));
        let dirty = capture("dirty");
        assert!(dirty.contains("\nreplay project: missing=0 unexpected=1\nreplay agent_name_claim: missing=1 unexpected=0\n"));
        assert_eq!(
            dirty
                .lines()
                .filter(|line| line.starts_with("replay "))
                .count(),
            2
        );
        assert!(!dirty.contains("missingKeys"));
        let json = capture("json");
        assert!(json.contains("\"replay\": {"));
        assert!(json.contains("\"missingKeys\": []"));
        assert!(!json.lines().any(|line| line.starts_with("replay ")));
    }
}

#[cfg(test)]
mod agent_tests {
    use super::*;
    use crate::agent_ops::{OperatorConfirmError, OperatorConfirmer};
    use crate::{HandlerOutcome, Principal, ProjectsHandler, RegistryStore, RouteAdmission};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };
    const ID: &str = "agent_0123456789abcdef";

    fn command(args: &[&str]) -> Command {
        match parse(
            Face::Agents,
            &args.iter().map(|s| (*s).into()).collect::<Vec<_>>(),
        ) {
            Invocation::Command(command) => command,
            _ => panic!("expected agent command: {args:?}"),
        }
    }
    async fn execute(caller: &impl CliCaller, args: &[&str]) -> Result<Value, (u8, String)> {
        let command = command(args);
        let (method, params) = build(&command).unwrap();
        execute_agents(caller, &command, &method, params).await
    }

    #[test]
    fn agents_verbs_build_wire_shapes_keys_and_require_tag() {
        for (args, method, fields) in [
            (
                vec![
                    "create",
                    "Ada",
                    "--role",
                    "workspace-head",
                    "--tag",
                    "helper",
                    "--workspace",
                    "W",
                    "--project",
                    "P",
                    "--supervisor",
                    "Boss",
                ],
                "agent.create",
                json!({"name":"Ada","role":"workspace_head","tag":"helper","workspace_id":"W","project_id":"P","supervisor_agent_id":"Boss"}),
            ),
            (
                vec!["rename", ID, "Grace"],
                "agent.rename",
                json!({"agent_id":ID,"name":"Grace"}),
            ),
            (vec!["retire", ID], "agent.dispose", json!({"agent_id":ID})),
            (
                vec!["tag", ID, "new tag"],
                "agent.update_tag",
                json!({"agent_id":ID,"tag":"new tag"}),
            ),
            (
                vec!["labels", ID, "a", "b"],
                "agent.set_labels",
                json!({"agent_id":ID,"labels":["a","b"]}),
            ),
            (
                vec!["labels", ID, "--none"],
                "agent.set_labels",
                json!({"agent_id":ID,"labels":[]}),
            ),
            (
                vec!["list", "--project", "P", "--workspace", "W"],
                "agent.list",
                json!({"project_id":"P","workspace_id":"W"}),
            ),
            (vec!["show", ID], "agent.resolve", json!({"agent_id":ID})),
        ] {
            let (actual, mut params) = build(&command(&args)).unwrap();
            assert_eq!(actual, method);
            if agent_write(method) {
                assert_eq!(
                    params.as_object_mut().unwrap().remove("actor"),
                    Some(json!("ck-agents"))
                );
                let key = params
                    .as_object_mut()
                    .unwrap()
                    .remove("request_key")
                    .unwrap();
                let key = key.as_str().unwrap();
                assert_eq!(key.len(), 32);
                assert!(key
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()));
                let (_, other) = build(&command(&args)).unwrap();
                assert_ne!(other["request_key"], key);
            }
            assert_eq!(params, fields);
        }
        for role in ["assistant", "head", "hiree"] {
            assert_eq!(
                build(&command(&["create", "Ada", "--role", role, "--tag", "t"]))
                    .unwrap()
                    .1["role"],
                role
            );
        }
        let error = build(&command(&["create", "Ada", "--role", "assistant"])).unwrap_err();
        assert!(error.contains("requires --tag"), "{error}");
        assert_eq!(
            build(&command(&["retire", ID, "--request-key", "verbatim key"]))
                .unwrap()
                .1["request_key"],
            "verbatim key"
        );
        for args in [
            vec!["labels", ID],
            vec!["labels", ID, "--none", "a"],
            vec!["show", ID, "extra"],
            vec!["list", "--request-key", "read"],
            vec!["retire", ID, "--tag", "bad"],
            vec!["create", "Ada", "--role", "other", "--tag", "x"],
        ] {
            assert!(build(&command(&args)).is_err(), "{args:?}");
        }
        for token in ["agent_01234567", ID] {
            assert!(is_agent_id(token));
        }
        for token in [
            "agent_012345678",
            "agent_ABCDEF00",
            "agent_0123456g",
            "Ada",
            "W/Ada",
        ] {
            assert!(!is_agent_id(token));
        }
    }

    type ObservedCall = (String, Value, Duration);
    struct Script {
        calls: Mutex<Vec<ObservedCall>>,
        replies: Mutex<std::collections::VecDeque<Result<Value, CallFailure>>>,
    }
    impl Script {
        fn new(replies: Vec<Result<Value, CallFailure>>) -> Self {
            Self {
                calls: Mutex::new(vec![]),
                replies: Mutex::new(replies.into()),
            }
        }
        fn calls(&self) -> Vec<ObservedCall> {
            self.calls.lock().unwrap().clone()
        }
    }
    #[async_trait::async_trait]
    impl CliCaller for Script {
        async fn call(
            &self,
            method: &str,
            params: Value,
            options: subc_client_rs::CallOptions,
        ) -> Result<Value, CallFailure> {
            self.calls
                .lock()
                .unwrap()
                .push((method.into(), params, options.timeout));
            self.replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected extra RPC")
        }
    }

    #[tokio::test]
    async fn agents_resolution_refuses_before_any_write_and_ids_skip_reads() {
        for code in ["name_ambiguous", "name_unknown"] {
            for args in [
                vec!["rename", "W/Ada", "Grace"],
                vec![
                    "create",
                    "Ada",
                    "--role",
                    "assistant",
                    "--tag",
                    "t",
                    "--supervisor",
                    "Boss",
                ],
            ] {
                let script = Script::new(vec![Ok(
                    json!({"refused":{"code":code,"details":{"name":"Ada"}}}),
                )]);
                let error = execute(&script, &args).await.unwrap_err();
                assert_eq!(error.0, EXIT_REFUSED);
                assert!(error.1.starts_with(code));
                assert_eq!(script.calls().len(), 1);
                assert_eq!(script.calls()[0].0, "agent.resolve_name");
            }
        }
        let script = Script::new(vec![
            Ok(json!({"agent_id":ID,"digest":{}})),
            Ok(json!({"ok":true})),
        ]);
        execute(&script, &["rename", "W/Ada", "Grace"])
            .await
            .unwrap();
        assert_eq!(script.calls()[0].1, json!({"name":"W/Ada"}));
        assert_eq!(script.calls()[1].1["agent_id"], ID);
        for id in [ID, "agent_01234567"] {
            let script = Script::new(vec![Ok(json!({"ok":true}))]);
            execute(&script, &["retire", id]).await.unwrap();
            assert_eq!(script.calls().len(), 1);
            assert_eq!(script.calls()[0].0, "agent.dispose");
            assert_eq!(script.calls()[0].1["agent_id"], id);
        }
    }

    #[tokio::test]
    async fn agents_transport_observes_300_second_writes_and_default_reads() {
        for args in [
            vec![
                "create",
                "Ada",
                "--role",
                "assistant",
                "--tag",
                "t",
                "--supervisor",
                "Boss",
            ],
            vec!["rename", "Ada", "Grace"],
            vec!["retire", "Ada"],
            vec!["tag", "Ada", "t"],
            vec!["labels", "Ada", "a"],
            vec!["labels", "Ada", "--none"],
        ] {
            let script = Script::new(vec![Ok(json!({"agent_id":ID})), Ok(json!({"ok":true}))]);
            execute(&script, &args).await.unwrap();
            let calls = script.calls();
            assert_eq!(calls.len(), 2);
            assert_eq!(calls[0].2, subc_client_rs::CallOptions::default().timeout);
            assert_eq!(
                calls[1].2,
                Duration::from_secs(300),
                "{} write budget",
                calls[1].0
            );
        }
        let script = Script::new(vec![Ok(json!({"agents":[]}))]);
        execute(&script, &["list"]).await.unwrap();
        assert_eq!(
            script.calls()[0].2,
            subc_client_rs::CallOptions::default().timeout
        );
    }

    #[tokio::test]
    async fn agents_list_and_show_page_digests_and_report_terminal_or_unknown() {
        let digest =
            json!({"agent_id":ID,"name":"Ada","role":"assistant","tag":"helper","labels":["a"]});
        let pages = || {
            vec![
                Ok(
                    json!({"agents":[{"agent_id":"agent_0000000000000001"}],"next_cursor":"agent_0000000000000001"}),
                ),
                Ok(json!({"agents":[digest.clone()]})),
            ]
        };
        let script = Script::new(pages());
        let listed = execute(
            &script,
            &["list", "--project", "P", "--workspace", "W", "--json"],
        )
        .await
        .unwrap();
        assert_eq!(
            listed,
            json!({"agents":[{"agent_id":"agent_0000000000000001"},digest.clone()]})
        );
        assert_eq!(
            script.calls()[1].1,
            json!({"project_id":"P","workspace_id":"W","cursor":"agent_0000000000000001"})
        );
        assert!(format_agents("list", &listed).contains("2 agent(s)"));
        let mut replies = vec![Ok(
            json!({"agent_id":ID,"status":"live","gone":null,"merged_into":null}),
        )];
        replies.extend(pages());
        let script = Script::new(replies);
        assert_eq!(execute(&script, &["show", ID]).await.unwrap(), digest);
        assert_eq!(script.calls()[0].1, json!({"agent_id":ID}));
        assert_eq!(script.calls().len(), 3);
        let script = Script::new(vec![Ok(json!({"agent_id":ID,"digest":digest.clone()}))]);
        assert_eq!(execute(&script, &["show", "W/Ada"]).await.unwrap(), digest);
        assert_eq!(script.calls()[0].0, "agent.resolve_name");
        assert!(format_agents("show", &digest).contains("helper"));
        for status in ["retired", "merged"] {
            let resolved = json!({"agent_id":ID,"status":status,"gone":{"reason":status},"merged_into":"agent_00000001"});
            let script = Script::new(vec![Ok(resolved.clone())]);
            assert_eq!(execute(&script, &["show", ID]).await.unwrap(), resolved);
            assert_eq!(script.calls().len(), 1);
            let output = format_agents("show", &resolved);
            assert!(
                output.contains(status)
                    && output.contains("gone:")
                    && output.contains("merged_into:")
                    && output.contains("agent_00000001")
            );
        }
        let script = Script::new(vec![Ok(json!({"status":"unknown"}))]);
        let error = execute(&script, &["show", ID]).await.unwrap_err();
        assert_eq!(error, (EXIT_REFUSED, format!("unknown_agent: {ID}")));
        let script = Script::new(vec![Ok(json!({"refused":{"code":"name_unknown"}}))]);
        assert!(execute(&script, &["show", "Retired Ada"])
            .await
            .unwrap_err()
            .1
            .starts_with("name_unknown"));
    }

    struct Confirm(AtomicUsize);
    #[async_trait::async_trait]
    impl OperatorConfirmer for Confirm {
        async fn confirm_operator(
            &self,
            _: &str,
            _: crate::RouteKey,
        ) -> Result<(), OperatorConfirmError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    struct Served {
        handler: ProjectsHandler,
        root: PathBuf,
        calls: Mutex<Vec<String>>,
        // Which failure to inject: 1 lets the write land and then loses its
        // reply; 2 loses the request before entorhinal's handler sees it.
        lose: AtomicUsize,
    }
    impl Served {
        fn new(confirm: Option<Arc<Confirm>>) -> Self {
            let (root, descriptor) = crate::tests::scratch_descriptor("agents-cli");
            let mut handler = ProjectsHandler::with_runtime("0123456789abcdef".into(), || 700);
            if let Some(confirm) = confirm {
                handler = handler.with_operator_confirmer(confirm);
            }
            let store = RegistryStore::open(&descriptor).unwrap();
            store
                .apply_entry("agent.cutover", "{}", "fixture", None, |_| Ok(()))
                .unwrap();
            *handler.store.lock().unwrap() = Some(store);
            handler.route_admissions().insert(
                (91, 1),
                RouteAdmission {
                    principal: Some(Principal::Direct),
                    flow_id: None,
                    handle: None,
                },
            );
            Self {
                handler,
                root,
                calls: Mutex::new(vec![]),
                lose: AtomicUsize::new(0),
            }
        }
        fn store(&self) -> RegistryStore {
            self.handler.with_store(|store| Ok(store.clone())).unwrap()
        }
        fn core_create(&self) -> String {
            let bytes = self.store().with_principal("reserved:prefrontal-core").agent_mutation_with_id("agent.create", json!({"name":"Ada","role":"assistant","tag":"helper","request_key":"core-create"}), 700, Some(ID)).unwrap();
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            value["result"]["agent"]["agent_id"]
                .as_str()
                .unwrap()
                .into()
        }
    }
    impl Drop for Served {
        fn drop(&mut self) {
            self.handler.store.lock().unwrap().take();
            std::fs::remove_dir_all(&self.root).unwrap();
        }
    }
    #[async_trait::async_trait]
    impl CliCaller for Served {
        async fn call(
            &self,
            method: &str,
            params: Value,
            options: subc_client_rs::CallOptions,
        ) -> Result<Value, CallFailure> {
            self.calls.lock().unwrap().push(method.into());
            assert_eq!(
                options.timeout,
                if agent_write(method) {
                    Duration::from_secs(300)
                } else {
                    subc_client_rs::CallOptions::default().timeout
                }
            );
            let lose = if agent_write(method) {
                self.lose.swap(0, Ordering::SeqCst)
            } else {
                0
            };
            if lose == 2 {
                return Err(CallFailure::Unknown(
                    "transport timeout: the write outcome is unknown".into(),
                ));
            }
            let outcome = self
                .handler
                .handle_served_request(
                    &serde_json::to_vec(&json!({"method":method,"params":params})).unwrap(),
                    (91, 1),
                )
                .await;
            if lose == 1 {
                assert!(matches!(outcome, HandlerOutcome::Response(_)));
                return Err(CallFailure::Unknown(
                    "transport timeout: the write outcome is unknown".into(),
                ));
            }
            match outcome {
                HandlerOutcome::Response(body) => {
                    Ok(unwrap_result(serde_json::from_slice(&body).unwrap()).unwrap())
                }
                HandlerOutcome::Error { code, message }
                | HandlerOutcome::ErrorWithDetail { code, message, .. } => {
                    Err(CallFailure::Refused { code, message })
                }
                _ => panic!("unexpected streamed reply"),
            }
        }
    }

    #[tokio::test]
    async fn agents_create_reaches_confirming_handler() {
        let confirm = Arc::new(Confirm(AtomicUsize::new(0)));
        let served = Served::new(Some(confirm.clone()));
        let value = execute(
            &served,
            &["create", "Ada", "--role", "assistant", "--tag", "helper"],
        )
        .await
        .unwrap();
        assert_eq!(value["agent"]["name"], "Ada");
        assert_eq!(confirm.0.load(Ordering::SeqCst), 1);
        let listed = execute(&served, &["list"]).await.unwrap();
        assert_eq!(listed["agents"].as_array().unwrap().len(), 1);
        assert_eq!(listed["agents"][0]["agent_id"], value["agent"]["agent_id"]);
    }

    #[tokio::test]
    async fn agents_create_unsupported_refuses_with_one_line_explanation() {
        let served = Served::new(None);
        let before = served.store().generation().unwrap();
        let error = execute(
            &served,
            &["create", "Ada", "--role", "assistant", "--tag", "helper"],
        )
        .await
        .unwrap_err();
        assert_eq!(
            error,
            (
                EXIT_REFUSED,
                "operator_presence_unavailable: this daemon does not support operator confirmation"
                    .into()
            )
        );
        assert_eq!(error.1.lines().count(), 1);
        assert_eq!(served.store().generation().unwrap(), before);
        assert!(execute(&served, &["list"]).await.unwrap()["agents"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn agents_timeout_retry_uses_resolved_id_and_replays_without_resolution_or_prompt() {
        for landed in [true, false] {
            let confirm = Arc::new(Confirm(AtomicUsize::new(0)));
            let served = Served::new(Some(confirm.clone()));
            assert_eq!(served.core_create(), ID);
            served
                .lose
                .store(if landed { 1 } else { 2 }, Ordering::SeqCst);
            let error = execute(
                &served,
                &["rename", "Ada", "Grace", "--request-key", "retry-key"],
            )
            .await
            .unwrap_err();
            assert_eq!(error.0, EXIT_TRANSPORT);
            assert!(error
                .1
                .starts_with("transport timeout: the write outcome is unknown"));
            let prefix = if cfg!(windows) {
                "retry (pwsh): ck agents "
            } else {
                "retry: ck agents "
            };
            let retry = error.1.split_once(prefix).unwrap().1;
            let expected = if cfg!(windows) {
                format!("'rename' '{ID}' 'Grace' '--request-key' 'retry-key'")
            } else {
                format!("rename {ID} Grace --request-key retry-key")
            };
            assert_eq!(retry, expected);
            let count = confirm.0.load(Ordering::SeqCst);
            assert_eq!(count, usize::from(landed));
            served.calls.lock().unwrap().clear();
            let retry_words: Vec<_> = retry
                .split_whitespace()
                .map(|word| {
                    if cfg!(windows) {
                        word.trim_matches('\'')
                    } else {
                        word
                    }
                })
                .collect();
            let value = execute(&served, &retry_words).await.unwrap();
            assert_eq!(value["agent"]["name"], "Grace");
            assert_eq!(*served.calls.lock().unwrap(), vec!["agent.rename"]);
            assert_eq!(confirm.0.load(Ordering::SeqCst), 1);
            let named = execute(&served, &["show", "Grace"]).await.unwrap();
            assert_eq!(named["agent_id"], ID);
        }
    }

    #[tokio::test]
    async fn agents_uncertain_retry_preserves_supervisor_ids_flags_and_shell_quoting() {
        let script = Script::new(vec![
            Ok(json!({"agent_id":ID})),
            Err(CallFailure::Refused {
                code: "engram_outcome_unknown".into(),
                message: "append outcome unknown".into(),
            }),
        ]);
        let error = execute(
            &script,
            &[
                "create",
                "O'Neil",
                "--role",
                "hiree",
                "--tag",
                "quoted tag",
                "--project",
                "P",
                "--workspace",
                "W",
                "--supervisor",
                "Boss",
                "--request-key",
                "key",
                "--subc",
                "/tmp/conn.json",
                "--json",
            ],
        )
        .await
        .unwrap_err();
        assert_eq!(error.0, EXIT_REFUSED);
        let expected = if cfg!(windows) {
            format!("retry (pwsh): ck agents 'create' 'O''Neil' '--role' 'hiree' '--tag' 'quoted tag' '--project' 'P' '--workspace' 'W' '--supervisor' '{ID}' '--request-key' 'key' '--subc' '/tmp/conn.json' '--json'")
        } else {
            format!("retry: ck agents create 'O'\\''Neil' --role hiree --tag 'quoted tag' --project P --workspace W --supervisor {ID} --request-key key --subc /tmp/conn.json --json")
        };
        assert!(error.1.contains(&expected), "{}", error.1);
        assert_eq!(script.calls()[1].1["supervisor_agent_id"], ID);
        for code in [
            "operator_declined",
            "operator_presence_unavailable",
            "operator_confirmation_busy",
            "operator_approval_stale",
            "operator_summary_too_long",
            "authority_not_cut_over",
        ] {
            let (_, message) = CallFailure::Refused {
                code: code.into(),
                message: String::new(),
            }
            .display();
            assert!(message.starts_with(code));
            assert_eq!(message.lines().count(), 1);
            assert!(message.len() > code.len() + 2);
        }
    }
}

#[cfg(test)]
mod cli_transport_tests {
    use super::*;
    use subc_protocol::{Flags, Frame, FrameType, Priority};
    use subc_transport::{
        authenticate_server, generate_daemon_id, generate_key, read_frame, write_atomic,
        write_frame, ConnectionInfo, Endpoint, SCHEMA_VERSION,
    };

    // The client tells a timeout from a closed route by the error's typed
    // cause, which only the subc client library can set. So this test makes a
    // real library call; an error built by hand from a string would not
    // exercise that classification.
    #[tokio::test]
    async fn agents_sdk_timeout_is_unknown_and_daemon_route_closure_is_distinct() {
        for close_route in [false, true] {
            let (root, _) = crate::tests::scratch_descriptor("cli-transport");
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let info = ConnectionInfo {
                schema: SCHEMA_VERSION,
                wire_version: None,
                endpoints: vec![Endpoint {
                    host: "127.0.0.1".into(),
                    port: listener.local_addr().unwrap().port(),
                }],
                key: generate_key().unwrap(),
                daemon_id: generate_daemon_id().unwrap(),
                pid: std::process::id(),
                daemon_ver: "cli-test".into(),
            };
            let path = root.join("connection.json");
            write_atomic(&path, &info).unwrap();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                authenticate_server(
                    &mut stream,
                    &info.key,
                    &info.daemon_id,
                    &info.daemon_ver,
                    Duration::from_secs(5),
                )
                .await
                .unwrap();
                while let Some(frame) = read_frame(&mut stream).await.unwrap() {
                    let (ty, body) = if frame.header.ty == FrameType::Ping {
                        (FrameType::Pong, vec![])
                    } else if frame.header.ty == FrameType::Request && frame.header.channel == 0 {
                        let request: Value = serde_json::from_slice(&frame.body).unwrap();
                        if request["op"] != "route.open" {
                            continue;
                        }
                        (
                            FrameType::Response,
                            serde_json::to_vec(
                                &json!({"op":"route.open","route_channel":41,"route_epoch":1}),
                            )
                            .unwrap(),
                        )
                    } else if frame.header.ty == FrameType::Request && close_route {
                        (FrameType::Goodbye, vec![])
                    } else {
                        continue;
                    };
                    let reply = Frame::build_with_version(
                        frame.header.ver,
                        ty,
                        Flags::new(false, Priority::Interactive, false),
                        frame.header.channel,
                        frame.header.epoch,
                        frame.header.corr,
                        body,
                    )
                    .unwrap();
                    write_frame(&mut stream, &reply).await.unwrap();
                }
            });
            let consumer = subc_client_rs::SubcConsumer::connect(
                &path,
                subc_client_rs::ConsumerOptions::default(),
            )
            .await
            .unwrap();
            let error = tokio::time::timeout(
                Duration::from_secs(10),
                CliCaller::call(
                    &ConsumerCaller {
                        consumer: &consumer,
                        cwd: "/tmp/project".into(),
                    },
                    "agent.dispose",
                    json!({"agent_id":"agent_01234567","request_key":"key"}),
                    subc_client_rs::CallOptions {
                        timeout: Duration::from_millis(100),
                        ..subc_client_rs::CallOptions::default()
                    },
                ),
            )
            .await
            .expect("transport test hung")
            .unwrap_err();
            if close_route {
                assert!(matches!(error, CallFailure::Transport(_)));
                assert!(error
                    .display()
                    .1
                    .starts_with("daemon reported route closure:"));
            } else {
                assert!(matches!(error, CallFailure::Unknown(_)));
                assert_eq!(
                    error.display(),
                    (
                        EXIT_TRANSPORT,
                        "transport timeout: the write outcome is unknown".into()
                    )
                );
            }
            consumer.close().await;
            server.abort();
            let _ = server.await;
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}

#[cfg(test)]
mod launch_path_tests {
    use super::*;

    fn command(face: Face, verb: &str, args: &[&str]) -> Command {
        Command {
            face,
            verb: verb.into(),
            args: args.iter().map(|arg| (*arg).into()).collect(),
            connection: None,
            json: false,
        }
    }

    #[test]
    fn every_windows_path_class_is_classified_on_every_os() {
        for input in ["C:", "C:repo"] {
            assert_eq!(path_class(input), PathClass::DriveRelative, "{input}");
        }
        for input in ["\\", "\\repo", "/repo"] {
            assert_eq!(path_class(input), PathClass::RootedNoDrive, "{input}");
        }
        for input in [
            "repo",
            "C:\\repo",
            "C:/repo",
            "\\\\server\\share\\repo",
            "//server/share/repo",
            "\\\\?\\C:\\repo",
            "1:repo",
            "",
            "\\/repo",
        ] {
            assert_eq!(path_class(input), PathClass::Ordinary, "{input}");
        }
    }

    #[test]
    fn face_mapping_accepts_paths_case_and_one_exe_suffix() {
        for (stem, expected) in [
            ("projects", Face::Projects),
            ("workspaces", Face::Workspaces),
            ("agents", Face::Agents),
            ("entorhinal", Face::Entorhinal),
        ] {
            for name in [
                format!("ck-{stem}"),
                format!("ckdev-{stem}"),
                format!("/usr/local/bin/ck-{stem}"),
                format!("C:\\tools\\ckdev-{stem}.exe"),
                format!("CKDEV-{}.EXE", stem.to_ascii_uppercase()),
            ] {
                assert_eq!(face_from_argv0(Some(&name)), expected, "{name}");
            }
        }
        assert_eq!(
            face_from_argv0(Some("ck-projects.exe.exe")),
            Face::Entorhinal
        );
        assert_eq!(face_from_argv0(Some("ck-projects.other")), Face::Entorhinal);
    }

    #[test]
    fn cwd_read_failure_refuses_before_dialing() {
        let mut stderr = Vec::new();
        let mut command = command(Face::Projects, "list", &[]);
        let (dir, _) = crate::tests::scratch_descriptor("cli-cwd-failure");
        command.connection = Some(dir.join("absent.json"));
        let exit = run_with_directory(
            command,
            || Err(std::io::Error::other("injected cwd failure")),
            &mut stderr,
        );
        assert_eq!(exit, ExitCode::from(EXIT_REFUSED));
        assert_eq!(
            String::from_utf8(stderr).unwrap(),
            "cwd_unreadable: injected cwd failure\n"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn all_mutations_use_core_paths_and_refuse_missing_directories() {
        let (dir, _) = crate::tests::scratch_descriptor("cli-paths");
        let root = dir.to_str().unwrap();
        let expected = entorhinal_core::RegistryStore::canonical_mutation_root(root).unwrap();
        for (face, verb, args, field) in [
            (Face::Projects, "register", vec!["name", root], "roots"),
            (Face::Projects, "add-root", vec!["p1", root], "root"),
            (Face::Projects, "attach", vec![root, "--yes"], "path"),
            (Face::Workspaces, "set-root", vec!["w1", root], "root"),
        ] {
            let params = build(&command(face, verb, &args)).unwrap().1;
            let sent = if field == "roots" {
                &params[field][0]
            } else {
                &params[field]
            };
            assert_eq!(sent, &json!(expected));
            let missing = dir.join("missing");
            let missing = missing.to_str().unwrap();
            let args: Vec<_> = args
                .iter()
                .map(|arg| if *arg == root { missing } else { *arg })
                .collect();
            let mut stderr = Vec::new();
            let mut missing_command = command(face, verb, &args);
            missing_command.connection = Some(dir.join("absent.json"));
            assert_eq!(
                run_with_directory(missing_command, std::env::current_dir, &mut stderr),
                ExitCode::from(EXIT_REFUSED)
            );
            assert!(String::from_utf8(stderr).unwrap().contains(missing));
        }
        #[cfg(unix)]
        for name in ["C:repo", "\\repo"] {
            std::fs::create_dir(dir.join(name)).unwrap();
            assert_eq!(
                cli_path(name, root, true).unwrap(),
                entorhinal_core::RegistryStore::canonical_mutation_root(
                    dir.join(name).to_str().unwrap()
                )
                .unwrap()
            );
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn default_connect_failure_preserves_sdk_display() {
        let (dir, _) = crate::tests::scratch_descriptor("discovery-display");
        let error = subc_client_rs::SubcConsumer::connect(
            &dir.join("absent.json"),
            subc_client_rs::ConsumerOptions::default(),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(connect_failure(None, &error), format!("connect: {error}"));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn non_unicode_canonical_paths_and_cwd_are_refused() {
        use std::os::unix::ffi::OsStringExt;
        let (dir, _) = crate::tests::scratch_descriptor("cli-non-unicode");
        let invalid = dir.join(std::ffi::OsString::from_vec(vec![0xff]));
        if let Err(error) = std::fs::create_dir(&invalid) {
            eprintln!("non_unicode_canonical_paths_and_cwd_are_refused skipped: {error}");
            std::fs::remove_dir_all(dir).unwrap();
            return;
        }
        let alias = dir.join("alias");
        std::os::unix::fs::symlink(&invalid, &alias).unwrap();
        let raw = alias.to_str().unwrap();
        assert!(cli_path(raw, "/", true)
            .unwrap_err()
            .contains("path_not_unicode"));
        assert!(cli_path(raw, "/", false)
            .unwrap_err()
            .contains("path_not_unicode"));
        assert!(
            cli_path(alias.join("missing").to_str().unwrap(), "/", false)
                .unwrap_err()
                .contains("path_not_unicode")
        );
        fn invalid_cwd() -> std::io::Result<PathBuf> {
            Ok(PathBuf::from(std::ffi::OsString::from_vec(vec![
                b'/', 0xff,
            ])))
        }
        let mut stderr = Vec::new();
        assert_eq!(
            run_with_directory(
                command(Face::Projects, "list", &[]),
                invalid_cwd,
                &mut stderr
            ),
            ExitCode::from(EXIT_REFUSED)
        );
        assert!(String::from_utf8(stderr)
            .unwrap()
            .contains("path_not_unicode"));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn windows_call_site_refuses_ambiguous_but_not_unc_paths() {
        for (value, class) in [
            ("C:", "drive-relative"),
            ("C:repo", "drive-relative"),
            ("\\", "rooted-no-drive"),
            ("\\repo", "rooted-no-drive"),
            ("/repo", "rooted-no-drive"),
        ] {
            let error = cli_path(value, "C:\\scratch", true).unwrap_err();
            assert!(error.contains(class));
            assert_eq!(build_failure_exit(&error), EXIT_USAGE);
        }
        let error = cli_path("\\\\server\\share\\absent", "C:\\scratch", true).unwrap_err();
        assert!(!error.contains("rooted-no-drive"));
    }
}
