//! Root bindings: which physical checkout a registered root is, since when, and
//! whether the operator has approved it.
//!
//! A registered root is a path, and a path outlives the checkout at it: delete a
//! repository, clone a different one to the same folder, and every record keyed
//! by the path now describes the new checkout. Three facts close that gap:
//!
//! - **Incarnation.** At binding, a random token is written to
//!   `<git common dir>/cortexkit-root-incarnation`. It is local git metadata,
//!   never tracked content, so a fresh clone has none and gets a new one. A
//!   worktree reaches the same common directory, so it shares its repository's
//!   token.
//! - **Registration epoch.** A random value minted for each binding lifetime.
//!   Removing a root retires its epoch; adding the same unchanged checkout back
//!   keeps the token but gets a new epoch, so anything captured under the old
//!   one stays unapproved.
//! - **Approval.** Keyed by epoch, so it can never carry over to a new lifetime
//!   or a replaced checkout.
//!
//! Every change is a journal entry carrying the values it wrote, so a rebuild
//! reproduces the bindings without reading the disk.

use std::{
    fs,
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
};

#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{
    canonical_query_path, implicit_project_id, mutations::domain, mutations::Action,
    path_prefix_or_equal, RegistryError, RegistryStore, ResolveReply,
};

/// Migration 3. `derived_root_parent` gains the binding a worker container was
/// attached under, so a worktree inside it can be checked against that binding.
pub const V3_ROOT_BINDINGS: &str = r#"
CREATE TABLE root_binding (
    canonical_root TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    incarnation TEXT NOT NULL,
    registration_epoch TEXT NOT NULL UNIQUE,
    bound_seq INTEGER NOT NULL
);
CREATE TABLE retired_binding (
    registration_epoch TEXT PRIMARY KEY,
    canonical_root TEXT NOT NULL,
    project_id TEXT NOT NULL,
    incarnation TEXT NOT NULL,
    reason TEXT NOT NULL CHECK (reason IN ('removed', 'replaced')),
    retired_seq INTEGER NOT NULL
);
CREATE TABLE root_approval (
    registration_epoch TEXT PRIMARY KEY,
    approved_seq INTEGER NOT NULL
);
ALTER TABLE derived_root_parent ADD COLUMN source_root TEXT NULL;
ALTER TABLE derived_root_parent ADD COLUMN registration_epoch TEXT NULL;
"#;

/// The token file's name inside the git common directory.
pub const INCARNATION_FILE: &str = "cortexkit-root-incarnation";

/// Where binding tokens and epochs come from. Random in production; a counter in
/// tests, so golden replies are byte-stable.
#[derive(Debug)]
pub(crate) enum IdSource {
    Random,
    #[cfg(test)]
    Sequence(AtomicU64),
}

impl IdSource {
    pub(crate) fn hex(&self, bytes: usize) -> Result<String, RegistryError> {
        let mut buffer = vec![0u8; bytes];
        match self {
            Self::Random => getrandom::getrandom(&mut buffer)
                .map_err(|error| domain("entropy_unavailable", error.to_string()))?,
            #[cfg(test)]
            Self::Sequence(next) => {
                let value = next.fetch_add(1, Ordering::Relaxed) + 1;
                buffer[bytes - 8..].copy_from_slice(&value.to_be_bytes());
            }
        }
        Ok(buffer.iter().map(|byte| format!("{byte:02x}")).collect())
    }
}

/// One root of a project, as a caller needs it to decide whether work may run
/// there.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RootRecord {
    pub root: String,
    pub remotes: Vec<GitRemote>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_key: Option<crate::RootKey>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_key_mismatch: Option<bool>,
    /// Null unless `identity` is `bound`.
    pub incarnation: Option<Incarnation>,
    /// Null unless `identity` is `bound`.
    pub registration_epoch: Option<String>,
    /// `bound`: the checkout on disk is the one bound.
    /// `unbound`: registered, never bound (no binding yet).
    /// `unverifiable`: the checkout's identity cannot be read (no git metadata,
    /// a missing worktree admin directory, a malformed token).
    /// `replaced`: the token on disk is not the bound one.
    /// `retired`: a worktree whose container was attached under a binding that
    /// has since been retired.
    pub identity: &'static str,
    pub approval: Approval,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Incarnation {
    pub kind: &'static str,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Approval {
    /// `approved` only for a `bound` root whose current epoch is approved.
    pub state: &'static str,
    /// The journal entry that approved it; null when unapproved.
    pub journal_seq: Option<i64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct GitRemote {
    pub name: String,
    pub owner: String,
    pub repo: String,
    pub owned: bool,
}

/// The fields a resolve reply carries when root records are enabled. All are
/// serialized, as null where they do not apply, so a reader can tell "not
/// applicable" from "an older producer".
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RootFields {
    /// The registered root (root or walk) or the worktree top level
    /// (containment) the query falls in. Never a query subfolder.
    pub matched_root: Option<String>,
    /// For containment, the registered root the worktree was made from.
    /// Provenance only, not an extra root.
    pub source_root: Option<String>,
    /// The whole project for root and walk; the single worktree for
    /// containment; empty when the query authorizes nothing.
    pub root_records: Vec<RootRecord>,
    /// Answers a request carrying `executionBinding`, null otherwise.
    /// `active`: that binding is the root's current one and the checkout on
    /// disk is still it. `retired`: that binding ended when the root was
    /// removed. `replaced`: the checkout at the path is a different one.
    /// `unknown`: no record of that binding, or the disk cannot be read.
    pub binding_status: Option<&'static str>,
}

/// What an execution recorded about the root it was admitted under.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionBinding {
    pub root: String,
    pub incarnation: String,
    pub registration_epoch: String,
}

/// Result of reading a checkout's identity from disk.
#[derive(Debug, Clone, PartialEq, Eq)]
enum DiskIdentity {
    Token(String),
    NoToken,
    Unverifiable(String),
}

/// The git common directory for a checkout, read from the filesystem only.
///
/// A `.git` directory is the common directory. A `.git` file names a worktree
/// admin directory, whose `commondir` names the common directory; a missing
/// admin directory or `commondir` is an error, because a pointer to where an
/// admin directory would be says nothing about which repository is there.
fn git_common_dir(root: &Path) -> Result<PathBuf, String> {
    let dot_git = root.join(".git");
    let metadata = fs::symlink_metadata(&dot_git)
        .map_err(|error| format!("{}: {error}", dot_git.display()))?;
    if metadata.is_dir() {
        return fs::canonicalize(&dot_git).map_err(|error| error.to_string());
    }
    let text = fs::read_to_string(&dot_git).map_err(|error| error.to_string())?;
    let target = text
        .trim()
        .strip_prefix("gitdir:")
        .map(str::trim)
        .ok_or_else(|| format!("{}: not a gitdir pointer", dot_git.display()))?;
    let admin = absolute_from(root, target);
    if !admin.is_dir() {
        return Err(format!(
            "worktree admin directory {} is missing",
            admin.display()
        ));
    }
    let common = fs::read_to_string(admin.join("commondir"))
        .map_err(|error| format!("{}/commondir: {error}", admin.display()))?;
    fs::canonicalize(absolute_from(&admin, common.trim())).map_err(|error| error.to_string())
}

fn absolute_from(base: &Path, value: &str) -> PathBuf {
    let path = Path::new(value);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

fn is_token(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn read_token(common: &Path) -> DiskIdentity {
    match fs::read_to_string(common.join(INCARNATION_FILE)) {
        Ok(text) => match text.strip_suffix('\n') {
            Some(token) if is_token(token) => DiskIdentity::Token(token.to_string()),
            _ => DiskIdentity::Unverifiable(format!(
                "{} is malformed",
                common.join(INCARNATION_FILE).display()
            )),
        },
        Err(error) if error.kind() == ErrorKind::NotFound => DiskIdentity::NoToken,
        Err(error) => DiskIdentity::Unverifiable(error.to_string()),
    }
}

fn disk_identity(root: &Path) -> DiskIdentity {
    match git_common_dir(root) {
        Ok(common) => read_token(&common),
        Err(reason) => DiskIdentity::Unverifiable(reason),
    }
}

/// Create the token file exclusively. If another writer got there first, its
/// token is the checkout's identity and is returned instead.
fn create_token(common: &Path, token: &str) -> Result<String, String> {
    let path = common.join(INCARNATION_FILE);
    match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
    {
        Ok(mut file) => {
            file.write_all(format!("{token}\n").as_bytes())
                .and_then(|()| file.sync_all())
                .map_err(|error| format!("{}: {error}", path.display()))?;
            Ok(token.to_string())
        }
        Err(error) if error.kind() == ErrorKind::AlreadyExists => match read_token(common) {
            DiskIdentity::Token(existing) => Ok(existing),
            DiskIdentity::NoToken => Err(format!("{} vanished while binding", path.display())),
            DiskIdentity::Unverifiable(reason) => Err(reason),
        },
        Err(error) => Err(format!("{}: {error}", path.display())),
    }
}

/// GitHub remotes from the checkout's git config: `[remote "<name>"]` sections
/// whose `url` names github.com. Other hosts are left out; the reply's remote
/// shape is GitHub's owner/repo.
pub(crate) fn github_remotes(root: &Path) -> Vec<GitRemote> {
    let Ok(common) = git_common_dir(root) else {
        return Vec::new();
    };
    let Ok(config) = fs::read_to_string(common.join("config")) else {
        return Vec::new();
    };
    let mut remotes = Vec::new();
    let mut current: Option<String> = None;
    for line in config.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            current = line
                .strip_prefix("[remote \"")
                .and_then(|rest| rest.strip_suffix("\"]"))
                .map(str::to_string);
            continue;
        }
        let (Some(name), Some((key, value))) = (current.as_ref(), line.split_once('=')) else {
            continue;
        };
        if key.trim() != "url" {
            continue;
        }
        if let Some((owner, repo)) = parse_github_url(value.trim()) {
            remotes.push(GitRemote {
                name: name.clone(),
                owner,
                repo,
                owned: name == "origin",
            });
        }
    }
    remotes.sort_by(|a, b| a.name.cmp(&b.name));
    remotes.dedup();
    remotes
}

fn parse_github_url(url: &str) -> Option<(String, String)> {
    let rest = url
        .strip_prefix("git@github.com:")
        .or_else(|| url.strip_prefix("https://github.com/"))
        .or_else(|| url.strip_prefix("http://github.com/"))
        .or_else(|| url.strip_prefix("ssh://git@github.com/"))?;
    let rest = rest.trim_end_matches('/');
    let rest = rest.strip_suffix(".git").unwrap_or(rest);
    let (owner, repo) = rest.split_once('/')?;
    (!owner.is_empty() && !repo.is_empty() && !repo.contains('/'))
        .then(|| (owner.to_string(), repo.to_string()))
}

#[derive(Debug, Clone)]
struct BindingRow {
    project_id: String,
    incarnation: String,
    epoch: String,
}

fn active_binding(conn: &Connection, root: &str) -> rusqlite::Result<Option<BindingRow>> {
    conn.query_row(
        "SELECT project_id, incarnation, registration_epoch FROM root_binding WHERE canonical_root = ?1",
        [root],
        |row| {
            Ok(BindingRow {
                project_id: row.get(0)?,
                incarnation: row.get(1)?,
                epoch: row.get(2)?,
            })
        },
    )
    .optional()
}

fn approval_for(conn: &Connection, epoch: &str) -> rusqlite::Result<Approval> {
    let seq = conn
        .query_row(
            "SELECT approved_seq FROM root_approval WHERE registration_epoch = ?1",
            [epoch],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;
    Ok(match seq {
        Some(seq) => Approval {
            state: "approved",
            journal_seq: Some(seq),
        },
        None => unapproved(),
    })
}

fn unapproved() -> Approval {
    Approval {
        state: "unapproved",
        journal_seq: None,
    }
}

/// The record for one registered root, with its identity checked against disk.
fn root_record(conn: &Connection, root: &str) -> rusqlite::Result<RootRecord> {
    let remotes = super::ownership::root_remotes(conn, root)?;
    let root_key = crate::root_keys::mapped_key(conn, root)?;
    let root_key_mismatch = root_key
        .as_ref()
        .map(|key| crate::root_keys::mismatch(key, &remotes));
    let binding = active_binding(conn, root)?;
    let disk = disk_identity(Path::new(root));
    let (identity, binding) = match (binding, &disk) {
        (None, DiskIdentity::Unverifiable(_)) => ("unverifiable", None),
        (None, _) => ("unbound", None),
        (Some(_), DiskIdentity::Unverifiable(_)) => ("unverifiable", None),
        (Some(bound), DiskIdentity::Token(token)) if *token == bound.incarnation => {
            ("bound", Some(bound))
        }
        (Some(_), _) => ("replaced", None),
    };
    let approval = match &binding {
        Some(bound) => approval_for(conn, &bound.epoch)?,
        None => unapproved(),
    };
    Ok(RootRecord {
        root: root.to_string(),
        remotes,
        root_key,
        root_key_mismatch,
        incarnation: binding.as_ref().map(|bound| Incarnation {
            kind: "token",
            value: bound.incarnation.clone(),
        }),
        registration_epoch: binding.map(|bound| bound.epoch),
        identity,
        approval,
    })
}

/// Records for every local root of a project, in root order.
pub(crate) fn root_records(
    conn: &Connection,
    project_id: &str,
) -> rusqlite::Result<Vec<RootRecord>> {
    let mut statement = conn.prepare(
        "SELECT canonical_root FROM project_root WHERE project_id = ?1 ORDER BY canonical_root",
    )?;
    let roots = statement
        .query_map([project_id], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    roots.iter().map(|root| root_record(conn, root)).collect()
}

/// A worktree inside an attached container. It carries its source root's
/// binding only when the container was attached under that root's current
/// epoch and the worktree reaches the same repository.
fn worktree_record(
    conn: &Connection,
    worktree: &str,
    source_root: Option<&str>,
    attached_epoch: Option<&str>,
) -> rusqlite::Result<RootRecord> {
    let names = super::ownership::effective_owned_names(conn, source_root.unwrap_or(worktree))?;
    let mut remotes = github_remotes(Path::new(worktree));
    for remote in &mut remotes {
        remote.owned = names.contains(&remote.name);
    }
    let none = |identity| RootRecord {
        root: worktree.to_string(),
        remotes: remotes.clone(),
        root_key: None,
        root_key_mismatch: None,
        incarnation: None,
        registration_epoch: None,
        identity,
        approval: unapproved(),
    };
    let (Some(source), Some(epoch)) = (source_root, attached_epoch) else {
        return Ok(none("unbound"));
    };
    let Some(bound) = active_binding(conn, source)? else {
        return Ok(none("retired"));
    };
    if bound.epoch != epoch {
        return Ok(none("retired"));
    }
    match disk_identity(Path::new(worktree)) {
        DiskIdentity::Token(token) if token == bound.incarnation => {}
        DiskIdentity::Unverifiable(_) => return Ok(none("unverifiable")),
        _ => return Ok(none("replaced")),
    }
    let approval = approval_for(conn, &bound.epoch)?;
    Ok(RootRecord {
        root: worktree.to_string(),
        remotes,
        root_key: None,
        root_key_mismatch: None,
        incarnation: Some(Incarnation {
            kind: "token",
            value: bound.incarnation,
        }),
        registration_epoch: Some(bound.epoch),
        identity: "bound",
        approval,
    })
}

fn binding_status(conn: &Connection, binding: &ExecutionBinding) -> rusqlite::Result<&'static str> {
    if let Some(bound) = active_binding(conn, &binding.root)? {
        if bound.epoch == binding.registration_epoch && bound.incarnation == binding.incarnation {
            return Ok(match disk_identity(Path::new(&binding.root)) {
                DiskIdentity::Token(token) if token == binding.incarnation => "active",
                DiskIdentity::Unverifiable(_) => "unknown",
                _ => "replaced",
            });
        }
    }
    let retired = conn
        .query_row(
            "SELECT canonical_root, incarnation, reason FROM retired_binding WHERE registration_epoch = ?1",
            [&binding.registration_epoch],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?;
    Ok(match retired {
        Some((root, incarnation, reason))
            if root == binding.root && incarnation == binding.incarnation =>
        {
            if reason == "replaced" {
                "replaced"
            } else {
                "retired"
            }
        }
        _ => "unknown",
    })
}

/// Retire every active binding of a project, as part of the journal entry that
/// removes it. Approvals go with their epoch.
pub(crate) fn retire_project_bindings(
    tx: &Transaction<'_>,
    project_id: &str,
    seq: i64,
) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO retired_binding (registration_epoch, canonical_root, project_id, incarnation, reason, retired_seq)
         SELECT registration_epoch, canonical_root, project_id, incarnation, 'removed', ?2
         FROM root_binding WHERE project_id = ?1",
        params![project_id, seq],
    )?;
    tx.execute(
        "DELETE FROM root_approval WHERE registration_epoch IN
         (SELECT registration_epoch FROM root_binding WHERE project_id = ?1)",
        [project_id],
    )?;
    tx.execute(
        "DELETE FROM root_binding WHERE project_id = ?1",
        [project_id],
    )?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BindPayload {
    canonical_root: String,
    project_id: String,
    incarnation: String,
    registration_epoch: String,
    /// The epoch this binding replaced, when the checkout on disk changed.
    #[serde(default)]
    replaced_epoch: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApprovalPayload {
    canonical_root: String,
    registration_epoch: String,
}

fn apply_bind(tx: &Transaction<'_>, payload: &BindPayload, seq: i64) -> rusqlite::Result<()> {
    if let Some(old) = &payload.replaced_epoch {
        tx.execute(
            "INSERT INTO retired_binding (registration_epoch, canonical_root, project_id, incarnation, reason, retired_seq)
             SELECT registration_epoch, canonical_root, project_id, incarnation, 'replaced', ?2
             FROM root_binding WHERE registration_epoch = ?1",
            params![old, seq],
        )?;
        tx.execute(
            "DELETE FROM root_approval WHERE registration_epoch = ?1",
            [old],
        )?;
        tx.execute(
            "DELETE FROM root_binding WHERE registration_epoch = ?1",
            [old],
        )?;
    }
    tx.execute(
        "INSERT INTO root_binding (canonical_root, project_id, incarnation, registration_epoch, bound_seq)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            payload.canonical_root,
            payload.project_id,
            payload.incarnation,
            payload.registration_epoch,
            seq
        ],
    )?;
    Ok(())
}

/// Replay one binding journal entry. Returns false for ops this module does not
/// own, so the caller replays those itself.
pub(crate) fn replay_binding_op(
    tx: &Transaction<'_>,
    seq: i64,
    op: &str,
    value: &Value,
) -> rusqlite::Result<bool> {
    let decode =
        |error: serde_json::Error| rusqlite::Error::ToSqlConversionFailure(Box::new(error));
    match op {
        "bind_root" => {
            let payload: BindPayload = serde_json::from_value(value.clone()).map_err(decode)?;
            apply_bind(tx, &payload, seq)?;
        }
        "approve_root" => {
            let payload: ApprovalPayload = serde_json::from_value(value.clone()).map_err(decode)?;
            tx.execute(
                "INSERT OR IGNORE INTO root_approval (registration_epoch, approved_seq) VALUES (?1, ?2)",
                params![payload.registration_epoch, seq],
            )?;
        }
        "unapprove_root" => {
            let payload: ApprovalPayload = serde_json::from_value(value.clone()).map_err(decode)?;
            tx.execute(
                "DELETE FROM root_approval WHERE registration_epoch = ?1",
                [payload.registration_epoch],
            )?;
        }
        _ => return Ok(false),
    }
    Ok(true)
}

/// What `bind_unbound_roots` did with each root it looked at.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BindReport {
    pub root: String,
    /// `bound`, or the refusal code.
    pub outcome: String,
}

impl RegistryStore {
    /// Turn root records on or off for this store's replies.
    pub fn set_root_records(&mut self, enabled: bool) {
        self.root_records = enabled;
    }

    pub fn root_records_enabled(&self) -> bool {
        self.root_records
    }

    #[cfg(test)]
    pub(crate) fn use_sequential_ids(&mut self) {
        self.ids = std::sync::Arc::new(IdSource::Sequence(AtomicU64::new(0)));
    }

    pub fn bind_root(&self, canonical_root: &str, actor: &str) -> Result<Vec<u8>, RegistryError> {
        self.with_principal("entorhinal")
            .bind_root(canonical_root, actor)
    }

    pub fn bind_unbound_roots(&self, actor: &str) -> Result<Vec<BindReport>, RegistryError> {
        self.with_principal("entorhinal").bind_unbound_roots(actor)
    }

    pub fn approve_root(
        &self,
        canonical_root: &str,
        actor: &str,
    ) -> Result<Vec<u8>, RegistryError> {
        self.with_principal("entorhinal")
            .approve_root(canonical_root, actor)
    }

    pub fn unapprove_root(
        &self,
        canonical_root: &str,
        actor: &str,
    ) -> Result<Vec<u8>, RegistryError> {
        self.with_principal("entorhinal")
            .unapprove_root(canonical_root, actor)
    }
}

impl super::JournalWriter<'_> {
    /// Bind a registered root to the checkout on disk. A root already bound to
    /// the checkout that is there is left alone. A root whose checkout was
    /// replaced gets a new binding; the old epoch is retired as `replaced` and
    /// its approval goes with it.
    pub fn bind_root(&self, canonical_root: &str, actor: &str) -> Result<Vec<u8>, RegistryError> {
        let root = Path::new(canonical_root);
        let common = git_common_dir(root).map_err(|reason| {
            domain(
                "root_identity_unverifiable",
                format!("{canonical_root}: {reason}"),
            )
        })?;
        let incarnation = match read_token(&common) {
            DiskIdentity::Token(token) => token,
            DiskIdentity::NoToken => create_token(&common, &self.ids.hex(32)?)
                .map_err(|reason| domain("root_identity_unverifiable", reason))?,
            DiskIdentity::Unverifiable(reason) => {
                return Err(domain("root_identity_unverifiable", reason))
            }
        };
        let epoch = self.ids.hex(16)?;
        let root_key = canonical_root.to_string();
        let actor = actor.to_string();
        self.mutation("bind_root", None, move |tx| {
            let project_id = tx
                .query_row(
                    "SELECT project_id FROM project_root WHERE canonical_root = ?1",
                    [&root_key],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
                .ok_or_else(|| {
                    domain("not_found", format!("{root_key} is not a registered root"))
                })?;
            let current = active_binding(tx, &root_key)?;
            if let Some(bound) = &current {
                if bound.incarnation == incarnation && bound.project_id == project_id {
                    let value =
                        json!({"canonicalRoot": root_key, "registrationEpoch": bound.epoch});
                    return Ok(Action {
                        changed: false,
                        seq: None,
                        value: value.clone(),
                        payload: value,
                    });
                }
            }
            let payload = BindPayload {
                canonical_root: root_key.clone(),
                project_id,
                incarnation: incarnation.clone(),
                registration_epoch: epoch.clone(),
                replaced_epoch: current.map(|bound| bound.epoch),
            };
            let payload_value = serde_json::to_value(&payload)
                .map_err(|error| domain("encode_failed", error.to_string()))?;
            let seq = super::mutations::append(
                tx,
                "bind_root",
                &payload_value,
                &actor,
                None,
                super::now_unix_millis(),
                self.principal,
            )?;
            apply_bind(tx, &payload, seq)?;
            Ok(Action {
                changed: true,
                seq: Some(seq),
                value: json!({"canonicalRoot": root_key, "registrationEpoch": epoch}),
                payload: payload_value,
            })
        })
    }

    /// Bind every registered root that has no binding. A root whose identity
    /// cannot be read stays unbound and is reported; it answers `unverifiable`
    /// and authorizes nothing, which is where it should stay until fixed.
    pub fn bind_unbound_roots(&self, actor: &str) -> Result<Vec<BindReport>, RegistryError> {
        let roots = self.read(|conn| {
            let mut statement = conn.prepare(
                "SELECT pr.canonical_root FROM project_root pr
                 LEFT JOIN root_binding rb ON rb.canonical_root = pr.canonical_root
                 WHERE rb.canonical_root IS NULL ORDER BY pr.canonical_root",
            )?;
            let rows = statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>();
            rows
        })?;
        let mut report = Vec::with_capacity(roots.len());
        for root in roots {
            let outcome = match self.bind_root(&root, actor) {
                Ok(_) => "bound".to_string(),
                Err(RegistryError::Domain { code, .. }) => code,
                Err(error) => return Err(error),
            };
            report.push(BindReport { root, outcome });
        }
        Ok(report)
    }

    /// Approve a root's current binding. Refused unless the checkout on disk is
    /// the bound one, so an approval can never land on a replaced checkout.
    pub fn approve_root(
        &self,
        canonical_root: &str,
        actor: &str,
    ) -> Result<Vec<u8>, RegistryError> {
        self.set_approval(canonical_root, actor, true)
    }

    /// Withdraw a root's approval.
    pub fn unapprove_root(
        &self,
        canonical_root: &str,
        actor: &str,
    ) -> Result<Vec<u8>, RegistryError> {
        self.set_approval(canonical_root, actor, false)
    }

    fn set_approval(
        &self,
        canonical_root: &str,
        actor: &str,
        approve: bool,
    ) -> Result<Vec<u8>, RegistryError> {
        let op = if approve {
            "approve_root"
        } else {
            "unapprove_root"
        };
        let root_key = canonical_query_path(Path::new(canonical_root))?.0;
        let actor = actor.to_string();
        self.mutation(op, None, move |tx| {
            let record = root_record(tx, &root_key)?;
            let Some(epoch) = record.registration_epoch.clone() else {
                return Err(domain(
                    if record.identity == "replaced" {
                        "root_replaced"
                    } else {
                        "root_not_bound"
                    },
                    format!("{root_key} is {}", record.identity),
                ));
            };
            let approved = record.approval.state == "approved";
            let value = json!({"canonicalRoot": root_key, "registrationEpoch": epoch});
            if approved == approve {
                return Ok(Action {
                    changed: false,
                    seq: None,
                    value: value.clone(),
                    payload: value,
                });
            }
            let payload = serde_json::to_value(ApprovalPayload {
                canonical_root: root_key.clone(),
                registration_epoch: epoch.clone(),
            })
            .map_err(|error| domain("encode_failed", error.to_string()))?;
            let seq = super::mutations::append(
                tx,
                op,
                &payload,
                &actor,
                None,
                super::now_unix_millis(),
                self.principal,
            )?;
            replay_binding_op(tx, seq, op, &payload)?;
            Ok(Action {
                changed: true,
                seq: Some(seq),
                value,
                payload,
            })
        })
    }
}

impl RegistryStore {
    /// Resolve with ancestor walking and root records. Used when root records
    /// are enabled; otherwise `resolve` answers in the legacy shape.
    ///
    /// From the query upward, at each directory: a registered root answers
    /// (`root` at the query itself, `walk` above it); a `.git` file inside an
    /// attached container is a worker worktree (`containment`); any other
    /// directory with its own `.git` is a separate checkout, and the walk stops
    /// there rather than reaching past it into an enclosing project. A path in
    /// a container that is not inside a worktree answers `containment` with no
    /// records, which authorizes nothing.
    pub(crate) fn resolve_walk(
        &self,
        raw_path: &str,
        binding: Option<&ExecutionBinding>,
    ) -> Result<ResolveReply, RegistryError> {
        let (canonical_path, path_exists) = canonical_query_path(Path::new(raw_path))?;
        self.read(|conn| {
            let generation = self.generation_from_connection(conn)?;
            let binding_status = match binding {
                Some(binding) => Some(binding_status(conn, binding)?),
                None => None,
            };
            let with_fields = |mut reply: ResolveReply,
                               matched: Option<String>,
                               source: Option<String>,
                               records: Vec<RootRecord>| {
                reply.canonical_root = Some(canonical_path.clone());
                reply.root_fields = Some(RootFields {
                    matched_root: matched,
                    source_root: source,
                    root_records: records,
                    binding_status,
                });
                reply
            };
            let containers = containers(conn)?;
            let query = PathBuf::from(&canonical_path);
            let mut dir = Some(query.as_path());
            while let Some(current) = dir {
                let key = current.to_string_lossy().into_owned();
                if let Some(project_id) = conn
                    .query_row(
                        "SELECT project_id FROM project_root WHERE canonical_root = ?1",
                        [&key],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?
                {
                    let via = if current == query { "root" } else { "walk" };
                    let reply =
                        self.reply_for_project(conn, &project_id, via, !path_exists, generation)?;
                    let records = root_records(conn, &project_id)?;
                    return Ok(with_fields(reply, Some(key), None, records));
                }
                if fs::symlink_metadata(current.join(".git")).is_ok() {
                    let is_worktree_pointer = current.join(".git").is_file();
                    if let Some(container) = containers
                        .iter()
                        .filter(|container| {
                            is_worktree_pointer
                                && path_prefix_or_equal(Path::new(&container.parent), current)
                        })
                        .max_by_key(|container| container.parent.len())
                    {
                        let reply = self.reply_for_project(
                            conn,
                            &container.project_id,
                            "containment",
                            !path_exists,
                            generation,
                        )?;
                        let record = worktree_record(
                            conn,
                            &key,
                            container.source_root.as_deref(),
                            container.epoch.as_deref(),
                        )?;
                        return Ok(with_fields(
                            reply,
                            Some(key),
                            container.source_root.clone(),
                            vec![record],
                        ));
                    }
                    break;
                }
                dir = current.parent();
            }
            if let Some(container) = containers
                .iter()
                .filter(|container| path_prefix_or_equal(Path::new(&container.parent), &query))
                .max_by_key(|container| container.parent.len())
            {
                let reply = self.reply_for_project(
                    conn,
                    &container.project_id,
                    "containment",
                    !path_exists,
                    generation,
                )?;
                return Ok(with_fields(reply, None, None, Vec::new()));
            }
            let reply = ResolveReply {
                project_id: implicit_project_id(&canonical_path),
                workspace_id: None,
                workspace_root: None,
                project_name: None,
                via: "implicit".to_string(),
                gone: !path_exists,
                generation,
                canonical_root: None,
                root_fields: None,
            };
            Ok(with_fields(reply, None, None, Vec::new()))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AddRootRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub project_id: String,
    pub root: String,
    #[serde(default)]
    pub actor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RemoveRootRequest {
    pub project_id: String,
    pub root: String,
    #[serde(default)]
    pub actor: Option<String>,
}

/// Attach a worker container to a root's current approved binding, before any
/// worktree is created in it. It can never add or replace a root.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AttachDerivedParentRequest {
    pub project_id: String,
    pub root: String,
    pub incarnation: String,
    pub registration_epoch: String,
    pub container: String,
    #[serde(default)]
    pub actor: Option<String>,
}

/// The registered root that contains `path` or that `path` contains, other
/// than `path` itself. Roots never nest, so every folder resolves to at most
/// one registered root.
fn nested_root(conn: &Connection, path: &str) -> rusqlite::Result<Option<(String, String)>> {
    let mut statement = conn.prepare("SELECT canonical_root, project_id FROM project_root")?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows.into_iter().find(|(root, _)| {
        root != path
            && (path_prefix_or_equal(Path::new(root), Path::new(path))
                || path_prefix_or_equal(Path::new(path), Path::new(root)))
    }))
}

/// Root attachment uses the same path fences as an explicit add_root.
pub(crate) fn check_root_location(conn: &Connection, root: &str) -> Result<(), RegistryError> {
    if let Some((nested, owner)) = nested_root(conn, root)? {
        return Err(domain(
            "root_nested",
            format!("{root} overlaps {nested}, a root of {owner}"),
        ));
    }
    if let Some(container) = containers(conn)?.into_iter().find(|c| {
        path_prefix_or_equal(Path::new(&c.parent), Path::new(root))
            || path_prefix_or_equal(Path::new(root), Path::new(&c.parent))
    }) {
        return Err(domain(
            "root_overlaps_container",
            format!("{root} overlaps worker container {}", container.parent),
        ));
    }
    Ok(())
}

/// A checkout gets a fresh registration epoch, never an inherited approval.
/// Non-git label roots remain unbound, just like non-git roots added explicitly.
pub(crate) fn fresh_binding(
    root: &str,
    project: &str,
    ids: &IdSource,
) -> Result<Option<Value>, RegistryError> {
    if !Path::new(root).join(".git").exists() {
        return Ok(None);
    }
    let common = git_common_dir(Path::new(root))
        .map_err(|reason| domain("root_identity_unverifiable", reason))?;
    let incarnation = match read_token(&common) {
        DiskIdentity::Token(token) => token,
        DiskIdentity::NoToken => create_token(&common, &ids.hex(32)?)
            .map_err(|reason| domain("root_identity_unverifiable", reason))?,
        DiskIdentity::Unverifiable(reason) => {
            return Err(domain("root_identity_unverifiable", reason))
        }
    };
    Ok(Some(
        serde_json::to_value(BindPayload {
            canonical_root: root.into(),
            project_id: project.into(),
            incarnation,
            registration_epoch: ids.hex(16)?,
            replaced_epoch: None,
        })
        .map_err(|error| domain("encode_failed", error.to_string()))?,
    ))
}

/// Another project that already owns one of these GitHub repositories through
/// one of its roots. Only effective owned remotes participate, so a fork's
/// upstream does not claim the upstream project's repository.
pub(crate) fn repository_owner(
    conn: &Connection,
    project_id: &str,
    remotes: &[GitRemote],
) -> rusqlite::Result<Option<(String, String)>> {
    if remotes.is_empty() {
        return Ok(None);
    }
    let mut statement =
        conn.prepare("SELECT canonical_root, project_id FROM project_root WHERE project_id <> ?1")?;
    let rows = statement
        .query_map([project_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (root, owner) in rows {
        for theirs in super::ownership::root_remotes(conn, &root)?
            .into_iter()
            .filter(|r| r.owned)
        {
            if let Some(mine) = remotes.iter().find(|mine| {
                mine.owned
                    && mine.owner.eq_ignore_ascii_case(&theirs.owner)
                    && mine.repo.eq_ignore_ascii_case(&theirs.repo)
            }) {
                return Ok(Some((format!("{}/{}", mine.owner, mine.repo), owner)));
            }
        }
    }
    Ok(None)
}

fn apply_add_root(
    tx: &Transaction<'_>,
    request: &AddRootRequest,
    now: i64,
) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO project_root (canonical_root, project_id, added_at) VALUES (?1, ?2, ?3)",
        params![request.root, request.project_id, now],
    )?;
    tx.execute(
        "INSERT OR IGNORE INTO project_alias (old_id, project_id, created_at) VALUES (?1, ?2, ?3)",
        params![implicit_project_id(&request.root), request.project_id, now],
    )?;
    Ok(())
}

/// Removing a root retires its binding (and approval) and revokes every worker
/// container attached under it, in the same journal entry.
fn apply_remove_root(
    tx: &Transaction<'_>,
    request: &RemoveRootRequest,
    seq: i64,
) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO retired_binding (registration_epoch, canonical_root, project_id, incarnation, reason, retired_seq)
         SELECT registration_epoch, canonical_root, project_id, incarnation, 'removed', ?2
         FROM root_binding WHERE canonical_root = ?1",
        params![request.root, seq],
    )?;
    tx.execute(
        "DELETE FROM root_approval WHERE registration_epoch IN
         (SELECT registration_epoch FROM root_binding WHERE canonical_root = ?1)",
        [&request.root],
    )?;
    tx.execute(
        "DELETE FROM root_binding WHERE canonical_root = ?1",
        [&request.root],
    )?;
    tx.execute(
        "DELETE FROM derived_root_parent WHERE source_root = ?1",
        [&request.root],
    )?;
    tx.execute(
        "DELETE FROM project_alias WHERE old_id = ?1 AND project_id = ?2",
        params![implicit_project_id(&request.root), request.project_id],
    )?;
    tx.execute(
        "DELETE FROM project_root WHERE canonical_root = ?1",
        [&request.root],
    )?;
    Ok(())
}

fn apply_attach(
    tx: &Transaction<'_>,
    request: &AttachDerivedParentRequest,
) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO derived_root_parent (canonical_parent, project_id, source_root, registration_epoch)
         VALUES (?1, ?2, ?3, ?4)",
        params![request.container, request.project_id, request.root, request.registration_epoch],
    )?;
    Ok(())
}

/// Replay the root-membership ops. Returns false for ops this module does not
/// own.
pub(crate) fn replay_root_op(
    tx: &Transaction<'_>,
    seq: i64,
    op: &str,
    value: &Value,
    now: i64,
) -> rusqlite::Result<bool> {
    let decode =
        |error: serde_json::Error| rusqlite::Error::ToSqlConversionFailure(Box::new(error));
    match op {
        "add_root" => apply_add_root(
            tx,
            &serde_json::from_value(value.clone()).map_err(decode)?,
            now,
        )?,
        "remove_root" => apply_remove_root(
            tx,
            &serde_json::from_value(value.clone()).map_err(decode)?,
            seq,
        )?,
        "attach_derived_parent" => {
            apply_attach(tx, &serde_json::from_value(value.clone()).map_err(decode)?)?
        }
        _ => return Ok(false),
    }
    Ok(true)
}

impl RegistryStore {
    pub fn add_root(&self, request: AddRootRequest) -> Result<Vec<u8>, RegistryError> {
        self.with_principal("entorhinal").add_root(request)
    }

    pub fn remove_root(&self, request: RemoveRootRequest) -> Result<Vec<u8>, RegistryError> {
        self.with_principal("entorhinal").remove_root(request)
    }

    pub fn attach_derived_parent(
        &self,
        request: AttachDerivedParentRequest,
    ) -> Result<Vec<u8>, RegistryError> {
        self.with_principal("entorhinal")
            .attach_derived_parent(request)
    }

    pub fn set_project_approval(
        &self,
        raw_path: &str,
        actor: &str,
        approve: bool,
    ) -> Result<Vec<u8>, RegistryError> {
        self.with_principal("entorhinal")
            .set_project_approval(raw_path, actor, approve)
    }
}

impl super::JournalWriter<'_> {
    /// Add a root to an existing project. Refused when the root belongs to
    /// another project, nests inside or around any registered root, sits in or
    /// around a worker container, or names a GitHub repository another project
    /// already owns. The new root is unbound and unapproved.
    pub fn add_root(&self, mut request: AddRootRequest) -> Result<Vec<u8>, RegistryError> {
        request.root = super::mutations::canonical(&request.root)?;
        let actor = request.actor.clone().unwrap_or_else(|| "module".into());
        self.mutation("add_root", None, move |tx| {
            let project = &request.project_id;
            if tx
                .query_row(
                    "SELECT 1 FROM project WHERE project_id = ?1",
                    [project],
                    |row| row.get::<_, i64>(0),
                )
                .optional()?
                .is_none()
            {
                return Err(domain("not_found", project.clone()));
            }
            if let Some(owner) = tx
                .query_row(
                    "SELECT project_id FROM project_root WHERE canonical_root = ?1",
                    [&request.root],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
            {
                if &owner == project {
                    let value = json!({"projectId": project, "root": request.root});
                    return Ok(Action {
                        changed: false,
                        seq: None,
                        value: value.clone(),
                        payload: value,
                    });
                }
                return Err(domain(
                    "root_conflict",
                    format!("{} is a root of {owner}", request.root),
                ));
            }
            check_root_location(tx, &request.root)?;
            let root_key =
                crate::root_keys::incoming(tx, project, &request.root, request.label.as_deref())?;
            let remotes = super::ownership::root_remotes(tx, &request.root)?;
            if let Some((repository, owner)) = repository_owner(tx, project, &remotes)? {
                return Err(domain(
                    "repository_owned",
                    format!("{repository} belongs to {owner}"),
                ));
            }
            let payload = serde_json::to_value(&request)
                .map_err(|error| domain("encode_failed", error.to_string()))?;
            let now = super::now_unix_millis();
            let seq = super::mutations::append(
                tx,
                "add_root",
                &payload,
                &actor,
                None,
                now,
                self.principal,
            )?;
            apply_add_root(tx, &request, now)?;
            let mut mappings = Vec::new();
            crate::root_keys::assign(tx, project, &request.root, root_key, now, &mut mappings)?;
            crate::root_keys::journal_assignments(tx, mappings, &actor, now, self.principal)?;
            Ok(Action {
                changed: true,
                seq: Some(seq),
                value: json!({"projectId": project, "root": request.root}),
                payload,
            })
        })
    }

    /// Remove one local root. With shared identity enabled, the project and its
    /// keys may still be in use on other machines, even after its last local root.
    pub fn remove_root(&self, mut request: RemoveRootRequest) -> Result<Vec<u8>, RegistryError> {
        request.root = canonical_query_path(Path::new(&request.root))?.0;
        let actor = request.actor.clone().unwrap_or_else(|| "module".into());
        self.mutation("remove_root", None, move |tx| {
            let owner = tx
                .query_row(
                    "SELECT project_id FROM project_root WHERE canonical_root = ?1",
                    [&request.root],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            match owner {
                Some(owner) if owner == request.project_id => {}
                Some(owner) => {
                    return Err(domain(
                        "root_conflict",
                        format!("{} is a root of {owner}", request.root),
                    ))
                }
                None => {
                    return Err(domain(
                        "not_found",
                        format!("{} is not a registered root", request.root),
                    ))
                }
            }
            let count: i64 = tx.query_row(
                "SELECT COUNT(*) FROM project_root WHERE project_id = ?1",
                [&request.project_id],
                |row| row.get(0),
            )?;
            if count <= 1 && !super::log_schema::log_enabled(tx)? {
                return Err(domain(
                    "last_root",
                    format!(
                        "{} is the only root of {}; remove the project instead",
                        request.root, request.project_id
                    ),
                ));
            }
            let payload = serde_json::to_value(&request)
                .map_err(|error| domain("encode_failed", error.to_string()))?;
            let seq = super::mutations::append(
                tx,
                "remove_root",
                &payload,
                &actor,
                None,
                super::now_unix_millis(),
                self.principal,
            )?;
            apply_remove_root(tx, &request, seq)?;
            Ok(Action {
                changed: true,
                seq: Some(seq),
                value: json!({"projectId": request.project_id, "root": request.root}),
                payload,
            })
        })
    }

    /// Attach a worker container to a root's current binding. The caller names
    /// the binding it verified; the container is recorded only if that is
    /// still the root's active, approved binding and the checkout on disk is
    /// still it. An identical attachment is a no-op.
    pub fn attach_derived_parent(
        &self,
        mut request: AttachDerivedParentRequest,
    ) -> Result<Vec<u8>, RegistryError> {
        request.container = super::mutations::canonical(&request.container)?;
        let actor = request.actor.clone().unwrap_or_else(|| "module".into());
        self.mutation("attach_derived_parent", None, move |tx| {
            let value = json!({"projectId": request.project_id, "root": request.root, "container": request.container, "registrationEpoch": request.registration_epoch});
            let record = root_record(tx, &request.root)?;
            let owner = tx
                .query_row("SELECT project_id FROM project_root WHERE canonical_root = ?1", [&request.root], |row| row.get::<_, String>(0))
                .optional()?;
            // Each refusal names what failed, in the order a caller would fix
            // it. Every one of them means "nothing was attached".
            match owner.as_deref() {
                Some(owner) if owner == request.project_id => {}
                Some(owner) => return Err(domain("root_conflict", format!("{} is a root of {owner}, not {}", request.root, request.project_id))),
                None => return Err(domain("root_not_registered", format!("{} is not a registered root", request.root))),
            }
            let retired = tx
                .query_row("SELECT 1 FROM retired_binding WHERE registration_epoch = ?1", [&request.registration_epoch], |row| row.get::<_, i64>(0))
                .optional()?
                .is_some();
            if retired {
                return Err(domain("binding_retired", format!("epoch {} of {} has been retired", request.registration_epoch, request.root)));
            }
            if record.identity == "replaced" {
                return Err(domain("root_replaced", format!("the checkout at {} is not the one that was bound", request.root)));
            }
            if record.registration_epoch.as_deref() != Some(request.registration_epoch.as_str()) {
                return Err(domain("binding_stale", format!("{} is not bound under epoch {}", request.root, request.registration_epoch)));
            }
            if record.incarnation.as_ref().map(|i| i.value.as_str()) != Some(request.incarnation.as_str()) {
                return Err(domain("incarnation_mismatch", format!("the incarnation given for {} is not the bound checkout's", request.root)));
            }
            if record.approval.state != "approved" {
                return Err(domain("binding_unapproved", format!("{} is not approved", request.root)));
            }
            let existing = containers(tx)?;
            if let Some(same) = existing.iter().find(|c| c.parent == request.container) {
                if same.project_id == request.project_id
                    && same.source_root.as_deref() == Some(request.root.as_str())
                    && same.epoch.as_deref() == Some(request.registration_epoch.as_str())
                {
                    return Ok(Action { changed: false, seq: None, value: value.clone(), payload: value });
                }
                return Err(domain("container_foreign", format!("{} is attached to another binding", request.container)));
            }
            if let Some(other) = existing.iter().find(|c| {
                path_prefix_or_equal(Path::new(&c.parent), Path::new(&request.container))
                    || path_prefix_or_equal(Path::new(&request.container), Path::new(&c.parent))
            }) {
                return Err(domain("container_overlaps", format!("{} overlaps container {}", request.container, other.parent)));
            }
            let mut roots = tx.prepare("SELECT canonical_root FROM project_root")?;
            let roots = roots.query_map([], |row| row.get::<_, String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
            if let Some(root) = roots.iter().find(|root| {
                path_prefix_or_equal(Path::new(root.as_str()), Path::new(&request.container))
                    || path_prefix_or_equal(Path::new(&request.container), Path::new(root.as_str()))
            }) {
                return Err(domain("container_overlaps", format!("{} overlaps registered root {root}", request.container)));
            }
            let payload = serde_json::to_value(&request).map_err(|error| domain("encode_failed", error.to_string()))?;
            let seq = super::mutations::append(tx, "attach_derived_parent", &payload, &actor, None, super::now_unix_millis(), self.principal)?;
            apply_attach(tx, &request)?;
            Ok(Action { changed: true, seq: Some(seq), value, payload })
        })
    }

    /// Approve (or withdraw) every root of the project a path belongs to. The
    /// operator approves a project, seeing all of its roots, so approval is
    /// refused unless every root's checkout is identified: approving a project
    /// with a root nobody can identify would approve whatever lands there.
    pub fn set_project_approval(
        &self,
        raw_path: &str,
        actor: &str,
        approve: bool,
    ) -> Result<Vec<u8>, RegistryError> {
        let reply = self.resolve_walk(raw_path, None)?;
        let fields = reply.root_fields.clone().unwrap_or(RootFields {
            matched_root: None,
            source_root: None,
            root_records: Vec::new(),
            binding_status: None,
        });
        if !matches!(reply.via.as_str(), "root" | "walk") || fields.root_records.is_empty() {
            return Err(domain(
                "not_found",
                format!("{raw_path} is not inside a registered root"),
            ));
        }
        if approve {
            if let Some(record) = fields.root_records.iter().find(|r| r.identity != "bound") {
                return Err(domain(
                    "root_not_bound",
                    format!(
                        "{} is {}; nothing is approved",
                        record.root, record.identity
                    ),
                ));
            }
        }
        let mut roots = Vec::new();
        for record in &fields.root_records {
            if record.identity != "bound" {
                continue;
            }
            let outcome: Value =
                serde_json::from_slice(&self.set_approval(&record.root, actor, approve)?)
                    .map_err(|error| domain("encode_failed", error.to_string()))?;
            roots.push(json!({
                "root": record.root,
                "registrationEpoch": outcome["result"]["registrationEpoch"],
                "noop": outcome["result"]["noop"],
            }));
        }
        super::mutations::wire_value(json!({"projectId": reply.project_id, "roots": roots}))
    }
}

impl RegistryStore {
    /// Resolve with root records even when `ENTORHINAL_ROOT_RECORDS` is off.
    /// `ck projects trust` uses it to show approval; it changes nothing.
    pub fn trust(&self, raw_path: &str) -> Result<ResolveReply, RegistryError> {
        self.resolve_walk(raw_path, None)
    }
}

struct Container {
    parent: String,
    project_id: String,
    source_root: Option<String>,
    epoch: Option<String>,
}

fn containers(conn: &Connection) -> rusqlite::Result<Vec<Container>> {
    let mut statement = conn.prepare(
        "SELECT canonical_parent, project_id, source_root, registration_epoch FROM derived_root_parent",
    )?;
    let rows = statement
        .query_map([], |row| {
            Ok(Container {
                parent: row.get(0)?,
                project_id: row.get(1)?,
                source_root: row.get(2)?,
                epoch: row.get(3)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>();
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutations::tests::{register, result, Fixture};
    use crate::{RegisterRequest, RemoveRequest};

    /// A checkout with a `.git` directory and, optionally, a GitHub remote.
    fn repo(fixture: &Fixture, name: &str, remote: Option<&str>) -> String {
        let root = fixture.dir(name);
        let git = Path::new(&root).join(".git");
        fs::create_dir_all(&git).unwrap();
        let mut config = String::from("[core]\n\tbare = false\n");
        if let Some(url) = remote {
            config.push_str(&format!("[remote \"origin\"]\n\turl = {url}\n\tfetch = +refs/heads/*:refs/remotes/origin/*\n"));
        }
        fs::write(git.join("config"), config).unwrap();
        root
    }

    /// A linked worktree of `source` at `path`, laid out the way git does it.
    fn worktree(source: &str, path: &str, name: &str) {
        let admin = Path::new(source).join(".git/worktrees").join(name);
        fs::create_dir_all(&admin).unwrap();
        fs::write(admin.join("commondir"), "../..\n").unwrap();
        fs::create_dir_all(path).unwrap();
        fs::write(
            Path::new(path).join(".git"),
            format!("gitdir: {}\n", admin.display()),
        )
        .unwrap();
    }

    fn store(label: &str) -> Fixture {
        let mut fixture = Fixture::new(label);
        fixture.store.use_sequential_ids();
        fixture.store.set_root_records(true);
        fixture
    }

    fn fields(reply: &ResolveReply) -> &RootFields {
        reply
            .root_fields
            .as_ref()
            .expect("root fields present when enabled")
    }

    fn first_record(fixture: &Fixture, path: &str) -> RootRecord {
        fields(&fixture.store.resolve(path).unwrap()).root_records[0].clone()
    }

    fn bind(fixture: &Fixture, root: &str) -> String {
        let value = result(&fixture.store.bind_root(root, "test").unwrap());
        value["registrationEpoch"].as_str().unwrap().to_string()
    }

    fn remove(fixture: &Fixture, id: &str) {
        fixture
            .store
            .remove(RemoveRequest {
                project_id: Some(id.into()),
                ..Default::default()
            })
            .unwrap();
    }

    #[test]
    fn binding_writes_a_token_and_identity_comes_only_from_it() {
        let f = store("token");
        let root = repo(&f, "alpha", None);
        register(&f, "pj-alpha", root.clone());
        bind(&f, &root);
        let token =
            fs::read_to_string(Path::new(&root).join(".git").join(INCARNATION_FILE)).unwrap();
        assert_eq!(token.len(), 65);
        assert!(token.ends_with('\n') && is_token(token.trim_end()));
        let record = first_record(&f, &root);
        assert_eq!(record.identity, "bound");
        assert_eq!(record.incarnation.as_ref().unwrap().value, token.trim_end());
        // Same path, same metadata, different token: another checkout.
        fs::write(
            Path::new(&root).join(".git").join(INCARNATION_FILE),
            format!("{}\n", "f".repeat(64)),
        )
        .unwrap();
        let record = first_record(&f, &root);
        assert_eq!(record.identity, "replaced");
        assert!(record.registration_epoch.is_none());
    }

    #[test]
    fn a_reclone_at_the_same_path_loses_approval_and_rebinding_retires_the_old_epoch() {
        let f = store("reclone");
        let root = repo(&f, "alpha", None);
        register(&f, "pj-alpha", root.clone());
        let old_epoch = bind(&f, &root);
        f.store.approve_root(&root, "test").unwrap();
        let token =
            fs::read_to_string(Path::new(&root).join(".git").join(INCARNATION_FILE)).unwrap();
        let captured = ExecutionBinding {
            root: root.clone(),
            incarnation: token.trim_end().to_string(),
            registration_epoch: old_epoch.clone(),
        };
        fs::remove_dir_all(Path::new(&root).join(".git")).unwrap();
        repo(&f, "alpha", None);
        let reply = f
            .store
            .resolve_with_binding(&root, Some(&captured))
            .unwrap();
        assert_eq!(fields(&reply).binding_status, Some("replaced"));
        assert_eq!(fields(&reply).root_records[0].approval.state, "unapproved");
        let err = f.store.approve_root(&root, "test").unwrap_err();
        assert!(err.to_string().starts_with("root_replaced"), "{err}");

        let new_epoch = bind(&f, &root);
        assert_ne!(new_epoch, old_epoch);
        let reply = f
            .store
            .resolve_with_binding(&root, Some(&captured))
            .unwrap();
        assert_eq!(fields(&reply).binding_status, Some("replaced"));
        let record = &fields(&reply).root_records[0];
        assert_eq!(
            (record.identity, record.approval.state),
            ("bound", "unapproved")
        );
    }

    #[test]
    fn remove_readd_unchanged_checkout_rotates_registration_epoch() {
        let f = store("readd");
        let root = repo(&f, "alpha", None);
        register(&f, "pj-alpha", root.clone());
        let first = bind(&f, &root);
        f.store.approve_root(&root, "test").unwrap();
        let token =
            fs::read_to_string(Path::new(&root).join(".git").join(INCARNATION_FILE)).unwrap();
        let captured = ExecutionBinding {
            root: root.clone(),
            incarnation: token.trim_end().to_string(),
            registration_epoch: first.clone(),
        };
        remove(&f, "pj-alpha");
        register(&f, "pj-alpha", root.clone());
        let second = bind(&f, &root);
        let token_after =
            fs::read_to_string(Path::new(&root).join(".git").join(INCARNATION_FILE)).unwrap();
        assert_eq!(token, token_after, "an unchanged checkout keeps its token");
        assert_ne!(
            first, second,
            "a new registration lifetime gets a new epoch"
        );
        let reply = f
            .store
            .resolve_with_binding(&root, Some(&captured))
            .unwrap();
        assert_eq!(fields(&reply).binding_status, Some("retired"));
        assert_eq!(fields(&reply).root_records[0].approval.state, "unapproved");
    }

    #[test]
    fn trust_walk_checks_matched_root_epoch_approval() {
        let f = store("walk");
        let root = repo(&f, "alpha", None);
        register(&f, "pj-alpha", root.clone());
        let epoch = bind(&f, &root);
        let sub = format!("{root}/src/deep");
        fs::create_dir_all(&sub).unwrap();
        let reply = f.store.resolve(&sub).unwrap();
        assert_eq!(reply.via, "walk");
        assert_eq!(reply.project_id, "pj-alpha");
        assert_eq!(fields(&reply).matched_root.as_deref(), Some(root.as_str()));
        let record = &fields(&reply).root_records[0];
        assert_eq!(record.registration_epoch.as_deref(), Some(epoch.as_str()));
        assert_eq!(record.approval.state, "unapproved");
        f.store.approve_root(&root, "test").unwrap();
        let record = first_record(&f, &sub);
        assert_eq!(record.approval.state, "approved");
    }

    #[test]
    fn the_walk_stops_at_a_separate_checkout_inside_a_project() {
        let f = store("nested");
        let root = repo(&f, "alpha", None);
        register(&f, "pj-alpha", root.clone());
        let vendored = format!("{root}/vendor/other");
        fs::create_dir_all(Path::new(&vendored).join(".git")).unwrap();
        let reply = f.store.resolve(&format!("{vendored}/src")).unwrap();
        assert_eq!(reply.via, "implicit");
        assert!(fields(&reply).root_records.is_empty());
    }

    #[test]
    fn missing_worktree_admin_dir_does_not_inherit_new_clone_identity() {
        let f = store("admin");
        let root = repo(&f, "alpha", None);
        register(&f, "pj-alpha", root.clone());
        let epoch = bind(&f, &root);
        f.store.approve_root(&root, "test").unwrap();
        let container = f.dir("worktrees");
        attach(&f, &container, "pj-alpha", &root, &epoch);
        let wt = format!("{container}/task-1");
        worktree(&root, &wt, "task-1");
        assert_eq!(first_record(&f, &wt).identity, "bound");
        // The admin directory goes (a reclone of the source), and the worktree's
        // pointer now names a place a new clone might fill: not proof.
        fs::remove_dir_all(Path::new(&root).join(".git/worktrees/task-1")).unwrap();
        let record = first_record(&f, &wt);
        assert_eq!(record.identity, "unverifiable");
        assert_eq!(record.approval.state, "unapproved");
    }

    #[test]
    fn a_worktree_attached_under_a_retired_epoch_carries_no_binding() {
        let f = store("wt-retired");
        let root = repo(&f, "alpha", None);
        register(&f, "pj-alpha", root.clone());
        let epoch = bind(&f, &root);
        f.store.approve_root(&root, "test").unwrap();
        let container = f.dir("worktrees");
        attach(&f, &container, "pj-alpha", &root, &epoch);
        let wt = format!("{container}/task-1");
        worktree(&root, &wt, "task-1");
        let record = first_record(&f, &wt);
        assert_eq!(
            (record.identity, record.approval.state),
            ("bound", "approved")
        );
        // Rebinding the source under a new epoch (a replaced checkout) retires
        // what the container was attached under.
        fs::write(
            Path::new(&root).join(".git").join(INCARNATION_FILE),
            format!("{}\n", "e".repeat(64)),
        )
        .unwrap();
        bind(&f, &root);
        let record = first_record(&f, &wt);
        assert_eq!(
            (record.identity, record.approval.state),
            ("retired", "unapproved")
        );
    }

    #[test]
    fn rebuild_reproduces_bindings_retirements_and_approvals() {
        let f = store("rebuild");
        let a = repo(&f, "alpha", Some("git@github.com:cortexkit/alpha.git"));
        let b = repo(&f, "beta", None);
        f.store
            .register(RegisterRequest {
                project_id: Some("pj-two".into()),
                name: "two".into(),
                roots: vec![a.clone(), b.clone()],
                ..Default::default()
            })
            .unwrap();
        f.store.bind_unbound_roots("test").unwrap();
        f.store.approve_root(&a, "test").unwrap();
        fs::write(
            Path::new(&b).join(".git").join(INCARNATION_FILE),
            format!("{}\n", "d".repeat(64)),
        )
        .unwrap();
        bind(&f, &b);
        let before = serde_json::to_string(&f.store.enumerate(None).unwrap()).unwrap();
        f.store.rebuild().unwrap();
        let after = serde_json::to_string(&f.store.enumerate(None).unwrap()).unwrap();
        assert_eq!(before, after);
        let retired: i64 = f
            .store
            .read(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM retired_binding WHERE reason='replaced'",
                    [],
                    |r| r.get(0),
                )
            })
            .unwrap();
        assert_eq!(retired, 1);
    }

    #[test]
    fn remotes_name_github_owner_and_repo_in_every_url_form() {
        for url in [
            "git@github.com:cortexkit/alpha.git",
            "https://github.com/cortexkit/alpha",
            "https://github.com/cortexkit/alpha.git/",
            "ssh://git@github.com/cortexkit/alpha.git",
        ] {
            assert_eq!(
                parse_github_url(url),
                Some(("cortexkit".into(), "alpha".into())),
                "{url}"
            );
        }
        for url in [
            "git@gitlab.com:a/b.git",
            "https://github.com/onlyowner",
            "https://github.com/a/b/c",
        ] {
            assert_eq!(parse_github_url(url), None, "{url}");
        }
    }

    fn token_of(root: &str) -> String {
        fs::read_to_string(Path::new(root).join(".git").join(INCARNATION_FILE))
            .unwrap()
            .trim_end()
            .to_string()
    }

    fn attach_request(
        container: &str,
        project: &str,
        source: &str,
        epoch: &str,
    ) -> AttachDerivedParentRequest {
        AttachDerivedParentRequest {
            project_id: project.into(),
            root: source.into(),
            incarnation: token_of(source),
            registration_epoch: epoch.into(),
            container: container.into(),
            actor: None,
        }
    }

    fn attach(f: &Fixture, container: &str, project: &str, source: &str, epoch: &str) {
        f.store
            .attach_derived_parent(attach_request(container, project, source, epoch))
            .unwrap();
    }

    // ---- Goldens -------------------------------------------------------
    //
    // The reply shapes prefrontal's decoder is tested against. Every file is
    // produced here by the real producer and compared byte for byte; `UPDATE_GOLDENS=1 cargo test`
    // rewrites them. The fixture's real temp path is replaced by `/fixture`,
    // and ids hashed from a path are re-derived from the replaced path, so a
    // golden is internally consistent.

    fn golden_dir() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden/resolve")
    }

    struct Normalizer {
        real: String,
        paths: Vec<String>,
    }

    impl Normalizer {
        fn apply(&self, value: &impl Serialize) -> String {
            let mut text = serde_json::to_string_pretty(&json!({ "result": value })).unwrap();
            for path in &self.paths {
                let fake = path.replacen(&self.real, "/fixture", 1);
                text = text.replace(&implicit_project_id(path), &implicit_project_id(&fake));
            }
            text.replace(&self.real, "/fixture") + "\n"
        }
    }

    fn check(name: &str, text: &str, mismatches: &mut Vec<String>) {
        let path = golden_dir().join(name);
        if std::env::var("UPDATE_GOLDENS").as_deref() == Ok("1") {
            fs::create_dir_all(golden_dir()).unwrap();
            fs::write(&path, text).unwrap();
            return;
        }
        match fs::read_to_string(&path) {
            Ok(expected) if expected == text => {}
            Ok(_) => mismatches.push(format!("{name} differs from the producer's output")),
            Err(error) => mismatches.push(format!("{name}: {error}")),
        }
    }

    #[test]
    fn resolve_and_enumerate_replies_match_the_committed_goldens() {
        let mut f = Fixture::new("golden");
        f.store.use_sequential_ids();
        let real = fs::canonicalize(&f.root)
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let home = repo(
            &f,
            "openai-auth",
            Some("git@github.com:cortexkit/openai-auth.git"),
        );
        let second = repo(
            &f,
            "common-auth",
            Some("https://github.com/cortexkit/common-auth.git"),
        );
        let lone = repo(&f, "lone", None);
        let plain = f.dir("plain-folder");
        let unregistered = repo(&f, "unregistered", None);
        let container = f.dir("worktrees");
        let wt = format!("{container}/task-1");
        let sub = format!("{home}/src/lib");
        fs::create_dir_all(&sub).unwrap();
        let missing = format!("{real}/does-not-exist");

        f.store
            .register(RegisterRequest {
                project_id: Some("pj-openai-auth".into()),
                name: "openai-auth".into(),
                roots: vec![home.clone(), second.clone()],
                ..Default::default()
            })
            .unwrap();
        register(&f, "pj-lone", lone.clone());
        register(&f, "pj-plain", plain.clone());

        let normalizer = Normalizer {
            real: real.clone(),
            paths: vec![
                home.clone(),
                second.clone(),
                lone.clone(),
                plain.clone(),
                unregistered.clone(),
                wt.clone(),
                sub.clone(),
                missing.clone(),
            ],
        };
        let mut mismatches = Vec::new();

        // Legacy: root records off, the shape every deployed consumer reads.
        check(
            "legacy-resolve-root.json",
            &normalizer.apply(&f.store.resolve(&home).unwrap()),
            &mut mismatches,
        );
        check(
            "legacy-enumerate.json",
            &normalizer.apply(&f.store.enumerate(None).unwrap()),
            &mut mismatches,
        );

        f.store.set_root_records(true);
        f.store.bind_unbound_roots("test").unwrap();
        f.store.approve_root(&home, "test").unwrap();
        let home_epoch = f
            .store
            .read(|conn| active_binding(conn, &home))
            .unwrap()
            .unwrap()
            .epoch;
        attach(&f, &container, "pj-openai-auth", &home, &home_epoch);
        worktree(&home, &wt, "task-1");

        check(
            "resolve-root.json",
            &normalizer.apply(&f.store.resolve(&home).unwrap()),
            &mut mismatches,
        );
        check(
            "resolve-walk.json",
            &normalizer.apply(&f.store.resolve(&sub).unwrap()),
            &mut mismatches,
        );
        check(
            "resolve-containment.json",
            &normalizer.apply(&f.store.resolve(&wt).unwrap()),
            &mut mismatches,
        );
        check(
            "resolve-implicit.json",
            &normalizer.apply(&f.store.resolve(&unregistered).unwrap()),
            &mut mismatches,
        );
        check(
            "resolve-no-project.json",
            &normalizer.apply(&f.store.resolve(&missing).unwrap()),
            &mut mismatches,
        );
        check(
            "resolve-unverifiable.json",
            &normalizer.apply(&f.store.resolve(&plain).unwrap()),
            &mut mismatches,
        );
        check(
            "resolve-project-id-alias.json",
            &normalizer.apply(
                &f.store
                    .resolve_project_id(&implicit_project_id(&lone))
                    .unwrap(),
            ),
            &mut mismatches,
        );

        let token =
            fs::read_to_string(Path::new(&home).join(".git").join(INCARNATION_FILE)).unwrap();
        let active = ExecutionBinding {
            root: home.clone(),
            incarnation: token.trim_end().to_string(),
            registration_epoch: home_epoch.clone(),
        };
        check(
            "resolve-binding-active.json",
            &normalizer.apply(&f.store.resolve_with_binding(&home, Some(&active)).unwrap()),
            &mut mismatches,
        );

        let lone_token =
            fs::read_to_string(Path::new(&lone).join(".git").join(INCARNATION_FILE)).unwrap();
        let lone_epoch = f
            .store
            .read(|conn| active_binding(conn, &lone))
            .unwrap()
            .unwrap()
            .epoch;
        let retired = ExecutionBinding {
            root: lone.clone(),
            incarnation: lone_token.trim_end().to_string(),
            registration_epoch: lone_epoch,
        };
        remove(&f, "pj-lone");
        register(&f, "pj-lone", lone.clone());
        f.store.bind_unbound_roots("test").unwrap();
        check(
            "resolve-binding-retired.json",
            &normalizer.apply(&f.store.resolve_with_binding(&lone, Some(&retired)).unwrap()),
            &mut mismatches,
        );

        fs::write(
            Path::new(&second).join(".git").join(INCARNATION_FILE),
            format!("{}\n", "c".repeat(64)),
        )
        .unwrap();
        check(
            "resolve-replaced.json",
            &normalizer.apply(&f.store.resolve(&second).unwrap()),
            &mut mismatches,
        );

        check(
            "enumerate.json",
            &normalizer.apply(&f.store.enumerate(None).unwrap()),
            &mut mismatches,
        );
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    }
}

#[cfg(test)]
mod root_membership_tests {
    use super::*;
    use crate::mutations::tests::{register, result, Fixture};
    use crate::{RegisterRequest, RemoveRequest};

    fn repo(f: &Fixture, name: &str, remote: Option<&str>) -> String {
        let root = f.dir(name);
        let git = Path::new(&root).join(".git");
        fs::create_dir_all(&git).unwrap();
        let mut config = String::new();
        if let Some(url) = remote {
            config.push_str(&format!("[remote \"origin\"]\n\turl = {url}\n"));
        }
        fs::write(git.join("config"), config).unwrap();
        root
    }

    fn store(label: &str) -> Fixture {
        let mut f = Fixture::new(label);
        f.store.use_sequential_ids();
        f.store.set_root_records(true);
        f
    }

    fn add(f: &Fixture, project: &str, root: &str) -> Result<Vec<u8>, RegistryError> {
        f.store.add_root(AddRootRequest {
            project_id: project.into(),
            root: root.into(),
            actor: None,
            label: None,
        })
    }

    fn refused(outcome: Result<Vec<u8>, RegistryError>, code: &str) {
        let error = outcome.expect_err("expected a refusal");
        assert!(
            error.to_string().starts_with(code),
            "expected {code}, got {error}"
        );
    }

    fn epoch_of(f: &Fixture, root: &str) -> String {
        f.store
            .read(|conn| active_binding(conn, root))
            .unwrap()
            .unwrap()
            .epoch
    }

    fn token(root: &str) -> String {
        fs::read_to_string(Path::new(root).join(".git").join(INCARNATION_FILE))
            .unwrap()
            .trim_end()
            .to_string()
    }

    #[test]
    fn add_root_joins_a_project_unbound_and_unapproved_and_refuses_what_it_must() {
        let f = store("add");
        let home = repo(&f, "home", Some("git@github.com:cortexkit/home.git"));
        let second = repo(&f, "second", None);
        let other = repo(&f, "other", Some("git@github.com:cortexkit/shared.git"));
        register(&f, "pj-home", home.clone());
        register(&f, "pj-other", other.clone());
        let joined = result(&add(&f, "pj-home", &second).unwrap());
        assert_eq!(joined["noop"], false);
        let reply = f.store.resolve(&second).unwrap();
        assert_eq!(reply.project_id, "pj-home");
        let records = &reply.root_fields.unwrap().root_records;
        assert_eq!(records.len(), 2);
        let added = records.iter().find(|r| r.root == second).unwrap();
        assert_eq!(
            (added.identity, added.approval.state),
            ("unbound", "unapproved")
        );
        assert_eq!(result(&add(&f, "pj-home", &second).unwrap())["noop"], true);

        refused(add(&f, "pj-home", &other), "root_conflict");
        let inner = format!("{home}/packages/inner");
        fs::create_dir_all(&inner).unwrap();
        refused(add(&f, "pj-home", &inner), "root_nested");
        refused(add(&f, "pj-other", &f.dir("")), "root_nested");
        let clone = repo(
            &f,
            "shared-clone",
            Some("https://github.com/cortexkit/shared"),
        );
        refused(add(&f, "pj-home", &clone), "repository_owned");
        refused(add(&f, "pj-missing", &repo(&f, "loose", None)), "not_found");
    }

    #[test]
    fn remove_root_retires_the_binding_revokes_its_containers_and_keeps_the_last_root() {
        let f = store("remove-root");
        let home = repo(&f, "home", None);
        let second = repo(&f, "second", None);
        register(&f, "pj-home", home.clone());
        add(&f, "pj-home", &second).unwrap();
        f.store.bind_unbound_roots("test").unwrap();
        f.store.approve_root(&second, "test").unwrap();
        let epoch = epoch_of(&f, &second);
        let container = f.dir("containers-second");
        f.store
            .attach_derived_parent(AttachDerivedParentRequest {
                project_id: "pj-home".into(),
                root: second.clone(),
                incarnation: token(&second),
                registration_epoch: epoch.clone(),
                container: container.clone(),
                actor: None,
            })
            .unwrap();
        let captured = ExecutionBinding {
            root: second.clone(),
            incarnation: token(&second),
            registration_epoch: epoch,
        };

        f.store
            .remove_root(RemoveRootRequest {
                project_id: "pj-home".into(),
                root: second.clone(),
                actor: None,
            })
            .unwrap();
        assert_eq!(f.store.resolve(&second).unwrap().via, "implicit");
        assert_eq!(
            f.store.resolve(&container).unwrap().via,
            "implicit",
            "the container is revoked with its root"
        );
        let reply = f
            .store
            .resolve_with_binding(&home, Some(&captured))
            .unwrap();
        assert_eq!(reply.root_fields.unwrap().binding_status, Some("retired"));
        refused(
            f.store.remove_root(RemoveRootRequest {
                project_id: "pj-home".into(),
                root: home.clone(),
                actor: None,
            }),
            "last_root",
        );

        let before = serde_json::to_string(&f.store.enumerate(None).unwrap()).unwrap();
        f.store.rebuild().unwrap();
        assert_eq!(
            before,
            serde_json::to_string(&f.store.enumerate(None).unwrap()).unwrap()
        );
    }

    #[test]
    fn project_approval_covers_every_root_and_refuses_an_unidentified_one() {
        let f = store("approve-project");
        let home = repo(&f, "home", None);
        let second = f.dir("no-git-yet");
        register(&f, "pj-home", home.clone());
        f.store.bind_unbound_roots("test").unwrap();
        // A root with no git metadata cannot be identified, so the project is
        // not approved at all, not even its identifiable root.
        f.store
            .add_root(AddRootRequest {
                project_id: "pj-home".into(),
                root: second.clone(),
                actor: None,
                label: None,
            })
            .unwrap();
        refused(
            f.store.set_project_approval(&second, "test", true),
            "root_not_bound",
        );
        let records = f
            .store
            .trust(&home)
            .unwrap()
            .root_fields
            .unwrap()
            .root_records;
        assert!(records.iter().all(|r| r.approval.state == "unapproved"));

        f.store
            .remove_root(RemoveRootRequest {
                project_id: "pj-home".into(),
                root: second.clone(),
                actor: None,
            })
            .unwrap();
        let third = repo(&f, "third", None);
        f.store
            .add_root(AddRootRequest {
                project_id: "pj-home".into(),
                root: third.clone(),
                actor: None,
                label: None,
            })
            .unwrap();
        f.store.bind_unbound_roots("test").unwrap();
        let sub = format!("{third}/src");
        fs::create_dir_all(&sub).unwrap();
        let approved = result(&f.store.set_project_approval(&sub, "test", true).unwrap());
        assert_eq!(approved["roots"].as_array().unwrap().len(), 2);
        let records = f
            .store
            .trust(&home)
            .unwrap()
            .root_fields
            .unwrap()
            .root_records;
        assert!(records.iter().all(|r| r.approval.state == "approved"));
        f.store.set_project_approval(&home, "test", false).unwrap();
        let records = f
            .store
            .trust(&home)
            .unwrap()
            .root_fields
            .unwrap()
            .root_records;
        assert!(records.iter().all(|r| r.approval.state == "unapproved"));
        refused(
            f.store
                .set_project_approval(&f.dir("elsewhere"), "test", true),
            "not_found",
        );
    }

    /// Pins the attach contract that callers decode: the request, the success
    /// and no-op replies, and one refusal per code, each produced by
    /// `attach_derived_parent` itself. `UPDATE_GOLDENS=1` rewrites the files;
    /// otherwise each is byte-compared.
    #[test]
    fn attach_replies_match_the_committed_goldens() {
        let f = store("attach-golden");
        let real = fs::canonicalize(&f.root)
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let home = repo(&f, "home", None);
        let other = repo(&f, "other", None);
        register(&f, "pj-home", home.clone());
        register(&f, "pj-other", other.clone());
        f.store.bind_unbound_roots("test").unwrap();
        f.store.approve_root(&home, "test").unwrap();
        let epoch = epoch_of(&f, &home);
        let container = f.dir("worktrees");
        let good = AttachDerivedParentRequest {
            project_id: "pj-home".into(),
            root: home.clone(),
            incarnation: token(&home),
            registration_epoch: epoch.clone(),
            container: container.clone(),
            actor: Some("prefrontal-core".into()),
        };
        let normalize = |text: String| text.replace(&real, "/fixture");
        let mut files: Vec<(String, String)> = Vec::new();
        let pretty = |bytes: &[u8]| {
            let value: Value = serde_json::from_slice(bytes).unwrap();
            serde_json::to_string_pretty(&value).unwrap() + "\n"
        };
        files.push((
            "attach-request.json".into(),
            serde_json::to_string_pretty(
                &json!({"method": "attach_derived_parent", "params": good}),
            )
            .unwrap()
                + "\n",
        ));
        files.push((
            "attach-ok.json".into(),
            pretty(&f.store.attach_derived_parent(good.clone()).unwrap()),
        ));
        files.push((
            "attach-noop.json".into(),
            pretty(&f.store.attach_derived_parent(good.clone()).unwrap()),
        ));

        let refuse = |request: AttachDerivedParentRequest| -> Value {
            match f.store.attach_derived_parent(request) {
                Err(RegistryError::Domain { code, message }) => {
                    json!({"code": code, "message": message})
                }
                other => panic!("expected a refusal, got {other:?}"),
            }
        };
        let with = |edit: &dyn Fn(&mut AttachDerivedParentRequest)| {
            let mut request = good.clone();
            request.container = f.dir("worktrees-2");
            edit(&mut request);
            request
        };
        let mut refusals = serde_json::Map::new();
        refusals.insert(
            "root_not_registered".into(),
            refuse(with(&|r| r.root = format!("{real}/nowhere"))),
        );
        refusals.insert(
            "root_conflict".into(),
            refuse(with(&|r| r.root = other.clone())),
        );
        refusals.insert(
            "binding_stale".into(),
            refuse(with(&|r| r.registration_epoch = "0".repeat(32))),
        );
        refusals.insert(
            "incarnation_mismatch".into(),
            refuse(with(&|r| r.incarnation = "f".repeat(64))),
        );
        refusals.insert("container_foreign".into(), {
            let mut request = good.clone();
            request.project_id = "pj-other".into();
            request.root = other.clone();
            request.incarnation = token(&other);
            request.registration_epoch = epoch_of(&f, &other);
            f.store.approve_root(&other, "test").unwrap();
            refuse(request)
        });
        refusals.insert("container_overlaps".into(), {
            let nested = format!("{container}/inner");
            fs::create_dir_all(&nested).unwrap();
            refuse(with(&|r| r.container = nested.clone()))
        });
        refusals.insert(
            "root_not_found".into(),
            refuse(with(&|r| r.container = format!("{real}/missing-container"))),
        );
        refusals.insert("binding_unapproved".into(), {
            f.store.unapprove_root(&home, "test").unwrap();
            let refusal = refuse(with(&|_| {}));
            f.store.approve_root(&home, "test").unwrap();
            refusal
        });
        refusals.insert("root_replaced".into(), {
            let path = Path::new(&home).join(".git").join(INCARNATION_FILE);
            let original = fs::read_to_string(&path).unwrap();
            fs::write(&path, format!("{}\n", "e".repeat(64))).unwrap();
            let refusal = refuse(with(&|_| {}));
            fs::write(&path, original).unwrap();
            refusal
        });
        refusals.insert("binding_retired".into(), {
            f.store
                .remove(RemoveRequest {
                    project_id: Some("pj-home".into()),
                    ..Default::default()
                })
                .unwrap();
            f.store
                .register(RegisterRequest {
                    project_id: Some("pj-home".into()),
                    name: "pj-home".into(),
                    roots: vec![home.clone()],
                    ..Default::default()
                })
                .unwrap();
            f.store.bind_unbound_roots("test").unwrap();
            refuse(with(&|_| {}))
        });
        files.push((
            "attach-refusals.json".into(),
            serde_json::to_string_pretty(&Value::Object(refusals)).unwrap() + "\n",
        ));

        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden/attach");
        let mut mismatches = Vec::new();
        for (name, text) in files {
            let text = normalize(text);
            let path = dir.join(&name);
            if std::env::var("UPDATE_GOLDENS").as_deref() == Ok("1") {
                fs::create_dir_all(&dir).unwrap();
                fs::write(&path, &text).unwrap();
            } else if fs::read_to_string(&path).ok().as_deref() != Some(text.as_str()) {
                mismatches.push(name);
            }
        }
        assert!(
            mismatches.is_empty(),
            "differ from the producer: {mismatches:?}"
        );
    }

    #[test]
    fn derived_parent_attach_is_epoch_scoped_and_idempotent() {
        let f = store("attach");
        let home = repo(&f, "home", None);
        register(&f, "pj-home", home.clone());
        let epoch = {
            f.store.bind_unbound_roots("test").unwrap();
            epoch_of(&f, &home)
        };
        let container = f.dir("containers");
        let request = |container: &str, epoch: &str| AttachDerivedParentRequest {
            project_id: "pj-home".into(),
            root: home.clone(),
            incarnation: token(&home),
            registration_epoch: epoch.into(),
            container: container.into(),
            actor: None,
        };
        refused(
            f.store.attach_derived_parent(request(&container, &epoch)),
            "binding_unapproved",
        );
        f.store.approve_root(&home, "test").unwrap();
        assert_eq!(
            result(
                &f.store
                    .attach_derived_parent(request(&container, &epoch))
                    .unwrap()
            )["noop"],
            false
        );
        assert_eq!(
            result(
                &f.store
                    .attach_derived_parent(request(&container, &epoch))
                    .unwrap()
            )["noop"],
            true
        );
        refused(
            f.store
                .attach_derived_parent(request(&container, "0".repeat(32).as_str())),
            "binding_stale",
        );
        let nested = format!("{container}/inner");
        fs::create_dir_all(&nested).unwrap();
        refused(
            f.store.attach_derived_parent(request(&nested, &epoch)),
            "container_overlaps",
        );
        let inside_root = format!("{home}/sub-container");
        fs::create_dir_all(&inside_root).unwrap();
        refused(
            f.store.attach_derived_parent(request(&inside_root, &epoch)),
            "container_overlaps",
        );

        // Remove and re-add the unchanged checkout: it keeps its incarnation
        // token but gets a new registration epoch. The old epoch is retired,
        // so attaching under it is refused by name.
        f.store
            .remove(RemoveRequest {
                project_id: Some("pj-home".into()),
                ..Default::default()
            })
            .unwrap();
        f.store
            .register(RegisterRequest {
                project_id: Some("pj-home".into()),
                name: "pj-home".into(),
                roots: vec![home.clone()],
                ..Default::default()
            })
            .unwrap();
        f.store.bind_unbound_roots("test").unwrap();
        f.store.approve_root(&home, "test").unwrap();
        let fresh = f.dir("containers-2");
        refused(
            f.store.attach_derived_parent(request(&fresh, &epoch)),
            "binding_retired",
        );
        let new_epoch = epoch_of(&f, &home);
        f.store
            .attach_derived_parent(request(&fresh, &new_epoch))
            .unwrap();
    }
}
