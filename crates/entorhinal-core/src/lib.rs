#![deny(unsafe_code)]

//! Domain logic and persistence for the fleet project/workspace registry.
//!
//! This crate knows nothing about subc. The module-facing crate opens the store
//! after HELLO_ACK and calls these synchronous query methods from its handlers.

use std::{
    cmp::Reverse,
    fmt,
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use blake3::Hasher;
use cortexkit_paths::ProjectRootId;
use cortexkit_store::{open_sqlite, Migration, SqliteStore, StorageDescriptor, StoreError};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::Serialize;

pub mod agent;
mod binding;
pub mod catalog;
pub mod catalog_agent;
pub mod catalog_grants;
pub mod catalog_resolution;
pub mod catalog_trust;
pub mod claim_cache;
pub mod context;
mod memory;
pub use memory::{SetProjectDomainRequest, SetWorkspaceDocumentRequest, WorkspaceDocument};
pub mod enable_state;
mod log_schema;
pub mod move_record;
pub mod move_store;
mod mutations;
mod ownership;
mod read_connection;
pub mod remote_apply;
pub mod role_cache;
mod root_keys;
pub mod session_carry;
pub mod session_lifecycle;
pub mod session_network;
pub mod shared_entry;
pub mod store_origin;
mod where_notify;
pub use binding::*;
pub use mutations::*;
pub use ownership::{ResolveRemoteReply, ResolveRemoteStatus, SetOwnedRemotesRequest};
pub use root_keys::{
    AttachRootPreview, AttachRootRequest, ResolveRootKeyReply, ResolveRootKeyRequest, RootKey,
};
pub use session_lifecycle::{
    AgentSession, AgentSessionPage, GoneSession, RunnerSessionBinding, SessionCursor, SessionEnd,
    SESSION_RETENTION_MS,
};
pub use session_network::{
    NetworkChange, NetworkEntry, NetworkStamp, NETWORK_ABSENT, NETWORK_HISTORY_MAX,
};

// The schema-migration namespace, NOT the module id, and deliberately left as
// "projects" while the module id moved to "entorhinal".
//
// This value is recorded in the store's own migration ledger, so changing it
// makes an existing database look unmigrated and re-runs migration 1 against
// tables that already exist. It is unsafe to change once any store exists, and
// changing it here would buy nothing: it is never seen outside the database
// file. A consumer dials the module id; nothing dials this.
const MIGRATION_NAMESPACE: &str = "projects";

/// The complete v1 projection schema. Production mutations write through
/// [`RegistryStore`]'s fenced mutation path in mutations.rs (journal append +
/// projection writes in one transaction); [`RegistryStore::apply_entry`]
/// so the journal append and projection update share one transaction.
pub const V1_SCHEMA: &str = r#"
CREATE TABLE registry_journal (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    op TEXT,
    payload_json TEXT,
    actor TEXT,
    request_key TEXT UNIQUE NULL,
    created_at INTEGER,
    response_json TEXT NULL
);

CREATE TABLE workspace (
    workspace_id TEXT PRIMARY KEY,
    name TEXT,
    created_at INTEGER,
    updated_at INTEGER
);

CREATE TABLE workspace_member (
    workspace_id TEXT REFERENCES workspace(workspace_id),
    ref_kind TEXT CHECK(ref_kind IN ('local','remote')),
    device_fingerprint TEXT NOT NULL,
    project_id TEXT NOT NULL,
    PRIMARY KEY (workspace_id, ref_kind, device_fingerprint, project_id),
    UNIQUE (ref_kind, device_fingerprint, project_id),
    CHECK ((ref_kind = 'local' AND device_fingerprint = '') OR
           (ref_kind = 'remote' AND device_fingerprint <> ''))
);

CREATE TABLE project (
    project_id TEXT PRIMARY KEY,
    name TEXT,
    implicit INTEGER NOT NULL DEFAULT 0,
    seed_identity TEXT NULL,
    created_at INTEGER,
    updated_at INTEGER
);

CREATE TABLE project_workspace (
    project_id TEXT PRIMARY KEY REFERENCES project(project_id),
    workspace_id TEXT REFERENCES workspace(workspace_id)
);

CREATE TABLE project_root (
    canonical_root TEXT PRIMARY KEY,
    project_id TEXT REFERENCES project(project_id),
    added_at INTEGER
);

CREATE TABLE derived_root_parent (
    canonical_parent TEXT PRIMARY KEY, project_id TEXT REFERENCES project(project_id)
);

CREATE TABLE project_alias (
    old_id TEXT PRIMARY KEY,
    project_id TEXT REFERENCES project(project_id),
    created_at INTEGER
);
"#;

/// Migration 2: a workspace may name its root directory. Set only by the
/// operator through `set_workspace_root`, never derived from member paths, so
/// NULL means "no root" and a reader must not guess one.
pub const V2_WORKSPACE_ROOT: &str = "ALTER TABLE workspace ADD COLUMN root TEXT NULL;";

/// Existing journal rows retain NULL; all new writers supply their principal.
pub const V5_JOURNAL_PRINCIPAL: &str =
    "ALTER TABLE registry_journal ADD COLUMN principal TEXT NULL;";

/// Migration 8: the agent session, claim, role and member-name tables.
///
/// Every table here except `member_name` is machine-local: none is listed in
/// the derived-table set that `verify` and `rebuild` replay, none is written
/// through `registry_journal`, and none is ever sent to the identity log.
/// `member_name` holds display names learned from the shared identity log,
/// keyed by the author pseudonym engram returns.
///
/// - `session_binding` maps an agent's session reference (`ref_kind`, `ref`)
///   to the host session that runs it (`harness`, `session_id`), recording
///   which reserved capability registered it. A session key binds at most one
///   reference.
/// - `claim_cache` and `role_cache` cache engram's claim records for agents
///   and roles. A NULL `holder` means the record was absent.
/// - `role_take_request` remembers each operator-approved role take so a
///   retry resends the approved bytes instead of prompting again.
/// - `self_identity` holds this device's own author pseudonym, once learned.
pub const V8_AGENT_SESSIONS: &str = r#"
CREATE TABLE session_binding (
    ref_kind TEXT NOT NULL,
    ref TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    harness TEXT NOT NULL,
    session_id TEXT NOT NULL,
    registered_by TEXT NOT NULL CHECK(registered_by IN ('orchestrator','host')),
    registered_at_ms INTEGER NOT NULL,
    PRIMARY KEY (ref_kind, ref)
);
CREATE UNIQUE INDEX session_binding_session ON session_binding(harness, session_id);

CREATE TABLE claim_cache (
    agent_id TEXT PRIMARY KEY,
    holder TEXT NULL,
    epoch INTEGER NOT NULL,
    cut_id TEXT NULL,
    version INTEGER NOT NULL,
    read_at_ms INTEGER NOT NULL,
    stale INTEGER NOT NULL CHECK(stale IN (0,1)),
    integrity_failed INTEGER NOT NULL DEFAULT 0 CHECK(integrity_failed IN (0,1))
);

CREATE TABLE role_cache (
    role TEXT PRIMARY KEY,
    holder TEXT NULL,
    epoch INTEGER NOT NULL DEFAULT 0,
    source TEXT NULL CHECK(source IN ('cloud','local')),
    stale INTEGER NOT NULL DEFAULT 1 CHECK(stale IN (0,1)),
    version INTEGER NOT NULL DEFAULT 0,
    read_at_ms INTEGER NULL,
    integrity_failed INTEGER NOT NULL DEFAULT 0 CHECK(integrity_failed IN (0,1))
);

CREATE TABLE role_take_request (
    request_id TEXT PRIMARY KEY,
    role TEXT NOT NULL,
    expected_epoch INTEGER NOT NULL,
    reason TEXT NULL,
    expected_version INTEGER NOT NULL,
    body BLOB NOT NULL,
    epoch INTEGER NULL,
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE self_identity (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    self_author TEXT NOT NULL,
    learned_at_ms INTEGER NOT NULL
);

CREATE TABLE member_name (
    author TEXT PRIMARY KEY,
    display_name TEXT NOT NULL
);
"#;

const MIGRATIONS: [Migration; 17] = [
    Migration {
        version: 1,
        statements: V1_SCHEMA,
    },
    Migration {
        version: 2,
        statements: V2_WORKSPACE_ROOT,
    },
    Migration {
        version: 3,
        statements: binding::V3_ROOT_BINDINGS,
    },
    Migration {
        version: 4,
        statements: agent::schema::V4_AGENT_IDENTITY,
    },
    Migration {
        version: 5,
        statements: V5_JOURNAL_PRINCIPAL,
    },
    Migration {
        version: 6,
        statements: ownership::V6_OWNED_REMOTES,
    },
    Migration {
        version: 7,
        statements: log_schema::V7_IDENTITY_LOG,
    },
    Migration {
        version: 8,
        statements: V8_AGENT_SESSIONS,
    },
    Migration {
        version: 9,
        statements: move_store::V9_AGENT_MOVES,
    },
    Migration {
        version: 10,
        statements: agent::consent::V10_CONSENT,
    },
    Migration {
        version: 11,
        statements: catalog::V11_CATALOG,
    },
    Migration {
        version: 12,
        statements: catalog_agent::V12_CATALOG_AGENT,
    },
    Migration {
        version: 13,
        statements: memory::V13_MEMORY_CONTEXT,
    },
    Migration {
        version: 14,
        statements: store_origin::V14_STORE_ORIGIN,
    },
    Migration {
        version: 15,
        statements: session_lifecycle::V15_SESSION_LIFECYCLE,
    },
    Migration {
        version: 16,
        statements: session_network::V16_SESSION_NETWORK,
    },
    Migration {
        version: 17,
        statements: session_lifecycle::V17_SESSION_LOCATION,
    },
];

/// A store opened from the descriptor resolved by subc.
#[derive(Clone)]
pub struct RegistryStore {
    db: Arc<SqliteStore>,
    store_folder: std::path::PathBuf,
    reader: Arc<read_connection::ReadConnection>,
    /// Whether replies carry the root-record fields and walk ancestors. Off,
    /// legacy resolution precedence is preserved and root-record fields are
    /// omitted; the shared-token worktree fallback is available in either mode.
    root_records: bool,
    ids: Arc<binding::IdSource>,
    where_notifications: where_notify::WhereNotifications,
    foreign: Arc<std::sync::atomic::AtomicBool>,
}

/// The durable local log progress. Reading status never contacts the log or
/// waits for an in-flight write's transaction.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct IdentityLogStatus {
    pub state: String,
    pub last_applied_position: i64,
    pub last_seen_head: i64,
    pub pending_write_count: i64,
}

/// A borrowed write context carrying the attested principal independently of
/// the request's actor. Nested writes use the same context, without changing
/// shared store state or allowing a request to set its principal in the body.
pub struct JournalWriter<'a> {
    store: &'a RegistryStore,
    principal: &'a str,
    /// How the caller confirmed a placement change that carries live heads'
    /// and hirees' name claims into the target workspace. Writes that carry
    /// nothing ignore it.
    confirmation: agent::PlacementConfirmation,
    /// Set only while the placement precheck runs a write on a throwaway copy
    /// of the store: the write records the operator summary of what it would
    /// carry here instead of checking `confirmation`.
    probe: Option<&'a std::cell::RefCell<Option<String>>>,
    consent: Option<agent::consent::ConsentCommit>,
    create_avatar: Option<agent::StoredAgentAvatar>,
}

impl std::ops::Deref for JournalWriter<'_> {
    type Target = RegistryStore;

    fn deref(&self) -> &Self::Target {
        self.store
    }
}

impl fmt::Debug for RegistryStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegistryStore")
            .field("epoch", &self.db.epoch())
            .finish_non_exhaustive()
    }
}

impl RegistryStore {
    pub fn identity_log_status(&self) -> Result<IdentityLogStatus, RegistryError> {
        self.read(|conn| {
            conn.query_row(
                "SELECT state,last_applied_position,last_seen_head,(SELECT COUNT(*) FROM pending_entry)
                 FROM identity_log_state WHERE id=1",
                [],
                |row| Ok(IdentityLogStatus {
                    state: row.get(0)?,
                    last_applied_position: row.get(1)?,
                    last_seen_head: row.get(2)?,
                    pending_write_count: row.get(3)?,
                }),
            )
        })
    }

    /// Attribute writes to a route principal. Calls directly on the store are
    /// internal writes and record `entorhinal` instead.
    pub fn with_principal<'a>(&'a self, principal: &'a str) -> JournalWriter<'a> {
        JournalWriter {
            store: self,
            principal,
            confirmation: agent::PlacementConfirmation::NotRequired,
            probe: None,
            consent: None,
            create_avatar: None,
        }
    }

    /// Open the descriptor's sqlite database, acquire its single-writer lease, and
    /// apply the namespaced schema chain exactly once.
    pub fn open(descriptor: &StorageDescriptor) -> Result<Self, RegistryError> {
        Self::open_with_migrations(descriptor, &MIGRATIONS)
    }

    fn open_with_migrations(
        descriptor: &StorageDescriptor,
        migrations: &[Migration],
    ) -> Result<Self, RegistryError> {
        let db = open_sqlite(descriptor).map_err(RegistryError::Store)?;
        let outcome = db
            .migrate(MIGRATION_NAMESPACE, migrations)
            .map_err(RegistryError::Store)?;
        // A store written by a newer binary may hold tables and columns this
        // one cannot query; serving it would answer nothing while looking up.
        // The registry has no additive-only guarantee across its chain, so it
        // refuses rather than guesses.
        if outcome.store_ahead() {
            return Err(RegistryError::Database(format!(
                "registry store is at schema version {}, ahead of this binary's highest migration {}; \
                 run a binary at or above the store's version",
                outcome.recorded, outcome.chain_max
            )));
        }
        let cortexkit_store_types::StorageBackend::Sqlite { path } = &descriptor.backend else {
            return Err(RegistryError::Database(
                "registry requires SQLite storage".into(),
            ));
        };
        let store_folder = std::path::Path::new(path)
            .parent()
            .ok_or_else(|| RegistryError::Database("store has no parent directory".into()))?
            .to_path_buf();
        Ok(Self {
            store_folder,
            reader: Arc::new(read_connection::ReadConnection::open(descriptor)?),
            db: Arc::new(db),
            root_records: false,
            ids: Arc::new(binding::IdSource::Random),
            where_notifications: where_notify::WhereNotifications::default(),
            foreign: Arc::default(),
        })
    }

    /// Folder containing this machine's SQLite store and catalog blobs.
    pub fn store_folder(&self) -> &std::path::Path {
        &self.store_folder
    }

    /// Read the journal head. A query never changes this value.
    pub fn generation(&self) -> Result<i64, RegistryError> {
        self.read(|conn| {
            conn.query_row(
                "SELECT COALESCE(MAX(seq), 0) FROM registry_journal",
                [],
                |row| row.get(0),
            )
        })
    }

    /// Read a resolved path using exact-root, containment, then implicit order.
    pub fn resolve(&self, raw_path: &str) -> Result<ResolveReply, RegistryError> {
        self.resolve_with_binding(raw_path, None)
    }

    /// Resolve, and when root records are enabled also answer whether the
    /// execution binding a caller captured is still the live one.
    pub fn resolve_with_binding(
        &self,
        raw_path: &str,
        binding: Option<&ExecutionBinding>,
    ) -> Result<ResolveReply, RegistryError> {
        self.read(|conn| Ok(self.resolve_in(conn, raw_path, binding)))?
    }

    pub(crate) fn resolve_in(
        &self,
        conn: &Connection,
        raw_path: &str,
        binding: Option<&ExecutionBinding>,
    ) -> Result<ResolveReply, RegistryError> {
        if self.root_records {
            return self.resolve_walk_in(conn, raw_path, binding);
        }
        let (canonical_path, path_exists) = canonical_query_path(Path::new(raw_path))?;
        let query_path = PathBuf::from(&canonical_path);
        let query_device = if path_exists {
            device_id(&query_path)
        } else {
            None
        };

        let query = || -> rusqlite::Result<ResolveReply> {
            if let Some(project_id) = conn
                .query_row(
                    "SELECT project_id FROM project_root WHERE canonical_root = ?1",
                    params![canonical_path],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
            {
                let mut reply = self.reply_for_project(
                    conn,
                    &project_id,
                    "root",
                    !path_exists,
                    self.generation_from_connection(conn)?,
                )?;
                reply.canonical_root = Some(canonical_path.clone());
                return Ok(reply);
            }

            let mut candidates = conn
                .prepare(
                    "SELECT canonical_parent, project_id FROM derived_root_parent ORDER BY canonical_parent",
                )?
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            candidates.sort_by_key(|(parent, _)| Reverse(path_depth(Path::new(parent))));

            for (parent, project_id) in candidates {
                let parent_path = Path::new(&parent);
                if !path_prefix_or_equal(parent_path, &query_path) {
                    continue;
                }

                // Existing queried paths use st_dev when both sides can answer. A
                // vanished registered parent is intentionally accepted by prefix
                // matching, because the path registry remains the fallback truth.
                if let (Some(query_device), Some(parent_device)) =
                    (query_device, device_id(parent_path))
                {
                    if query_device != parent_device {
                        continue;
                    }
                }

                let mut reply = self.reply_for_project(
                    conn,
                    &project_id,
                    "containment",
                    !path_exists,
                    self.generation_from_connection(conn)?,
                )?;
                reply.canonical_root = Some(canonical_path.clone());
                return Ok(reply);
            }

            if let Some(reply) = self.reply_for_worktree(
                conn,
                &query_path,
                !path_exists,
                self.generation_from_connection(conn)?,
            )? {
                return Ok(reply);
            }

            Ok(ResolveReply {
                project_id: implicit_project_id(&canonical_path),
                workspace_id: None,
                workspace_root: None,
                project_name: None,
                via: "implicit".to_string(),
                gone: !path_exists,
                generation: self.generation_from_connection(conn)?,
                canonical_root: Some(canonical_path),
                worktree_of: None,
                root_fields: None,
            })
        };
        query().map_err(Into::into)
    }

    /// Resolve a persisted project id through one live alias hop, or report it as
    /// gone. This is total over all input strings.
    pub fn resolve_project_id(
        &self,
        project_id: &str,
    ) -> Result<ResolveProjectIdReply, RegistryError> {
        self.read(|conn| {
            if conn
                .query_row(
                    "SELECT 1 FROM project WHERE project_id = ?1",
                    params![project_id],
                    |row| row.get::<_, i64>(0),
                )
                .optional()?
                .is_some()
            {
                return Ok(ResolveProjectIdReply {
                    project_id: project_id.to_string(),
                    via: "current".to_string(),
                    gone: false,
                    generation: self.generation_from_connection(conn)?,
                });
            }

            let target = conn
                .query_row(
                    "SELECT project_id FROM project_alias WHERE old_id = ?1",
                    params![project_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            Ok(match target {
                Some(target) => ResolveProjectIdReply {
                    project_id: target,
                    via: "alias".to_string(),
                    gone: false,
                    generation: self.generation_from_connection(conn)?,
                },
                None => ResolveProjectIdReply {
                    project_id: project_id.to_string(),
                    via: "gone".to_string(),
                    gone: true,
                    generation: self.generation_from_connection(conn)?,
                },
            })
        })
    }

    /// Enumerate local projections and remote workspace references without
    /// materializing implicit projects or changing the journal head.
    pub fn enumerate(&self, workspace_id: Option<&str>) -> Result<EnumerateReply, RegistryError> {
        self.read(|conn| {
            let workspaces = if let Some(workspace_id) = workspace_id {
                conn.query_row(
                    "SELECT w.workspace_id, w.name, r.root FROM workspace w
                     LEFT JOIN workspace_root r ON r.workspace_id=w.workspace_id
                     WHERE w.workspace_id = ?1",
                    params![workspace_id],
                    |row| {
                        Ok(vec![WorkspaceSummary {
                            workspace_id: row.get(0)?,
                            name: row.get(1)?,
                            root: row.get(2)?,
                        }])
                    },
                )
                .optional()?
                .unwrap_or_default()
            } else {
                let mut statement = conn.prepare(
                    "SELECT w.workspace_id, w.name, r.root FROM workspace w
                     LEFT JOIN workspace_root r ON r.workspace_id=w.workspace_id
                     ORDER BY w.workspace_id",
                )?;
                let rows = statement
                    .query_map([], |row| {
                        Ok(WorkspaceSummary {
                            workspace_id: row.get(0)?,
                            name: row.get(1)?,
                            root: row.get(2)?,
                        })
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                rows
            };

            let mut projects = Vec::new();
            if let Some(workspace_id) = workspace_id {
                let mut statement = conn.prepare(
                    "SELECT ref_kind, device_fingerprint, project_id
                     FROM workspace_member
                     WHERE workspace_id = ?1
                     ORDER BY ref_kind, device_fingerprint, project_id",
                )?;
                let members = statement
                    .query_map(params![workspace_id], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                for (ref_kind, device_fingerprint, project_id) in members {
                    projects.push(self.project_view(
                        conn,
                        &project_id,
                        &ref_kind,
                        if ref_kind == "remote" {
                            Some(device_fingerprint)
                        } else {
                            None
                        },
                    )?);
                }
            } else {
                let mut statement = conn.prepare(
                    "SELECT project_id, name, implicit FROM project ORDER BY project_id",
                )?;
                let local_projects = statement
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(2)?,
                        ))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                for (project_id, name, implicit) in local_projects {
                    projects.push(self.project_view_from_parts(
                        conn,
                        project_id,
                        name,
                        implicit != 0,
                        "local".to_string(),
                        None,
                    )?);
                }

                let mut statement = conn.prepare(
                    "SELECT device_fingerprint, project_id
                     FROM workspace_member
                     WHERE ref_kind = 'remote'
                     ORDER BY device_fingerprint, project_id",
                )?;
                let remote_projects = statement
                    .query_map([], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                for (device_fingerprint, project_id) in remote_projects {
                    projects.push(self.project_view_from_parts(
                        conn,
                        project_id.clone(),
                        project_id,
                        false,
                        "remote".to_string(),
                        Some(device_fingerprint),
                    )?);
                }
            }

            Ok(EnumerateReply {
                workspaces,
                projects,
                generation: self.generation_from_connection(conn)?,
            })
        })
    }

    /// Read project journal rows after `after_seq`. Advance a full page to its
    /// last seq, or a partial page to generation, passing skipped identity rows.
    pub fn journal_tail(
        &self,
        after_seq: i64,
        limit: i64,
    ) -> Result<JournalTailReply, RegistryError> {
        let limit = limit.clamp(1, 1_000);
        self.read(|conn| {
            let mut statement = conn.prepare(&format!(
                "SELECT seq, op, payload_json, actor, request_key, created_at, principal
                 FROM registry_journal
                  WHERE seq > ?1 AND op NOT IN ({}, '{}', '{}', 'store.adopt')
                 ORDER BY seq
                 LIMIT ?2",
                agent::change_ops_sql(),
                agent::IDENTITY_MARKER_OP,
                // An agent's catalog pins are agent state, not project history.
                catalog_agent::PIN_OP,
            ))?;
            let entries = statement
                .query_map(params![after_seq, limit], |row| {
                    Ok(JournalEntry {
                        seq: row.get(0)?,
                        op: row.get(1)?,
                        payload_json: row.get(2)?,
                        actor: row.get(3)?,
                        request_key: row.get(4)?,
                        created_at: row.get(5)?,
                        principal: row.get(6)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(JournalTailReply {
                entries,
                generation: self.generation_from_connection(conn)?,
            })
        })
    }

    /// Canonicalize a mutation path with cortexkit-paths. Mutations require an
    /// existing path and therefore cannot accept the query-side gone fallback.
    pub fn canonical_mutation_root(raw_path: &str) -> Result<String, RegistryError> {
        let id = ProjectRootId::from_path(raw_path)
            .map_err(|error| RegistryError::Path(error.to_string()))?;
        unicode_path(&id.into_path_buf())
    }

    /// Reduce a query path, canonicalizing its existing prefix while preserving
    /// the caller's spelling of missing tail components, without case folding
    /// those components even on a case-insensitive filesystem. The boolean reports existence.
    pub fn canonical_query_path(path: &Path) -> Result<(String, bool), RegistryError> {
        canonical_query_path(path)
    }

    /// The only projection-write entry point. The journal row is inserted before
    /// the supplied projection closure and both commit atomically.
    pub fn apply_entry<T>(
        &self,
        op: &str,
        payload_json: &str,
        actor: &str,
        request_key: Option<&str>,
        apply_projection: impl FnOnce(&Transaction<'_>) -> rusqlite::Result<T>,
    ) -> Result<(i64, T), RegistryError> {
        self.with_principal("entorhinal").apply_entry(
            op,
            payload_json,
            actor,
            request_key,
            apply_projection,
        )
    }

    fn read<T>(
        &self,
        query: impl FnOnce(&Connection) -> rusqlite::Result<T>,
    ) -> Result<T, RegistryError> {
        self.reader.read(query).map_err(RegistryError::from)
    }

    fn generation_from_connection(&self, conn: &Connection) -> rusqlite::Result<i64> {
        conn.query_row(
            "SELECT COALESCE(MAX(seq), 0) FROM registry_journal",
            [],
            |row| row.get(0),
        )
    }

    fn reply_for_project(
        &self,
        conn: &Connection,
        project_id: &str,
        via: &str,
        gone: bool,
        generation: i64,
    ) -> rusqlite::Result<ResolveReply> {
        let context = conn.query_row(
            "SELECT p.name, w.workspace_id, r.root
             FROM project AS p
             LEFT JOIN project_workspace AS pw ON pw.project_id = p.project_id
             LEFT JOIN workspace AS w ON w.workspace_id = pw.workspace_id
             LEFT JOIN workspace_root AS r ON r.workspace_id = w.workspace_id
             WHERE p.project_id = ?1",
            params![project_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )?;
        Ok(ResolveReply {
            project_id: project_id.to_string(),
            workspace_id: context.1,
            workspace_root: context.2,
            project_name: context.0.into(),
            via: via.to_string(),
            gone,
            generation,
            canonical_root: None,
            worktree_of: None,
            root_fields: None,
        })
    }

    fn project_view(
        &self,
        conn: &Connection,
        project_id: &str,
        ref_kind: &str,
        device_fingerprint: Option<String>,
    ) -> rusqlite::Result<ProjectView> {
        let row = conn
            .query_row(
                "SELECT name, implicit FROM project WHERE project_id = ?1",
                params![project_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()?;
        match row {
            Some((name, implicit)) => self.project_view_from_parts(
                conn,
                project_id.to_string(),
                name,
                implicit != 0,
                ref_kind.to_string(),
                device_fingerprint,
            ),
            None => self.project_view_from_parts(
                conn,
                project_id.to_string(),
                project_id.to_string(),
                false,
                ref_kind.to_string(),
                device_fingerprint,
            ),
        }
    }

    fn project_view_from_parts(
        &self,
        conn: &Connection,
        project_id: String,
        name: String,
        implicit: bool,
        ref_kind: String,
        device_fingerprint: Option<String>,
    ) -> rusqlite::Result<ProjectView> {
        let mut statement = conn.prepare(
            "SELECT canonical_root FROM project_root
             WHERE project_id = ?1 ORDER BY canonical_root",
        )?;
        let roots = statement
            .query_map(params![project_id], |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<String>>>()?;
        // The same placement `resolve` reports, read from the same table, so
        // the two surfaces cannot disagree about where a project lives.
        let workspace_id = conn
            .query_row(
                "SELECT workspace_id FROM project_workspace WHERE project_id = ?1",
                params![project_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let root_records = if self.root_records {
            Some(binding::root_records(conn, &project_id)?)
        } else {
            None
        };
        Ok(ProjectView {
            project_id,
            name,
            roots,
            implicit,
            ref_kind,
            device_fingerprint,
            workspace_id,
            last_route_activity_ms: None,
            root_records,
        })
    }
}

impl JournalWriter<'_> {
    /// Record how the operator confirmed this write. A placement change that
    /// would carry live agents checks it under the writer lock: `Approved`
    /// must match the recomputed summary, and `NoPrompt` refuses any carry.
    pub fn with_confirmation(mut self, confirmation: agent::PlacementConfirmation) -> Self {
        self.confirmation = confirmation;
        self
    }

    /// Append a row and apply its projection atomically under this principal.
    pub fn apply_entry<T>(
        &self,
        op: &str,
        payload_json: &str,
        actor: &str,
        request_key: Option<&str>,
        apply_projection: impl FnOnce(&Transaction<'_>) -> rusqlite::Result<T>,
    ) -> Result<(i64, T), RegistryError> {
        self.with_where_changes(|tx| {
            // With the log on, compare the shared tables before and after
            // this write, inside its own transaction, to record what it
            // changed. Domain writes and this lower-level append both do.
            let before = if log_schema::log_enabled(tx)? {
                Some(shared_entry::SharedState::capture(tx)?)
            } else {
                None
            };
            tx.execute(
                "INSERT INTO registry_journal
                     (op, payload_json, actor, request_key, created_at, principal)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    op,
                    payload_json,
                    actor,
                    request_key,
                    now_unix_millis(),
                    self.principal
                ],
            )?;
            let actual_seq = tx.last_insert_rowid();
            let result = apply_projection(tx)?;
            if let Some(before) = before {
                let after = shared_entry::SharedState::capture(tx)?;
                shared_entry::record_local_entry(
                    tx,
                    op,
                    actual_seq,
                    payload_json,
                    &before,
                    &after,
                    Vec::new(),
                )?;
            }
            Ok((actual_seq, result))
        })
        .map_err(RegistryError::Store)
    }
}

/// A session registration as `agent.register_session` admits it. The caller's
/// role is decided by the module from the resolved capability providers;
/// the store only records it.
#[derive(Debug, Clone)]
pub struct SessionRegistration<'a> {
    pub agent_id: &'a str,
    pub ref_kind: &'a str,
    pub session_ref: &'a str,
    pub harness: &'a str,
    pub session_id: &'a str,
    /// Omission keeps a known root; a new binding stores an unknown root.
    pub project_root: Option<&'a str>,
    /// `orchestrator` or `host`.
    pub registered_by: &'a str,
    /// The llm-runner module hosting the session, when the caller named one.
    /// Re-registering without one keeps the runner already recorded.
    pub runner: Option<&'a str>,
    /// Whether the session's agent can reach the network, when the caller
    /// said. It is appended to the binding's network history only when it
    /// differs from the current value; `None` records nothing.
    pub network: Option<session_network::NetworkStamp<'a>>,
    pub now_ms: i64,
}

/// One row of `session_binding`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SessionBinding {
    pub agent_id: String,
    pub ref_kind: String,
    #[serde(rename = "ref")]
    pub session_ref: String,
    pub harness: Option<String>,
    pub session_id: Option<String>,
    pub project_root: Option<String>,
    pub registered_by: String,
    pub registered_at_ms: i64,
    #[serde(rename = "heldOn", skip_serializing_if = "Option::is_none")]
    pub held_on: Option<String>,
}

impl SessionBinding {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            agent_id: row.get(0)?,
            ref_kind: row.get(1)?,
            session_ref: row.get(2)?,
            harness: row.get(3)?,
            session_id: row.get(4)?,
            project_root: row.get(5)?,
            registered_by: row.get(6)?,
            registered_at_ms: row.get(7)?,
            held_on: row.get(8)?,
        })
    }
}

const SESSION_BINDING_COLUMNS: &str =
    "agent_id,ref_kind,ref,harness,session_id,NULLIF(project_root,''),registered_by,registered_at_ms,held_on";

fn session_refusal(
    code: &str,
    message: impl Into<String>,
    detail: Option<serde_json::Value>,
) -> agent::AgentMutationError {
    agent::AgentMutationError {
        code: code.into(),
        message: message.into(),
        detail,
    }
}

impl RegistryStore {
    /// Bind a session reference to an agent and to the host session running
    /// it. The binding is machine-local: it is never journaled and never sent
    /// to the identity log.
    ///
    /// Every check reads the same SQLite transaction that writes the binding,
    /// so nothing can change the agent or the bindings between the check and
    /// the write. Refusals, in order: `authority_not_cut_over` (the journal
    /// holds no agent-identity cutover marker yet, so this store does not own
    /// agent identity), `unknown_agent`, `gone` (the agent is retired or
    /// merged), `agent_moving` (an open move fences this registration),
    /// `claim_integrity_failed`, `held_elsewhere` (a cached claim at
    /// epoch 1 or higher belongs to another device), `ref_bound_elsewhere` (the reference already belongs to
    /// another agent, or a re-key's destination is already occupied) and
    /// `session_bound_elsewhere` (the session key already binds another agent
    /// or reference kind; also when the
    /// runner the binding would carry and the session id already bind
    /// another reference).
    /// Registering the same reference again for the same agent moves it to the
    /// new session key and records the new caller role and time. It also
    /// clears the binding's end time, and records the runner when one is
    /// named, keeping the recorded one otherwise. A network value that
    /// differs from the binding's current one is appended to its history; a
    /// value other than `yes`, `no` or `unknown` refuses `invalid_request`
    /// and a full history `network_history_full`, both with nothing written.
    /// A new reference for the same agent, reference kind and host session
    /// re-keys the existing binding and its network history, preserving its
    /// caller role. Omitting the project root keeps a recorded root on either
    /// kind of registration; a new binding without one reads as unknown.
    pub fn register_session(
        &self,
        request: &SessionRegistration<'_>,
    ) -> Result<SessionBinding, agent::AgentMutationError> {
        let outcome = self
            .db
            .with_conn_fenced(|tx| Ok(Self::register_session_in(tx, request)));
        match outcome {
            Ok(result) => result,
            Err(error) => Err(session_refusal("storage_error", error.to_string(), None)),
        }
    }

    // A refusal is returned inside `Ok`, which commits the transaction. That
    // is safe only because every check runs before the writes at the end,
    // so a refused registration has written nothing.
    fn register_session_in(
        tx: &Transaction<'_>,
        request: &SessionRegistration<'_>,
    ) -> Result<SessionBinding, agent::AgentMutationError> {
        let cut_over = tx
            .query_row(
                "SELECT 1 FROM registry_journal WHERE op=?1 LIMIT 1",
                [agent::IDENTITY_MARKER_OP],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !cut_over {
            return Err(session_refusal(
                "authority_not_cut_over",
                "agent identity authority is not cut over",
                None,
            ));
        }
        let row = agent::load_row(tx, request.agent_id)?.ok_or_else(|| {
            session_refusal(
                "unknown_agent",
                format!("unknown agent {}", request.agent_id),
                None,
            )
        })?;
        if let Some(gone) = row.gone() {
            return Err(session_refusal("gone", "agent is gone", Some(gone)));
        }
        // Remote metadata is never an invitation to revive a conversation,
        // even if the agent's claim or move state has changed since import.
        let remote: Option<String> = tx
            .query_row(
                "SELECT held_on FROM session_binding WHERE held_on IS NOT NULL AND
             ((ref_kind=?1 AND ref=?2) OR (harness=?3 AND session_id=?4)) LIMIT 1",
                params![
                    request.ref_kind,
                    request.session_ref,
                    request.harness,
                    request.session_id
                ],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(machine_id) = remote {
            return Err(session_refusal(
                "session_on_other_machine",
                "session data is held on another machine",
                Some(serde_json::json!({"machine_id": machine_id})),
            ));
        }
        move_store::check_moving_registration(tx, request)?;
        claim_cache::check_registration_claim(tx, request.agent_id)?;
        let by_ref = tx
            .query_row(
                &format!("SELECT {SESSION_BINDING_COLUMNS} FROM session_binding WHERE ref_kind=?1 AND ref=?2"),
                params![request.ref_kind, request.session_ref],
                SessionBinding::from_row,
            )
            .optional()?;
        if let Some(bound) = &by_ref {
            if bound.agent_id != request.agent_id {
                return Err(session_refusal(
                    "ref_bound_elsewhere",
                    format!("this reference is bound to agent {}", bound.agent_id),
                    Some(serde_json::json!({"agent_id": bound.agent_id})),
                ));
            }
        }
        let by_session = tx
            .query_row(
                &format!("SELECT {SESSION_BINDING_COLUMNS} FROM session_binding WHERE harness=?1 AND session_id=?2"),
                params![request.harness, request.session_id],
                SessionBinding::from_row,
            )
            .optional()?;
        if let Some(bound) = &by_session {
            if bound.agent_id != request.agent_id || bound.ref_kind != request.ref_kind {
                return Err(session_refusal(
                    "session_bound_elsewhere",
                    format!(
                        "this session already binds reference {}:{} of agent {}",
                        bound.ref_kind, bound.session_ref, bound.agent_id
                    ),
                    Some(serde_json::json!({
                        "ref_kind": bound.ref_kind,
                        "ref": bound.session_ref,
                        "agent_id": bound.agent_id,
                    })),
                ));
            }
        }
        let rekey = by_session
            .as_ref()
            .filter(|bound| bound.session_ref != request.session_ref);
        if rekey.is_some() {
            if let Some(bound) = &by_ref {
                return Err(session_refusal(
                    "ref_bound_elsewhere",
                    format!("this reference is bound to agent {}", bound.agent_id),
                    Some(serde_json::json!({"agent_id": bound.agent_id})),
                ));
            }
        }
        let previous_ref = rekey.map_or(request.session_ref, |bound| bound.session_ref.as_str());
        // A runner owns its session namespace, so one runner and session id
        // name one binding. The runner this registration leaves on the row is
        // the one it names, or else the one the binding already had.
        let runner = match request.runner {
            Some(runner) => Some(runner.to_owned()),
            None => tx
                .query_row(
                    "SELECT runner FROM session_binding WHERE ref_kind=?1 AND ref=?2",
                    params![request.ref_kind, previous_ref],
                    |row| row.get::<_, Option<String>>(0),
                )
                .optional()?
                .flatten(),
        };
        if let Some(runner) = &runner {
            if let Some((ref_kind, session_ref, agent_id)) =
                session_lifecycle::runner_session_owner(tx, runner, request.session_id)?
            {
                if ref_kind != request.ref_kind || session_ref != previous_ref {
                    return Err(session_refusal(
                        "session_bound_elsewhere",
                        format!(
                            "this session already binds reference {ref_kind}:{session_ref} of agent {agent_id}"
                        ),
                        Some(serde_json::json!({
                            "ref_kind": ref_kind,
                            "ref": session_ref,
                            "agent_id": agent_id,
                        })),
                    ));
                }
            }
        }
        // The network checks run with the others, before the insert, so a
        // refused value or a full history leaves the binding untouched.
        let network_plan = match request.network {
            Some(stamp) => Some(session_network::plan_network(
                tx,
                request.ref_kind,
                previous_ref,
                stamp.network,
            )?),
            None => None,
        };
        // The existing foreign key is immediate. Defer it until commit so both
        // keys can be updated without deleting the binding or its history.
        // Roll back all writes on error, since refusals are returned as values
        // to the caller's transaction rather than causing it to roll back.
        tx.execute_batch("SAVEPOINT session_registration")?;
        let written = (|| -> Result<SessionBinding, agent::AgentMutationError> {
            if let Some(bound) = rekey {
                tx.execute_batch("PRAGMA defer_foreign_keys=ON")?;
                tx.execute(
                    "UPDATE session_binding SET ref=?1 WHERE ref_kind=?2 AND ref=?3",
                    params![request.session_ref, request.ref_kind, bound.session_ref],
                )?;
                tx.execute(
                    "UPDATE session_network SET ref=?1 WHERE ref_kind=?2 AND ref=?3",
                    params![request.session_ref, request.ref_kind, bound.session_ref],
                )?;
            }
            let registered_by =
                rekey.map_or(request.registered_by, |bound| bound.registered_by.as_str());
            // A registration means the session is live, so it clears any end
            // time an earlier run of the same reference left behind.
            tx.execute(
                "INSERT INTO session_binding(agent_id,ref_kind,ref,harness,session_id,project_root,registered_by,registered_at_ms,runner,ended_at_ms)
             VALUES(?1,?2,?3,?4,?5,COALESCE(?6,''),?7,?8,?9,NULL)
             ON CONFLICT(ref_kind,ref) DO UPDATE SET harness=excluded.harness,
                  session_id=excluded.session_id,
                  project_root=COALESCE(?6, session_binding.project_root),
                 registered_by=excluded.registered_by, registered_at_ms=excluded.registered_at_ms,
                 runner=COALESCE(excluded.runner, session_binding.runner), ended_at_ms=NULL",
                params![
                    request.agent_id,
                    request.ref_kind,
                    request.session_ref,
                    request.harness,
                    request.session_id,
                    request.project_root,
                    registered_by,
                    request.now_ms,
                    request.runner
                ],
            )?;
            // After the binding exists, because each history row references it.
            if let (Some(stamp), Some(plan)) = (request.network, network_plan) {
                session_network::apply_network_plan(
                    tx,
                    request.ref_kind,
                    request.session_ref,
                    stamp.network,
                    plan,
                    stamp.clock,
                )?;
            }
            Ok(tx.query_row(
                &format!("SELECT {SESSION_BINDING_COLUMNS} FROM session_binding WHERE ref_kind=?1 AND ref=?2"),
                params![request.ref_kind, request.session_ref],
                SessionBinding::from_row,
            )?)
        })();
        if written.is_err() {
            tx.execute_batch("ROLLBACK TO session_registration")?;
        }
        tx.execute_batch("RELEASE session_registration")?;
        written
    }

    /// The binding a host session key selects, if any. Reads never wait for a
    /// writer.
    pub fn session_binding_for(
        &self,
        harness: &str,
        session_id: &str,
    ) -> Result<Option<SessionBinding>, RegistryError> {
        self.read(|conn| {
            conn.query_row(
                &format!("SELECT {SESSION_BINDING_COLUMNS} FROM session_binding WHERE harness=?1 AND session_id=?2"),
                params![harness, session_id],
                SessionBinding::from_row,
            )
            .optional()
        })
    }

    /// The binding of each session reference, in the order given, read in one
    /// snapshot. `None` marks a reference that was never registered.
    pub fn session_bindings_for_refs(
        &self,
        refs: &[(&str, &str)],
    ) -> Result<Vec<Option<SessionBinding>>, RegistryError> {
        self.read(|conn| {
            let mut statement = conn.prepare(&format!(
                "SELECT {SESSION_BINDING_COLUMNS} FROM session_binding WHERE ref_kind=?1 AND ref=?2"
            ))?;
            refs.iter()
                .map(|(ref_kind, session_ref)| {
                    statement
                        .query_row(params![ref_kind, session_ref], SessionBinding::from_row)
                        .optional()
                })
                .collect()
        })
    }
}

/// A domain failure that can be mapped to a module error without importing subc.
/// `detail` carries structured data for refusals that need it (for example
/// which agent holds a name a placement change would carry into a workspace).
#[derive(Debug)]
pub enum RegistryError {
    Store(StoreError),
    Database(String),
    Path(String),
    Domain {
        code: String,
        message: String,
        detail: Option<serde_json::Value>,
    },
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(error) => write!(f, "{error}"),
            Self::Database(error) => write!(f, "database: {error}"),
            Self::Path(error) => write!(f, "path: {error}"),
            Self::Domain { code, message, .. } => write!(f, "{code}: {message}"),
        }
    }
}

impl RegistryError {
    /// True when the store could not be opened because another process still
    /// holds its single-writer lease. The lease is a kernel advisory lock
    /// released the instant the holder exits, so at startup this means the
    /// previous instance of this module has not finished exiting yet — a
    /// condition that clears by itself — and never a store defect.
    pub fn is_lease_held(&self) -> bool {
        matches!(
            self,
            Self::Store(StoreError::Lease(cortexkit_lease::LeaseError::Held { .. }))
        )
    }
}

impl std::error::Error for RegistryError {}

impl From<rusqlite::Error> for RegistryError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Database(error.to_string())
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolveReply {
    pub project_id: String,
    pub workspace_id: Option<String>,
    /// The workspace's root directory as the operator set it, or null when the
    /// project has no workspace or its workspace has no root. Never derived
    /// from member paths: absent means "no root", not "unknown".
    pub workspace_root: Option<String>,
    pub project_name: Option<String>,
    pub via: String,
    pub gone: bool,
    pub generation: i64,
    pub canonical_root: Option<String>,
    /// The registered checkout whose current binding identifies this worktree.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worktree_of: Option<String>,
    /// Present only when root records are enabled, except for worktree replies;
    /// see [`RootFields`]. A main checkout's approval does not approve its worktrees.
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub root_fields: Option<RootFields>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolveProjectIdReply {
    pub project_id: String,
    pub via: String,
    pub gone: bool,
    pub generation: i64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceSummary {
    pub workspace_id: String,
    pub name: String,
    /// Same meaning as `ResolveReply::workspace_root`.
    pub root: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectView {
    pub project_id: String,
    pub name: String,
    pub roots: Vec<String>,
    pub implicit: bool,
    pub ref_kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_fingerprint: Option<String>,
    /// Workspace this project is placed in; absent when unplaced. Additive for
    /// consumers joining projects onto workspace rosters (prefrontal's peer
    /// roster) so membership costs no second per-workspace call.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_route_activity_ms: Option<i64>,
    /// Present only when root records are enabled: one record per root, in
    /// the same order as `roots`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_records: Option<Vec<RootRecord>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EnumerateReply {
    pub workspaces: Vec<WorkspaceSummary>,
    pub projects: Vec<ProjectView>,
    pub generation: i64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JournalEntry {
    pub seq: i64,
    pub op: String,
    pub payload_json: String,
    pub actor: String,
    pub request_key: Option<String>,
    pub created_at: i64,
    /// The principal recorded with this row; NULL for rows written before the
    /// column existed. Not serialised, so `journal_tail` replies keep the shape
    /// their existing consumers decode.
    #[serde(skip)]
    pub principal: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JournalTailReply {
    pub entries: Vec<JournalEntry>,
    pub generation: i64,
}

/// Derive the reserved offline implicit id from the canonical UTF-8 path bytes.
pub fn implicit_project_id(canonical_path: &str) -> String {
    let mut hasher = Hasher::new();
    hasher.update(canonical_path.as_bytes());
    let digest = hasher.finalize();
    format!("pj-implicit1-{}", hex_prefix(digest.as_bytes()))
}

fn hex_prefix(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(16);
    for byte in bytes.iter().take(8) {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn canonical_query_path(path: &Path) -> Result<(String, bool), RegistryError> {
    let lexical = lexical_absolute(path)?;
    if lexical
        .try_exists()
        .map_err(|error| RegistryError::Path(error.to_string()))?
    {
        let canonical = ProjectRootId::from_path(&lexical)
            .map_err(|error| RegistryError::Path(error.to_string()))?;
        return Ok((unicode_path(&canonical.into_path_buf())?, true));
    }

    let mut probe = lexical.clone();
    let mut missing_components = Vec::new();
    while !probe
        .try_exists()
        .map_err(|error| RegistryError::Path(error.to_string()))?
    {
        let Some(component) = probe.file_name().map(|value| value.to_os_string()) else {
            break;
        };
        missing_components.push(component);
        if !probe.pop() {
            break;
        }
    }

    if probe
        .try_exists()
        .map_err(|error| RegistryError::Path(error.to_string()))?
    {
        let existing = ProjectRootId::from_path(&probe)
            .map_err(|error| RegistryError::Path(error.to_string()))?;
        let mut canonical = existing.into_path_buf();
        for component in missing_components.iter().rev() {
            canonical.push(component);
        }
        Ok((unicode_path(&canonical)?, false))
    } else {
        Ok((unicode_path(&lexical)?, false))
    }
}

fn lexical_absolute(path: &Path) -> Result<PathBuf, RegistryError> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| RegistryError::Path(error.to_string()))?
            .join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(Path::new(std::path::MAIN_SEPARATOR_STR)),
            Component::CurDir => {}
            Component::ParentDir => {
                let _ = normalized.pop();
            }
            Component::Normal(value) => normalized.push(value),
        }
    }
    Ok(normalized)
}

fn unicode_path(path: &Path) -> Result<String, RegistryError> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| RegistryError::Path(format!("path_not_unicode: {}", path.display())))
}

pub fn path_prefix_or_equal(parent: &Path, query: &Path) -> bool {
    let mut parent_components = parent.components();
    let mut query_components = query.components();
    loop {
        match parent_components.next() {
            Some(parent_component) => {
                if Some(parent_component) != query_components.next() {
                    return false;
                }
            }
            None => return true,
        }
    }
}

fn path_depth(path: &Path) -> usize {
    path.components().count()
}

#[cfg(unix)]
fn device_id(path: &Path) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).ok().map(|metadata| metadata.dev())
}

#[cfg(windows)]
// Windows exposes directory volume identity only through its handle-based FFI.
#[allow(unsafe_code)]
fn device_id(path: &Path) -> Option<u64> {
    use std::os::windows::{fs::OpenOptionsExt, io::AsRawHandle};
    use windows_sys::Win32::Storage::FileSystem::{
        GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION, FILE_FLAG_BACKUP_SEMANTICS,
    };
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
        .ok()?;
    let mut information: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    // The owned file keeps the directory handle valid for this metadata read.
    let succeeded = unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut information) };
    (succeeded != 0).then_some(u64::from(information.dwVolumeSerialNumber))
}

#[cfg(not(any(unix, windows)))]
fn device_id(_path: &Path) -> Option<u64> {
    None
}

pub(crate) fn now_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
#[path = "../tests/support/scratch_cleanup.rs"]
pub(crate) mod scratch_cleanup;

#[cfg(test)]
mod tests {
    include!("catalog_tests.rs");
    mod store_origin_tests {
        include!("store_origin_tests.rs");
    }
    use super::*;
    use std::{
        fs,
        sync::atomic::{AtomicU64, Ordering},
    };

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestStore {
        root: PathBuf,
        store: RegistryStore,
        _cleanup: crate::scratch_cleanup::ScratchCleanup,
    }

    impl TestStore {
        fn new(label: &str) -> Self {
            let root = std::env::temp_dir().join(format!(
                "ck-entorhinal-{label}-{}-{}",
                std::process::id(),
                TEST_COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
            let descriptor = StorageDescriptor {
                module_id: format!("projects-test-{}", TEST_COUNTER.load(Ordering::Relaxed)),
                storage_namespace: "test".to_string(),
                isolation: cortexkit_store::Isolation::Module,
                backend: cortexkit_store::StorageBackend::Sqlite {
                    path: root.join("store.db").to_string_lossy().into_owned(),
                },
            };
            let store = RegistryStore::open(&descriptor).expect("open test store");
            Self {
                _cleanup: crate::scratch_cleanup::ScratchCleanup(root.clone()),
                root,
                store,
            }
        }
    }

    fn add_project(store: &RegistryStore, id: &str, root: Option<&Path>, parent: Option<&Path>) {
        store
            .apply_entry("test_register", "{}", "test", None, |tx| {
                tx.execute(
                    "INSERT INTO project (project_id, name, implicit, created_at, updated_at)
                     VALUES (?1, ?2, 0, 0, 0)",
                    params![id, id],
                )?;
                if let Some(root) = root {
                    tx.execute(
                        "INSERT INTO project_root (canonical_root, project_id, added_at)
                         VALUES (?1, ?2, 0)",
                        params![ProjectRootId::from_path(root).unwrap().to_string(), id],
                    )?;
                }
                if let Some(parent) = parent {
                    tx.execute(
                        "INSERT INTO derived_root_parent (canonical_parent, project_id)
                         VALUES (?1, ?2)",
                        params![ProjectRootId::from_path(parent).unwrap().to_string(), id],
                    )?;
                }
                Ok(())
            })
            .expect("insert test project");
    }

    fn legacy_descriptor(scratch: &TestStore, file: &str) -> StorageDescriptor {
        StorageDescriptor {
            module_id: "session-migration-test".into(),
            storage_namespace: "test".into(),
            isolation: cortexkit_store::Isolation::Module,
            backend: cortexkit_store::StorageBackend::Sqlite {
                path: scratch.root.join(file).to_string_lossy().into_owned(),
            },
        }
    }

    /// A store last opened by a binary whose chain ends at migration 7 gains
    /// all six session, claim, role and name tables, with exactly these
    /// columns, when this binary opens it.
    #[test]
    fn opening_a_v7_store_creates_every_session_and_claim_table() {
        let scratch = TestStore::new("v8-upgrade");
        let descriptor = legacy_descriptor(&scratch, "v7.db");
        let old = RegistryStore::open_with_migrations(&descriptor, &MIGRATIONS[..7]).unwrap();
        let missing = old
            .read(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name='session_binding'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
            })
            .unwrap();
        assert_eq!(missing, 0, "a V7 store has no session tables yet");
        drop(old);
        let store = RegistryStore::open(&descriptor).unwrap();
        let expected: [(&str, &[&str]); 6] = [
            (
                "session_binding",
                &[
                    "ref_kind",
                    "ref",
                    "agent_id",
                    "harness",
                    "session_id",
                    "project_root",
                    "registered_by",
                    "registered_at_ms",
                    // Added by migration 15.
                    "runner",
                    "ended_at_ms",
                    "held_on",
                ],
            ),
            (
                "claim_cache",
                &[
                    "agent_id",
                    "holder",
                    "epoch",
                    "cut_id",
                    "version",
                    "read_at_ms",
                    "stale",
                    "integrity_failed",
                ],
            ),
            (
                "role_cache",
                &[
                    "role",
                    "holder",
                    "epoch",
                    "source",
                    "stale",
                    "version",
                    "read_at_ms",
                    "integrity_failed",
                ],
            ),
            (
                "role_take_request",
                &[
                    "request_id",
                    "role",
                    "expected_epoch",
                    "reason",
                    "expected_version",
                    "body",
                    "epoch",
                    "created_at_ms",
                ],
            ),
            ("self_identity", &["id", "self_author", "learned_at_ms"]),
            ("member_name", &["author", "display_name"]),
        ];
        store
            .read(|conn| {
                for (table, columns) in expected {
                    let found = conn
                        .prepare(&format!("PRAGMA table_info({table})"))?
                        .query_map([], |row| row.get::<_, String>(1))?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    assert_eq!(found, columns, "{table}");
                }
                let unique: Vec<String> = conn
                    .prepare("PRAGMA index_info(session_binding_session)")?
                    .query_map([], |row| row.get::<_, String>(2))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                assert_eq!(unique, ["harness", "session_id"]);
                let is_unique: i64 = conn.query_row(
                    "SELECT \"unique\" FROM pragma_index_list('session_binding') WHERE name='session_binding_session'",
                    [],
                    |row| row.get(0),
                )?;
                assert_eq!(is_unique, 1, "one session key binds one reference");
                Ok(())
            })
            .unwrap();
    }

    /// A binary whose chain stops at migration 7 must refuse a store this
    /// binary migrated, naming the chain's current version, and leave it untouched.
    #[test]
    fn binary_without_session_migration_refuses_migrated_store_as_ahead() {
        let scratch = TestStore::new("v8-ahead");
        let descriptor = legacy_descriptor(&scratch, "v8.db");
        drop(RegistryStore::open(&descriptor).unwrap());
        let error = RegistryStore::open_with_migrations(&descriptor, &MIGRATIONS[..7]).unwrap_err();
        assert!(matches!(error, RegistryError::Database(_)));
        assert_eq!(
            error.to_string(),
            format!(
                "database: registry store is at schema version {}, ahead of this binary's highest migration 7; run a binary at or above the store's version",
                MIGRATIONS.last().unwrap().version
            )
        );
        drop(RegistryStore::open(&descriptor).unwrap());
    }

    #[test]
    fn journal_principal_migration_keeps_legacy_cache_actor_and_projection() {
        let scratch = TestStore::new("legacy-journal-principal");
        let descriptor = StorageDescriptor {
            module_id: "legacy-journal-test".into(),
            storage_namespace: "test".into(),
            isolation: cortexkit_store::Isolation::Module,
            backend: cortexkit_store::StorageBackend::Sqlite {
                path: scratch
                    .root
                    .join("legacy.db")
                    .to_string_lossy()
                    .into_owned(),
            },
        };
        let old = RegistryStore::open_with_migrations(&descriptor, &MIGRATIONS[..4]).unwrap();
        let request = RegisterRequest {
            project_id: Some("legacy".into()),
            name: "Legacy".into(),
            request_key: Some("K".into()),
            actor: Some("original actor".into()),
            ..Default::default()
        };
        let cached =
            r#"{"result":{"projectId":"legacy","name":"Legacy","generation":1,"noop":false}}"#;
        old.db.with_conn_fenced(|tx| {
            tx.execute(
                "INSERT INTO registry_journal(op,payload_json,actor,request_key,created_at,response_json)
                 VALUES('register',?1,'original actor','K',123,?2)",
                params![serde_json::to_string(&request).unwrap(), cached],
            )?;
            tx.execute("INSERT INTO project(project_id,name,implicit,created_at,updated_at) VALUES('legacy','Legacy',0,123,123)", [])?;
            Ok(())
        }).unwrap();
        drop(old);

        let store = RegistryStore::open(&descriptor).unwrap();
        store
            .read(|conn| {
                let columns = conn
                    .prepare("PRAGMA table_info(registry_journal)")?
                    .query_map([], |row| row.get::<_, String>(1))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                assert_eq!(
                    columns,
                    [
                        "seq",
                        "op",
                        "payload_json",
                        "actor",
                        "request_key",
                        "created_at",
                        "response_json",
                        "principal",
                        "stream",
                        "entry_id",
                        "log_position",
                        "origin",
                        "entry"
                    ]
                );
                Ok(())
            })
            .unwrap();
        let before = serde_json::to_value(store.enumerate(None).unwrap()).unwrap();
        assert_eq!(
            store.with_principal("direct").register(request).unwrap(),
            cached.as_bytes()
        );
        assert_eq!(store.generation().unwrap(), 1);
        store.rebuild().unwrap();
        assert_eq!(
            serde_json::to_value(store.enumerate(None).unwrap()).unwrap(),
            before
        );
        let row = store.journal_tail(0, 10).unwrap().entries.remove(0);
        assert_eq!(row.principal, None);
        assert_eq!(row.actor, "original actor");
        assert_eq!(row.request_key.as_deref(), Some("K"));
        assert_eq!(row.op, "register");
        drop(store);
    }

    #[test]
    fn journal_writer_principal_is_independent_of_actor_and_does_not_leak() {
        let scratch = TestStore::new("journal-writers");
        for (index, principal) in ["direct", "reserved:prefrontal-core", "unverified"]
            .into_iter()
            .enumerate()
        {
            scratch
                .store
                .with_principal(principal)
                .register(RegisterRequest {
                    project_id: Some(format!("project-{index}")),
                    name: format!("Project {index}"),
                    actor: Some("body actor".into()),
                    ..Default::default()
                })
                .unwrap();
        }
        scratch
            .store
            .apply_entry("internal", "{}", "internal actor", None, |_| Ok(()))
            .unwrap();
        scratch
            .store
            .with_principal("reserved:other")
            .apply_entry("explicit", "{}", "another actor", None, |_| Ok(()))
            .unwrap();
        let entries = scratch.store.journal_tail(0, 10).unwrap().entries;
        assert_eq!(
            entries
                .iter()
                .map(|row| row.principal.as_deref())
                .collect::<Vec<_>>(),
            [
                Some("direct"),
                Some("reserved:prefrontal-core"),
                Some("unverified"),
                Some("entorhinal"),
                Some("reserved:other")
            ]
        );
        assert_eq!(
            entries
                .iter()
                .take(3)
                .map(|row| row.actor.as_str())
                .collect::<Vec<_>>(),
            ["body actor"; 3]
        );
        assert!(
            serde_json::to_value(&entries[0])
                .unwrap()
                .get("principal")
                .is_none(),
            "the project journal wire shape stays unchanged"
        );
    }

    #[test]
    fn implicit_id_matches_the_blake3_contract_golden_vector() {
        assert_eq!(
            implicit_project_id("/Users/example/Projects/alpha"),
            "pj-implicit1-c79cc26141bb294a"
        );
    }

    #[test]
    fn resolution_order_and_prefix_boundary_are_explicit() {
        let test = TestStore::new("resolution");
        let root = test.root.join("tree");
        let parent = root.join("repo");
        let nested = parent.join("worktrees");
        let exact = nested.join("checkout");
        let boundary = root.join("repository-other");
        fs::create_dir_all(&exact).expect("create resolution directories");
        fs::create_dir_all(&boundary).expect("create boundary directory");

        add_project(&test.store, "p-containment", None, Some(&parent));
        add_project(&test.store, "p-nested", None, Some(&nested));
        add_project(&test.store, "p-exact", Some(&exact), None);

        assert_eq!(
            test.store.resolve(&exact.to_string_lossy()).unwrap().via,
            "root"
        );
        assert_eq!(
            test.store
                .resolve(&nested.to_string_lossy())
                .unwrap()
                .project_id,
            "p-nested"
        );
        assert_eq!(
            test.store
                .resolve(&parent.to_string_lossy())
                .unwrap()
                .project_id,
            "p-containment"
        );
        assert_eq!(
            test.store.resolve(&boundary.to_string_lossy()).unwrap().via,
            "implicit"
        );
    }

    #[test]
    fn schema_enforces_root_pk_check_both_ways_and_reference_unique() {
        let test = TestStore::new("constraints");
        test.store
            .apply_entry("setup", "{}", "test", None, |tx| {
                tx.execute(
                    "INSERT INTO workspace (workspace_id, name, created_at, updated_at)
                     VALUES ('w1', 'one', 0, 0)",
                    [],
                )?;
                tx.execute(
                    "INSERT INTO workspace (workspace_id, name, created_at, updated_at)
                     VALUES ('w2', 'two', 0, 0)",
                    [],
                )?;
                tx.execute(
                    "INSERT INTO project (project_id, name, created_at, updated_at)
                     VALUES ('p1', 'one', 0, 0)",
                    [],
                )?;
                tx.execute(
                    "INSERT INTO project (project_id, name, created_at, updated_at)
                     VALUES ('p2', 'two', 0, 0)",
                    [],
                )?;
                tx.execute(
                    "INSERT INTO project_root (canonical_root, project_id, added_at)
                     VALUES ('/same', 'p1', 0)",
                    [],
                )?;
                Ok(())
            })
            .expect("schema setup");

        assert!(test
            .store
            .apply_entry("duplicate_root", "{}", "test", None, |tx| {
                tx.execute(
                    "INSERT INTO project_root (canonical_root, project_id, added_at)
                     VALUES ('/same', 'p2', 0)",
                    [],
                )?;
                Ok(())
            })
            .is_err());

        assert!(test
            .store
            .apply_entry("invalid_local", "{}", "test", None, |tx| {
                tx.execute(
                    "INSERT INTO workspace_member
                     (workspace_id, ref_kind, device_fingerprint, project_id)
                     VALUES ('w1', 'local', 'device', 'p1')",
                    [],
                )?;
                Ok(())
            })
            .is_err());
        assert!(test
            .store
            .apply_entry("invalid_remote", "{}", "test", None, |tx| {
                tx.execute(
                    "INSERT INTO workspace_member
                     (workspace_id, ref_kind, device_fingerprint, project_id)
                     VALUES ('w1', 'remote', '', 'remote-p')",
                    [],
                )?;
                Ok(())
            })
            .is_err());

        test.store
            .apply_entry("valid_local", "{}", "test", None, |tx| {
                tx.execute(
                    "INSERT INTO workspace_member
                     (workspace_id, ref_kind, device_fingerprint, project_id)
                     VALUES ('w1', 'local', '', 'p1')",
                    [],
                )?;
                Ok(())
            })
            .expect("valid local reference");
        assert!(test
            .store
            .apply_entry("duplicate_local", "{}", "test", None, |tx| {
                tx.execute(
                    "INSERT INTO workspace_member
                     (workspace_id, ref_kind, device_fingerprint, project_id)
                     VALUES ('w2', 'local', '', 'p1')",
                    [],
                )?;
                Ok(())
            })
            .is_err());

        test.store
            .apply_entry("valid_remote", "{}", "test", None, |tx| {
                tx.execute(
                    "INSERT INTO workspace_member
                     (workspace_id, ref_kind, device_fingerprint, project_id)
                     VALUES ('w1', 'remote', 'device-a', 'remote-p')",
                    [],
                )?;
                Ok(())
            })
            .expect("valid remote reference");
        assert!(test
            .store
            .apply_entry("duplicate_remote", "{}", "test", None, |tx| {
                tx.execute(
                    "INSERT INTO workspace_member
                     (workspace_id, ref_kind, device_fingerprint, project_id)
                     VALUES ('w2', 'remote', 'device-a', 'remote-p')",
                    [],
                )?;
                Ok(())
            })
            .is_err());
    }
}

#[cfg(test)]
mod path_identity_tests {
    use super::*;
    use crate::mutations::tests::Fixture;

    #[test]
    fn missing_query_tail_preserves_the_callers_case() {
        let f = Fixture::new("query-case");
        let prefix = f.dir("MiXeD");
        let path = Path::new(&prefix).join("mIxEdMissing").join("TaIl");
        let (reduced, exists) = RegistryStore::canonical_query_path(&path).unwrap();
        assert!(!exists);
        assert_eq!(Path::new(&reduced), path);
        let (existing, exists) = RegistryStore::canonical_query_path(Path::new(&prefix)).unwrap();
        assert!(exists);
        assert_eq!(existing, prefix);
    }

    #[test]
    fn missing_query_tail_canonicalizes_its_existing_symlink_prefix() {
        let f = Fixture::new("query-symlink");
        let prefix = f.dir("real");
        let alias = f.root.join("alias");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&prefix, &alias).unwrap();
        #[cfg(windows)]
        if let Err(error) = std::os::windows::fs::symlink_dir(&prefix, &alias) {
            eprintln!("SKIP missing_query_tail_canonicalizes_its_existing_symlink_prefix: {error}");
            return;
        }
        let path = alias.join("MiSsInG").join("TaIl");
        let (reduced, exists) = RegistryStore::canonical_query_path(&path).unwrap();
        assert!(!exists);
        assert_eq!(
            Path::new(&reduced),
            Path::new(&prefix).join("MiSsInG").join("TaIl")
        );
    }

    #[cfg(windows)]
    #[test]
    fn verbatim_mutation_input_is_still_refused_as_not_canonical() {
        let f = Fixture::new("verbatim");
        let canonical = f.dir("root");
        let verbatim = std::fs::canonicalize(&canonical).unwrap();
        assert_ne!(verbatim.to_str().unwrap(), canonical);
        let error = f
            .store
            .register(RegisterRequest {
                name: "verbatim".into(),
                roots: vec![verbatim.to_str().unwrap().into()],
                ..Default::default()
            })
            .unwrap_err();
        assert!(error.to_string().starts_with("not_canonical"), "{error}");
    }

    #[cfg(windows)]
    #[allow(unsafe_code)]
    fn volume_serial(path: &Path) -> u64 {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::{GetVolumeInformationW, GetVolumePathNameW};
        let path: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
        let mut volume = vec![0u16; 32768];
        assert_ne!(
            unsafe { GetVolumePathNameW(path.as_ptr(), volume.as_mut_ptr(), volume.len() as u32) },
            0,
            "{}",
            std::io::Error::last_os_error()
        );
        let mut serial = 0;
        assert_ne!(
            unsafe {
                GetVolumeInformationW(
                    volume.as_ptr(),
                    std::ptr::null_mut(),
                    0,
                    &mut serial,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    0,
                )
            },
            0,
            "{}",
            std::io::Error::last_os_error()
        );
        u64::from(serial)
    }

    #[cfg(windows)]
    #[test]
    fn directory_device_ids_equal_the_volume_serial() {
        let f = Fixture::new("volume");
        let a = f.dir("a");
        let b = f.dir("b");
        let serial = volume_serial(Path::new(&a));
        assert_eq!(device_id(Path::new(&a)), Some(serial));
        assert_eq!(device_id(Path::new(&b)), Some(serial));
    }

    /// On Windows set CK_TEST_VOLUME_A and CK_TEST_VOLUME_B to existing directories
    /// on different volumes, then run `cargo test --workspace --all-targets --locked
    /// directory_device_ids_differ_across_volumes -- --ignored`.
    #[cfg(windows)]
    #[test]
    #[ignore = "requires two independently mounted volumes"]
    fn directory_device_ids_differ_across_volumes() {
        let a = std::env::var_os("CK_TEST_VOLUME_A").expect("set CK_TEST_VOLUME_A");
        let b = std::env::var_os("CK_TEST_VOLUME_B").expect("set CK_TEST_VOLUME_B");
        let a = Path::new(&a);
        let b = Path::new(&b);
        assert_ne!(
            volume_serial(a),
            volume_serial(b),
            "choose different volumes"
        );
        assert_eq!(device_id(a), Some(volume_serial(a)));
        assert_eq!(device_id(b), Some(volume_serial(b)));
        assert_ne!(device_id(a), device_id(b));
    }
}
