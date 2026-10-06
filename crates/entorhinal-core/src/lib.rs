#![forbid(unsafe_code)]

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
mod log_schema;
mod mutations;
mod ownership;
mod read_connection;
pub mod remote_apply;
mod root_keys;
pub mod shared_entry;
pub use binding::*;
pub use mutations::*;
pub use ownership::{ResolveRemoteReply, ResolveRemoteStatus, SetOwnedRemotesRequest};
pub use root_keys::{
    AttachRootPreview, AttachRootRequest, ResolveRootKeyReply, ResolveRootKeyRequest, RootKey,
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

const MIGRATIONS: [Migration; 7] = [
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
];

/// A store opened from the descriptor resolved by subc.
#[derive(Clone)]
pub struct RegistryStore {
    db: Arc<SqliteStore>,
    reader: Arc<read_connection::ReadConnection>,
    /// Whether replies carry the root-record fields and walk ancestors. Off,
    /// every reply is byte-for-byte the pre-multi-root shape, so a consumer
    /// that cannot read the new fields is never sent them.
    root_records: bool,
    ids: Arc<binding::IdSource>,
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
        Ok(Self {
            reader: Arc::new(read_connection::ReadConnection::open(descriptor)?),
            db: Arc::new(db),
            root_records: false,
            ids: Arc::new(binding::IdSource::Random),
        })
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
        if self.root_records {
            return self.resolve_walk(raw_path, binding);
        }
        let (canonical_path, path_exists) = canonical_query_path(Path::new(raw_path))?;
        let query_path = PathBuf::from(&canonical_path);
        let query_device = if path_exists {
            device_id(&query_path)
        } else {
            None
        };

        self.read(|conn| {
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

            Ok(ResolveReply {
                project_id: implicit_project_id(&canonical_path),
                workspace_id: None,
                workspace_root: None,
                project_name: None,
                via: "implicit".to_string(),
                gone: !path_exists,
                generation: self.generation_from_connection(conn)?,
                canonical_root: Some(canonical_path),
                root_fields: None,
            })
        })
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
                  WHERE seq > ?1 AND op NOT IN ({}, '{}')
                 ORDER BY seq
                 LIMIT ?2",
                agent::change_ops_sql(),
                agent::IDENTITY_MARKER_OP,
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
        ProjectRootId::from_path(raw_path)
            .map(|id| id.to_string())
            .map_err(|error| RegistryError::Path(error.to_string()))
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
    /// Append a row and apply its projection atomically under this principal.
    pub fn apply_entry<T>(
        &self,
        op: &str,
        payload_json: &str,
        actor: &str,
        request_key: Option<&str>,
        apply_projection: impl FnOnce(&Transaction<'_>) -> rusqlite::Result<T>,
    ) -> Result<(i64, T), RegistryError> {
        self.db
            .with_conn_fenced(|tx| {
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
                    )?;
                }
                Ok((actual_seq, result))
            })
            .map_err(RegistryError::Store)
    }
}

/// A domain failure that can be mapped to a module error without importing subc.
#[derive(Debug)]
pub enum RegistryError {
    Store(StoreError),
    Database(String),
    Path(String),
    Domain { code: String, message: String },
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(error) => write!(f, "{error}"),
            Self::Database(error) => write!(f, "database: {error}"),
            Self::Path(error) => write!(f, "path: {error}"),
            Self::Domain { code, message } => write!(f, "{code}: {message}"),
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
    /// Present only when root records are enabled; see [`RootFields`].
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
    if lexical.exists() {
        let canonical = ProjectRootId::from_path(&lexical)
            .map_err(|error| RegistryError::Path(error.to_string()))?;
        return Ok((canonical.to_string(), true));
    }

    let mut probe = lexical.clone();
    let mut missing_components = Vec::new();
    while !probe.exists() {
        let Some(component) = probe.file_name().map(|value| value.to_os_string()) else {
            break;
        };
        missing_components.push(component);
        if !probe.pop() {
            break;
        }
    }

    if probe.exists() {
        let existing = ProjectRootId::from_path(&probe)
            .map_err(|error| RegistryError::Path(error.to_string()))?;
        let mut canonical = existing.into_path_buf();
        for component in missing_components.iter().rev() {
            canonical.push(component);
        }
        Ok((canonical.to_string_lossy().into_owned(), false))
    } else {
        Ok((lexical.to_string_lossy().into_owned(), false))
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

pub(crate) fn path_prefix_or_equal(parent: &Path, query: &Path) -> bool {
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

#[cfg(not(unix))]
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
mod tests {
    use super::*;
    use std::{
        fs,
        sync::atomic::{AtomicU64, Ordering},
    };

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestStore {
        root: PathBuf,
        store: RegistryStore,
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
            Self { root, store }
        }
    }

    impl Drop for TestStore {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
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
