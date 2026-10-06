//! Read core's frozen registry and commit its identity after-images together with
//! the cutover marker. Runtime state remains in core and is never projected here.

use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction};
use serde::Deserialize;
use serde_json::{json, Value};

use super::{
    claims::write_claim, normalize_agent_name, store::write_row, valid_agent_id, AgentChangeEntry,
    AgentMutationError, AgentNameClaim, AgentRow, StoredAgentAvatar,
};
use crate::{mutations::append, mutations::Action, JournalWriter, RegistryStore};

/// The complete source column list, including runtime fields that are discarded.
/// Keeping the actual SELECT public lets the migration fixture test detect drift
/// even when a newly added source column would otherwise be silently ignored.
/// Source: prefrontal 873870be8 crates/prefrontal-core-store/migrations/
/// 082_wake_delivery.sql:9-32, 109_agent_generation.sql:1-2,
/// 112_agent_avatar.sql:14-17, 126_agent_labels.sql:3.
pub const SNAPSHOT_AGENT_COLUMNS: &str = "agent_id,name,name_normalization_version,tag,role,project_id,workspace_id,persona_ref,name_version,wake_policy_json,wake_policy_version,residence_machine_id,residence_harness,residence_address_json,residence_epoch,residence_state,sleep,created_at_ms,updated_at_ms,terminal_reason,terminal_at_ms,merged_into_agent_id,github_identity_json,generation,avatar_genome,avatar_type,avatar_version,labels_json";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ImportRequest {
    snapshot_path: String,
    request_key: Option<String>,
    actor: Option<String>,
}

struct Snapshot {
    agents: Vec<AgentRow>,
    claims: Vec<AgentNameClaim>,
}

fn table_present(conn: &Connection, table: &str) -> rusqlite::Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1)",
        [table],
        |r| r.get(0),
    )
}

fn source_json<T: serde::de::DeserializeOwned>(value: String) -> rusqlite::Result<T> {
    serde_json::from_str(&value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })
}

fn read_snapshot(path: &str) -> Result<Snapshot, AgentMutationError> {
    let read = || -> rusqlite::Result<Snapshot> {
        // READ_ONLY also prevents a missing path from creating an empty database.
        // A read transaction pins both tables to the same source snapshot.
        let mut conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        let tx = conn.transaction()?;
        if !table_present(&tx, "agent")? {
            return Ok(Snapshot {
                agents: vec![],
                claims: vec![],
            });
        }
        let agents = tx
            .prepare(&format!(
                "SELECT {SNAPSHOT_AGENT_COLUMNS} FROM agent ORDER BY agent_id COLLATE BINARY"
            ))?
            .query_map([], |r| {
                let genome: Option<String> = r.get("avatar_genome")?;
                let avatar = genome
                    .map(|genome| -> rusqlite::Result<_> {
                        Ok(StoredAgentAvatar {
                            genome,
                            avatar_type: r.get("avatar_type")?,
                            version: r.get("avatar_version")?,
                        })
                    })
                    .transpose()?;
                let terminal: Option<String> = r.get("terminal_reason")?;
                Ok(AgentRow {
                    agent_id: r.get("agent_id")?,
                    name: r.get("name")?,
                    name_version: r.get("name_version")?,
                    name_normalization_version: r.get("name_normalization_version")?,
                    tag: r.get("tag")?,
                    labels: source_json(r.get("labels_json")?)?,
                    role: r.get("role")?,
                    project_id: r.get("project_id")?,
                    workspace_id: r.get("workspace_id")?,
                    avatar,
                    github_identity: r
                        .get::<_, Option<String>>("github_identity_json")?
                        .map(source_json)
                        .transpose()?,
                    status: match terminal.as_deref() {
                        None => "live".into(),
                        Some("deleted") => "retired".into(),
                        Some(reason) => reason.into(),
                    },
                    merged_into: r.get("merged_into_agent_id")?,
                    supervisor_agent_id: None,
                    request_key: None,
                    created_at_ms: r.get("created_at_ms")?,
                    updated_at_ms: r.get("updated_at_ms")?,
                    terminal_at_ms: r.get("terminal_at_ms")?,
                    agent_generation: r.get("generation")?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let claims = if table_present(&tx, "agent_name_claim")? {
            tx.prepare("SELECT claim_id,agent_id,namespace_kind,namespace_key,normalized_name,name_normalization_version,display_name,claimed_at_ms,released_at_ms FROM agent_name_claim ORDER BY claim_id")?
                .query_map([], |r| Ok(AgentNameClaim {
                    claim_id: r.get(0)?, agent_id: r.get(1)?, namespace_kind: r.get(2)?,
                    namespace_key: r.get(3)?, normalized_name: r.get(4)?,
                    name_normalization_version: r.get(5)?, display_name: r.get(6)?,
                    claimed_at_ms: r.get(7)?, released_at_ms: r.get(8)?,
                }))?.collect::<rusqlite::Result<Vec<_>>>()?
        } else {
            vec![]
        };
        Ok(Snapshot { agents, claims })
    };
    read().map_err(|error| {
        AgentMutationError::new(
            "invalid_request",
            format!("cannot read snapshot_path '{path}': {error}"),
        )
    })
}

fn invariant(detail: Value) -> AgentMutationError {
    AgentMutationError {
        code: "import_invariant_failed".into(),
        message: format!("source registry fails {}", detail["check"]),
        detail: Some(detail),
    }
}

impl Snapshot {
    fn validate(&self, tx: &Transaction<'_>) -> Result<(), AgentMutationError> {
        // Source rows and claims are ordered at read time, so each pass reports
        // the smallest offending id rather than whichever row SQLite finds first.
        let ids: BTreeSet<_> = self
            .agents
            .iter()
            .map(|row| row.agent_id.as_str())
            .collect();
        for row in &self.agents {
            if !valid_agent_id(&row.agent_id) {
                return Err(invariant(
                    json!({"check":"agent_id", "agent_id":row.agent_id}),
                ));
            }
        }
        for claim in &self.claims {
            if !ids.contains(claim.agent_id.as_str()) {
                return Err(invariant(
                    json!({"check":"claim_owner", "claim_id":claim.claim_id, "agent_id":claim.agent_id}),
                ));
            }
        }
        let mut by_owner: BTreeMap<&str, Vec<&AgentNameClaim>> = BTreeMap::new();
        for claim in &self.claims {
            by_owner.entry(&claim.agent_id).or_default().push(claim);
        }
        for row in self.agents.iter().filter(|row| row.status == "live") {
            let active: Vec<_> = by_owner
                .get(row.agent_id.as_str())
                .into_iter()
                .flatten()
                .filter(|claim| claim.released_at_ms.is_none())
                .collect();
            let normalized = normalize_agent_name(&row.name);
            if active.len() != 1
                || !normalized.is_ok_and(|name| {
                    active[0].normalized_name == name.normalized_name
                        && active[0].display_name == row.name
                        && active[0].name_normalization_version == row.name_normalization_version
                })
            {
                return Err(invariant(
                    json!({"check":"claim_mismatch", "agent_id":row.agent_id}),
                ));
            }
        }
        let mut heads: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for row in self
            .agents
            .iter()
            .filter(|row| row.status == "live" && row.role == "head")
        {
            if let Some(project) = &row.project_id {
                heads.entry(project).or_default().push(&row.agent_id);
            }
        }
        if let Some((project, heads)) = heads
            .iter()
            .filter(|(_, heads)| heads.len() > 1)
            .min_by_key(|(_, heads)| heads[0])
        {
            return Err(invariant(
                json!({"check":"live_head", "project_id":project, "agent_ids":heads}),
            ));
        }
        for row in &self.agents {
            if row
                .merged_into
                .as_deref()
                .is_some_and(|id| !ids.contains(id))
            {
                return Err(invariant(
                    json!({"check":"merged_into", "agent_id":row.agent_id, "merged_into":row.merged_into}),
                ));
            }
        }
        for row in self.agents.iter().filter(|row| row.status == "live") {
            if let Some(project) = &row.project_id {
                let exists: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM project WHERE project_id=?1)",
                    [project],
                    |r| r.get(0),
                )?;
                if !exists {
                    return Err(invariant(
                        json!({"check":"binding", "agent_id":row.agent_id, "project_id":project, "workspace_id":null}),
                    ));
                }
            }
            if row.role == "workspace_head" {
                let exists: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM workspace WHERE workspace_id=?1)",
                    [&row.workspace_id],
                    |r| r.get(0),
                )?;
                if !exists {
                    return Err(invariant(
                        json!({"check":"binding", "agent_id":row.agent_id, "project_id":null, "workspace_id":row.workspace_id}),
                    ));
                }
            }
        }
        Ok(())
    }
}

impl RegistryStore {
    /// Callers must have admitted the route first: this does no permission check
    /// of its own. The serving path calls it through `with_principal`, so the
    /// journal records the caller the daemon verified, not the `actor` the caller
    /// wrote into the request. Called directly, the writer is recorded as `entorhinal`.
    pub fn agent_import(&self, params: Value, now: i64) -> Result<Vec<u8>, AgentMutationError> {
        self.with_principal("entorhinal").agent_import(params, now)
    }
}

impl JournalWriter<'_> {
    pub fn agent_import(&self, params: Value, now: i64) -> Result<Vec<u8>, AgentMutationError> {
        if matches!(
            self.identity_log_status()?.state.as_str(),
            "enabling" | "joining"
        ) {
            return Err(AgentMutationError::new(
                "identity_log_enabling",
                "identity log bootstrap is in progress",
            ));
        }
        let request: ImportRequest = serde_json::from_value(params)
            .map_err(|error| AgentMutationError::new("invalid_request", error.to_string()))?;
        let key = request
            .request_key
            .as_deref()
            .filter(|key| !key.is_empty())
            .ok_or_else(|| {
                AgentMutationError::new("request_key_required", "request_key is required")
            })?;
        // The marker owns the import's key and cached reply. Using its journal
        // op here lets the shared transaction/cache path handle restart retries
        // and cross-operation key reuse without an extra identity journal entry.
        self.mutation_with_error("agent.cutover", Some(key), |tx| {
            if tx.query_row("SELECT 1 FROM registry_journal WHERE op='agent.cutover' LIMIT 1", [], |_| Ok(())).optional()?.is_some() {
                return Err(AgentMutationError::new("import_already_done", "agent identity import is already complete"));
            }
            let snapshot = read_snapshot(&request.snapshot_path)?;
            snapshot.validate(tx)?;
            // A merged target can follow its source in id order. Delay reference
            // checks until every imported row has been written in this transaction.
            tx.execute_batch("PRAGMA defer_foreign_keys=ON")?;
            let actor = request.actor.as_deref().unwrap_or("module");
            let mut by_owner: BTreeMap<&str, Vec<AgentNameClaim>> = BTreeMap::new();
            for claim in &snapshot.claims {
                by_owner.entry(&claim.agent_id).or_default().push(claim.clone());
            }
            for row in &snapshot.agents {
                write_row(tx, row)?;
                let claims = by_owner.remove(row.agent_id.as_str()).unwrap_or_default();
                for claim in &claims { write_claim(tx, claim)?; }
                let mut entry = AgentChangeEntry::new("agent.import", row.clone(), claims);
                entry.seq = append(tx, "agent.import", &json!({}), actor, None, now, self.principal)?;
                tx.execute("UPDATE registry_journal SET payload_json=?1 WHERE seq=?2", rusqlite::params![json!({"entry":entry}).to_string(), entry.seq])?;
            }
            let seq = append(tx, "agent.cutover", &json!({}), actor, Some(key), now, self.principal)?;
            Ok(Action {
                changed: true, seq: Some(seq),
                value: json!({"agents_imported":snapshot.agents.len(), "claims_imported":snapshot.claims.len()}),
                payload: json!({"snapshot_path":request.snapshot_path}),
            })
        })
    }
}
