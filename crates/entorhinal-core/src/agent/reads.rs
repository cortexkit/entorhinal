//! Identity reads share a SQLite snapshot with the journal head. Incarnation is
//! attached by the serving process, never persisted in the identity store.

use std::collections::BTreeSet;

use rusqlite::Connection;
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{json, Value};

use super::{
    normalize_agent_name, store::load_row, validate_project_id, validate_workspace_id,
    AgentMutationError, AgentRow,
};
use crate::RegistryStore;

pub(super) fn decode<T: DeserializeOwned>(params: Value) -> Result<T, AgentMutationError> {
    serde_json::from_value(params)
        .map_err(|error| AgentMutationError::new("invalid_request", error.to_string()))
}

pub(super) fn all_rows(conn: &Connection) -> rusqlite::Result<Vec<AgentRow>> {
    let ids = conn
        .prepare("SELECT agent_id FROM agent ORDER BY agent_id")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ids.iter()
        .map(|id| load_row(conn, id)?.ok_or(rusqlite::Error::QueryReturnedNoRows))
        .collect()
}

pub(super) fn activated(conn: &Connection) -> rusqlite::Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM registry_journal WHERE op='agent.cutover')",
        [],
        |r| r.get(0),
    )
}

// Request bodies for the agent reads: core's field names and casing for the same
// op, minus the caller-session and residence fields core uses for its own
// residence checks. Core strips those before relaying, so they are refused here.
// Source: prefrontal 873870be8 crates/prefrontal-core-module/src/agent_registry_ops.rs:558-601,658-705.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IdRequest {
    agent_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NameRequest {
    name: String,
    workspace_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Role {
    Assistant,
    WorkspaceHead,
    Head,
    Hiree,
}

impl Role {
    fn as_str(&self) -> &str {
        match self {
            Self::Assistant => "assistant",
            Self::WorkspaceHead => "workspace_head",
            Self::Head => "head",
            Self::Hiree => "hiree",
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListRequest {
    role: Option<Role>,
    project_id: Option<String>,
    workspace_id: Option<String>,
    include_gone: Option<bool>,
    activated_only: Option<bool>,
    cursor: Option<String>,
    // Include the full JSON integer range so out-of-range integers reach the
    // invalid_cursor check instead of failing decode as invalid_request.
    limit: Option<i128>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PeerRequest {
    workspace_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AvatarRequest {
    agent_ids: Vec<String>,
}

impl RegistryStore {
    pub(super) fn read_agent<T>(
        &self,
        query: impl FnOnce(&Connection) -> Result<T, AgentMutationError>,
    ) -> Result<T, AgentMutationError> {
        self.read(|conn| Ok(query(conn)))?
    }

    /// Serve inherited reads and the new resolve shape. The module adds its
    /// current incarnation to the result object, as it does for mutation replies.
    pub fn agent_read(&self, op: &str, params: Value) -> Result<Vec<u8>, AgentMutationError> {
        let value = match op {
            "agent.resolve" => {
                let request: IdRequest = decode(params)?;
                self.read_agent(|conn| {
                    let row = load_row(conn, &request.agent_id)?;
                    Ok(json!({"agent_id":request.agent_id,
                        "status":row.as_ref().map_or("unknown", |row| row.status.as_str()),
                        "merged_into":row.as_ref().and_then(|row| row.merged_into.as_ref()),
                        "gone":row.as_ref().and_then(AgentRow::gone),
                        "generation":self.generation_from_connection(conn)?}))
                })?
            }
            "agent.resolve_name" => {
                let request: NameRequest = decode(params)?;
                self.read_agent(|conn| {
                    let mut reply = resolve_name(conn, &request)?;
                    reply["generation"] = json!(self.generation_from_connection(conn)?);
                    Ok(reply)
                })?
            }
            "agent.list" => {
                let request: ListRequest = decode(params)?;
                let limit = request.limit.unwrap_or(50);
                if !(1..=200).contains(&limit) || request.cursor.as_deref() == Some("") {
                    return Err(AgentMutationError::new(
                        "invalid_cursor",
                        "limit must be 1..=200 and cursor must not be empty",
                    ));
                }
                // An invalid `project_id` or `workspace_id` filter is a malformed
                // query, not a bad agent shape, so core answers `invalid_request`
                // here rather than create's `invalid_role_shape`. Kept the same.
                let project = request
                    .project_id
                    .as_deref()
                    .map(validate_project_id)
                    .transpose()
                    .map_err(|_| {
                        AgentMutationError::new("invalid_request", "invalid project_id filter")
                    })?;
                let workspace = request
                    .workspace_id
                    .as_deref()
                    .map(validate_workspace_id)
                    .transpose()
                    .map_err(|_| {
                        AgentMutationError::new("invalid_request", "invalid workspace_id filter")
                    })?;
                self.read_agent(|conn| {
                    let generation = self.generation_from_connection(conn)?;
                    if request.activated_only.unwrap_or(false) && !activated(conn)? {
                        return Ok(json!({"agents":[], "generation":generation}));
                    }
                    let mut rows = all_rows(conn)?.into_iter().filter(|row| {
                        (request.include_gone.unwrap_or(false) || row.status == "live")
                            && request.role.as_ref().is_none_or(|role| row.role == role.as_str())
                            && project.as_deref().is_none_or(|id| row.project_id.as_deref() == Some(id))
                            && workspace.as_deref().is_none_or(|id| row.workspace_id.as_deref() == Some(id))
                            && request.cursor.as_ref().is_none_or(|id| &row.agent_id > id)
                    }).take(limit as usize + 1).collect::<Vec<_>>();
                    let more = rows.len() > limit as usize;
                    rows.truncate(limit as usize);
                    let mut reply = json!({"agents":rows.iter().filter(|r| r.status == "live").map(AgentRow::digest).collect::<Vec<_>>(), "generation":generation});
                    let gone = rows.iter().filter_map(|row| row.gone().map(|gone| json!({"agent_id":row.agent_id,"gone":gone}))).collect::<Vec<_>>();
                    if !gone.is_empty() { reply["gone"] = json!(gone); }
                    if more { reply["next_cursor"] = json!(rows.last().map(|row| &row.agent_id)); }
                    Ok(reply)
                })?
            }
            "agent.peer_roster" => {
                let request: PeerRequest = decode(params)?;
                // As with the list filters, an invalid `workspace_id` here is a
                // malformed query, and core answers `invalid_request`.
                validate_workspace_id(&request.workspace_id).map_err(|_| {
                    AgentMutationError::new("invalid_request", "invalid workspace_id")
                })?;
                self.read_agent(|conn| {
                    if !activated(conn)? {
                        return Err(AgentMutationError::new("registry_not_activated", "agent registry identity is not activated"));
                    }
                    let projects = conn.prepare("SELECT project_id FROM project_workspace WHERE workspace_id=?1")?
                        .query_map([&request.workspace_id], |r| r.get::<_, String>(0))?
                        .collect::<rusqlite::Result<BTreeSet<_>>>()?;
                    // A workspace's peers are its live workspace head plus the live heads of
                    // projects placed in it, ordered by agent id, exactly as core lists them.
                    // Whether a peer is reachable right now is core's to add: it depends on
                    // residence, which entorhinal doesn't hold.
                    // Source: prefrontal 873870be8 crates/prefrontal-core-module/src/agent_registry_ops.rs:1450-1464,1585-1626.
                    let peers = all_rows(conn)?.iter().filter(|row| row.status == "live" &&
                        ((row.role == "workspace_head" && row.workspace_id.as_deref() == Some(&request.workspace_id)) ||
                        (row.role == "head" && row.project_id.as_ref().is_some_and(|id| projects.contains(id)))))
                        .map(|row| {
                            let mut peer = json!({"agent_id":row.agent_id,"name":row.name,"tag":row.tag,"role":row.role});
                            if let Some(id) = &row.project_id { peer["project_id"] = json!(id); }
                            if let Some(identity) = &row.github_identity { peer["github_identity"] = json!(identity); }
                            peer
                        }).collect::<Vec<_>>();
                    Ok(json!({"peers":peers,"generation":self.generation_from_connection(conn)?}))
                })?
            }
            "agent.avatar_read" => {
                let request: AvatarRequest = decode(params)?;
                if !(1..=64).contains(&request.agent_ids.len())
                    || request.agent_ids.iter().any(String::is_empty)
                {
                    return Err(AgentMutationError::new(
                        "invalid_request",
                        "agentIds must contain 1..=64 non-empty entries",
                    ));
                }
                self.read_agent(|conn| {
                    let mut avatars = Vec::new();
                    // Unknown ids and unset avatars are omitted; request order is retained.
                    // Source: prefrontal 873870be8 crates/prefrontal-core-module/src/agent_registry_ops.rs:3247-3276.
                    for id in &request.agent_ids {
                        if let Some(avatar) = load_row(conn, id)?.and_then(|row| row.avatar) {
                            let mut value = json!({"agentId":id,"genome":avatar.genome,"type":avatar.avatar_type});
                            if let Some(version) = avatar.version { value["version"] = json!(version); }
                            avatars.push(value);
                        }
                    }
                    Ok(json!({"avatars":avatars,"generation":self.generation_from_connection(conn)?}))
                })?
            }
            "agent.github_identity" => {
                let request: IdRequest = decode(params)?;
                self.read_agent(|conn| {
                    let row = load_row(conn, &request.agent_id)?.ok_or_else(|| AgentMutationError::new("unknown_agent", format!("unknown agent {}", request.agent_id)))?;
                    if let Some(gone) = row.gone() {
                        return Err(AgentMutationError { code:"gone".into(), message:"agent is gone".into(), detail:Some(gone) });
                    }
                    let mut reply = json!({"github_identity":row.github_identity,"agent_generation":row.agent_generation,"generation":self.generation_from_connection(conn)?});
                    if let Some(id) = row.project_id { reply["projectId"] = json!(id); }
                    Ok(reply)
                })?
            }
            _ => {
                return Err(AgentMutationError::new(
                    "unknown_method",
                    format!("unknown method {op}"),
                ))
            }
        };
        crate::mutations::wire_value(value).map_err(Into::into)
    }
}

// Resolve a name the way core does, so a lookup gives the same answer before
// and after the move: a `workspace:` prefix restricts the match to that
// workspace, while a `workspace_id` hint only prefers it and falls back to
// matches elsewhere. Matching uses each agent's stored `workspace_id`, which is
// the namespace its name was claimed in, not where its project sits today.
// Source: prefrontal 873870be8 crates/prefrontal-core-module/src/agent_registry_ops.rs:1168-1289.
fn resolve_name(conn: &Connection, request: &NameRequest) -> Result<Value, AgentMutationError> {
    let (prefix, raw_name) = match request.name.split_once('/') {
        Some((workspace, name)) => (Some(workspace), name),
        None => (None, request.name.as_str()),
    };
    let name = normalize_agent_name(raw_name)?.normalized_name;
    let scope = prefix
        .map(normalize_agent_name)
        .transpose()?
        .map(|n| n.normalized_name)
        .or(request
            .workspace_id
            .as_deref()
            .map(normalize_agent_name)
            .transpose()?
            .map(|n| n.normalized_name));
    let mut matches = Vec::new();
    for row in all_rows(conn)?.into_iter().filter(|r| r.status == "live") {
        if normalize_agent_name(&row.name)?.normalized_name == name {
            matches.push(row);
        }
    }
    let candidates = if let Some(scope) = scope.as_deref() {
        let mut local = Vec::new();
        let mut foreign = Vec::new();
        for row in matches {
            let workspace = row
                .workspace_id
                .as_deref()
                .map(normalize_agent_name)
                .transpose()?
                .map(|n| n.normalized_name);
            if workspace.as_deref().unwrap_or("global") == scope {
                local.push(row);
            } else {
                foreign.push(row);
            }
        }
        if local.len() == 1 {
            local
        } else if prefix.is_some() {
            Vec::new()
        } else {
            foreign
        }
    } else {
        matches
    };
    match candidates.len() {
        0 => Ok(
            json!({"refused":{"code":"name_unknown","details":{"name":request.name,"workspace":scope}}}),
        ),
        1 => Ok(json!({"agent_id":candidates[0].agent_id,"digest":candidates[0].digest()})),
        _ => {
            let names = candidates
                .iter()
                .map(|row| {
                    format!(
                        "{}/{}",
                        row.workspace_id.as_deref().unwrap_or("global"),
                        row.name
                    )
                })
                .collect::<BTreeSet<_>>();
            Ok(json!({"refused":{"code":"name_ambiguous","details":{"candidates":names}}}))
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::RegisterRequest;
    use cortexkit_store::{Isolation, StorageBackend, StorageDescriptor};
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    static NEXT: AtomicU64 = AtomicU64::new(0);
    pub(in crate::agent) struct Fixture {
        pub store: RegistryStore,
        pub root: PathBuf,
        _cleanup: crate::scratch_cleanup::ScratchCleanup,
    }

    pub(in crate::agent) fn fixture(active: bool) -> Fixture {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/agent-read-fixtures")
            .join(format!(
                "{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir_all(&root).unwrap();
        let root =
            PathBuf::from(RegistryStore::canonical_mutation_root(root.to_str().unwrap()).unwrap());
        let descriptor = StorageDescriptor {
            module_id: "agent-read-test".into(),
            storage_namespace: "test".into(),
            isolation: Isolation::Module,
            backend: StorageBackend::Sqlite {
                path: root.join("store.db").to_string_lossy().into_owned(),
            },
        };
        let mut store = RegistryStore::open(&descriptor).unwrap();
        store.use_sequential_ids();
        if active {
            store
                .apply_entry("agent.cutover", "{}", "test", None, |_| Ok(()))
                .unwrap();
        }
        Fixture {
            _cleanup: crate::scratch_cleanup::ScratchCleanup(root.clone()),
            store,
            root,
        }
    }

    pub(in crate::agent) fn result(bytes: &[u8]) -> Value {
        serde_json::from_slice::<Value>(bytes).unwrap()["result"].clone()
    }
    pub(in crate::agent) fn read(f: &Fixture, op: &str, params: Value) -> Value {
        result(&f.store.agent_read(op, params).unwrap())
    }
    pub(in crate::agent) fn call(f: &Fixture, op: &str, params: Value) -> Value {
        result(&f.store.agent_mutation(op, params, 100).unwrap())
    }
    pub(in crate::agent) fn create(
        f: &Fixture,
        name: &str,
        role: &str,
        project: Option<&str>,
        workspace: Option<&str>,
    ) -> String {
        call(f,"agent.create",json!({"name":name,"tag":"tag","role":role,"project_id":project,"workspace_id":workspace,"request_key":format!("create-{name}-{project:?}-{workspace:?}")}))["agent"]["agent_id"].as_str().unwrap().into()
    }
    pub(in crate::agent) fn placed(f: &Fixture, project: &str, workspace: &str) {
        f.store
            .register(RegisterRequest {
                project_id: Some(project.into()),
                name: project.into(),
                workspace_id: Some(workspace.into()),
                ..Default::default()
            })
            .unwrap();
    }

    pub(in crate::agent) fn import_core(f: &Fixture, sql: &str) {
        let path = f.root.join("core.db");
        let source = Connection::open(&path).unwrap();
        // These are core's actual migration SQL, not a proxy agent schema.
        // Source: prefrontal 873870be8 crates/prefrontal-core-store/migrations/{076,080,082,109,112,126}.
        for migration in [
            include_str!("../../tests/fixtures/core-agent-migrations/076_agent_registry.sql"),
            include_str!(
                "../../tests/fixtures/core-agent-migrations/080_agent_github_identity.sql"
            ),
            include_str!("../../tests/fixtures/core-agent-migrations/082_wake_delivery.sql"),
            include_str!("../../tests/fixtures/core-agent-migrations/109_agent_generation.sql"),
            include_str!("../../tests/fixtures/core-agent-migrations/112_agent_avatar.sql"),
            include_str!("../../tests/fixtures/core-agent-migrations/126_agent_labels.sql"),
        ] {
            source.execute_batch(migration).unwrap();
        }
        source.execute_batch(sql).unwrap();
        f.store
            .agent_import(json!({"snapshot_path":path,"request_key":"import"}), 100)
            .unwrap();
    }

    #[test]
    fn resolve_pins_live_retired_merged_imported_deleted_and_unknown_shapes() {
        let f = fixture(false);
        assert_eq!(
            read(&f, "agent.resolve", json!({"agent_id":"agent_16013c86"})),
            json!({"agent_id":"agent_16013c86","status":"unknown","merged_into":null,"gone":null,"generation":0})
        );
        import_core(&f,"INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms,terminal_reason,terminal_at_ms) VALUES('agent_16013c86','Deleted','old','assistant',1,2,'deleted',3);");
        assert_eq!(
            read(&f, "agent.resolve", json!({"agent_id":"agent_16013c86"})),
            json!({"agent_id":"agent_16013c86","status":"retired","merged_into":null,"gone":{"reason":"deleted","at":3},"generation":2})
        );
        let a = create(&f, "Live", "assistant", None, None);
        let b = create(&f, "Retire", "assistant", None, None);
        let c = create(&f, "Merge", "assistant", None, None);
        call(
            &f,
            "agent.dispose",
            json!({"agent_id":b,"request_key":"dispose"}),
        );
        call(
            &f,
            "agent.merge",
            json!({"agent_id":c,"into_agent_id":a,"request_key":"merge"}),
        );
        let generation = f.store.generation().unwrap();
        assert_eq!(
            read(&f, "agent.resolve", json!({"agent_id":a})),
            json!({"agent_id":a,"status":"live","merged_into":null,"gone":null,"generation":generation})
        );
        assert_eq!(
            read(&f, "agent.resolve", json!({"agent_id":b})),
            json!({"agent_id":b,"status":"retired","merged_into":null,"gone":{"reason":"deleted","at":100},"generation":generation})
        );
        assert_eq!(
            read(&f, "agent.resolve", json!({"agent_id":c})),
            json!({"agent_id":c,"status":"merged","merged_into":a,"gone":{"reason":"merged","at":100,"into_agent_id":a},"generation":generation})
        );
        for id in [
            "agent_FFFFFFFFFFFFFFFF",
            "agent_0000000",
            "agent_00000000000000000",
            "bad",
            "agent_12345678",
            "agent_ffffffffffffffff",
        ] {
            assert_eq!(
                read(&f, "agent.resolve", json!({"agent_id":id})),
                json!({"agent_id":id,"status":"unknown","merged_into":null,"gone":null,"generation":generation})
            );
        }
        f.store
            .db
            .with_conn_fenced(|tx| {
                tx.execute_batch("DROP TABLE agent_name_claim; DROP TABLE agent;")
            })
            .unwrap();
        assert_eq!(
            f.store
                .agent_read("agent.resolve", json!({"agent_id":a}))
                .unwrap_err()
                .code,
            "storage_error"
        );
    }

    #[test]
    fn list_activation_filters_paging_and_terminal_gone_match_core() {
        let f = fixture(false);
        for active in [false, true] {
            assert_eq!(
                read(&f, "agent.list", json!({"activated_only":active})),
                json!({"agents":[],"generation":0})
            );
        }
        f.store
            .apply_entry("agent.cutover", "{}", "test", None, |_| Ok(()))
            .unwrap();
        placed(&f, "P", "W");
        let a = create(&f, "Assistant", "assistant", None, None);
        let b = create(&f, "Head", "head", Some("P"), None);
        let c = create(&f, "Hire", "hiree", Some("P"), None);
        call(
            &f,
            "agent.dispose",
            json!({"agent_id":a,"request_key":"dispose"}),
        );
        let generation = f.store.generation().unwrap();
        let page = read(&f, "agent.list", json!({"include_gone":true,"limit":1}));
        assert_eq!(
            page,
            json!({"agents":[],"gone":[{"agent_id":a,"gone":{"reason":"deleted","at":100}}],"next_cursor":a,"generation":generation})
        );
        let page = read(&f, "agent.list", json!({"cursor":a,"limit":1}));
        assert_eq!(page["agents"][0]["agent_id"], b);
        assert_eq!(page["next_cursor"], b);
        let last = read(&f, "agent.list", json!({"cursor":b,"limit":1}));
        assert_eq!(last["agents"][0]["agent_id"], c);
        assert!(last.get("next_cursor").is_none());
        assert_eq!(
            read(&f, "agent.list", json!({"activated_only":true})),
            read(&f, "agent.list", json!({"activated_only":false}))
        );
        let filtered = read(
            &f,
            "agent.list",
            json!({"role":"hiree","project_id":" P ","workspace_id":" W "}),
        );
        assert_eq!(filtered["agents"].as_array().unwrap().len(), 1);
        assert_eq!(
            filtered["agents"][0],
            json!({"agent_id":c,"name":"Hire","name_version":1,"tag":"tag","labels":[],"role":"hiree","created_at":100,"agent_generation":1,"project_id":"P","workspace_id":"W"})
        );
        // Stored rows alone do not activate the registry.
        f.store
            .db
            .with_conn_fenced(|tx| {
                tx.execute("DELETE FROM registry_journal WHERE op='agent.cutover'", [])?;
                Ok(())
            })
            .unwrap();
        assert_eq!(
            read(&f, "agent.list", json!({"activated_only":true}))["agents"],
            json!([])
        );
        assert_eq!(
            read(&f, "agent.list", json!({}))["agents"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            read(&f, "agent.list", json!({"limit":200})),
            read(&f, "agent.list", json!({}))
        );
        for params in [
            json!({"limit":0}),
            json!({"limit":201}),
            json!({"limit":-1}),
            json!({"limit":u64::MAX}),
            json!({"cursor":""}),
        ] {
            assert_eq!(
                f.store.agent_read("agent.list", params).unwrap_err().code,
                "invalid_cursor"
            );
        }
    }

    #[test]
    fn resolve_name_pins_core_refusals_prefix_hint_and_stored_workspace() {
        let f = fixture(true);
        placed(&f, "P1", "W1");
        placed(&f, "P2", "W2");
        let a = create(&f, "Café", "head", Some("P1"), None);
        let b = create(&f, "Café", "head", Some("P2"), None);
        let c = create(&f, "Assistant", "assistant", None, None);
        assert_eq!(
            read(&f, "agent.resolve_name", json!({"name":"Missing"}))["refused"],
            json!({"code":"name_unknown","details":{"name":"Missing","workspace":null}})
        );
        assert_eq!(
            read(&f, "agent.resolve_name", json!({"name":" Café "}))["refused"],
            json!({"code":"name_ambiguous","details":{"candidates":["W1/Café","W2/Café"]}})
        );
        assert_eq!(
            read(&f, "agent.resolve_name", json!({"name":"w1/CAFE\u{301}"}))["agent_id"],
            a
        );
        assert_eq!(
            read(
                &f,
                "agent.resolve_name",
                json!({"name":"Café","workspace_id":"W2"})
            )["agent_id"],
            b
        );
        assert_eq!(
            read(&f, "agent.resolve_name", json!({"name":"w3/Café"}))["refused"]["code"],
            "name_unknown"
        );
        assert_eq!(
            read(&f, "agent.resolve_name", json!({"name":"global/assistant"}))["agent_id"],
            c
        );
        assert_eq!(
            read(&f, "agent.resolve_name", json!({"name":"w1/assistant"}))["refused"]["code"],
            "name_unknown"
        );
        // Core's workspace_id is a preference, unlike a name's strict prefix.
        assert_eq!(
            read(
                &f,
                "agent.resolve_name",
                json!({"name":"Assistant","workspace_id":"W1"})
            )["agent_id"],
            c
        );
        f.store
            .db
            .with_conn_fenced(|tx| {
                tx.execute("UPDATE agent SET workspace_id='W3' WHERE agent_id=?1", [&a])?;
                Ok(())
            })
            .unwrap();
        let stored = read(&f, "agent.resolve_name", json!({"name":"w3/Café"}));
        assert_eq!(stored["agent_id"], a);
        assert_eq!(stored["digest"]["workspace_id"], "W3");
        assert_eq!(stored["digest"]["agent_generation"], 1);
        assert_eq!(
            read(&f, "agent.resolve_name", json!({"name":"w1/Café"}))["refused"]["code"],
            "name_unknown"
        );
    }

    #[test]
    fn peer_roster_pins_core_membership_order_and_keys_without_reachability() {
        let f = fixture(false);
        assert_eq!(
            f.store
                .agent_read("agent.peer_roster", json!({"workspace_id":"W"}))
                .unwrap_err()
                .code,
            "registry_not_activated"
        );
        f.store
            .apply_entry("agent.cutover", "{}", "test", None, |_| Ok(()))
            .unwrap();
        placed(&f, "P1", "W");
        placed(&f, "P2", "W");
        placed(&f, "P3", "Elsewhere");
        placed(&f, "P4", "W");
        let w = create(&f, "Workspace", "workspace_head", None, Some("W"));
        let a = create(&f, "First", "head", Some("P1"), None);
        let b = create(&f, "Second", "head", Some("P2"), None);
        create(&f, "Elsewhere", "head", Some("P3"), None);
        create(&f, "Hire", "hiree", Some("P1"), None);
        create(&f, "Assistant", "assistant", None, None);
        let retired = create(&f, "Retired", "head", Some("P4"), None);
        call(
            &f,
            "agent.dispose",
            json!({"agent_id":retired,"request_key":"dispose"}),
        );
        let identity = json!({"kind":"user_token","login":"first","credential_ref":"cred","coauthor_line":null});
        call(
            &f,
            "agent.set_github_identity",
            json!({"agent_id":a,"github_identity":identity,"request_key":"github"}),
        );
        // Placement determines head membership even if its stored workspace differs.
        f.store
            .db
            .with_conn_fenced(|tx| {
                tx.execute(
                    "UPDATE agent SET workspace_id='Old' WHERE agent_id=?1",
                    [&a],
                )?;
                Ok(())
            })
            .unwrap();
        assert_eq!(
            read(&f, "agent.peer_roster", json!({"workspace_id":"W"})),
            json!({"peers":[
            {"agent_id":w,"name":"Workspace","tag":"tag","role":"workspace_head"},
            {"agent_id":a,"name":"First","tag":"tag","role":"head","project_id":"P1","github_identity":identity},
            {"agent_id":b,"name":"Second","tag":"tag","role":"head","project_id":"P2"}],"generation":f.store.generation().unwrap()})
        );
    }

    #[test]
    fn avatar_and_github_reads_pin_core_presence_and_agent_generation() {
        let f = fixture(true);
        placed(&f, "P", "W");
        let a = create(&f, "Head", "head", Some("P"), None);
        let b = create(&f, "Other", "assistant", None, None);
        call(
            &f,
            "agent.set_avatar",
            json!({"agentId":a,"genome":"a".repeat(2048),"type":"creature.classic","request_key":"avatar"}),
        );
        assert_eq!(
            read(&f, "agent.avatar_read", json!({"agentIds":[b,"bad",a,a]})),
            json!({"avatars":[
            {"agentId":a,"genome":"a".repeat(2048),"type":"creature.classic"},
            {"agentId":a,"genome":"a".repeat(2048),"type":"creature.classic"}],"generation":f.store.generation().unwrap()})
        );
        for identity in [
            Value::Null,
            json!({"kind":"app","app_id":1,"app_slug":"app","installation_id":2,"client_id":"client","credential_ref":"cred","coauthor_line":"author"}),
            json!({"kind":"user_token","login":"login","credential_ref":"cred","coauthor_line":"author"}),
        ] {
            call(
                &f,
                "agent.set_github_identity",
                json!({"agent_id":a,"github_identity":identity,"request_key":format!("github-{identity}")}),
            );
            let reply = read(&f, "agent.github_identity", json!({"agent_id":a}));
            assert_eq!(
                reply,
                json!({"github_identity":identity,"projectId":"P","agent_generation":f.store.agent_row(&a).unwrap().unwrap().agent_generation,"generation":f.store.generation().unwrap()})
            );
            assert_ne!(reply["agent_generation"], reply["generation"]);
        }
        assert_eq!(
            read(&f, "agent.github_identity", json!({"agent_id":b})),
            json!({"github_identity":null,"agent_generation":1,"generation":f.store.generation().unwrap()})
        );
        call(
            &f,
            "agent.dispose",
            json!({"agent_id":a,"request_key":"dispose"}),
        );
        assert_eq!(
            f.store
                .agent_read("agent.github_identity", json!({"agent_id":a}))
                .unwrap_err()
                .detail,
            Some(json!({"reason":"deleted","at":100}))
        );
        for id in ["agent_FFFFFFFFFFFFFFFF", "bad", "agent_ffffffffffffffff"] {
            assert_eq!(
                f.store
                    .agent_read("agent.github_identity", json!({"agent_id":id}))
                    .unwrap_err()
                    .code,
                "unknown_agent"
            );
        }
    }

    #[test]
    fn inherited_read_bodies_refuse_removed_unknown_and_wrong_casing_fields() {
        let f = fixture(true);
        for (op, base) in [
            ("agent.resolve", json!({"agent_id":"bad"})),
            ("agent.resolve_name", json!({"name":"Missing"})),
            ("agent.list", json!({})),
            ("agent.peer_roster", json!({"workspace_id":"W"})),
            ("agent.avatar_read", json!({"agentIds":["bad"]})),
            ("agent.github_identity", json!({"agent_id":"bad"})),
        ] {
            for field in [
                "persona_ref",
                "residence",
                "wake_policy",
                "sleep",
                "callerHarness",
                "callerSession",
                "harness",
                "session_id",
                "session",
                "caller_directory",
                "caller_session",
            ] {
                let mut body = base.clone();
                body[field] = json!("removed");
                assert_eq!(
                    f.store.agent_read(op, body).unwrap_err().code,
                    "invalid_request",
                    "{op} {field}"
                );
            }
        }
        for params in [
            json!({"agent_ids":["bad"]}),
            json!({"agentIds":[]}),
            json!({"agentIds":[""]}),
            json!({"agentIds":vec!["bad";65]}),
        ] {
            assert_eq!(
                f.store
                    .agent_read("agent.avatar_read", params)
                    .unwrap_err()
                    .code,
                "invalid_request"
            );
        }
        assert_eq!(
            f.store
                .agent_read("agent.list", json!({"role":"hire"}))
                .unwrap_err()
                .code,
            "invalid_request"
        );
        assert_eq!(
            read(
                &f,
                "agent.list",
                json!({"limit":null,"include_gone":null,"activated_only":null})
            ),
            read(&f, "agent.list", json!({}))
        );
    }
}
