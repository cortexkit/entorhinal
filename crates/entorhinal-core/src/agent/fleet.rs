//! The identity half of the fleet view, without runtime residence or activity.

use std::collections::BTreeMap;

use rusqlite::{Connection, OptionalExtension};
use serde_json::{json, Value};

use super::{reads::all_rows, AgentAvatar, AgentMutationError, IDENTITY_CHANGE_OPS};
use crate::RegistryStore;

/// Fingerprints invalidate avatar caches when the genome changes without a new
/// agent id. Missing type/version contribute empty strings, with delimiters.
/// Source: prefrontal 873870be8 crates/prefrontal-core-module/src/fleet_overview.rs:96-109.
pub fn avatar_fingerprint(avatar: &AgentAvatar) -> String {
    let version = avatar.version.map(|v| v.to_string()).unwrap_or_default();
    let bytes = format!(
        "{}\n{}\n{version}",
        avatar.genome,
        avatar.avatar_type.as_deref().unwrap_or_default()
    );
    blake3::hash(bytes.as_bytes()).to_hex().as_str()[..32].to_owned()
}

impl RegistryStore {
    /// Build a fleet token from the serving incarnation and journal head. Every
    /// append invalidates it, including project changes, and so does a restart.
    pub fn agent_fleet_identity(
        &self,
        token: Option<&str>,
        incarnation: &str,
    ) -> Result<Vec<u8>, AgentMutationError> {
        let value = self.read_agent(|conn| self.fleet_value(conn, token, incarnation))?;
        crate::mutations::wire_value(value).map_err(Into::into)
    }

    /// Read the fleet and the complete resume range in the same existing read
    /// transaction. `None` means the range cannot be reconstructed safely.
    pub fn agent_fleet_watch_snapshot(
        &self,
        incarnation: &str,
        since: Option<i64>,
    ) -> Result<FleetWatchSnapshot, AgentMutationError> {
        self.read_agent(|conn| {
            let fleet = self.fleet_value(conn, None, incarnation)?;
            let generation = fleet["generation"].as_i64().expect("fleet generation");
            let resume = (|| {
                let since = since?;
                if since < 0 || since > generation {
                    return None;
                }
                let mut changed = BTreeMap::new();
                let mut statement = conn.prepare(
                    "SELECT seq,op,payload_json FROM registry_journal WHERE seq>?1 ORDER BY seq LIMIT 1001",
                ).ok()?;
                let entries = statement.query_map([since], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?))
                }).ok()?;
                let mut expected = since + 1;
                for (count, entry) in (1..).zip(entries) {
                    let (seq, op, payload) = entry.ok()?;
                    if seq != expected {
                        return None;
                    }
                    expected += 1;
                    // Identity entries name exactly one changed row, including
                    // import, merge/dispose, namespace repair and avatar manifests
                    // (journaled as set_namespace and set_avatar). Project ops
                    // register/upgrade_implicit/seed_import/assign_workspace can
                    // alter project names, workspace names or placement;
                    // register/upgrade_implicit/seed_import/add_root/remove_root/
                    // attach_root and remove with a successor can change the
                    // first canonical root of a live agent's project. Their
                    // payloads do not enumerate all affected fleet rows. Reject
                    // every non-identity op, including future ops, rather than
                    // mistaking the identity-only change feed for a complete log.
                    if !IDENTITY_CHANGE_OPS.contains(&op.as_str()) || count > 1000 {
                        return None;
                    }
                    let payload: Value = serde_json::from_str(&payload).ok()?;
                    let entry = payload.get("entry").unwrap_or(&payload);
                    let id = entry["agent_id"].as_str()?;
                    if entry["row"]["agent_id"].as_str()? != id {
                        return None;
                    }
                    changed.insert(id.to_owned(), seq);
                }
                if expected != generation + 1 {
                    return None;
                }
                Some(changed)
            })();
            Ok(FleetWatchSnapshot { fleet, resume })
        })
    }

    fn fleet_value(
        &self,
        conn: &Connection,
        token: Option<&str>,
        incarnation: &str,
    ) -> Result<Value, AgentMutationError> {
        let generation = self.generation_from_connection(conn)?;
        let current = format!("{incarnation}:{generation}");
        // The last shared identity-log position this machine has applied
        // (0 while sharing is off). `generation` and `token` count this
        // machine's own journal, so they can't be compared between
        // machines; a client merging several machines' replies keeps the
        // one with the highest `logPosition`, which has seen the most of
        // the shared history.
        let log_position: i64 = conn.query_row(
            "SELECT last_applied_position FROM identity_log_state WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        let mut reply = json!({"incarnation":incarnation,"generation":generation,"logPosition":log_position,"token":current,"unchanged":token == Some(current.as_str())});
        if token == Some(current.as_str()) {
            return Ok(reply);
        }
        let mut agents = Vec::new();
        // One result per live agent, even when its project has several roots.
        // The output workspace comes from project_workspace, not the agent's
        // stored workspace_id. Agents with role workspace_head have no project
        // and therefore omit the output workspace.
        // Source: prefrontal 873870be8 crates/prefrontal-core-module/src/fleet_overview.rs:53-83,349-379,418-436.
        for row in all_rows(conn)?.into_iter().filter(|r| r.status == "live") {
            let mut agent = json!({"agentId":row.agent_id,"displayName":row.name,"tag":row.tag,"labels":row.labels,"role":row.role});
            if let Some(avatar) = row.avatar {
                let avatar = AgentAvatar {
                    genome: avatar.genome,
                    avatar_type: Some(avatar.avatar_type),
                    version: avatar.version,
                };
                let mut value =
                    json!({"fingerprint":avatar_fingerprint(&avatar),"type":avatar.avatar_type});
                if let Some(version) = avatar.version {
                    value["version"] = json!(version);
                }
                agent["avatar"] = value;
            }
            if let Some(id) = row.project_id {
                let name: String =
                    conn.query_row("SELECT name FROM project WHERE project_id=?1", [&id], |r| {
                        r.get(0)
                    })?;
                let mut project = json!({"id":id,"name":name});
                let root: Option<String> = conn.query_row("SELECT canonical_root FROM project_root WHERE project_id=?1 ORDER BY canonical_root LIMIT 1", [&id], |r| r.get(0)).optional()?;
                if let Some(root) = root {
                    project["canonicalRoot"] = json!(root);
                }
                agent["project"] = project;
                let workspace: Option<(String,String)> = conn.query_row("SELECT w.workspace_id,w.name FROM project_workspace pw JOIN workspace w ON w.workspace_id=pw.workspace_id WHERE pw.project_id=?1", [&id], |r| Ok((r.get(0)?,r.get(1)?))).optional()?;
                if let Some((id, name)) = workspace {
                    agent["workspace"] = json!({"id":id,"name":name});
                }
            }
            agents.push(agent);
        }
        reply["agents"] = Value::Array(agents);
        Ok(reply)
    }
}

/// Fleet rows and a bounded, conservative replay plan from one committed view.
pub struct FleetWatchSnapshot {
    pub fleet: Value,
    pub resume: Option<BTreeMap<String, i64>>,
}

#[cfg(test)]
mod tests {
    use super::super::reads::tests::{
        call, create, fixture, import_core, placed, read, result, Fixture,
    };
    use super::*;
    use crate::{AssignWorkspaceRequest, RegisterRequest};

    const INCARNATION: &str = "0123456789abcdef";
    fn fleet(f: &Fixture, token: Option<&str>) -> Value {
        result(&f.store.agent_fleet_identity(token, INCARNATION).unwrap())
    }

    #[test]
    fn avatar_fingerprint_pins_core_full_and_absent_type_version_vectors() {
        // Fixed core integration-test input/expected value, not this function's
        // output used as its own expected value.
        // Source: prefrontal 873870be8 crates/prefrontal-core-module/tests/it/agent_fleet_overview.rs:559-582.
        let mut avatar = AgentAvatar {
            genome: "a".repeat(2048),
            avatar_type: Some("creature.classic".into()),
            version: Some(2),
        };
        assert_eq!(
            avatar_fingerprint(&avatar),
            "b4c04a5d931e65a48e022b8943678d55"
        );
        avatar.avatar_type = None;
        avatar.version = None;
        // Computed once by an independent Rust program hashing "a" × 2048 +
        // "\n\n" with BLAKE3 and retaining 32 hex characters. This shape is not
        // storable under the avatar CHECK, but the recipe still defines it.
        // Source: prefrontal 873870be8 crates/prefrontal-core-module/src/fleet_overview.rs:96-109.
        assert_eq!(
            avatar_fingerprint(&avatar),
            "cb8a629ba6a6aab4882a5aabe10f1c19"
        );
    }

    #[test]
    fn fleet_pins_one_row_first_root_placement_only_workspace_and_omitted_optionals() {
        let f = fixture(true);
        let z = f.root.join("z-root");
        let a = f.root.join("a-root");
        std::fs::create_dir_all(&z).unwrap();
        std::fs::create_dir_all(&a).unwrap();
        let a_root = crate::RegistryStore::canonical_mutation_root(a.to_str().unwrap()).unwrap();
        f.store
            .register(RegisterRequest {
                project_id: Some("P".into()),
                name: "Project".into(),
                workspace_id: Some("W2".into()),
                roots: vec![
                    crate::RegistryStore::canonical_mutation_root(z.to_str().unwrap()).unwrap(),
                    a_root.clone(),
                ],
                ..Default::default()
            })
            .unwrap();
        placed(&f, "Spare", "W1");
        let head = create(&f, "Head", "head", Some("P"), None);
        let w = create(&f, "Workspace", "workspace_head", None, Some("W1"));
        let assistant = create(&f, "Assistant", "assistant", None, None);
        let retired = create(&f, "Retired", "assistant", None, None);
        let merged = create(&f, "Merged", "assistant", None, None);
        call(
            &f,
            "agent.dispose",
            json!({"agent_id":retired,"request_key":"dispose"}),
        );
        call(
            &f,
            "agent.merge",
            json!({"agent_id":merged,"into_agent_id":assistant,"request_key":"merge"}),
        );
        call(
            &f,
            "agent.set_avatar",
            json!({"agentId":head,"genome":"a".repeat(2048),"type":"creature.classic","version":2,"request_key":"avatar"}),
        );
        f.store
            .db
            .with_conn_fenced(|tx| {
                tx.execute(
                    "UPDATE agent SET workspace_id='W1' WHERE agent_id=?1",
                    [&head],
                )?;
                Ok(())
            })
            .unwrap();
        let generation = f.store.generation().unwrap();
        assert_eq!(
            fleet(&f, None),
            json!({"incarnation":INCARNATION,"generation":generation,"logPosition":0,"token":format!("{INCARNATION}:{generation}"),"unchanged":false,"agents":[
            {"agentId":head,"displayName":"Head","tag":"tag","labels":[],"role":"head","project":{"id":"P","name":"Project","canonicalRoot":a_root},"workspace":{"id":"W2","name":"W2"},"avatar":{"fingerprint":"b4c04a5d931e65a48e022b8943678d55","type":"creature.classic","version":2}},
            {"agentId":w,"displayName":"Workspace","tag":"tag","labels":[],"role":"workspace_head"},
            {"agentId":assistant,"displayName":"Assistant","tag":"tag","labels":[],"role":"assistant"}]})
        );
        assert_eq!(
            read(&f, "agent.list", json!({"role":"head"}))["agents"][0]["workspace_id"],
            "W1"
        );
        assert_eq!(
            f.store
                .agent_snapshot()
                .unwrap()
                .agents
                .iter()
                .find(|r| r.agent_id == head)
                .unwrap()
                .workspace_id
                .as_deref(),
            Some("W1")
        );
        call(
            &f,
            "agent.set_avatar",
            json!({"agentId":head,"genome":"a".repeat(2048),"type":"creature.classic","version":null,"request_key":"avatar-no-version"}),
        );
        assert!(fleet(&f, None)["agents"][0]["avatar"]
            .get("version")
            .is_none());
    }

    #[test]
    fn fleet_import_omits_terminals_and_empty_success_is_distinct_from_storage_failure() {
        let f = fixture(false);
        assert_eq!(
            fleet(&f, None),
            json!({"incarnation":INCARNATION,"generation":0,"logPosition":0,"token":format!("{INCARNATION}:0"),"unchanged":false,"agents":[]})
        );
        import_core(&f,"INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms,terminal_reason,terminal_at_ms,merged_into_agent_id) VALUES
            ('agent_16013c86','Deleted','tag','assistant',1,2,'deleted',3,NULL),
            ('agent_0000000000000001','Merged','tag','assistant',1,2,'merged',3,'agent_0000000000000002'),
            ('agent_0000000000000002','Live','tag','assistant',1,2,NULL,NULL,NULL);
            INSERT INTO agent_name_claim(agent_id,namespace_kind,namespace_key,normalized_name,display_name,claimed_at_ms) VALUES('agent_0000000000000002','assistant','global','live','Live',1);");
        assert_eq!(
            fleet(&f, None)["agents"],
            json!([{"agentId":"agent_0000000000000002","displayName":"Live","tag":"tag","labels":[],"role":"assistant"}])
        );
        assert_eq!(f.store.agent_snapshot().unwrap().agents.len(), 3);
        f.store
            .db
            .with_conn_fenced(|tx| {
                tx.execute_batch("DROP TABLE agent_name_claim; DROP TABLE agent;")
            })
            .unwrap();
        assert_eq!(
            f.store
                .agent_fleet_identity(None, INCARNATION)
                .unwrap_err()
                .code,
            "storage_error"
        );
    }

    #[test]
    fn fleet_tokens_invalidate_on_every_identity_edit_project_append_and_restart() {
        let f = fixture(true);
        placed(&f, "P", "W");
        let mut token = fleet(&f, None)["token"].as_str().unwrap().to_owned();
        assert_eq!(
            fleet(&f, Some(&token)),
            json!({"incarnation":INCARNATION,"generation":f.store.generation().unwrap(),"logPosition":0,"token":token,"unchanged":true})
        );
        for bad in [
            "malformed",
            "ffffffffffffffff:2",
            "0123456789abcdef:999999",
            "",
        ] {
            assert_eq!(fleet(&f, Some(bad))["unchanged"], false);
        }
        let a = create(&f, "Head", "head", Some("P"), None);
        token = invalidated(&f, &token);
        let b = create(&f, "Assistant", "assistant", None, None);
        token = invalidated(&f, &token);
        for (index,(op,mut body)) in [
            ("agent.rename",json!({"agent_id":a,"name":"New"})),
            ("agent.update_tag",json!({"agent_id":a,"tag":"new"})),
            ("agent.set_labels",json!({"agent_id":a,"labels":["new"]})),
            ("agent.set_avatar",json!({"agentId":a,"genome":"a".repeat(2048),"type":"creature.classic"})),
            ("agent.set_avatar",json!({"agentId":a,"genome":"b".repeat(2048),"type":"creature.classic"})),
            ("agent.set_github_identity",json!({"agent_id":a,"github_identity":{"kind":"user_token","login":"login","credential_ref":"cred"}})),
            ("agent.merge",json!({"agent_id":b,"into_agent_id":a})),
            ("agent.dispose",json!({"agent_id":a})),
        ].into_iter().enumerate() {
            body["request_key"] = json!(format!("edit-{index}"));
            call(&f,op,body); token = invalidated(&f,&token);
        }
        f.store
            .register(RegisterRequest {
                project_id: Some("P".into()),
                name: "Renamed Project".into(),
                ..Default::default()
            })
            .unwrap();
        token = invalidated(&f, &token);
        f.store
            .assign_workspace(AssignWorkspaceRequest {
                project_id: "P".into(),
                workspace_id: "Other".into(),
                ..Default::default()
            })
            .unwrap();
        token = invalidated(&f, &token);
        f.store
            .apply_entry("approve_root", "{}", "test", None, |_| Ok(()))
            .unwrap();
        token = invalidated(&f, &token);
        let restart = result(
            &f.store
                .agent_fleet_identity(Some(&token), "fedcba9876543210")
                .unwrap(),
        );
        assert_eq!(restart["unchanged"], false);
        assert!(restart.get("agents").is_some());
        assert_ne!(restart["token"], token);
    }

    fn invalidated(f: &Fixture, old: &str) -> String {
        let reply = fleet(f, Some(old));
        assert_eq!(reply["unchanged"], false);
        assert!(reply.get("agents").is_some());
        assert_ne!(reply["token"], old);
        reply["token"].as_str().unwrap().into()
    }
}
