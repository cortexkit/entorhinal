//! Durable enable plans and transitions. The module holds its writer lock while
//! capturing a plan; retries use the saved image even if registry rows change.

use std::collections::BTreeMap;

use rusqlite::{params, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    mutations::{append, domain},
    remote_apply::RemoteEntry,
    shared_entry::{
        decode_error, ProjectEntry, SharedRow, SharedState, TableChanges, AGENT_SHARED_TABLES,
        PROJECT_SHARED_TABLES,
    },
    RegistryError, RegistryStore,
};

pub(crate) const HUB_SNAPSHOT: &str = "hub.snapshot";

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HubSnapshot {
    pub engram_through: i64,
    pub excluded: Vec<String>,
    pub image: HubImage,
    pub migration_id: String,
    pub tables: BTreeMap<String, Vec<SharedRow>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HubImage {
    pub claims: Vec<HubClaim>,
    pub roles: Vec<HubRole>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HubClaim {
    pub agent_id: String,
    pub generation: i64,
    pub holder: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HubRole {
    pub generation: i64,
    pub holder: Option<String>,
    pub role: String,
}

impl HubSnapshot {
    pub fn preimage(&self) -> Result<String, RegistryError> {
        let value = serde_json::to_value(self).map_err(decode_error)?;
        entorhinal_catalog::jcs::to_string(&value)
            .map_err(|error| domain("hub_migration_mismatch", error.to_string()))
    }
}

#[derive(PartialEq, Eq)]
pub struct HubEnablePlan {
    pub enable_id: String,
    pub snapshot_digest: String,
    pub preimage: String,
}

impl RegistryStore {
    pub fn hub_enable_plan(&self) -> Result<Option<HubEnablePlan>, RegistryError> {
        self.read(read_hub_plan)
    }

    /// Capture the local registry once and save that capture before any
    /// network write. A resumed enable after a crash publishes exactly these
    /// saved bytes; it never captures the registry again, so the snapshot
    /// other computers receive can't change between attempts.
    pub fn begin_hub_enable(&self, enable_id: &str, box_id: &str) -> Result<(), RegistryError> {
        self.with_where_changes(|tx| {
            let mut tables = hub_tables(tx)?;
            let names = tables.get_mut("member_name").expect("shared table");
            let own: Option<String> = tx.query_row("SELECT self_author FROM self_identity WHERE id=1", [], |r| r.get(0)).optional()?.flatten();
            let name = names.iter().find(|row| row["author"] == box_id)
                .or_else(|| names.iter().find(|row| row["author"].as_str() == own.as_deref())).cloned();
            *names = name.into_iter().map(|mut row| {
                row.insert("author".into(), box_id.into());
                row
            }).collect();
            let claims = tables["agent"].iter().map(|row| {
                Ok(HubClaim {
                    agent_id: row["agent_id"].as_str().ok_or_else(invalid_hub_snapshot)?.into(),
                    holder: Some(box_id.into()),
                    generation: 1,
                })
            }).collect::<rusqlite::Result<_>>()?;
            let snapshot = HubSnapshot {
                engram_through: 0,
                excluded: vec![],
                image: HubImage { claims, roles: vec![] },
                migration_id: enable_id.into(),
                tables,
            };
            let preimage = snapshot.preimage().map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
            let digest = entorhinal_catalog::sha256_hex(preimage.as_bytes());
            if tx.execute(
                "UPDATE identity_log_state SET state='enabling',enable_id=?1,enable_snapshot_digest=?2,enable_parts=?3
                 WHERE id=1 AND state='disabled' AND hub_log_base IS NULL AND enable_id IS NULL",
                params![enable_id, digest, preimage],
            )? != 1 {
                return Err(invalid_hub_snapshot());
            }
            Ok(())
        }).map_err(RegistryError::Store)
    }

    pub fn installed_hub_snapshot(&self) -> Result<HubSnapshot, RegistryError> {
        self.read(|conn| {
            let bytes: Vec<u8> = conn.query_row(
                "SELECT entry FROM registry_journal WHERE op=?1 AND entry IS NOT NULL ORDER BY seq DESC LIMIT 1",
                [HUB_SNAPSHOT],
                |r| r.get(0),
            )?;
            serde_json::from_slice(&bytes).map_err(decode_error)
        })
    }

    pub fn install_hub_snapshot(
        &self,
        snapshot: &HubSnapshot,
        parts: &[(RemoteEntry, u64)],
        own_box: &str,
    ) -> Result<(), RegistryError> {
        let preimage = snapshot.preimage()?;
        let digest = entorhinal_catalog::sha256_hex(preimage.as_bytes());
        let base = snapshot.engram_through;
        let final_position = i64::try_from(parts.len())
            .ok()
            .filter(|n| *n != 0)
            .and_then(|n| base.checked_add(n))
            .ok_or_else(invalid_hub_snapshot)?;
        self.with_where_changes(|tx| {
            let (state, cursor, old_base): (String, i64, Option<i64>) = tx.query_row(
                "SELECT state,last_applied_position,hub_log_base FROM identity_log_state WHERE id=1",
                [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
            let allowed = match (state.as_str(), read_hub_plan(tx)?) {
                ("enabling", Some(plan)) => base == 0 && cursor == 0
                    && plan.enable_id == snapshot.migration_id && plan.snapshot_digest == digest
                    && plan.preimage == preimage,
                ("disabled", None) => cursor == 0 && tx.query_row(
                    "SELECT NOT EXISTS(SELECT 1 FROM agent) AND NOT EXISTS(SELECT 1 FROM project) AND NOT EXISTS(SELECT 1 FROM workspace)", [], |r| r.get(0),
                )?,
                _ => false,
            };
            if old_base.is_some() || !allowed {
                return Err(invalid_hub_snapshot());
            }
            tx.execute("INSERT INTO self_identity(id,self_author,learned_at_ms) VALUES(1,?1,?2)
                ON CONFLICT(id) DO UPDATE SET self_author=excluded.self_author,learned_at_ms=excluded.learned_at_ms",
                params![own_box, crate::now_unix_millis()])?;
            ensure_cutover(tx, 0, "entorhinal")?;
            if state != "enabled" {
                append(tx, "identity_log.enable", &json!({}), "log", None, 0, "entorhinal")?;
            }
            for (index, (part, episode)) in parts.iter().enumerate() {
                if part.position != base + index as i64 + 1 {
                    return Err(invalid_hub_snapshot());
                }
                let seq = append(tx, HUB_SNAPSHOT, &json!({"snapshot_digest":digest}), "log", None, 0, "entorhinal")?;
                let complete = part.position == final_position;
                tx.execute(
                    "UPDATE registry_journal SET stream=?1, origin='log', entry_id=?2, log_position=?3, entry=?4 WHERE seq=?5",
                    params![if complete { "shared" } else { "local" }, part.entry_id, part.position, complete.then_some(preimage.as_bytes()), seq],
                )?;
                crate::hub_store::record_log_writer(tx, part, *episode)?;
                if complete {
                    restore_hub_snapshot(tx, snapshot, seq)?;
                }
            }
            tx.execute(
                "UPDATE identity_log_state SET state='enabled',hub_log_base=?1,last_applied_position=?2,last_seen_head=?2,
                    hub_ready=0,hub_quieting=0,enable_id=NULL,enable_snapshot_digest=NULL,enable_parts=NULL WHERE id=1",
                params![base, final_position],
            )?;
            Ok(())
        }).map_err(RegistryError::Store)
    }

    pub fn enable_registry_is_empty(&self) -> Result<bool, RegistryError> {
        self.read(|conn| conn.query_row(
            "SELECT NOT EXISTS(SELECT 1 FROM project) AND NOT EXISTS(SELECT 1 FROM workspace) AND NOT EXISTS(SELECT 1 FROM agent)",
            [], |r| r.get(0)))
    }
    /// Check the existing agent.cutover journal row and registry counts without
    /// writing. A refusal must leave the journal, saved identity-log state and
    /// saved log positions unchanged, so local writes remain available.
    pub fn check_enable_preconditions(&self, head: u64) -> Result<(), RegistryError> {
        self.read(|conn| {
            let counts: (i64, i64, i64) = conn.query_row(
                "SELECT (SELECT COUNT(*) FROM project),(SELECT COUNT(*) FROM workspace),(SELECT COUNT(*) FROM agent)", [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
            let marker: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM registry_journal WHERE op='agent.cutover')", [], |r| r.get(0))?;
            Ok(if counts.2 > 0 && !marker {
                Err(domain("authority_not_cut_over", "agents exist without agent.cutover"))
            } else if head > 0 && counts != (0, 0, 0) {
                Err(domain("join_requires_empty_registry", format!("projects={}, workspaces={}, agents={}", counts.0, counts.1, counts.2)))
            } else { Ok(()) })
        })?
    }

    /// Refuses to start an enable that would silently close the agent import.
    ///
    /// Finishing an enable writes the `agent.cutover` marker, and once that
    /// marker exists `agent.import` refuses forever. So a machine that writes
    /// a snapshot of its own state to the log, with no agents and no
    /// marker, must either import its agents first or say explicitly, with
    /// `without_agents`, that there are none to import. The flag is refused
    /// whenever agents or a marker already exist, so it can never hide a real
    /// import.
    ///
    /// Call it only before saving a new enable plan. A machine joining an
    /// existing log gets its marker from the snapshot it applies, and resuming
    /// a saved plan was already checked when the plan was saved, so neither
    /// path calls this.
    pub fn check_agent_import_preconditions(
        &self,
        without_agents: bool,
    ) -> Result<(), RegistryError> {
        self.read(|conn| {
            let agents: i64 = conn.query_row("SELECT COUNT(*) FROM agent", [], |r| r.get(0))?;
            let marker: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM registry_journal WHERE op='agent.cutover')",
                [],
                |r| r.get(0),
            )?;
            if without_agents && (agents > 0 || marker) {
                return Ok(Err(domain(
                    "invalid_request",
                    "--without-agents requires no agents and no agent.cutover marker",
                )));
            }
            if agents == 0 && !marker && !without_agents {
                return Ok(Err(domain(
                    "agents_not_imported",
                    "run the agent import before enabling the log, or pass \
                     --without-agents when the fleet has no agents to import",
                )));
            }
            Ok(Ok(()))
        })?
    }
}

fn invalid_hub_snapshot() -> rusqlite::Error {
    rusqlite::Error::InvalidParameterName("invalid hub snapshot or local bootstrap state".into())
}

fn read_hub_plan(conn: &rusqlite::Connection) -> rusqlite::Result<Option<HubEnablePlan>> {
    conn.query_row(
        "SELECT enable_id,enable_snapshot_digest,enable_parts FROM identity_log_state
         WHERE id=1 AND enable_id IS NOT NULL AND hub_log_base IS NULL",
        [],
        |r| {
            Ok(HubEnablePlan {
                enable_id: r.get(0)?,
                snapshot_digest: r.get(1)?,
                preimage: r.get(2)?,
            })
        },
    )
    .optional()
}

fn hub_tables(conn: &rusqlite::Connection) -> rusqlite::Result<BTreeMap<String, Vec<SharedRow>>> {
    let bytes = SharedState::capture(conn)?
        .canonical_bytes()
        .map_err(decode_error)?;
    let mut tables: BTreeMap<String, Vec<SharedRow>> =
        serde_json::from_slice(&bytes).map_err(decode_error)?;
    tables.remove("member_box");
    Ok(tables)
}

pub(crate) fn restore_hub_snapshot(
    tx: &Transaction<'_>,
    snapshot: &HubSnapshot,
    seq: i64,
) -> rusqlite::Result<()> {
    let tables = &snapshot.tables;
    let before = SharedState::capture(tx)?;
    let expected = PROJECT_SHARED_TABLES
        .iter()
        .chain(AGENT_SHARED_TABLES)
        .copied()
        .chain(["member_name"]);
    if tables.len() != expected.clone().count()
        || !expected.into_iter().all(|name| tables.contains_key(name))
    {
        return Err(invalid_hub_snapshot());
    }
    let mut project = ProjectEntry {
        op: "shared.snapshot".into(),
        tables: PROJECT_SHARED_TABLES
            .iter()
            .map(|&name| {
                (
                    name.into(),
                    TableChanges {
                        upsert: tables[name].clone(),
                        delete: vec![],
                    },
                )
            })
            .collect(),
        agents: vec![],
        member_name: None,
        member_box: None,
        requesting_agent_id: None,
        authorization: None,
    };
    project.validate(tx)?;
    project.delete_absent(tx, &before)?;
    tx.execute_batch("PRAGMA defer_foreign_keys=ON; DELETE FROM agent_name_claim; DELETE FROM agent; DELETE FROM member_name;")?;
    for name in ["agent", "agent_name_claim", "member_name"] {
        let columns = tx
            .prepare(&format!("PRAGMA table_info({name})"))?
            .query_map([], |r| r.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let sql = format!(
            "INSERT INTO {name}({}) VALUES({})",
            columns.join(","),
            vec!["?"; columns.len()].join(",")
        );
        let mut insert = tx.prepare(&sql)?;
        for row in &tables[name] {
            if row.len() != columns.len() {
                return Err(invalid_hub_snapshot());
            }
            let values = columns
                .iter()
                .map(|key| match row.get(key) {
                    Some(serde_json::Value::Null) => Ok(rusqlite::types::Value::Null),
                    Some(serde_json::Value::String(value)) => Ok(value.clone().into()),
                    Some(value) if value.is_i64() => Ok(value.as_i64().unwrap().into()),
                    _ => Err(invalid_hub_snapshot()),
                })
                .collect::<rusqlite::Result<Vec<_>>>()?;
            insert.execute(rusqlite::params_from_iter(values))?;
        }
    }
    project.restore(tx, seq, false)?;
    if hub_tables(tx)? != *tables {
        return Err(invalid_hub_snapshot());
    }
    Ok(())
}

fn ensure_cutover(tx: &Transaction<'_>, now: i64, principal: &str) -> rusqlite::Result<()> {
    let marked: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM registry_journal WHERE op='agent.cutover')",
        [],
        |r| r.get(0),
    )?;
    if !marked {
        append(
            tx,
            "agent.cutover",
            &json!({}),
            "operator",
            None,
            now,
            principal,
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::remote_apply::RemoteEntry;

    #[test]
    fn hub_snapshot_replay_reproduces_existing_agents_and_filters_both_feeds() {
        let f = crate::mutations::tests::Fixture::new("hub-snapshot-replay");
        let store = &f.store;
        let holder = "ab".repeat(16);
        let id = "cd".repeat(16);
        store.apply_entry("fixture.rows", "{}", "test", None, |tx| {
            tx.execute_batch("INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms)
              VALUES('agent_0000000000000001','Ada','helper','assistant',1,1);
              INSERT INTO agent_name_claim(agent_id,namespace_kind,namespace_key,normalized_name,display_name,claimed_at_ms)
              VALUES('agent_0000000000000001','assistant','global','ada','Ada',1);")?;
            tx.execute("INSERT INTO member_name(author,display_name) VALUES(?1,'Here')", [&holder])?;
            Ok(())
        }).unwrap();
        store.begin_hub_enable(&id, &holder).unwrap();
        let plan = store.hub_enable_plan().unwrap().unwrap();
        let snapshot: HubSnapshot = serde_json::from_str(&plan.preimage).unwrap();
        assert_eq!(
            snapshot.image.claims[0].holder.as_deref(),
            Some(holder.as_str())
        );
        let entry = RemoteEntry {
            position: 1,
            entry_id: "11".repeat(16),
            author: holder.clone(),
            signer: "test".into(),
            key_id: "test".into(),
            envelope_version: 1,
            kind: "snapshot".into(),
            entry: vec![],
        };
        let before = store.generation().unwrap();
        store
            .install_hub_snapshot(&snapshot, &[(entry, 0)], &holder)
            .unwrap();
        assert_eq!(store.hub_log_base().unwrap(), Some(0));
        assert!(store.hub_snapshot_applied(0).unwrap());
        assert!(store
            .agent_changes(before, None)
            .unwrap()
            .entries
            .is_empty());
        assert!(store
            .journal_tail(before, 30)
            .unwrap()
            .entries
            .iter()
            .all(|row| row.op != HUB_SNAPSHOT));
        assert!(store.verify().unwrap().ok);
        store.rebuild().unwrap();
        assert_eq!(store.read(hub_tables).unwrap(), snapshot.tables);
        assert!(store.verify().unwrap().ok);
    }

    #[test]
    fn without_agents_refuses_existing_agents_even_without_a_cutover_marker() {
        let f = crate::mutations::tests::Fixture::new("without-agents-unmarked");
        // Legacy agents may exist before the cutover marker. Test this helper
        // directly so an earlier enable precondition cannot mask its refusal.
        f.store.apply_entry("fixture.legacy_agent", "{}", "test", None, |tx| {
            tx.execute("INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms) VALUES('agent_0000000000000001','Ada','helper','assistant',1,1)", [])?;
            Ok(())
        }).unwrap();
        let marked: bool = f
            .store
            .read(|conn| {
                conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM registry_journal WHERE op='agent.cutover')",
                    [],
                    |r| r.get(0),
                )
            })
            .unwrap();
        assert!(!marked);
        let before = serde_json::to_value(f.store.agent_snapshot().unwrap()).unwrap();
        assert_eq!(before["agents"].as_array().unwrap().len(), 1);
        let generation = f.store.generation().unwrap();
        let error = f.store.check_agent_import_preconditions(true).unwrap_err();
        assert!(matches!(&error, RegistryError::Domain { code, .. } if code == "invalid_request"));
        assert!(error.to_string().contains("--without-agents"));
        assert_eq!(
            serde_json::to_value(f.store.agent_snapshot().unwrap()).unwrap(),
            before
        );
        assert_eq!(f.store.generation().unwrap(), generation);
    }

    #[test]
    fn hub_snapshot_carries_root_keys_for_attach_and_rebuild_without_git_config() {
        use crate::mutations::tests::Fixture;

        let source = Fixture::new("hub-root-source");
        let target = Fixture::new("hub-root-target");
        let a = &source.store;
        let b = &target.store;
        let root = source.dir("root");
        let checkout = target.dir("checkout");
        for path in [&root, &checkout] {
            std::fs::create_dir_all(std::path::Path::new(path).join(".git")).unwrap();
            std::fs::write(
                std::path::Path::new(path).join(".git/config"),
                "[remote \"origin\"]\nurl = https://github.com/owner/repo.git\n",
            )
            .unwrap();
        }
        // Start from a project with a durable key and a local mapping, as on
        // a restored store. Enabling the hub captures durable keys; it does not
        // inspect the checkout to create replacements.
        a.db.with_conn_fenced(|tx| {
            tx.execute(
                "UPDATE identity_log_state SET state='enabled',hub_log_base=0",
                [],
            )?;
            Ok(())
        })
        .unwrap();
        a.register(crate::RegisterRequest {
            project_id: Some("P".into()),
            name: "Project".into(),
            roots: vec![root.clone()],
            ..Default::default()
        })
        .unwrap();
        a.db.with_conn_fenced(|tx| {
            tx.execute(
                "UPDATE identity_log_state SET state='disabled',hub_log_base=NULL",
                [],
            )?;
            Ok(())
        })
        .unwrap();
        let own = "01".repeat(16);
        a.begin_hub_enable(&"cd".repeat(16), &own).unwrap();
        let plan = a.hub_enable_plan().unwrap().unwrap();
        let image: HubSnapshot = serde_json::from_str(&plan.preimage).unwrap();
        let entry = RemoteEntry {
            position: 1,
            entry_id: "10".repeat(16),
            author: own.clone(),
            signer: "test".into(),
            key_id: "test".into(),
            envelope_version: 1,
            kind: "snapshot".into(),
            entry: plan.preimage.into_bytes(),
        };
        let parts = [(entry, 0)];
        a.install_hub_snapshot(&image, &parts, &own).unwrap();
        let received = serde_json::from_slice(&parts[0].0.entry).unwrap();
        b.install_hub_snapshot(&received, &parts, &"02".repeat(16))
            .unwrap();
        b.attach_root(crate::AttachRootRequest {
            path: checkout.clone(),
            ..Default::default()
        })
        .unwrap();
        for (store, path) in [(a, root), (b, checkout)] {
            std::fs::remove_file(std::path::Path::new(&path).join(".git/config")).unwrap();
            let resolve = || {
                store
                    .resolve_root_key(crate::ResolveRootKeyRequest {
                        project_id: "P".into(),
                        kind: "remote".into(),
                        root_key: "owner/repo".into(),
                    })
                    .unwrap()
                    .roots
            };
            assert_eq!(resolve(), vec![path.clone()]);
            let before = store.read(hub_tables).unwrap();
            assert!(store.verify().unwrap().replay.ok);
            assert!(store.rebuild().unwrap().replay.ok);
            assert_eq!(store.read(hub_tables).unwrap(), before);
            assert_eq!(resolve(), vec![path]);
        }
    }
}
