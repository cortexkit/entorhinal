//! Applies entries received from the identity log, which engram has already
//! verified, to this machine's store. An entry that can't be decoded, names an
//! unknown operation or leaves the store inconsistent is refused and rolled
//! back, never skipped. Journal replay tolerates some old entry shapes for
//! compatibility; that tolerance is deliberately not used here, because a
//! received entry was written by current code and must decode exactly.

use rusqlite::{params, Transaction};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    agent::AgentChangeEntry,
    mutations::{append, domain},
    shared_entry::{canonical_bytes, decode_error, ProjectEntry},
    RegistryError, RegistryStore,
};

pub const ENVELOPE_VERSION: i64 = 1;
const AGENT_OPS: &[&str] = &[
    "agent.create",
    "agent.rename",
    "agent.update_tag",
    "agent.set_labels",
    "agent.set_avatar",
    "agent.set_github_identity",
    "agent.dispose",
    "agent.merge",
];

/// The log coordinator supplies decoded bytes, not hex, after checking paging
/// and signatures. This layer independently checks the supported envelope.
pub struct RemoteEntry {
    pub position: i64,
    pub entry_id: String,
    pub signer: String,
    pub key_id: String,
    pub envelope_version: i64,
    pub kind: String,
    pub entry: Vec<u8>,
}

enum Decoded {
    Project(ProjectEntry),
    Agent(Box<AgentChangeEntry>),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotPart {
    snapshot_id: String,
    part: u64,
    parts: u64,
    op: String,
    tables: std::collections::BTreeMap<String, crate::shared_entry::TableChanges>,
    #[serde(default)]
    agents: Vec<AgentChangeEntry>,
}

fn snapshot_part(entry: &RemoteEntry) -> Result<SnapshotPart, RegistryError> {
    let part: SnapshotPart =
        serde_json::from_slice(&entry.entry).map_err(|e| domain("apply_failed", e.to_string()))?;
    if entry.kind != "snapshot"
        || entry.envelope_version != ENVELOPE_VERSION
        || part.op != "shared.snapshot"
        || part.snapshot_id.is_empty()
        || part.part == 0
        || part.parts == 0
        || part.part > part.parts
    {
        return Err(domain("apply_failed", "invalid snapshot part"));
    }
    Ok(part)
}

/// Inspect a part without installing it. The coordinator uses this to buffer
/// incomplete snapshots; installation independently validates the whole group.
pub fn snapshot_part_info(entry: &RemoteEntry) -> Result<(String, u64, u64), RegistryError> {
    let part = snapshot_part(entry)?;
    Ok((part.snapshot_id, part.part, part.parts))
}

impl RegistryStore {
    /// Install all snapshot parts atomically, preserving each part's authorship
    /// and original body for audit. The merged entry remains in replay's format.
    pub fn apply_remote_snapshot(&self, entries: &[RemoteEntry]) -> Result<i64, RegistryError> {
        self.install_snapshot(entries, None)
    }

    /// Commit the incoming shared rows, their journal entries and the joining
    /// state together; if they reach the observed final log position, commit the
    /// enabled state instead. A crash cannot leave imported identities committed
    /// while the state is still disabled, which would incorrectly allow local
    /// writes before the remaining log entries have been imported.
    pub fn apply_join_snapshot(
        &self,
        entries: &[RemoteEntry],
        head: i64,
    ) -> Result<i64, RegistryError> {
        self.install_snapshot(entries, Some(head))
    }

    fn install_snapshot(
        &self,
        entries: &[RemoteEntry],
        join_head: Option<i64>,
    ) -> Result<i64, RegistryError> {
        let parts = entries
            .iter()
            .map(snapshot_part)
            .collect::<Result<Vec<_>, _>>()?;
        let first = parts
            .first()
            .ok_or_else(|| domain("apply_failed", "empty snapshot"))?;
        if first.parts != parts.len() as u64
            || parts.iter().enumerate().any(|(i, part)| {
                part.snapshot_id != first.snapshot_id
                    || part.parts != first.parts
                    || part.part != i as u64 + 1
            })
        {
            return Err(domain(
                "apply_failed",
                "snapshot parts are not complete and ordered",
            ));
        }
        let mut merged = ProjectEntry {
            op: "shared.snapshot".into(),
            tables: Default::default(),
            agents: vec![],
        };
        for part in parts {
            for (table, changes) in part.tables {
                if !changes.delete.is_empty() {
                    return Err(domain("apply_failed", "snapshot cannot delete rows"));
                }
                merged
                    .tables
                    .entry(table)
                    .or_default()
                    .upsert
                    .extend(changes.upsert);
            }
            merged.agents.extend(part.agents);
        }
        let empty = ProjectEntry {
            op: "shared.snapshot".into(),
            tables: Default::default(),
            agents: vec![],
        };
        let error = std::cell::RefCell::new(None);
        self.db
            .with_conn_fenced(|tx| {
                let install = || -> rusqlite::Result<i64> {
                    let position: i64 = tx.query_row(
                        "SELECT last_applied_position FROM identity_log_state WHERE id=1",
                        [],
                        |r| r.get(0),
                    )?;
                    if entries
                        .iter()
                        .enumerate()
                        .any(|(i, entry)| entry.position != position + i as i64 + 1)
                    {
                        return Err(decode_error(serde_json::Error::io(std::io::Error::other(
                            "snapshot positions are not contiguous",
                        ))));
                    }
                    // Run the same image validation before inserting any journal row.
                    merged.validate(tx)?;
                    let mut seq = 0;
                    for (i, entry) in entries.iter().enumerate() {
                        let replay_entry = RemoteEntry {
                            position: entry.position,
                            entry_id: entry.entry_id.clone(),
                            signer: entry.signer.clone(),
                            key_id: entry.key_id.clone(),
                            envelope_version: entry.envelope_version,
                            kind: entry.kind.clone(),
                            entry: canonical_bytes(if i == 0 { &merged } else { &empty }),
                        };
                        seq = apply(tx, &replay_entry)?;
                        let payload: String = tx.query_row(
                            "SELECT payload_json FROM registry_journal WHERE seq=?1",
                            [seq],
                            |r| r.get(0),
                        )?;
                        let mut payload: Value =
                            serde_json::from_str(&payload).map_err(decode_error)?;
                        // Keep the original multipart body alongside the merged bytes
                        // used by replay; no transport dependency is needed to audit it.
                        payload["snapshot_part"] =
                            serde_json::from_slice(&entry.entry).map_err(decode_error)?;
                        tx.execute(
                            "UPDATE registry_journal SET payload_json=?1 WHERE seq=?2",
                            params![String::from_utf8(canonical_bytes(&payload)).unwrap(), seq],
                        )?;
                    }
                    if let Some(head) = join_head {
                        tx.execute("UPDATE identity_log_state SET state='joining',last_seen_head=?1 WHERE id=1 AND state='disabled'", [head])?;
                        crate::enable_state::finish_join_at_head(tx, head)?;
                    }
                    Ok(seq)
                };
                install().inspect_err(|cause| *error.borrow_mut() = Some(cause.to_string()))
            })
            .map_err(|cause| {
                domain(
                    "apply_failed",
                    error.into_inner().unwrap_or_else(|| cause.to_string()),
                )
            })
    }

    /// Commit one complete change or assembled snapshot. A failure rolls back
    /// projection, journal and applied position together. Multipart buffering
    /// belongs to the coordinator, which must pass only a complete snapshot.
    pub fn apply_remote_entry(&self, entry: &RemoteEntry) -> Result<i64, RegistryError> {
        let error = std::cell::RefCell::new(None);
        let result = self.db.with_conn_fenced(|tx| {
            apply(tx, entry).inspect_err(|cause| {
                *error.borrow_mut() = Some(cause.to_string());
            })
        });
        result.map_err(|cause| {
            domain(
                "apply_failed",
                error.into_inner().unwrap_or_else(|| cause.to_string()),
            )
        })
    }
}

fn apply(tx: &Transaction<'_>, entry: &RemoteEntry) -> rusqlite::Result<i64> {
    let invalid = |message: &str| {
        rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            message,
        )))
    };
    if entry.envelope_version != ENVELOPE_VERSION {
        return Err(invalid("unknown envelope_version"));
    }
    if !matches!(entry.kind.as_str(), "change" | "snapshot") {
        return Err(invalid("unknown entry kind"));
    }
    let value: Value = serde_json::from_slice(&entry.entry).map_err(decode_error)?;
    let op = value
        .get("op")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("missing entry op"))?;
    let decoded = match (entry.kind.as_str(), op) {
        ("change", "project.shared") | ("snapshot", "shared.snapshot") => {
            let project: ProjectEntry = serde_json::from_value(value).map_err(decode_error)?;
            project.validate(tx)?;
            Decoded::Project(project)
        }
        ("change", op) if AGENT_OPS.contains(&op) => {
            let agent: AgentChangeEntry = serde_json::from_value(value).map_err(decode_error)?;
            if agent.agent_id != agent.row.agent_id
                || agent.agent_generation != agent.row.agent_generation
            {
                return Err(invalid("inconsistent agent after-image"));
            }
            // Claims belong to the row being changed. Check stored ownership
            // too: claim replay updates an existing id without changing its owner.
            for claim in &agent.claims {
                let owner: Option<String> = tx
                    .query_row(
                        "SELECT agent_id FROM agent_name_claim WHERE claim_id=?1",
                        [claim.claim_id],
                        |r| r.get(0),
                    )
                    .optional()?;
                if claim.agent_id != agent.agent_id
                    || owner.is_some_and(|owner| owner != agent.agent_id)
                {
                    return Err(invalid("agent entry changes another agent's claim"));
                }
            }
            Decoded::Agent(Box::new(agent))
        }
        _ => return Err(invalid("unknown entry op or kind/op mismatch")),
    };
    let position: i64 = tx.query_row(
        "SELECT last_applied_position FROM identity_log_state WHERE id=1",
        [],
        |r| r.get(0),
    )?;
    if entry.position != position + 1 {
        return Err(invalid("entry is not the next log position"));
    }
    let own: Option<i64> = tx
        .query_row(
            "SELECT seq FROM registry_journal WHERE entry_id=?1 AND origin='here'",
            [&entry.entry_id],
            |r| r.get(0),
        )
        .optional()?;
    let seq = if let Some(seq) = own {
        seq
    } else {
        // Bootstrap identities can refer to another identity restored later in
        // the same snapshot. Validate those references against the final state.
        tx.execute_batch("PRAGMA defer_foreign_keys=ON;")?;
        match decoded {
            Decoded::Project(project) => {
                if project.op == "shared.snapshot" {
                    for mut agent in project.agents.clone() {
                        let agent_seq = append(
                            tx,
                            "agent.import",
                            &json!({}),
                            "log",
                            None,
                            agent.row.updated_at_ms,
                            "entorhinal",
                        )?;
                        agent.seq = agent_seq;
                        let payload = json!({"entry": agent});
                        crate::agent::replay_agent_entry(tx, "agent.import", &payload)?;
                        tx.execute(
                            "UPDATE registry_journal SET payload_json=?1 WHERE seq=?2",
                            params![payload.to_string(), agent_seq],
                        )?;
                    }
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
                            "log",
                            None,
                            0,
                            "entorhinal",
                        )?;
                    }
                }
                let seq = append(tx, &project.op, &json!({}), "log", None, 0, "entorhinal")?;
                project.apply(tx, seq)?;
                tag(
                    tx,
                    seq,
                    entry,
                    &json!({"signer":entry.signer,"key_id":entry.key_id}),
                )?;
                seq
            }
            Decoded::Agent(mut agent) => {
                let seq = append(
                    tx,
                    &agent.op,
                    &json!({}),
                    "log",
                    None,
                    agent.row.updated_at_ms,
                    "entorhinal",
                )?;
                agent.seq = seq;
                let payload = json!({"entry":agent,"signer":entry.signer,"key_id":entry.key_id});
                crate::agent::replay_agent_entry(tx, &agent.op, &payload)?;
                let active = crate::agent::load_claims(tx, &agent.agent_id)?
                    .iter()
                    .filter(|claim| claim.released_at_ms.is_none())
                    .count();
                if active != usize::from(agent.row.status == "live") {
                    return Err(invalid(
                        "agent after-image has an invalid active claim count",
                    ));
                }
                // The journal payload, which `agent.changes` serves, carries
                // this machine's own seq. The bytes received from the log are
                // stored alongside it unchanged, for audit and so replay
                // restores exactly what was received.
                tag(tx, seq, entry, &payload)?;
                seq
            }
        }
    };
    tx.execute("UPDATE identity_log_state SET last_applied_position=?1,last_seen_head=MAX(last_seen_head,?1) WHERE id=1", [entry.position])?;
    let head: i64 = tx.query_row(
        "SELECT last_seen_head FROM identity_log_state WHERE id=1",
        [],
        |r| r.get(0),
    )?;
    crate::enable_state::finish_join_at_head(tx, head)?;
    tx.execute(
        "DELETE FROM pending_entry WHERE expected_head < ?1",
        [entry.position],
    )?;
    // Foreign-key checks are deferred while rows are installed, so a violation
    // would otherwise surface only at commit, as a generic SQLite error. Check
    // now, so a received entry that leaves dangling references is refused with
    // the same `apply_failed` error as any other invalid entry.
    if tx.prepare("PRAGMA foreign_key_check")?.exists([])? {
        return Err(invalid("shared after-image violates a foreign key"));
    }
    Ok(seq)
}

use rusqlite::OptionalExtension;

fn tag(
    tx: &Transaction<'_>,
    seq: i64,
    entry: &RemoteEntry,
    payload: &Value,
) -> rusqlite::Result<()> {
    tx.execute("UPDATE registry_journal SET payload_json=?1,stream='shared',origin='log',entry_id=?2,log_position=?3,entry=?4 WHERE seq=?5", params![String::from_utf8(canonical_bytes(payload)).unwrap(),entry.entry_id,entry.position,entry.entry,seq])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        mutations::tests::Fixture,
        shared_entry::{SharedState, TableChanges},
        *,
    };
    use std::{collections::BTreeMap, sync::mpsc, thread, time::Duration};

    fn enable(f: &Fixture) {
        f.store
            .apply_entry("identity_log.enable", "{}", "test", None, |tx| {
                tx.execute("UPDATE identity_log_state SET state='enabled'", [])?;
                Ok(())
            })
            .unwrap();
    }

    fn register(f: &Fixture, id: &str, roots: Vec<String>, workspace: Option<&str>) {
        f.store
            .register(RegisterRequest {
                project_id: Some(id.into()),
                name: id.into(),
                roots,
                workspace_id: workspace.map(str::to_string),
                ..Default::default()
            })
            .unwrap();
    }

    fn state(f: &Fixture) -> SharedState {
        f.store.read(SharedState::capture).unwrap()
    }

    fn envelope(position: i64, value: &impl serde::Serialize, snapshot: bool) -> RemoteEntry {
        RemoteEntry {
            position,
            entry_id: format!("fake-{position}"),
            signer: "fake-source".into(),
            key_id: "key".into(),
            envelope_version: ENVELOPE_VERSION,
            kind: if snapshot { "snapshot" } else { "change" }.into(),
            entry: canonical_bytes(value),
        }
    }

    fn empty_project() -> ProjectEntry {
        ProjectEntry {
            op: "project.shared".into(),
            tables: BTreeMap::new(),
            agents: vec![],
        }
    }

    fn delta(f: &Fixture, before: SharedState) -> ProjectEntry {
        f.store
            .read(|conn| before.project_delta(&SharedState::capture(conn)?, conn))
            .unwrap()
    }

    fn append_local(f: &Fixture, op: &str, payload: Value) {
        f.store
            .apply_entry(op, &payload.to_string(), "test", None, |tx| {
                let seq = tx.last_insert_rowid();
                if op.starts_with("root_key.") {
                    crate::shared_entry::replay_root_keys(tx, payload.clone())?;
                } else if !crate::binding::replay_binding_op(tx, seq, op, &payload)?
                    && !crate::binding::replay_root_op(tx, seq, op, &payload, 0)?
                {
                    crate::ownership::replay_ownership_op(tx, op, &payload)?;
                }
                Ok(())
            })
            .unwrap();
    }

    fn local_checkout(f: &Fixture) -> (String, String) {
        let root = f.dir("checkout");
        let parent = f.dir("parent");
        register(f, "P", vec![root.clone()], Some("W"));
        register(f, "S", vec![], None);
        append_local(
            f,
            "bind_root",
            json!({"canonicalRoot":root,"projectId":"P","incarnation":"inc","registrationEpoch":"epoch"}),
        );
        append_local(
            f,
            "approve_root",
            json!({"canonicalRoot":root,"registrationEpoch":"epoch"}),
        );
        append_local(
            f,
            "attach_derived_parent",
            json!({"projectId":"P","root":root,"incarnation":"inc","registrationEpoch":"epoch","container":parent}),
        );
        append_local(f, "set_owned_remotes", json!({"root":root,"remotes":[]}));
        let mut image = empty_project();
        image.tables.insert("project_root_key".into(), TableChanges { upsert:vec![serde_json::from_value(json!({"project_id":"P","kind":"remote","root_key":"owner/repo","created_at":10})).unwrap()], delete:vec![] });
        f.store
            .apply_remote_entry(&envelope(1, &image, false))
            .unwrap();
        append_local(
            f,
            "root_key.backfill",
            json!({"mappings":[{"canonical_root":root,"kind":"remote","root_key":"owner/repo"}]}),
        );
        append_local(
            f,
            "root_key.assign",
            json!({"mappings":[{"canonical_root":root,"kind":"remote","root_key":"owner/repo"}]}),
        );
        (root, parent)
    }

    fn removal(f: &Fixture, successor: bool) -> ProjectEntry {
        let source = Fixture::new("cascade-source");
        // Install exactly this machine's shared rows, without any paths or epochs.
        source
            .store
            .apply_remote_entry(&envelope(1, &state(f).snapshot(vec![]), true))
            .unwrap();
        let before = state(&source);
        source
            .store
            .remove(RemoveRequest {
                project_id: Some("P".into()),
                successor_project_id: successor.then(|| "S".into()),
                ..Default::default()
            })
            .unwrap();
        delta(&source, before)
    }

    fn check_rebuild(f: &Fixture) {
        let verified = f.store.verify().unwrap();
        assert!(verified.ok, "{verified:?}");
        let before = image(f);
        let rebuilt = f.store.rebuild().unwrap();
        assert!(rebuilt.replay.ok, "{rebuilt:?}");
        assert_eq!(before, image(f));
    }

    #[test]
    fn received_changes_refuse_occupied_and_older_positions_without_writes() {
        let f = Fixture::new("remote-old-position");
        enable(&f);
        f.store
            .apply_remote_entry(&envelope(1, &empty_project(), false))
            .unwrap();
        f.store
            .apply_remote_entry(&envelope(2, &empty_project(), false))
            .unwrap();
        let before = image(&f);
        // A delayed or duplicated read must not rewind progress, even when its
        // entry id is new locally and therefore cannot hit a uniqueness constraint.
        for position in [2, 1, 0] {
            let mut entry = envelope(position, &empty_project(), false);
            entry.entry_id = format!("late-{position}");
            let error = f.store.apply_remote_entry(&entry).unwrap_err();
            assert!(error
                .to_string()
                .contains("entry is not the next log position"));
            assert_eq!(image(&f), before);
        }
        f.store
            .apply_remote_entry(&envelope(3, &empty_project(), false))
            .unwrap();
        assert_eq!(
            f.store.identity_log_status().unwrap().last_applied_position,
            3
        );
    }

    #[test]
    fn snapshot_installer_refuses_a_valid_but_incomplete_prefix_atomically() {
        let f = Fixture::new("remote-snapshot-prefix");
        enable(&f);
        let entries = (1..=2)
            .map(|part| {
                envelope(
                    part,
                    &json!({
                        "snapshot_id":"two-part-snapshot", "part":part, "parts":2,
                        "op":"shared.snapshot", "tables":{}
                    }),
                    true,
                )
            })
            .collect::<Vec<_>>();
        let before = image(&f);
        // Each part is individually valid. Completeness must be checked by the
        // installer itself, not only by the network reader that normally calls it.
        let error = f.store.apply_remote_snapshot(&entries[..1]).unwrap_err();
        assert!(error
            .to_string()
            .contains("snapshot parts are not complete and ordered"));
        assert_eq!(image(&f), before);
        f.store.apply_remote_snapshot(&entries).unwrap();
        assert_eq!(
            f.store.identity_log_status().unwrap().last_applied_position,
            2
        );
    }

    #[test]
    fn remote_settlement_keeps_attempts_for_current_and_future_heads() {
        let f = Fixture::new("remote-pending-boundary");
        enable(&f);
        // These durable rows represent unresolved appends after a restart. A
        // reported head alone does not say which entry occupies any attempt's slot.
        f.store.apply_entry("test-pending", "{}", "test", None, |tx| {
            tx.execute("INSERT INTO pending_entry(entry_id,expected_head) VALUES('slot-one',0),('slot-two',1),('slot-three',2)", [])?;
            Ok(())
        }).unwrap();
        f.store.observe_log_head(10).unwrap();
        assert_eq!(
            f.store.identity_log_status().unwrap().pending_write_count,
            3
        );
        f.store
            .apply_remote_entry(&envelope(1, &empty_project(), false))
            .unwrap();
        assert!(!f.store.is_pending_entry("slot-one").unwrap());
        assert!(f.store.is_pending_entry("slot-two").unwrap());
        assert!(f.store.is_pending_entry("slot-three").unwrap());
        assert_eq!(
            f.store.identity_log_status().unwrap().pending_write_count,
            2
        );
        f.store
            .apply_remote_entry(&envelope(2, &empty_project(), false))
            .unwrap();
        assert!(!f.store.is_pending_entry("slot-two").unwrap());
        assert!(f.store.is_pending_entry("slot-three").unwrap());
        assert_eq!(
            f.store.identity_log_status().unwrap().pending_write_count,
            1
        );
    }

    #[test]
    fn own_agent_receipt_advances_without_replaying_or_appending_to_the_feed() {
        let f = Fixture::new("remote-own-agent");
        enable(&f);
        f.store
            .apply_entry("agent.cutover", "{}", "test", None, |_| Ok(()))
            .unwrap();
        let params = json!({
            "request_key":"own-create", "name":"Own Agent", "tag":"tag", "role":"assistant"
        });
        f.store
            .shared_attempt(
                "own-agent-entry".into(),
                0,
                |_| crate::pending::SharedSettlement::Commit(1),
                || f.store.agent_mutation("agent.create", params, 50),
            )
            .unwrap();
        let (seq, bytes) = f.store.read(|conn| conn.query_row(
            "SELECT seq,entry FROM registry_journal WHERE entry_id='own-agent-entry' AND origin='here'", [],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?))
        )).unwrap();
        // Simulate reading the durable receipt again. Agent replay can look
        // idempotent in the projection while silently duplicating the identity feed.
        f.store
            .apply_entry("test-rewind", "{}", "test", None, |tx| {
                tx.execute("UPDATE identity_log_state SET last_applied_position=0", [])?;
                Ok(())
            })
            .unwrap();
        let before = f.store.agent_snapshot().unwrap();
        let generation = f.store.generation().unwrap();
        let mut entry = envelope(1, &empty_project(), false);
        entry.entry_id = "own-agent-entry".into();
        entry.entry = bytes;
        assert_eq!(f.store.apply_remote_entry(&entry).unwrap(), seq);
        assert_eq!(f.store.generation().unwrap(), generation);
        assert_eq!(
            serde_json::to_value(f.store.agent_snapshot().unwrap()).unwrap(),
            serde_json::to_value(before).unwrap()
        );
        assert_eq!(
            f.store.identity_log_status().unwrap().last_applied_position,
            1
        );
    }

    #[test]
    fn remote_delete_cannot_erase_a_machine_local_implicit_alias() {
        let f = Fixture::new("remote-delete-implicit-alias");
        let root = f.dir("checkout");
        register(&f, "Local", vec![root.clone()], None);
        enable(&f);
        let source = Fixture::new("remote-delete-alias-source");
        let start = state(&source);
        register(&source, "Remote", vec![], None);
        let mut change = delta(&source, start);
        // Delete rows contain only primary keys. They must obey the same path
        // isolation rule as upserts, or a peer can destroy local checkout aliases.
        change.tables.insert(
            "project_alias".into(),
            TableChanges {
                upsert: vec![],
                delete: vec![
                    serde_json::from_value(json!({"old_id": implicit_project_id(&root)})).unwrap(),
                ],
            },
        );
        let before = image(&f);
        let error = f
            .store
            .apply_remote_entry(&envelope(1, &change, false))
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("implicit aliases are machine-local"));
        assert_eq!(image(&f), before);
        change.tables.remove("project_alias");
        f.store
            .apply_remote_entry(&envelope(1, &change, false))
            .unwrap();
        assert_eq!(f.store.enumerate(None).unwrap().projects.len(), 2);
    }

    fn image(f: &Fixture) -> BTreeMap<String, Vec<Vec<rusqlite::types::Value>>> {
        f.store.read(|conn| {
            let names = conn.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name <> 'cortexkit_fence' ORDER BY name")?.query_map([],|r| r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
            names.into_iter().map(|name| {
                let mut stmt = conn.prepare(&format!("SELECT * FROM {name}"))?;
                let count = stmt.column_count();
                let mut rows = stmt.query_map([],|r| (0..count).map(|i| r.get(i)).collect::<rusqlite::Result<Vec<rusqlite::types::Value>>>())?.collect::<rusqlite::Result<Vec<_>>>()?;
                rows.sort_by_key(|row| format!("{row:?}"));
                Ok((name,rows))
            }).collect()
        }).unwrap()
    }

    #[test]
    fn remote_project_delete_retires_authority_and_deletes_local_dependents() {
        let f = Fixture::new("remote-delete");
        let (root, _) = local_checkout(&f);
        f.store
            .apply_remote_entry(&envelope(2, &removal(&f, false), false))
            .unwrap();
        f.store
            .read(|conn| {
                for table in [
                    "project_root",
                    "derived_root_parent",
                    "root_binding",
                    "root_approval",
                    "root_owned_remotes",
                    "project_root_key",
                    "project_alias",
                    "project_workspace",
                    "workspace_member",
                ] {
                    let n: i64 =
                        conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))?;
                    assert_eq!(n, 0, "{table} survived deletion");
                }
                let row: (String, String, String) = conn.query_row(
                    "SELECT canonical_root,project_id,reason FROM retired_binding",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )?;
                assert_eq!(row, (root, "P".into(), "removed".into()));
                Ok(())
            })
            .unwrap();
        check_rebuild(&f);
    }

    #[test]
    fn remote_successor_moves_local_rows_without_binding_or_approval() {
        let f = Fixture::new("remote-successor");
        let (root, parent) = local_checkout(&f);
        f.store
            .apply_remote_entry(&envelope(2, &removal(&f, true), false))
            .unwrap();
        f.store
            .read(|conn| {
                for table in [
                    "project_root",
                    "derived_root_parent",
                    "project_root_key",
                    "project_alias",
                ] {
                    let wrong: i64 = conn.query_row(
                        &format!("SELECT COUNT(*) FROM {table} WHERE project_id <> 'S'"),
                        [],
                        |r| r.get(0),
                    )?;
                    assert_eq!(wrong, 0, "{table} was not moved");
                    let n: i64 =
                        conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))?;
                    assert!(n > 0, "{table} was deleted instead of moved");
                }
                assert_eq!(
                    conn.query_row(
                        "SELECT root_key FROM project_root WHERE canonical_root=?1",
                        [&root],
                        |r| r.get::<_, String>(0)
                    )?,
                    "owner/repo"
                );
                assert_eq!(
                    conn.query_row(
                        "SELECT canonical_parent FROM derived_root_parent",
                        [],
                        |r| r.get::<_, String>(0)
                    )?,
                    parent
                );
                for table in ["root_binding", "root_approval"] {
                    assert_eq!(
                        conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r
                            .get::<_, i64>(0))?,
                        0
                    );
                }
                assert_eq!(
                    conn.query_row("SELECT reason FROM retired_binding", [], |r| r
                        .get::<_, String>(0))?,
                    "removed"
                );
                assert_eq!(
                    conn.query_row("SELECT COUNT(*) FROM root_owned_remotes", [], |r| r
                        .get::<_, i64>(0))?,
                    1
                );
                Ok(())
            })
            .unwrap();
        check_rebuild(&f);
    }

    #[test]
    fn remote_workspace_delete_drops_local_path_and_all_members() {
        let f = Fixture::new("remote-workspace");
        register(&f, "P", vec![], Some("W"));
        let root = f.dir("workspace");
        f.store
            .set_workspace_root(SetWorkspaceRootRequest {
                workspace_id: "W".into(),
                root: Some(root),
                ..Default::default()
            })
            .unwrap();
        f.store.db.with_conn_fenced(|tx| {
            tx.execute("INSERT INTO workspace_member VALUES('W','remote','legacy-device','remote-project')", [])?;
            Ok(())
        }).unwrap();
        let mut image = empty_project();
        image.tables.insert(
            "project_workspace".into(),
            TableChanges {
                upsert: vec![],
                delete: vec![serde_json::from_value(json!({"project_id":"P"})).unwrap()],
            },
        );
        image.tables.insert(
            "workspace".into(),
            TableChanges {
                upsert: vec![],
                delete: vec![serde_json::from_value(json!({"workspace_id":"W"})).unwrap()],
            },
        );
        f.store
            .apply_remote_entry(&envelope(1, &image, false))
            .unwrap();
        register(&f, "Q", vec![], Some("W"));
        f.store
            .read(|conn| {
                assert_eq!(
                    conn.query_row("SELECT COUNT(*) FROM workspace_root", [], |r| r
                        .get::<_, i64>(0))?,
                    0
                );
                assert_eq!(
                    conn.query_row("SELECT project_id FROM workspace_member", [], |r| r
                        .get::<_, String>(0))?,
                    "Q"
                );
                Ok(())
            })
            .unwrap();
        check_rebuild(&f);
    }

    #[test]
    fn remote_apply_rejects_unknown_envelopes_and_constraints_atomically() {
        let f = Fixture::new("remote-invalid");
        register(&f, "P", vec![], None);
        let before = image(&f);
        let mut cases = vec![];
        let valid = envelope(1, &empty_project(), false);
        cases.push(RemoteEntry {
            envelope_version: 99,
            ..envelope(1, &empty_project(), false)
        });
        cases.push(RemoteEntry {
            kind: "other".into(),
            ..envelope(1, &empty_project(), false)
        });
        cases.push(envelope(1, &json!({"op":"future.op","tables":{}}), false));
        cases.push(envelope(
            1,
            &json!({"op":"project.shared","tables":{"future_table":{"upsert":[],"delete":[]}}}),
            false,
        ));
        cases.push(envelope(1,&json!({"op":"project.shared","tables":{"workspace":{"upsert":[{"workspace_id":"W","name":"W","created_at":0,"updated_at":0,"root":"/foreign"}],"delete":[]}}}),false));
        cases.push(envelope(1,&json!({"op":"project.shared","tables":{"project_root_key":{"upsert":[{"project_id":"P","kind":"unknown","root_key":"x","created_at":0}],"delete":[]}}}),false));
        cases.push(envelope(1,&json!({"op":"project.shared","tables":{"workspace":{"upsert":[{"workspace_id":"W","name":"W","created_at":0,"updated_at":0}],"delete":[]},"project_workspace":{"upsert":[{"project_id":"missing","workspace_id":"W"}],"delete":[]}}}),false));
        cases.push(envelope(1, &empty_project(), true));
        for entry in cases {
            let error = f.store.apply_remote_entry(&entry).unwrap_err();
            assert!(error.to_string().starts_with("apply_failed"), "{error}");
            assert_eq!(before, image(&f), "invalid entry applied partially");
        }
        f.store.apply_remote_entry(&valid).unwrap();
        assert_eq!(f.store.generation().unwrap(), 2);
        check_rebuild(&f);
    }

    #[test]
    fn mixed_history_restores_here_entries_remote_projects_and_agent_ops() {
        let f = Fixture::new("mixed-history");
        register(&f, "legacy", vec![f.dir("legacy")], Some("W"));
        enable(&f);
        let local_root = f.dir("local");
        register(&f, "local", vec![local_root.clone()], None);
        // The recorded after-image, not the request, is authoritative for shared
        // cells. Keep the original request's path for machine-local replay.
        f.store
            .db
            .with_conn_fenced(|tx| {
                let bytes: Vec<u8> = tx.query_row(
                    "SELECT entry FROM registry_journal WHERE op='register' AND stream='shared'",
                    [],
                    |r| r.get(0),
                )?;
                let mut stored: ProjectEntry =
                    serde_json::from_slice(&bytes).map_err(decode_error)?;
                stored.tables.get_mut("project").unwrap().upsert[0]
                    .insert("name".into(), json!("after-image"));
                tx.execute(
                    "UPDATE project SET name='after-image' WHERE project_id='local'",
                    [],
                )?;
                tx.execute(
                    "UPDATE registry_journal SET entry=?1 WHERE stream='shared'",
                    [canonical_bytes(&stored)],
                )?;
                Ok(())
            })
            .unwrap();
        let source = Fixture::new("mixed-source");
        register(
            &source,
            "remote",
            vec![source.dir("foreign-checkout")],
            Some("RW"),
        );
        // Fake source's agent entries retain their operation names and local
        // feed cursors even when their source seqs differ.
        source
            .store
            .apply_entry("agent.cutover", "{}", "test", None, |_| Ok(()))
            .unwrap();
        source
            .store
            .agent_mutation(
                "agent.create",
                json!({"request_key":"bootstrap","name":"bootstrap","tag":"tag","role":"assistant"}),
                50,
            )
            .unwrap();
        let agents = source.store.agent_snapshot().unwrap();
        let bootstrap =
            AgentChangeEntry::new("agent.import", agents.agents[0].clone(), agents.claims);
        let snapshot = state(&source).snapshot(vec![bootstrap]);
        f.store
            .apply_remote_entry(&envelope(1, &snapshot, true))
            .unwrap();
        let created: Value = serde_json::from_slice(
            &source
                .store
                .agent_mutation(
                    "agent.create",
                    json!({"request_key":"create","name":"one","tag":"tag","role":"assistant"}),
                    100,
                )
                .unwrap(),
        )
        .unwrap();
        let id = created["result"]["agent"]["agent_id"].as_str().unwrap();
        source
            .store
            .agent_mutation(
                "agent.rename",
                json!({"request_key":"rename","agent_id":id,"name":"two"}),
                200,
            )
            .unwrap();
        let entries:Vec<AgentChangeEntry> = source.store.read(|conn| conn.prepare("SELECT payload_json FROM registry_journal WHERE request_key IN ('create','rename') ORDER BY seq")?.query_map([],|r| {
            let payload:String = r.get(0)?;
            let payload:Value = serde_json::from_str(&payload).map_err(decode_error)?;
            serde_json::from_value(payload["entry"].clone()).map_err(decode_error)
        })?.collect()).unwrap();
        for (i, entry) in entries.iter().enumerate() {
            f.store
                .apply_remote_entry(&envelope(i as i64 + 2, entry, false))
                .unwrap();
        }
        let bootstrap_id = snapshot.agents[0].agent_id.clone();
        f.store
            .agent_mutation(
                "agent.rename",
                json!({"request_key":"here-rename","agent_id":bootstrap_id,"name":"local-agent"}),
                300,
            )
            .unwrap();
        f.store
            .db
            .with_conn_fenced(|tx| {
                let payload: String = tx.query_row(
                    "SELECT payload_json FROM registry_journal WHERE request_key='here-rename'",
                    [],
                    |r| r.get(0),
                )?;
                let mut payload: Value = serde_json::from_str(&payload).map_err(decode_error)?;
                payload["entry"]["row"]["name"] = json!("non-authoritative payload");
                tx.execute(
                    "UPDATE registry_journal SET payload_json=?1 WHERE request_key='here-rename'",
                    [payload.to_string()],
                )?;
                Ok(())
            })
            .unwrap();
        let changes = f.store.agent_changes(0, Some(100)).unwrap();
        assert_eq!(
            changes
                .entries
                .iter()
                .map(|entry| entry.op.as_str())
                .collect::<Vec<_>>(),
            vec![
                "agent.import",
                "agent.create",
                "agent.rename",
                "agent.rename"
            ]
        );
        assert!(changes.entries[0].seq < changes.entries[1].seq);
        assert_eq!(
            f.store
                .resolve(&local_root)
                .unwrap()
                .project_name
                .as_deref(),
            Some("after-image")
        );
        f.store
            .read(|conn| {
                assert_eq!(
                    conn.query_row("SELECT COUNT(*) FROM project_root", [], |r| r
                        .get::<_, i64>(0))?,
                    2
                );
                assert_eq!(
                    conn.query_row(
                        "SELECT project_id FROM workspace_member WHERE workspace_id='RW'",
                        [],
                        |r| r.get::<_, String>(0)
                    )?,
                    "remote"
                );
                Ok(())
            })
            .unwrap();
        check_rebuild(&f);
    }

    #[test]
    fn remote_agent_claims_cannot_release_another_owner_or_leave_invalid_liveness() {
        let f = Fixture::new("remote-agent-claims");
        enable(&f);
        f.store
            .apply_entry("agent.cutover", "{}", "test", None, |_| Ok(()))
            .unwrap();
        let a = "agent_0000000000000001";
        let b = "agent_0000000000000002";
        for (id, name) in [(a, "Ada"), (b, "Grace")] {
            f.store
                .with_principal("test")
                .agent_mutation_with_id(
                    "agent.create",
                    json!({"role":"assistant","name":name,"tag":"helper","request_key":id}),
                    10,
                    Some(id),
                )
                .unwrap();
        }
        let row = f.store.agent_row(a).unwrap().unwrap();
        let mut foreign = f.store.agent_claims(b).unwrap();
        foreign[0].released_at_ms = Some(20);
        let bad = AgentChangeEntry::new("agent.rename", row.clone(), foreign.clone());
        let before = image(&f);
        let agents_before = f.store.agent_snapshot().unwrap();
        let error = f
            .store
            .apply_remote_entry(&envelope(1, &bad, false))
            .unwrap_err();
        assert!(error.to_string().starts_with("apply_failed"));
        assert_eq!(image(&f), before);
        assert_eq!(
            serde_json::to_value(f.store.agent_snapshot().unwrap()).unwrap(),
            serde_json::to_value(&agents_before).unwrap()
        );
        // Renaming the owner field cannot disguise a claim id belonging to B.
        foreign[0].agent_id = a.into();
        let disguised = AgentChangeEntry::new("agent.rename", row.clone(), foreign);
        assert!(f
            .store
            .apply_remote_entry(&envelope(1, &disguised, false))
            .is_err());
        assert_eq!(
            serde_json::to_value(f.store.agent_snapshot().unwrap()).unwrap(),
            serde_json::to_value(&agents_before).unwrap()
        );
        let mut own = f.store.agent_claims(a).unwrap();
        own[0].released_at_ms = Some(20);
        let no_active = AgentChangeEntry::new("agent.rename", row.clone(), own.clone());
        assert!(f
            .store
            .apply_remote_entry(&envelope(1, &no_active, false))
            .is_err());
        assert_eq!(image(&f), before);
        assert_eq!(
            serde_json::to_value(f.store.agent_snapshot().unwrap()).unwrap(),
            serde_json::to_value(&agents_before).unwrap()
        );
        let mut retired = row.clone();
        retired.status = "retired".into();
        retired.terminal_at_ms = Some(20);
        let active_retired = AgentChangeEntry::new("agent.dispose", retired, vec![]);
        assert!(f
            .store
            .apply_remote_entry(&envelope(1, &active_retired, false))
            .is_err());
        assert_eq!(
            serde_json::to_value(f.store.agent_snapshot().unwrap()).unwrap(),
            serde_json::to_value(&agents_before).unwrap()
        );
        let mut replacement = own[0].clone();
        replacement.claim_id += 2;
        replacement.display_name = "Augusta".into();
        replacement.normalized_name = "augusta".into();
        replacement.claimed_at_ms = 20;
        replacement.released_at_ms = None;
        own.push(replacement);
        let mut renamed = row;
        renamed.name = "Augusta".into();
        renamed.updated_at_ms = 20;
        renamed.name_version += 1;
        let good = AgentChangeEntry::new("agent.rename", renamed, own);
        f.store
            .apply_remote_entry(&envelope(1, &good, false))
            .unwrap();
        assert_eq!(f.store.agent_row(a).unwrap().unwrap().name, "Augusta");
        assert_eq!(
            f.store.agent_claims(b).unwrap(),
            agents_before
                .claims
                .into_iter()
                .filter(|claim| claim.agent_id == b)
                .collect::<Vec<_>>()
        );
        check_rebuild(&f);
    }

    #[test]
    fn enable_marker_preserves_legacy_workspace_timestamp_then_uses_local_rules() {
        let f = Fixture::new("replay-marker");
        register(&f, "P", vec![f.dir("checkout")], Some("W"));
        let root = f.dir("workspace");
        f.store
            .set_workspace_root(SetWorkspaceRootRequest {
                workspace_id: "W".into(),
                root: Some(root),
                ..Default::default()
            })
            .unwrap();
        let legacy = f
            .store
            .read(|conn| {
                conn.query_row(
                    "SELECT updated_at FROM workspace WHERE workspace_id='W'",
                    [],
                    |r| r.get::<_, i64>(0),
                )
            })
            .unwrap();
        enable(&f);
        let second = f.dir("second-workspace");
        f.store
            .set_workspace_root(SetWorkspaceRootRequest {
                workspace_id: "W".into(),
                root: Some(second.clone()),
                ..Default::default()
            })
            .unwrap();
        // Distinct deterministic timestamps prevent a same-millisecond update
        // from accidentally masking the enabled workspace timestamp rule.
        f.store.db.with_conn_fenced(|tx| {
            tx.execute("UPDATE registry_journal SET created_at=?1 WHERE op='set_workspace_root' AND seq=(SELECT MAX(seq) FROM registry_journal)",[legacy+50])?;
            tx.execute("UPDATE workspace_root SET updated_at=?1",[legacy+50])?;
            Ok(())
        }).unwrap();
        let checkout = f.dir("checkout");
        append_local(&f, "remove_root", json!({"projectId":"P","root":checkout}));
        f.store.read(|conn| {
            assert_eq!(conn.query_row("SELECT updated_at FROM workspace",[],|r| r.get::<_,i64>(0))?,legacy);
            assert_eq!(conn.query_row("SELECT COUNT(*) FROM project_root",[],|r| r.get::<_,i64>(0))?,0);
            assert_eq!(conn.query_row("SELECT stream FROM registry_journal WHERE op='set_workspace_root' ORDER BY seq DESC LIMIT 1",[],|r| r.get::<_,String>(0))?,"local");
            Ok(())
        }).unwrap();
        check_rebuild(&f);
    }

    #[test]
    fn verify_shared_history_uses_private_committed_snapshot_without_writer_lock() {
        let f = Fixture::new("shared-verify-reader");
        enable(&f);
        register(&f, "P", vec![], None);
        let before = image(&f);
        let (parked, received) = mpsc::channel();
        let (release, resume) = mpsc::channel();
        let writer = f.store.clone();
        let handle = thread::spawn(move || {
            writer.db.with_conn_fenced(|tx| {
                tx.execute("UPDATE project SET name='uncommitted'", [])?;
                parked.send(()).unwrap();
                resume.recv().unwrap();
                Err::<(), _>(rusqlite::Error::QueryReturnedNoRows)
            })
        });
        received.recv_timeout(Duration::from_secs(5)).unwrap();
        let reader = f.store.clone();
        let (done, result) = mpsc::channel();
        let verify = thread::spawn(move || done.send(reader.verify()).unwrap());
        let observed = result.recv_timeout(Duration::from_millis(100));
        release.send(()).unwrap();
        assert!(handle.join().unwrap().is_err());
        verify.join().unwrap();
        assert!(
            observed
                .expect("verify waited for the live writer")
                .unwrap()
                .ok
        );
        assert_eq!(before, image(&f), "verify wrote the live database");
    }
}
