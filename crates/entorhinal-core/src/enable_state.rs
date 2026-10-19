//! Durable enable plans and transitions. The module holds its writer lock while
//! capturing a plan; retries use these stored bytes rather than rereading git.

use std::collections::BTreeMap;

use rusqlite::{params, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    agent::AgentChangeEntry,
    mutations::{append, domain},
    shared_entry::{canonical_bytes, decode_error, ProjectEntry, RootKeyMapping, SharedState},
    RegistryError, RegistryStore,
};

pub const SNAPSHOT_CAP: usize = 200 * 1024;

#[derive(Clone, Serialize, Deserialize)]
pub struct EnablePart {
    pub entry_id: [u8; 16],
    pub data: Vec<u8>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Backfill {
    image: ProjectEntry,
    mappings: Vec<RootKeyMapping>,
    // Kept with the saved backfill so old plans (which omit it) retain their
    // original bootstrap positions and bytes when reopened by a newer build.
    #[serde(default)]
    base: u64,
}

pub struct EnablePlan {
    pub bodies: Vec<Vec<u8>>,
    pub backfill: Backfill,
}

#[derive(Serialize)]
struct WirePart<'a> {
    snapshot_id: &'a str,
    part: usize,
    parts: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    supersedes_through: Option<u64>,
    #[serde(flatten)]
    image: &'a ProjectEntry,
}

fn body(id: &str, part: usize, parts: usize, image: &ProjectEntry, base: Option<u64>) -> Vec<u8> {
    canonical_bytes(&WirePart {
        snapshot_id: id,
        part,
        parts,
        supersedes_through: base,
        image,
    })
}

fn partition(id: &str, image: ProjectEntry, base: u64) -> Result<Vec<Vec<u8>>, RegistryError> {
    let supersedes = (base > 0).then_some(base);
    let estimate = supersedes.map(|_| u64::MAX);
    let empty = || ProjectEntry {
        op: "shared.snapshot".into(),
        tables: BTreeMap::new(),
        agents: vec![],
        member_name: None,
        requesting_agent_id: None,
        authorization: None,
    };
    let mut units = vec![];
    for (table, changes) in image.tables {
        for row in changes.upsert {
            let key = row
                .get("project_id")
                .or_else(|| row.get("workspace_id"))
                .or_else(|| row.get("old_id"));
            let name = format!(
                "{table} {}",
                key.map(|value| value.to_string()).unwrap_or_default()
            );
            let mut unit = empty();
            unit.tables
                .entry(table.clone())
                .or_default()
                .upsert
                .push(row);
            units.push((name, unit));
        }
    }
    for agent in image.agents {
        let name = format!("agent {}", agent.agent_id);
        let mut unit = empty();
        unit.agents.push(agent);
        units.push((name, unit));
    }
    let mut images = vec![];
    let mut current = empty();
    current.member_name = image.member_name;
    for (name, unit) in units {
        // Reserve maximum-width counts and replacement metadata before packing
        // rows; adding supersedes_through must never push a part over the cap.
        let size = body(id, usize::MAX, usize::MAX, &unit, estimate).len();
        if size > SNAPSHOT_CAP {
            return Err(domain(
                "shared_entry_too_large",
                format!("snapshot row {name} encodes to {size} bytes; cap {SNAPSHOT_CAP}"),
            ));
        }
        let mut next = current.clone();
        for (table, changes) in &unit.tables {
            next.tables
                .entry(table.clone())
                .or_default()
                .upsert
                .extend(changes.upsert.clone());
        }
        next.agents.extend(unit.agents.clone());
        if body(id, usize::MAX, usize::MAX, &next, estimate).len() > SNAPSHOT_CAP {
            images.push(current);
            current = unit;
        } else {
            current = next;
        }
    }
    images.push(current);
    Ok(images
        .iter()
        .enumerate()
        .map(|(i, image)| body(id, i + 1, images.len(), image, supersedes))
        .collect())
}

impl RegistryStore {
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

    pub fn prepare_enable(&self, snapshot_id: &str, now: i64) -> Result<EnablePlan, RegistryError> {
        self.prepare_enable_plan(snapshot_id, now, 0, &[])
    }

    /// Read-only preparation: validate the replaced log's agent history before
    /// saving a plan, so a writer cannot publish a snapshot readers must refuse.
    pub fn prepare_supersede(
        &self,
        snapshot_id: &str,
        now: i64,
        base: u64,
        log_entries: &[crate::remote_apply::RemoteEntry],
    ) -> Result<EnablePlan, RegistryError> {
        if base == 0 || base != log_entries.len() as u64 {
            return Err(domain(
                "identity_log_invariant",
                "supersede requires the complete log prefix",
            ));
        }
        let history = agent_history(log_entries)?;
        let plan = self.prepare_enable_plan(snapshot_id, now, base, &history)?;
        let prefix_has_name = log_entries.iter().any(|entry| {
            let value: serde_json::Value = serde_json::from_slice(&entry.entry).unwrap_or_default();
            value["op"] == "member.set_name"
                || value.get("member_name").is_some_and(|name| !name.is_null())
        });
        let first: serde_json::Value = serde_json::from_slice(&plan.bodies[0])
            .map_err(|e| domain("identity_log_invariant", e.to_string()))?;
        if prefix_has_name && first.get("member_name").is_none() {
            return Err(domain(
                "supersede_drops_member_name",
                "snapshot drops member name",
            ));
        }
        Ok(plan)
    }

    fn prepare_enable_plan(
        &self,
        snapshot_id: &str,
        now: i64,
        base: u64,
        history: &[AgentChangeEntry],
    ) -> Result<EnablePlan, RegistryError> {
        let agents = self
            .agent_snapshot()
            .map_err(|e| domain(&e.code, e.message))?;
        let entries = agents
            .agents
            .into_iter()
            .map(|row| {
                let claims = agents
                    .claims
                    .iter()
                    .filter(|c| c.agent_id == row.agent_id)
                    .cloned()
                    .collect();
                AgentChangeEntry::new("agent.import", row, claims)
            })
            .collect::<Vec<_>>();
        if base > 0 {
            crate::remote_apply::validate_agent_extension(history, &entries)?;
        }
        let (mut image, backfill) = self.read(|conn| {
            let mut image = SharedState::capture(conn)?.snapshot(entries);
            image.member_name = conn.query_row("SELECT author,display_name FROM member_name WHERE author=(SELECT self_author FROM self_identity WHERE id=1)", [], |r| Ok(crate::shared_entry::MemberName { author:r.get(0)?, display_name:r.get(1)? })).optional()?;
            let mut keys = crate::shared_entry::TableChanges::default();
            let mut mappings = vec![];
            let roots = conn.prepare("SELECT project_id,canonical_root FROM project_root ORDER BY canonical_root")?
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let mut owners = BTreeMap::<String, String>::new();
            for (project, root) in roots {
                let (remotes, error) = crate::ownership::root_remotes(conn, &root)?;
                let (key, created_at) = if error.is_some() {
                    // Unreadable metadata cannot justify replacing a durable key.
                    // Carry its row and mapping with the original creation time.
                    let key = conn.query_row(
                        "SELECT root_key_kind,root_key FROM project_root WHERE canonical_root=?1 AND root_key IS NOT NULL",
                        [&root], |r| Ok(crate::root_keys::RootKey { kind: r.get(0)?, root_key: r.get(1)? }),
                    ).optional()?;
                    let created_at = match &key {
                        Some(key) => conn.query_row(
                            "SELECT created_at FROM project_root_key WHERE project_id=?1 AND kind=?2 AND root_key=?3",
                            [&project, &key.kind, &key.root_key], |r| r.get::<_, i64>(0),
                        )?,
                        None => now,
                    };
                    (key, created_at)
                } else {
                    (crate::root_keys::choose(&remotes, None), now)
                };
                if let Some(key) = key {
                    if let Some(owner) = owners.insert(key.root_key.clone(), project.clone()) {
                        if owner != project {
                            return Ok(Err(domain("root_key_exists", format!("{} belongs to both project_id {owner} and {project}", key.root_key))));
                        }
                    }
                    let row: crate::shared_entry::SharedRow = serde_json::from_value(json!({"project_id":project,"kind":key.kind,"root_key":key.root_key,"created_at":created_at})).map_err(decode_error)?;
                    if let Some(existing) = keys.upsert.iter_mut().find(|existing| {
                        ["project_id", "kind", "root_key"].iter().all(|column| existing[*column] == row[*column])
                    }) {
                        // Two roots of one project can yield the same key. When one of
                        // them has an unreadable git config, its row was copied from
                        // the store with the key's original created_at; keep that row
                        // rather than the readable root's new one stamped `now`.
                        if error.is_some() { *existing = row; }
                    } else {
                        keys.upsert.push(row);
                    }
                    mappings.push(RootKeyMapping { canonical_root: root, kind: key.kind, root_key: key.root_key });
                }
            }
            image.tables.insert("project_root_key".into(), keys.clone());
            let backfill = Backfill { image: ProjectEntry { op: "project.shared".into(), tables: [("project_root_key".into(), keys)].into(), agents: vec![], member_name: None, requesting_agent_id: None, authorization: None }, mappings, base };
            Ok(Ok((image, backfill)))
        })??;
        image.agents.sort_by(|a, b| a.agent_id.cmp(&b.agent_id));
        Ok(EnablePlan {
            bodies: partition(snapshot_id, image, base)?,
            backfill,
        })
    }

    pub fn begin_enable(
        &self,
        parts: &[EnablePart],
        backfill: &Backfill,
    ) -> Result<(), RegistryError> {
        self.with_where_changes(|tx| {
            tx.execute("UPDATE identity_log_state SET state='enabling',enable_parts=?1,enable_backfill=?2 WHERE id=1 AND state='disabled'",
                params![serde_json::to_string(parts).map_err(decode_error)?, serde_json::to_string(backfill).map_err(decode_error)?])?;
            Ok(())
        }).map_err(RegistryError::Store)
    }

    pub fn enable_parts(&self) -> Result<Vec<EnablePart>, RegistryError> {
        self.read(|conn| {
            let parts: String = conn.query_row(
                "SELECT enable_parts FROM identity_log_state WHERE id=1",
                [],
                |r| r.get(0),
            )?;
            serde_json::from_str(&parts).map_err(decode_error)
        })
    }

    pub fn enable_base(&self) -> Result<u64, RegistryError> {
        self.read(|conn| {
            let backfill: String = conn.query_row(
                "SELECT enable_backfill FROM identity_log_state WHERE id=1",
                [],
                |r| r.get(0),
            )?;
            let backfill: Backfill = serde_json::from_str(&backfill).map_err(decode_error)?;
            Ok(backfill.base)
        })
    }

    /// Discard the saved snapshot and return to disabled only after a log read
    /// finds a different entry id at base + 1. Our first append requires head
    /// base; another entry permanently occupying base + 1 means neither
    /// a retry nor an earlier unanswered send can succeed, so discarding is safe.
    pub fn abandon_enable(&self) -> Result<(), RegistryError> {
        self.with_where_changes(|tx| {
            tx.execute("UPDATE identity_log_state SET state='disabled',enable_parts=NULL,enable_backfill=NULL WHERE id=1 AND state='enabling'", [])?;
            Ok(())
        }).map_err(RegistryError::Store)
    }

    pub fn finish_enable(
        &self,
        now: i64,
        principal: &str,
        self_author: Option<&str>,
    ) -> Result<(), RegistryError> {
        self.with_where_changes(|tx| {
            crate::remote_apply::learn_identity(tx, self_author)?;
            let (parts, backfill): (String, String) = tx.query_row("SELECT enable_parts,enable_backfill FROM identity_log_state WHERE id=1 AND state='enabling'", [], |r| Ok((r.get(0)?, r.get(1)?)))?;
            let parts: Vec<EnablePart> = serde_json::from_str(&parts).map_err(decode_error)?;
            let backfill: Backfill = serde_json::from_str(&backfill).map_err(decode_error)?;
            let seq = append(tx, "root_key.backfill", &json!({"mappings":backfill.mappings}), "operator", None, now, principal)?;
            backfill.image.restore(tx, seq, false)?;
            crate::shared_entry::replay_root_keys(tx, json!({"mappings":backfill.mappings}))?;
            tx.execute("UPDATE registry_journal SET stream='shared',entry=?1 WHERE seq=?2", params![canonical_bytes(&backfill.image), seq])?;
            ensure_cutover(tx, now, principal)?;
            let payload = if backfill.base > 0 { json!({"supersedes_through":backfill.base}) } else { json!({}) };
            append(tx, "identity_log.enable", &payload, "operator", None, now, principal)?;
            let position = backfill.base.checked_add(parts.len() as u64).and_then(|p| i64::try_from(p).ok())
                .ok_or_else(|| decode_error(serde_json::Error::io(std::io::Error::other("enable position exceeds local range"))))?;
            tx.execute("UPDATE identity_log_state SET state='enabled',last_applied_position=?1,last_seen_head=?1,enable_parts=NULL,enable_backfill=NULL WHERE id=1", [position])?;
            Ok(())
        }).map_err(RegistryError::Store)
    }

    /// Call after importing the complete initial snapshot and applying entries
    /// through the final position returned by the latest log read. If the saved
    /// state is joining and its applied position equals that final position,
    /// append identity_log.enable to the journal and set the state to enabled in
    /// one transaction. Repeating the call after completion adds no journal row.
    pub fn finish_join(&self, head: i64) -> Result<(), RegistryError> {
        self.with_where_changes(|tx| {
            finish_join_at_head(tx, head)?;
            Ok(())
        })
        .map_err(RegistryError::Store)
    }
}

/// Reconstruct only agent history in memory. Change entries carry complete rows
/// but only the claims inserted or released by that operation, so claims must
/// accumulate across changes, even when a later tag/label update has none.
fn agent_history(
    entries: &[crate::remote_apply::RemoteEntry],
) -> Result<Vec<AgentChangeEntry>, RegistryError> {
    use crate::remote_apply::{snapshot_part_info, snapshot_supersedes_through};
    let mut agents = BTreeMap::<String, AgentChangeEntry>::new();
    let mut parts = vec![];
    let mut snapshot = None;
    for (i, entry) in entries.iter().enumerate() {
        let invalid = |reason: String| {
            domain(
                "identity_log_invariant",
                format!("{reason} at position {}", entry.position),
            )
        };
        if entry.position != i as i64 + 1
            || entry.envelope_version != crate::remote_apply::ENVELOPE_VERSION
        {
            return Err(invalid("invalid log position or envelope".into()));
        }
        let mut value: serde_json::Value =
            serde_json::from_slice(&entry.entry).map_err(|e| invalid(e.to_string()))?;
        let after_images = if entry.kind == "snapshot" || !parts.is_empty() {
            let info = snapshot_part_info(entry).map_err(|e| invalid(e.to_string()))?;
            let expected = snapshot.get_or_insert_with(|| (info.0.clone(), info.2));
            if info.0 != expected.0 || info.2 != expected.1 || info.1 != parts.len() as u64 + 1 {
                return Err(invalid("snapshot parts are not in order".into()));
            }
            parts.push(entry);
            if info.1 == info.2 {
                let group = parts
                    .iter()
                    .map(|e| crate::remote_apply::RemoteEntry {
                        position: e.position,
                        entry_id: e.entry_id.clone(),
                        author: e.author.clone(),
                        signer: e.signer.clone(),
                        key_id: e.key_id.clone(),
                        envelope_version: e.envelope_version,
                        kind: e.kind.clone(),
                        entry: e.entry.clone(),
                    })
                    .collect::<Vec<_>>();
                snapshot_supersedes_through(&group).map_err(|e| invalid(e.to_string()))?;
                parts.clear();
                snapshot = None;
            }
            let object = value
                .as_object_mut()
                .ok_or_else(|| invalid("invalid snapshot".into()))?;
            for key in ["snapshot_id", "part", "parts", "supersedes_through"] {
                object.remove(key);
            }
            let image: ProjectEntry =
                serde_json::from_value(value).map_err(|e| invalid(e.to_string()))?;
            image.agents
        } else if entry.kind == "change" && value["op"] == "project.shared" {
            let image: ProjectEntry =
                serde_json::from_value(value).map_err(|e| invalid(e.to_string()))?;
            // A placement carry or the workspace correction embeds the
            // `agent.set_namespace` after-images of the agents it changed.
            if image
                .agents
                .iter()
                .any(|agent| agent.op != "agent.set_namespace")
            {
                return Err(invalid("unknown agent operation".into()));
            }
            image.agents
        } else if entry.kind == "change" && value["op"] == "member.set_name" {
            let name: crate::shared_entry::MemberNameEntry =
                serde_json::from_value(value).map_err(|e| invalid(e.to_string()))?;
            name.name.validate().map_err(|e| invalid(e.to_string()))?;
            if name.name.author != entry.author {
                return Err(invalid("member name author differs from log author".into()));
            }
            vec![]
        } else if entry.kind == "change" {
            let agent: AgentChangeEntry =
                serde_json::from_value(value).map_err(|e| invalid(e.to_string()))?;
            // Every agent change op except `agent.import`, which is never a
            // shared write: imports reach the log only inside snapshots.
            if agent.op == "agent.import"
                || !crate::agent::IDENTITY_CHANGE_OPS.contains(&agent.op.as_str())
            {
                return Err(invalid("unknown agent operation".into()));
            }
            vec![agent]
        } else {
            return Err(invalid("unknown entry kind".into()));
        };
        for mut agent in after_images {
            if agent.agent_id != agent.row.agent_id
                || agent.agent_generation != agent.row.agent_generation
                || agent
                    .claims
                    .iter()
                    .any(|claim| claim.agent_id != agent.agent_id)
            {
                return Err(invalid("inconsistent agent after-image".into()));
            }
            let mut claims = agents
                .remove(&agent.agent_id)
                .map(|old| old.claims)
                .unwrap_or_default()
                .into_iter()
                .map(|claim| (claim.claim_id, claim))
                .collect::<BTreeMap<_, _>>();
            for claim in agent.claims {
                claims.insert(claim.claim_id, claim);
            }
            agent.claims = claims.into_values().collect();
            agents.insert(agent.agent_id.clone(), agent);
        }
    }
    if !parts.is_empty() {
        return Err(domain(
            "identity_log_invariant",
            format!(
                "snapshot incomplete at position {}",
                entries.last().unwrap().position
            ),
        ));
    }
    Ok(agents.into_values().collect())
}

pub(crate) fn finish_join_at_head(tx: &Transaction<'_>, head: i64) -> rusqlite::Result<()> {
    let finish: bool = tx.query_row(
        "SELECT state='joining' AND last_applied_position=?1 FROM identity_log_state WHERE id=1",
        [head],
        |r| r.get(0),
    )?;
    if finish {
        append(
            tx,
            "identity_log.enable",
            &json!({}),
            "log",
            None,
            0,
            "entorhinal",
        )?;
        tx.execute(
            "UPDATE identity_log_state SET state='enabled' WHERE id=1",
            [],
        )?;
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
    use cortexkit_store::{Isolation, StorageBackend};
    use std::path::Path;

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
    fn supersede_partition_reserves_replacement_metadata_at_cap() {
        let id = "0123456789abcdef0123456789abcdef";
        let mut image = ProjectEntry {
            op: "shared.snapshot".into(),
            tables: BTreeMap::new(),
            agents: vec![],
            member_name: None,
            requesting_agent_id: None,
            authorization: None,
        };
        let changes = image.tables.entry("project".into()).or_default();
        for _ in 0..2 {
            changes.upsert.push([("name".into(), json!(""))].into());
        }
        let overhead = body(id, usize::MAX, usize::MAX, &image, None).len();
        // Without replacement metadata these two rows fit, just one byte under
        // the estimator's cap. The real replacement field is wider than the
        // digits freed by the final part count, so omitting it would overflow.
        let padding = SNAPSHOT_CAP - overhead - 1;
        for (i, row) in image
            .tables
            .get_mut("project")
            .unwrap()
            .upsert
            .iter_mut()
            .enumerate()
        {
            row.insert(
                "name".into(),
                json!("x".repeat(padding / 2 + if i == 0 { padding % 2 } else { 0 })),
            );
        }
        assert_eq!(
            body(id, usize::MAX, usize::MAX, &image, None).len(),
            SNAPSHOT_CAP - 1
        );
        let parts = partition(id, image, i64::MAX as u64 - 2).unwrap();
        assert_eq!(parts.len(), 2, "replacement metadata requires another part");
        for part in parts {
            assert!(part.len() <= SNAPSHOT_CAP);
            let value: serde_json::Value = serde_json::from_slice(&part).unwrap();
            assert_eq!(value["supersedes_through"], i64::MAX as u64 - 2);
        }
    }

    #[test]
    fn s3_unnamed_supersede_bytes_are_unchanged_and_name_is_only_on_part_one() {
        let id = "0123456789abcdef0123456789abcdef";
        let image = ProjectEntry {
            op: "shared.snapshot".into(),
            tables: BTreeMap::new(),
            agents: vec![],
            member_name: None,
            requesting_agent_id: None,
            authorization: None,
        };
        let bodies = partition(id, image.clone(), 5).unwrap();
        assert_eq!(bodies, vec![br#"{"op":"shared.snapshot","part":1,"parts":1,"snapshot_id":"0123456789abcdef0123456789abcdef","supersedes_through":5,"tables":{}}"#.to_vec()]);
        let mut image = image;
        image.member_name = Some(crate::shared_entry::MemberName {
            author: "01".repeat(16),
            display_name: "Studio".into(),
        });
        let rows = image.tables.entry("project".into()).or_default();
        for _ in 0..3 {
            rows.upsert
                .push([("name".into(), json!("x".repeat(SNAPSHOT_CAP / 2)))].into());
        }
        let parts = partition(id, image, 5).unwrap();
        assert!(parts.len() > 1);
        for (i, part) in parts.iter().enumerate() {
            assert!(part.len() <= SNAPSHOT_CAP);
            let value: serde_json::Value = serde_json::from_slice(part).unwrap();
            assert_eq!(value.get("member_name").is_some(), i == 0);
        }
    }

    #[test]
    fn s3_writer_supersede_refuses_to_drop_prefix_name() {
        let f = crate::mutations::tests::Fixture::new("writer-name-keep");
        let prefix = crate::remote_apply::RemoteEntry {
            position: 1,
            entry_id: "name".into(),
            author: "01".repeat(16),
            signer: "key".into(),
            key_id: "key".into(),
            envelope_version: 1,
            kind: "change".into(),
            entry: canonical_bytes(
                &json!({"op":"member.set_name","author":"01".repeat(16),"display_name":"Studio"}),
            ),
        };
        assert!(f
            .store
            .prepare_supersede("snapshot", 700, 1, &[prefix])
            .err()
            .unwrap()
            .to_string()
            .contains("supersede_drops_member_name"));
    }

    fn git(root: &Path, args: &[&str]) {
        let out = std::process::Command::new("git")
            .current_dir(root)
            .args(args)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    #[test]
    fn v6_backfill_bootstrap_attaches_and_rebuilds_without_git_config() {
        let dir = std::env::temp_dir().join(format!(
            "enable-v6-{}-{}",
            std::process::id(),
            crate::now_unix_millis()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let dir = dir.canonicalize().unwrap();
        let descriptor = |name: &str| cortexkit_store::StorageDescriptor {
            module_id: "entorhinal".into(),
            storage_namespace: format!("enable-v6-{}-{name}", dir.display()),
            isolation: Isolation::Module,
            backend: StorageBackend::Sqlite {
                path: dir.join(name).to_string_lossy().into(),
            },
        };
        let a_desc = descriptor("a.db");
        let a = RegistryStore::open_with_migrations(&a_desc, &crate::MIGRATIONS[..6]).unwrap();
        let root = dir.join("a");
        std::fs::create_dir_all(&root).unwrap();
        git(&root, &["init", "-q"]);
        git(
            &root,
            &[
                "remote",
                "add",
                "origin",
                "https://github.com/Owner/Repo.git",
            ],
        );
        // Populate the historical schema with historical SQL: the current writer
        // requires columns introduced after migration 6.
        let root_text = RegistryStore::canonical_mutation_root(root.to_str().unwrap()).unwrap();
        let payload = serde_json::json!({"projectId":"P","name":"Project","roots":[root_text]});
        a.db.with_conn_fenced(|tx| {
            tx.execute("INSERT INTO registry_journal(op,payload_json,actor,created_at,principal) VALUES('register',?1,'fixture',1,'entorhinal')",[payload.to_string()])?;
            tx.execute("INSERT INTO project(project_id,name,implicit,created_at,updated_at) VALUES('P','Project',0,1,1)",[])?;
            tx.execute("INSERT INTO project_root(canonical_root,project_id,added_at) VALUES(?1,'P',1)",[&root_text])?;
            tx.execute("INSERT INTO project_alias(old_id,project_id,created_at) VALUES(?1,'P',1)",[crate::implicit_project_id(&root_text)])?;
            Ok(())
        }).unwrap();
        drop(a);
        let a = RegistryStore::open(&a_desc).unwrap();
        a.check_enable_preconditions(0).unwrap();
        let plan = a.prepare_enable("v6-bootstrap", 100).unwrap();
        let parts: Vec<_> = plan
            .bodies
            .into_iter()
            .enumerate()
            .map(|(i, data)| EnablePart {
                entry_id: [i as u8 + 1; 16],
                data,
            })
            .collect();
        a.begin_enable(&parts, &plan.backfill).unwrap();
        // Feed B copies of the bytes A would append, numbered from position 1.
        // B must obtain the root keys from these encoded snapshot entries, not
        // directly from A's database tables, to prove the transfer includes them.
        let log: Vec<_> = parts
            .iter()
            .enumerate()
            .map(|(i, p)| RemoteEntry {
                position: i as i64 + 1,
                entry_id: format!("part-{i}"),
                author: "01".repeat(16),
                signer: "fake".into(),
                key_id: "fake".into(),
                envelope_version: 1,
                kind: "snapshot".into(),
                entry: p.data.clone(),
            })
            .collect();
        a.finish_enable(100, "direct", None).unwrap();
        let b = RegistryStore::open(&descriptor("b.db")).unwrap();
        b.apply_join_snapshot(&log, log.len() as i64, None).unwrap();
        let checkout = dir.join("b");
        std::fs::create_dir_all(&checkout).unwrap();
        git(&checkout, &["init", "-q"]);
        git(
            &checkout,
            &[
                "remote",
                "add",
                "origin",
                "https://github.com/owner/repo.git",
            ],
        );
        b.attach_root(crate::AttachRootRequest {
            path: RegistryStore::canonical_mutation_root(checkout.to_str().unwrap()).unwrap(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            b.resolve_root_key(crate::ResolveRootKeyRequest {
                project_id: "P".into(),
                kind: "remote".into(),
                root_key: "owner/repo".into()
            })
            .unwrap()
            .roots,
            vec![RegistryStore::canonical_mutation_root(checkout.to_str().unwrap()).unwrap()]
        );
        for path in [&root, &checkout] {
            std::fs::remove_file(path.join(".git/config")).unwrap();
        }
        for store in [&a, &b] {
            let before = store.read(SharedState::capture).unwrap();
            assert!(store.verify().unwrap().replay.ok);
            assert!(store.rebuild().unwrap().replay.ok);
            assert_eq!(store.read(SharedState::capture).unwrap(), before);
        }
        assert_eq!(
            a.resolve_root_key(crate::ResolveRootKeyRequest {
                project_id: "P".into(),
                kind: "remote".into(),
                root_key: "owner/repo".into()
            })
            .unwrap()
            .roots,
            vec![RegistryStore::canonical_mutation_root(root.to_str().unwrap()).unwrap()]
        );
        drop(a);
        drop(b);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn agent_history_folds_the_agents_a_project_entry_carries() {
        let row = |generation: i64, workspace: Option<&str>| crate::agent::AgentRow {
            agent_id: "agent_0000000000000001".into(),
            name: "Ada".into(),
            name_version: 1,
            name_normalization_version: 1,
            tag: "helper".into(),
            labels: vec![],
            role: "head".into(),
            project_id: Some("P".into()),
            workspace_id: workspace.map(str::to_owned),
            avatar: None,
            github_identity: None,
            status: "live".into(),
            merged_into: None,
            supervisor_agent_id: None,
            request_key: None,
            created_at_ms: 1,
            updated_at_ms: generation,
            terminal_at_ms: None,
            agent_generation: generation,
        };
        let claim = |id: i64, key: &str, released: Option<i64>| crate::agent::AgentNameClaim {
            claim_id: id,
            agent_id: "agent_0000000000000001".into(),
            namespace_kind: "workspace".into(),
            namespace_key: key.into(),
            normalized_name: "ada".into(),
            name_normalization_version: 1,
            display_name: "Ada".into(),
            claimed_at_ms: id,
            released_at_ms: released,
        };
        let created =
            AgentChangeEntry::new("agent.create", row(1, None), vec![claim(1, "W1", None)]);
        let moved = AgentChangeEntry::new(
            "agent.set_namespace",
            row(2, None),
            vec![claim(1, "W1", Some(2)), claim(2, "W2", None)],
        );
        let carry = ProjectEntry {
            op: "project.shared".into(),
            tables: Default::default(),
            agents: vec![moved.clone()],
            member_name: None,
            requesting_agent_id: None,
            authorization: None,
        };
        let entry = |position: i64, bytes: Vec<u8>| RemoteEntry {
            position,
            entry_id: format!("e{position}"),
            author: "01".repeat(16),
            signer: "fake".into(),
            key_id: "fake".into(),
            envelope_version: 1,
            kind: "change".into(),
            entry: bytes,
        };
        let history = agent_history(&[
            entry(1, canonical_bytes(&created)),
            entry(2, canonical_bytes(&carry)),
        ])
        .unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].row, moved.row);
        assert_eq!(history[0].claims, moved.claims);
        // Only `agent.set_namespace` after-images may ride on a project entry.
        let mut forged = carry;
        forged.agents[0].op = "agent.rename".into();
        assert!(agent_history(&[
            entry(1, canonical_bytes(&created)),
            entry(2, canonical_bytes(&forged)),
        ])
        .is_err());
    }
}
