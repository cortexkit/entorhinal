//! Apply owner-minted genomes without deriving or transforming them here.
use std::collections::BTreeSet;

use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{
    reads::decode,
    store::{load_row, require_cutover, write_row},
    validate_agent_avatar, AgentChangeEntry, AgentMutationError, AgentPrecheck, AgentRow,
    OperatorApproval, StoredAgentAvatar, OPERATOR_SUMMARY_LIMIT,
};
use crate::{mutations::Action, JournalWriter, RegistryStore};

pub(crate) const APPLY_METHOD: &str = "agent.apply_avatar_manifest";
const SET_AVATAR: &str = "agent.set_avatar";

fn cached(
    conn: &Connection,
    op: &str,
    key: Option<&str>,
) -> Result<Option<Vec<u8>>, AgentMutationError> {
    let Some(key) = key else {
        return Ok(None);
    };
    let row = conn
        .query_row(
            "SELECT op,response_json FROM registry_journal WHERE request_key=?1",
            [key],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?)),
        )
        .optional()?;
    match row {
        Some((stored_op, _)) if stored_op != op => Err(AgentMutationError::new(
            "request_key_reused_across_ops",
            format!("request_key is already bound to op '{stored_op}'"),
        )),
        Some((_, reply)) => Ok(reply.map(String::into_bytes)),
        None => Ok(None),
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestEntry {
    agent_id: String,
    avatar_genome: String,
    avatar_type: String,
    avatar_version: i64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PreviewRequest {
    entries: Vec<ManifestEntry>,
    #[serde(default)]
    replace: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ApplyRequest {
    entries: Vec<ManifestEntry>,
    #[serde(default)]
    replace: bool,
    request_key: Option<String>,
    actor: Option<String>,
}

impl ApplyRequest {
    fn decode(params: Value) -> Result<Self, AgentMutationError> {
        let request: Self = decode(params)?;
        if request.request_key.as_deref().is_none_or(str::is_empty) {
            return Err(AgentMutationError::new(
                "request_key_required",
                "request_key is required",
            ));
        }
        Ok(request)
    }
}

#[derive(Clone, Serialize)]
enum State {
    Pending,
    Skipped,
    Written(Value),
}

#[derive(Clone, Serialize)]
struct Planned {
    row: Option<AgentRow>,
    entry: ManifestEntry,
    key: Option<String>,
    state: State,
}

impl Planned {
    fn description(&self) -> Value {
        if let State::Written(value) = &self.state {
            return value.clone();
        }
        let row = self.row.as_ref().expect("planned live row");
        json!({
            "agent_id": row.agent_id,
            "name": row.name,
            "from": row.avatar.as_ref().map(|a| json!({"type":a.avatar_type,"version":a.version})),
            "to": {"type":self.entry.avatar_type,"version":self.entry.avatar_version},
        })
    }
}

/// An immutable validated batch. Its before-images bind the operator's approval
/// and let each separately committed write detect changes to unfinished agents.
#[derive(Clone, Serialize)]
pub struct AvatarManifestPlan {
    rows: Vec<Planned>,
    replace: bool,
    actor: String,
    generation: i64,
}

impl AvatarManifestPlan {
    pub fn pending(&self) -> Vec<usize> {
        self.rows
            .iter()
            .enumerate()
            .filter(|(_, p)| matches!(p.state, State::Pending))
            .map(|(i, _)| i)
            .collect()
    }

    pub fn summary(&self) -> Result<String, AgentMutationError> {
        // Bind complete before-images and supplied genomes, not just count/type.
        let bytes = serde_json::to_vec(self)
            .map_err(|e| AgentMutationError::new("encode_failed", e.to_string()))?;
        let text = format!(
            "Apply avatars to {} agents ({}); plan {}",
            self.pending().len(),
            self.rows
                .first()
                .map(|p| p.entry.avatar_type.as_str())
                .unwrap_or("none"),
            blake3::hash(&bytes).to_hex()
        );
        if text.chars().count() > OPERATOR_SUMMARY_LIMIT {
            return Err(AgentMutationError::new(
                "operator_summary_too_long",
                "avatar batch summary exceeds 200 characters",
            ));
        }
        Ok(text)
    }

    pub fn report(&self) -> Value {
        let written: Vec<_> = self
            .rows
            .iter()
            .filter(|p| matches!(p.state, State::Written(_)))
            .map(Planned::description)
            .collect();
        json!({
            "applied":written.len(), "written":written,
            "remaining":self.rows.iter().filter(|p|matches!(p.state, State::Pending)).map(Planned::description).collect::<Vec<_>>(),
            "skipped":self.rows.iter().filter(|p|matches!(p.state, State::Skipped)).map(Planned::description).collect::<Vec<_>>(),
            "generation":self.generation,
        })
    }

    pub fn entry_key(&self, index: usize) -> Option<&str> {
        self.rows.get(index).and_then(|p| p.key.as_deref())
    }

    /// Replays are checked before live identity/identical-genome rules, because
    /// the previous attempt may already have installed this exact genome.
    pub fn probe_entry(
        &self,
        store: &RegistryStore,
        index: usize,
    ) -> Result<Option<Vec<u8>>, AgentMutationError> {
        Ok(store.shared_request_cache(SET_AVATAR, self.entry_key(index))?)
    }
}

fn plan(
    conn: &Connection,
    request: PreviewRequest,
    batch_key: Option<&str>,
    actor: String,
    generation: i64,
) -> Result<AvatarManifestPlan, AgentMutationError> {
    require_cutover(conn)?;
    let mut problems = Vec::new();
    if request.entries.is_empty() {
        problems.push("manifest must not be empty".to_owned());
    }
    let mut ids = BTreeSet::new();
    let mut types = BTreeSet::new();
    let mut rows = Vec::new();
    let current_generation = generation;
    let mut generation = 0;
    for entry in request.entries {
        let id = &entry.agent_id;
        if !ids.insert(id.clone()) {
            problems.push(format!("{id}: duplicate agent_id"));
        }
        types.insert(entry.avatar_type.clone());
        if let Err(error) = validate_agent_avatar(&entry.avatar_genome, Some(&entry.avatar_type)) {
            problems.push(format!("{id}: {error}"));
        }
        let key = batch_key.map(|key| format!("{key}:{id}"));
        let reply = cached(conn, SET_AVATAR, key.as_deref())?;
        if let Some(reply) = reply {
            let reply: Value = serde_json::from_slice(&reply)
                .map_err(|e| AgentMutationError::new("decode_failed", e.to_string()))?;
            generation = generation.max(reply["result"]["generation"].as_i64().unwrap_or(0));
            rows.push(Planned {
                row: None,
                entry,
                key,
                state: State::Written(reply["result"]["agent"].clone()),
            });
            continue;
        }
        match load_row(conn, id)? {
            Some(row) if row.status == "live" => {
                if request.replace
                    && row
                        .avatar
                        .as_ref()
                        .is_some_and(|avatar| avatar.genome == entry.avatar_genome)
                {
                    problems.push(format!(
                        "{id} ({}): replacement genome is identical to current genome",
                        row.name
                    ));
                }
                let state = if !request.replace && row.avatar.is_some() {
                    State::Skipped
                } else {
                    State::Pending
                };
                if matches!(state, State::Pending) && row.agent_generation == i64::MAX {
                    problems.push(format!("{id}: agent generation overflow"));
                }
                rows.push(Planned {
                    row: Some(row),
                    entry,
                    key,
                    state,
                });
            }
            _ => problems.push(format!("{id}: not a live agent")),
        }
    }
    if types.len() > 1 {
        problems.push(format!(
            "manifest entries disagree on avatar_type: {types:?}"
        ));
    }
    if !problems.is_empty() {
        return Err(
            AgentMutationError::new("invalid_avatar_manifest", problems.join("; "))
                .with_detail(json!({"problems":problems})),
        );
    }
    rows.sort_by(|a, b| a.entry.agent_id.cmp(&b.entry.agent_id));
    if generation == 0 || rows.iter().any(|p| matches!(p.state, State::Pending)) {
        generation = current_generation;
    }
    let plan = AvatarManifestPlan {
        rows,
        replace: request.replace,
        actor,
        generation,
    };
    plan.summary()?;
    Ok(plan)
}

pub(crate) fn preview(conn: &Connection, params: Value) -> Result<Value, AgentMutationError> {
    let request: PreviewRequest = decode(params)?;
    let plan = plan(conn, request, None, "module".into(), 0)?;
    let mut result = plan.report();
    result["changes"] = result["remaining"].take();
    result.as_object_mut().unwrap().remove("remaining");
    result.as_object_mut().unwrap().remove("applied");
    Ok(result)
}

impl RegistryStore {
    pub fn plan_avatar_manifest(
        &self,
        params: Value,
    ) -> Result<AvatarManifestPlan, AgentMutationError> {
        let request = ApplyRequest::decode(params)?;
        self.read_agent(|conn| {
            plan(
                conn,
                PreviewRequest {
                    entries: request.entries,
                    replace: request.replace,
                },
                request.request_key.as_deref(),
                request.actor.unwrap_or_else(|| "module".into()),
                self.generation_from_connection(conn)?,
            )
        })
    }

    pub(crate) fn probe_avatar_manifest(
        &self,
        params: Value,
    ) -> Result<Option<Vec<u8>>, AgentMutationError> {
        let plan = self.plan_avatar_manifest(params)?;
        if plan.pending().is_empty() {
            Ok(Some(crate::mutations::wire_value(plan.report())?))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn precheck_avatar_manifest(
        &self,
        params: Value,
    ) -> Result<AgentPrecheck, AgentMutationError> {
        let plan = self.plan_avatar_manifest(params)?;
        if plan.pending().is_empty() {
            Ok(AgentPrecheck::Cached(crate::mutations::wire_value(
                plan.report(),
            )?))
        } else {
            Ok(AgentPrecheck::Ready(plan.summary()?))
        }
    }
}

impl JournalWriter<'_> {
    /// One ordinary identity transaction and shared entry. The complete approved
    /// plan is checked again, ignoring only writes already cached under its keys.
    pub fn apply_avatar_manifest_entry(
        &self,
        plan: &AvatarManifestPlan,
        index: usize,
        approval: &OperatorApproval,
        now: i64,
    ) -> Result<Vec<u8>, AgentMutationError> {
        let planned = plan
            .rows
            .get(index)
            .filter(|p| matches!(p.state, State::Pending))
            .ok_or_else(|| {
                AgentMutationError::new("invalid_request", "not a pending manifest entry")
            })?;
        self.mutation_with_error(SET_AVATAR, planned.key.as_deref(), |tx| {
            require_cutover(tx)?;
            if !approval.approves(&plan.summary()?) {
                return Err(stale());
            }
            for expected in &plan.rows {
                if matches!(expected.state, State::Written(_))
                    || cached(tx, SET_AVATAR, expected.key.as_deref())?.is_some()
                {
                    continue;
                }
                if load_row(tx, &expected.entry.agent_id)? != expected.row {
                    return Err(stale());
                }
                validate_agent_avatar(
                    &expected.entry.avatar_genome,
                    Some(&expected.entry.avatar_type),
                )?;
            }
            let mut row = planned.row.clone().ok_or_else(stale)?;
            row.avatar = Some(StoredAgentAvatar {
                genome: planned.entry.avatar_genome.clone(),
                avatar_type: planned.entry.avatar_type.clone(),
                version: Some(planned.entry.avatar_version),
            });
            row.agent_generation = row.agent_generation.checked_add(1).ok_or_else(|| {
                AgentMutationError::new("storage_error", "agent generation overflow")
            })?;
            row.updated_at_ms = now;
            write_row(tx, &row)?;
            let mut action: Action = super::journal::record(
                tx,
                AgentChangeEntry::new(SET_AVATAR, row, vec![]),
                json!({"agent":planned.description(),"applied":true}),
                &plan.actor,
                planned.key.as_deref(),
                now,
                "direct",
            )?;
            action.payload["operator_confirmed"] = json!(true);
            Ok(action)
        })
    }
}

fn stale() -> AgentMutationError {
    AgentMutationError::new(
        "operator_approval_stale",
        "agent identities or avatars changed after operator approval",
    )
}

#[cfg(test)]
mod tests {
    use super::super::reads::tests::{call, create, fixture, read, Fixture};
    use super::*;

    fn entry(id: &str, ch: &str) -> Value {
        json!({"agent_id":id,"avatar_genome":ch.repeat(32_768),"avatar_type":"creature.wild","avatar_version":1})
    }

    fn request(entries: Value, replace: bool) -> Value {
        json!({"entries":entries,"replace":replace,"request_key":"avatars"})
    }

    fn apply(f: &Fixture, params: Value) -> Value {
        let plan = f.store.plan_avatar_manifest(params.clone()).unwrap();
        let approval = OperatorApproval::new(plan.summary().unwrap());
        for index in plan.pending() {
            f.store
                .with_principal("direct")
                .apply_avatar_manifest_entry(&plan, index, &approval, 200)
                .unwrap();
        }
        f.store.plan_avatar_manifest(params).unwrap().report()
    }

    fn classic(f: &Fixture, id: &str) {
        call(
            f,
            SET_AVATAR,
            json!({"agentId":id,"genome":"a".repeat(2048),"type":"creature.classic","version":2,"request_key":format!("classic-{id}")}),
        );
    }

    fn unchanged_refusal(f: &Fixture, params: Value, fragment: &str) {
        let before = serde_json::to_value(f.store.agent_snapshot().unwrap()).unwrap();
        let head = f.store.generation().unwrap();
        let precheck = f
            .store
            .precheck_agent_confirmation(APPLY_METHOD, params.clone(), 200)
            .err()
            .unwrap();
        assert!(precheck.message.contains(fragment), "{precheck:?}");
        let error = f.store.plan_avatar_manifest(params).err().unwrap();
        assert!(error.message.contains(fragment), "{error:?}");
        assert_eq!(f.store.generation().unwrap(), head);
        assert_eq!(
            serde_json::to_value(f.store.agent_snapshot().unwrap()).unwrap(),
            before
        );
    }

    #[test]
    fn avatar_manifest_preserves_store_schema_version_10() {
        let f = fixture(true);
        let schema_version = || {
            f.store
                .read(|conn| {
                    conn.query_row(
                        "SELECT COALESCE(MAX(version), 0) FROM cortexkit_schema_version WHERE namespace='projects'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                })
                .unwrap()
        };
        assert_eq!(schema_version(), 10);
        let id = create(&f, "Ada", "assistant", None, None);
        assert_eq!(
            apply(&f, request(json!([entry(&id, "b")]), false))["applied"],
            1
        );
        assert_eq!(schema_version(), 10);
    }

    #[test]
    fn avatar_manifest_seed_only_previews_skips_and_seeds_new_agents() {
        let f = fixture(true);
        let a = create(&f, "Ada", "assistant", None, None);
        let b = create(&f, "Grace", "assistant", None, None);
        classic(&f, &a);
        let entries = json!([entry(&a, "b"), entry(&b, "c")]);
        let head = f.store.generation().unwrap();
        let preview = read(
            &f,
            "agent.preview_avatar_manifest",
            json!({"entries":entries}),
        );
        assert_eq!(
            f.store.generation().unwrap(),
            head,
            "preview writes nothing"
        );
        assert_eq!(preview["changes"].as_array().unwrap().len(), 1);
        assert_eq!(preview["changes"][0]["name"], "Grace");
        assert!(preview["changes"][0]["from"].is_null());
        assert_eq!(
            preview["changes"][0]["to"],
            json!({"type":"creature.wild","version":1})
        );
        assert_eq!(preview["skipped"][0]["name"], "Ada");
        let reply = apply(&f, request(entries, false));
        assert_eq!(reply["applied"], 1);
        assert_eq!(reply["skipped"].as_array().unwrap().len(), 1);
        assert_eq!(
            f.store
                .agent_row(&a)
                .unwrap()
                .unwrap()
                .avatar
                .unwrap()
                .avatar_type,
            "creature.classic"
        );
        assert_eq!(
            f.store
                .agent_row(&b)
                .unwrap()
                .unwrap()
                .avatar
                .unwrap()
                .genome,
            "c".repeat(32_768)
        );
        let entries = f.store.agent_changes(head, Some(100)).unwrap();
        assert_eq!(entries.entries.len(), 1);
        assert_eq!(entries.entries[0].op, SET_AVATAR);
        assert_eq!(entries.entries[0].agent_id, b);
    }

    #[test]
    fn avatar_manifest_replace_accepts_full_genome_with_classic_prefix() {
        let f = fixture(true);
        let id = create(&f, "Ada", "assistant", None, None);
        classic(&f, &id);
        let genome = format!("{}{}", "a".repeat(2048), "b".repeat(32_768 - 2048));
        let mut e = entry(&id, "b");
        e["avatar_genome"] = json!(genome);
        let preview = read(
            &f,
            "agent.preview_avatar_manifest",
            json!({"entries":[e],"replace":true}),
        );
        assert_eq!(
            preview["changes"][0]["from"],
            json!({"type":"creature.classic","version":2})
        );
        assert_eq!(apply(&f, request(json!([e]), true))["applied"], 1);
        let stored = f.store.agent_row(&id).unwrap().unwrap().avatar.unwrap();
        assert_eq!(stored.genome, genome);
        assert_eq!(stored.avatar_type, "creature.wild");
        assert_eq!(stored.version, Some(1));
        let before = serde_json::to_value(f.store.agent_snapshot().unwrap()).unwrap();
        assert!(f.store.rebuild().unwrap().replay.ok);
        assert_eq!(
            serde_json::to_value(f.store.agent_snapshot().unwrap()).unwrap(),
            before
        );
    }

    #[test]
    fn avatar_manifest_identical_replacement_is_refused() {
        let f = fixture(true);
        let id = create(&f, "Ada", "assistant", None, None);
        apply(&f, request(json!([entry(&id, "b")]), false));
        let mut params = request(json!([entry(&id, "b")]), true);
        params["request_key"] = json!("different-run");
        unchanged_refusal(&f, params, "identical");
    }

    #[test]
    fn avatar_manifest_wrong_length_is_refused_atomically() {
        let f = fixture(true);
        let a = create(&f, "Ada", "assistant", None, None);
        let b = create(&f, "Grace", "assistant", None, None);
        let mut bad = entry(&b, "b");
        bad["avatar_genome"] = json!("a".repeat(2048));
        unchanged_refusal(&f, request(json!([entry(&a, "c"), bad]), false), "32768");
    }

    #[test]
    fn avatar_manifest_unknown_and_retired_agents_are_refused_atomically() {
        let f = fixture(true);
        let a = create(&f, "Ada", "assistant", None, None);
        let b = create(&f, "Grace", "assistant", None, None);
        call(
            &f,
            "agent.dispose",
            json!({"agent_id":b,"request_key":"retire"}),
        );
        for id in ["agent_ffffffffffffffff", b.as_str()] {
            unchanged_refusal(
                &f,
                request(json!([entry(&a, "c"), entry(id, "b")]), false),
                "not a live agent",
            );
        }
    }

    #[test]
    fn avatar_manifest_unknown_type_is_refused() {
        let f = fixture(true);
        let id = create(&f, "Ada", "assistant", None, None);
        let mut bad = entry(&id, "b");
        bad["avatar_type"] = json!("unknown.type");
        unchanged_refusal(&f, request(json!([bad]), false), "unknown avatar type");
    }

    #[test]
    fn avatar_manifest_mixed_types_are_refused() {
        let f = fixture(true);
        let a = create(&f, "Ada", "assistant", None, None);
        let b = create(&f, "Grace", "assistant", None, None);
        let bad = json!({"agent_id":b,"avatar_genome":"a".repeat(2048),"avatar_type":"creature.classic","avatar_version":2});
        unchanged_refusal(&f, request(json!([entry(&a, "b"), bad]), false), "disagree");
    }

    #[test]
    fn avatar_manifest_duplicate_ids_are_refused() {
        let f = fixture(true);
        let id = create(&f, "Ada", "assistant", None, None);
        unchanged_refusal(
            &f,
            request(json!([entry(&id, "b"), entry(&id, "c")]), false),
            "duplicate",
        );
    }

    #[test]
    fn avatar_manifest_empty_is_refused() {
        let f = fixture(true);
        unchanged_refusal(&f, request(json!([]), false), "empty");
    }

    #[test]
    fn avatar_manifest_strict_shape_and_aggregated_errors() {
        let f = fixture(true);
        let id = create(&f, "Ada", "assistant", None, None);
        let mut bad = entry(&id, "b");
        bad["unknown"] = json!(true);
        unchanged_refusal(&f, request(json!([bad]), false), "unknown field");
        for field in ["agent_id", "avatar_genome", "avatar_type", "avatar_version"] {
            let mut missing = entry(&id, "b");
            missing.as_object_mut().unwrap().remove(field);
            unchanged_refusal(&f, request(json!([missing]), false), "missing field");
        }
        let mut extra = request(json!([entry(&id, "b")]), false);
        extra["unknown"] = json!(true);
        unchanged_refusal(&f, extra, "unknown field");
        unchanged_refusal(&f, request(entry(&id, "b"), false), "invalid type");
        for malformed in [json!({}), json!(null), json!("bad")] {
            unchanged_refusal(&f, request(malformed, false), "invalid type");
        }
        let bad = json!({"agent_id":"missing","avatar_genome":"short","avatar_type":"creature.wild","avatar_version":1});
        let error = f
            .store
            .precheck_agent_confirmation(APPLY_METHOD, request(json!([bad]), false), 200)
            .err()
            .unwrap();
        assert_eq!(
            error.detail.unwrap()["problems"].as_array().unwrap().len(),
            2
        );
        assert!(error.message.contains("missing") && error.message.contains("32768"));
    }

    #[test]
    fn avatar_manifest_approval_binds_full_plan_and_rechecks_under_write_lock() {
        let f = fixture(true);
        let id = create(&f, "Ada", "assistant", None, None);
        let other = create(&f, "Grace", "assistant", None, None);
        let params = request(json!([entry(&id, "b"), entry(&other, "c")]), false);
        let plan = f.store.plan_avatar_manifest(params.clone()).unwrap();
        let summary = plan.summary().unwrap();
        assert!(summary.contains("2 agents (creature.wild)"));
        assert!(summary.chars().count() <= 200);
        let approval = OperatorApproval::new(summary);
        let mut changed_manifest = params.clone();
        changed_manifest["entries"][0]["avatar_genome"] = json!("c".repeat(32_768));
        let changed = f.store.plan_avatar_manifest(changed_manifest).unwrap();
        let mismatch = f
            .store
            .with_principal("direct")
            .apply_avatar_manifest_entry(&changed, 0, &approval, 200)
            .err()
            .unwrap();
        assert_eq!(mismatch.code, "operator_approval_stale");
        call(
            &f,
            "agent.update_tag",
            json!({"agent_id":other,"tag":"changed","request_key":"tag"}),
        );
        let head = f.store.generation().unwrap();
        let error = f
            .store
            .with_principal("direct")
            .apply_avatar_manifest_entry(&plan, 0, &approval, 200)
            .err()
            .unwrap();
        assert_eq!(error.code, "operator_approval_stale");
        assert_eq!(f.store.generation().unwrap(), head);
        assert!(f.store.agent_row(&id).unwrap().unwrap().avatar.is_none());
        call(
            &f,
            "agent.dispose",
            json!({"agent_id":id,"request_key":"gone"}),
        );
        assert_eq!(
            f.store.plan_avatar_manifest(params).err().unwrap().code,
            "invalid_avatar_manifest"
        );
    }

    #[test]
    fn avatar_manifest_generation_overflow_refuses_before_first_write() {
        let f = fixture(true);
        let a = create(&f, "Ada", "assistant", None, None);
        let b = create(&f, "Grace", "assistant", None, None);
        f.store
            .apply_entry("fixture", "{}", "test", None, |tx| {
                let mut row = load_row(tx, &b).unwrap().unwrap();
                row.agent_generation = i64::MAX;
                write_row(tx, &row)
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
            })
            .unwrap();
        let head = f.store.generation().unwrap();
        assert_eq!(
            f.store
                .plan_avatar_manifest(request(json!([entry(&a, "b"), entry(&b, "c")]), false))
                .err()
                .unwrap()
                .code,
            "invalid_avatar_manifest"
        );
        assert_eq!(f.store.generation().unwrap(), head);
        assert!(f.store.agent_row(&a).unwrap().unwrap().avatar.is_none());
        assert!(f.store.agent_row(&b).unwrap().unwrap().avatar.is_none());
    }

    #[test]
    fn avatar_manifest_summary_refuses_overlong_type_without_truncation() {
        let f = fixture(true);
        let id = create(&f, "Ada", "assistant", None, None);
        let mut plan = f
            .store
            .plan_avatar_manifest(request(json!([entry(&id, "b")]), false))
            .unwrap();
        // Supported type names currently fit. Exercise the summary boundary
        // directly so a future layout name cannot turn refusal into truncation.
        plan.rows[0].entry.avatar_type = "future.layout".repeat(30);
        assert_eq!(
            plan.summary().err().unwrap().code,
            "operator_summary_too_long"
        );
    }

    #[test]
    fn avatar_manifest_owner_sample_parses_exact_per_entry_layout() {
        let entries: Vec<ManifestEntry> =
            serde_json::from_str(include_str!("wild-manifest-sample.json")).unwrap();
        assert_eq!(entries.len(), 2);
        for entry in entries {
            assert_eq!(entry.avatar_type, "creature.wild");
            assert_eq!(entry.avatar_version, 1);
            validate_agent_avatar(&entry.avatar_genome, Some(&entry.avatar_type)).unwrap();
        }
    }
}
