//! A head or hiree belongs to its project's workspace: it stores no workspace
//! itself, and its active name claim is keyed on the workspace its project is
//! placed in. This module keeps that true in two ways:
//!
//! - a carry: a write that places a project, or moves it to another workspace,
//!   moves the claims of the project's live heads and hirees into the target
//!   workspace in the same transaction;
//! - the correction (`agent.repair_workspaces`): an operator write that clears
//!   every head's and hiree's stored workspace and moves every live one's claim
//!   to its project's placement, for rows imported before this rule held.
//!
//! Both journal one unkeyed `agent.set_namespace` row per changed agent, in
//! agent id order, shaped like a rename: the after-image row, then the
//! released claim and the new active claim (no claims when only the stored
//! workspace is cleared). Every name is checked before anything is written.

use std::collections::BTreeMap;

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::Deserialize;
use serde_json::{json, Value};

use super::{
    claims,
    reads::decode,
    store::{load_row, require_cutover, write_row},
    AgentChangeEntry, AgentMutationError, AgentNameClaim, AgentPrecheck, AgentRow,
    NormalizedAgentName, OperatorApproval, PlacementConfirmation, OPERATOR_SUMMARY_LIMIT,
};
use crate::{
    mutations::{append, Action},
    JournalWriter, RegistryStore,
};

/// The journal op every carried or corrected agent is recorded under. The
/// correction's request key and cached reply live on the last of these rows.
pub(crate) const SET_NAMESPACE_OP: &str = "agent.set_namespace";

/// The request method of the correction. It is never a journal op: the
/// correction journals `agent.set_namespace` rows only.
pub(crate) const REPAIR_METHOD: &str = "agent.repair_workspaces";

/// One agent's change. The stored workspace is always cleared; when `target`
/// is set, the active claim also moves to that workspace.
#[derive(Debug, Clone)]
pub(crate) struct NamespaceMove {
    row: AgentRow,
    /// The active claim. Agents that are no longer live have none, and their
    /// released claims never move.
    claim: Option<AgentNameClaim>,
    target: Option<String>,
}

impl NamespaceMove {
    fn clears_workspace(&self) -> bool {
        self.row.workspace_id.is_some()
    }

    fn moves_claim(&self) -> bool {
        self.target.is_some()
    }
}

/// The live heads and hirees of one project a placement change carries into
/// `workspace_id`, in agent id order.
#[derive(Debug, Clone)]
pub(crate) struct Carry {
    project_id: String,
    workspace_id: String,
    moves: Vec<NamespaceMove>,
}

fn keyed_on(claim: &AgentNameClaim, workspace: &str) -> bool {
    claim.namespace_kind == "workspace" && claim.namespace_key == workspace
}

/// The workspace a project is placed in, if any. Same join as agent create's
/// placement lookup, so a carry and a create agree on where a project lives.
fn placement_of(conn: &Connection, project_id: &str) -> rusqlite::Result<Option<String>> {
    conn.query_row(
        "SELECT w.workspace_id FROM project_workspace pw JOIN workspace w ON w.workspace_id=pw.workspace_id WHERE pw.project_id=?1",
        [project_id],
        |r| r.get(0),
    )
    .optional()
}

/// Plan one head's or hiree's change: clear its stored workspace, and when it
/// is live and `workspace` is given, move its active claim there unless the
/// claim already sits there. `None` when nothing would change.
fn plan_one(
    conn: &Connection,
    row: AgentRow,
    workspace: Option<&str>,
) -> Result<Option<NamespaceMove>, AgentMutationError> {
    let claim = if row.status == "live" {
        Some(claims::active_claim(conn, &row.agent_id)?)
    } else {
        None
    };
    let target = match (&claim, workspace) {
        (Some(claim), Some(workspace)) if !keyed_on(claim, workspace) => Some(workspace.to_owned()),
        _ => None,
    };
    let planned = NamespaceMove { row, claim, target };
    Ok((planned.clears_workspace() || planned.moves_claim()).then_some(planned))
}

/// What placing `project_id` in `workspace_id` does to its live heads and
/// hirees. Retired and merged agents are never carried: their names stay
/// reserved where they were released.
pub(crate) fn plan_carry(
    conn: &Connection,
    project_id: &str,
    workspace_id: &str,
) -> Result<Carry, AgentMutationError> {
    let ids = conn
        .prepare("SELECT agent_id FROM agent WHERE project_id=?1 AND role IN ('head','hiree') AND terminal_reason IS NULL ORDER BY agent_id")?
        .query_map([project_id], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut moves = Vec::new();
    for id in ids {
        let row = load_row(conn, &id)?.ok_or_else(|| {
            AgentMutationError::new("storage_error", format!("agent {id} has an invalid id"))
        })?;
        moves.extend(plan_one(conn, row, Some(workspace_id))?);
    }
    Ok(Carry {
        project_id: project_id.into(),
        workspace_id: workspace_id.into(),
        moves,
    })
}

/// The correction's plan: every head and hiree whose row stores a workspace or
/// whose live claim is not keyed on its project's placement, in agent id
/// order. The second value is the refusal for the first live head or hiree
/// whose project is unplaced; that agent is left out of the plan.
fn plan_repair(
    conn: &Connection,
) -> Result<(Vec<NamespaceMove>, Option<AgentMutationError>), AgentMutationError> {
    let ids = conn
        .prepare("SELECT agent_id FROM agent WHERE role IN ('head','hiree') ORDER BY agent_id")?
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut moves = Vec::new();
    let mut refusal = None;
    for id in ids {
        let row = load_row(conn, &id)?.ok_or_else(|| {
            AgentMutationError::new("storage_error", format!("agent {id} has an invalid id"))
        })?;
        if row.status != "live" {
            moves.extend(plan_one(conn, row, None)?);
            continue;
        }
        let project = row.project_id.clone().unwrap_or_default();
        match placement_of(conn, &project)? {
            Some(workspace) => moves.extend(plan_one(conn, row, Some(&workspace))?),
            None => {
                refusal.get_or_insert_with(|| {
                    AgentMutationError::new(
                        "unresolved_workspace",
                        format!("agent {id}'s project {project} is not placed in a workspace"),
                    )
                    .with_detail(json!({"agent_id": id, "project_id": project}))
                });
            }
        }
    }
    Ok((moves, refusal))
}

/// Refuse with `name_conflict` when a moving name is already held by another
/// live claim in its target workspace, or when an earlier agent of the same
/// batch moves the same name into the same workspace. The unique index would
/// otherwise fail part-way through the writes. `moves` is in agent id order.
fn check_names(conn: &Connection, moves: &[&NamespaceMove]) -> Result<(), AgentMutationError> {
    let mut batch = BTreeMap::<(&str, i64, &str), &str>::new();
    for planned in moves {
        let (Some(target), Some(claim)) = (planned.target.as_deref(), planned.claim.as_ref())
        else {
            continue;
        };
        let agent_id = planned.row.agent_id.as_str();
        let key = (
            target,
            claim.name_normalization_version,
            claim.normalized_name.as_str(),
        );
        let stored: Option<String> = conn
            .query_row(
                "SELECT agent_id FROM agent_name_claim WHERE namespace_kind='workspace' AND namespace_key=?1 AND name_normalization_version=?2 AND normalized_name=?3 AND released_at_ms IS NULL AND agent_id<>?4",
                params![target, claim.name_normalization_version, claim.normalized_name, agent_id],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(holder) = stored.or_else(|| batch.get(&key).map(|id| id.to_string())) {
            return Err(AgentMutationError::new(
                "name_conflict",
                format!(
                    "name '{}' is claimed in workspace/{target}",
                    claim.display_name
                ),
            )
            .with_detail(json!({
                "agent_id": agent_id,
                "holder_agent_id": holder,
                "workspace_id": target,
            })));
        }
        batch.insert(key, agent_id);
    }
    Ok(())
}

/// Write each change in `moves` (agent id order) and journal it as one
/// `agent.set_namespace` row. Only the last row may carry a request key: the
/// correction's key goes there, carries pass `None`. Every changed agent gets
/// a cleared stored workspace and a bumped generation and update time.
fn apply_moves(
    tx: &Transaction<'_>,
    moves: &[&NamespaceMove],
    actor: &str,
    last_key: Option<&str>,
    now: i64,
    principal: &str,
) -> Result<Vec<AgentChangeEntry>, AgentMutationError> {
    let mut entries = Vec::with_capacity(moves.len());
    for (index, planned) in moves.iter().enumerate() {
        let mut row = planned.row.clone();
        row.workspace_id = None;
        row.agent_generation = row
            .agent_generation
            .checked_add(1)
            .ok_or_else(|| AgentMutationError::new("storage_error", "agent generation overflow"))?;
        row.updated_at_ms = now;
        let mut changed_claims = Vec::new();
        match (planned.target.as_deref(), planned.claim.as_ref()) {
            (Some(target), Some(claim)) => {
                // An agent holds one active claim at a time, so release the
                // old one before inserting its replacement, as a rename does.
                // The entry lists them in that order so replay can follow it.
                changed_claims.push(claims::release(tx, &row.agent_id, now)?);
                write_row(tx, &row)?;
                let name = NormalizedAgentName {
                    stored_name: claim.display_name.clone(),
                    normalized_name: claim.normalized_name.clone(),
                    normalization_version: claim.name_normalization_version,
                };
                changed_claims.push(claims::claim(
                    tx,
                    &row.agent_id,
                    "workspace",
                    target,
                    &name,
                    now,
                )?);
            }
            _ => write_row(tx, &row)?,
        }
        let key = (index + 1 == moves.len()).then_some(last_key).flatten();
        let mut entry = AgentChangeEntry::new(SET_NAMESPACE_OP, row, changed_claims);
        entry.seq = append(tx, SET_NAMESPACE_OP, &json!({}), actor, key, now, principal)?;
        tx.execute(
            "UPDATE registry_journal SET payload_json=?1 WHERE seq=?2",
            params![json!({"entry": entry}).to_string(), entry.seq],
        )?;
        entries.push(entry);
    }
    Ok(entries)
}

/// The operator prompt for a carry, built by one function for both the
/// precheck and the re-check under the writer lock: `move <project> to
/// <workspace>: <names>`, names in agent id order, one clause per project in
/// project id order, joined by `; `. Data is Debug-escaped like every other
/// operator summary. Names that don't fit the prompt limit are counted in a
/// trailing `and N more`.
fn carry_summary(carries: &[&Carry]) -> Result<String, AgentMutationError> {
    let total: usize = carries.iter().map(|carry| carry.moves.len()).sum();
    for shown in (0..=total).rev() {
        let mut left = shown;
        let mut text = carries
            .iter()
            .map(|carry| {
                let count = left.min(carry.moves.len());
                left -= count;
                let mut clause = format!("move {:?} to {:?}", carry.project_id, carry.workspace_id);
                if count > 0 {
                    let names = carry.moves[..count]
                        .iter()
                        .map(|planned| format!("{:?}", planned.row.name))
                        .collect::<Vec<_>>()
                        .join(", ");
                    clause.push_str(&format!(": {names}"));
                }
                clause
            })
            .collect::<Vec<_>>()
            .join("; ");
        if shown < total {
            text.push_str(&format!(" and {} more", total - shown));
        }
        if text.chars().count() <= OPERATOR_SUMMARY_LIMIT {
            return Ok(text);
        }
    }
    Err(AgentMutationError::new(
        "operator_summary_too_long",
        "the project and workspace ids do not fit the operator prompt",
    ))
}

/// The operator prompt for the correction: both counts, always within the limit.
fn repair_summary(moves: &[NamespaceMove]) -> String {
    let (cleared, moved) = counts(moves);
    format!("repair agent workspaces: clear {cleared} stored workspaces, move {moved} name claims")
}

fn counts(moves: &[NamespaceMove]) -> (usize, usize) {
    (
        moves.iter().filter(|m| m.clears_workspace()).count(),
        moves.iter().filter(|m| m.moves_claim()).count(),
    )
}

fn stale() -> AgentMutationError {
    AgentMutationError::new(
        "operator_approval_stale",
        "agent identity or placement changed after operator approval",
    )
}

impl JournalWriter<'_> {
    /// Admit a placement change's carries before anything is written: the
    /// switchover must have happened, every moving name must be free in its
    /// target, and the confirmation must cover exactly what is carried. A
    /// placement that carries nothing passes unconditionally.
    pub(crate) fn admit_carry(
        &self,
        conn: &Connection,
        carries: &[Carry],
    ) -> Result<(), AgentMutationError> {
        self.admit_carry_and_domain(conn, carries, None)
    }

    pub(crate) fn admit_carry_and_domain(
        &self,
        conn: &Connection,
        carries: &[Carry],
        domain: Option<String>,
    ) -> Result<(), AgentMutationError> {
        let mut carrying = carries
            .iter()
            .filter(|carry| !carry.moves.is_empty())
            .collect::<Vec<_>>();
        if carrying.is_empty() && domain.is_none() {
            return Ok(());
        }
        carrying.sort_by(|a, b| a.project_id.cmp(&b.project_id));
        if !carrying.is_empty() {
            require_cutover(conn)?;
        }
        let mut moves = carrying
            .iter()
            .flat_map(|carry| &carry.moves)
            .collect::<Vec<_>>();
        moves.sort_by(|a, b| a.row.agent_id.cmp(&b.row.agent_id));
        check_names(conn, &moves)?;
        let domain_change = domain.is_some();
        let summary = match (carrying.is_empty(), domain) {
            (true, Some(domain)) => domain,
            (false, Some(domain)) => format!("{}; {domain}", carry_summary(&carrying)?),
            _ => carry_summary(&carrying)?,
        };
        if summary.chars().count() > super::OPERATOR_SUMMARY_LIMIT {
            return Err(AgentMutationError::new(
                "operator_summary_too_long",
                "shorten names to fit the combined operator prompt",
            ));
        }
        if let Some(probe) = self.probe {
            *probe.borrow_mut() = Some(summary);
            return Ok(());
        }
        match &self.confirmation {
            PlacementConfirmation::NotRequired if self.principal == "direct" && domain_change => {
                Err(stale())
            }
            PlacementConfirmation::NotRequired => Ok(()),
            PlacementConfirmation::Approved(approval) if approval.approves(&summary) => Ok(()),
            PlacementConfirmation::Approved(_) | PlacementConfirmation::NoPrompt => Err(stale()),
        }
    }

    /// Journal the admitted carries after the project op's own row, so that
    /// row stays the one the request key and reply are recorded on.
    pub(crate) fn apply_carry(
        &self,
        tx: &Transaction<'_>,
        carries: &[Carry],
        actor: &str,
        now: i64,
    ) -> Result<(), AgentMutationError> {
        let mut moves = carries
            .iter()
            .flat_map(|carry| &carry.moves)
            .collect::<Vec<_>>();
        moves.sort_by(|a, b| a.row.agent_id.cmp(&b.row.agent_id));
        apply_moves(tx, &moves, actor, None, now, self.principal)?;
        Ok(())
    }
}

/// `agent.repair_workspaces` takes only the request key (and the optional
/// actor every agent write accepts).
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RepairRequest {
    request_key: Option<String>,
    actor: Option<String>,
}

fn repair_key(request: &RepairRequest) -> Result<&str, AgentMutationError> {
    request
        .request_key
        .as_deref()
        .filter(|key| !key.is_empty())
        .ok_or_else(|| AgentMutationError::new("request_key_required", "request_key is required"))
}

/// The correction refuses as a whole: before the switchover, when a live head
/// or hiree has no placement to move to, or when a moving name is taken.
fn repair_refusal(
    conn: &Connection,
    moves: &[NamespaceMove],
    unresolved: Option<AgentMutationError>,
) -> Result<Option<AgentMutationError>, AgentMutationError> {
    if let Err(refusal) = require_cutover(conn) {
        return Ok(Some(refusal));
    }
    if unresolved.is_some() {
        return Ok(unresolved);
    }
    match check_names(conn, &moves.iter().collect::<Vec<_>>()) {
        Ok(()) => Ok(None),
        Err(refusal) if refusal.code == "name_conflict" => Ok(Some(refusal)),
        Err(error) => Err(error),
    }
}

impl JournalWriter<'_> {
    /// The correction. Under the writer lock it plans again, refuses before
    /// writing anything, re-checks an operator approval against the
    /// recomputed summary, then journals one `agent.set_namespace` row per
    /// changed agent. The request key and cached reply go on the last row, so
    /// the cache is looked up under `agent.set_namespace`. A run that changes
    /// nothing journals and caches nothing, so a retry is evaluated afresh.
    pub(crate) fn repair_agent_workspaces(
        &self,
        params: Value,
        now: i64,
        approval: Option<&OperatorApproval>,
    ) -> Result<Vec<u8>, AgentMutationError> {
        let request: RepairRequest = decode(params)?;
        let key = repair_key(&request)?;
        let actor = request.actor.as_deref().unwrap_or("module");
        let principal = if approval.is_some() {
            "direct"
        } else {
            self.principal
        };
        self.mutation_with_error(SET_NAMESPACE_OP, Some(key), |tx| {
            let (moves, unresolved) = plan_repair(tx)?;
            if let Some(refusal) = repair_refusal(tx, &moves, unresolved)? {
                return Err(refusal);
            }
            if approval.is_some_and(|approval| !approval.approves(&repair_summary(&moves))) {
                return Err(stale());
            }
            let (cleared, moved) = counts(&moves);
            let entries = apply_moves(
                tx,
                &moves.iter().collect::<Vec<_>>(),
                actor,
                Some(key),
                now,
                principal,
            )?;
            let Some(last) = entries.last() else {
                return Ok(Action {
                    changed: false,
                    seq: None,
                    value: json!({"cleared": 0, "moved": 0, "seq": null}),
                    payload: json!({}),
                });
            };
            let mut payload = json!({"entry": last});
            if approval.is_some() {
                payload["operator_confirmed"] = json!(true);
            }
            Ok(Action {
                changed: true,
                seq: Some(last.seq),
                value: json!({"cleared": cleared, "moved": moved, "seq": last.seq}),
                payload,
            })
        })
    }
}

impl RegistryStore {
    /// The correction's precheck on the read connection: a committed reply
    /// for this key first, then the write's own refusals, then the summary
    /// the operator approves.
    pub(crate) fn precheck_repair(
        &self,
        params: Value,
    ) -> Result<AgentPrecheck, AgentMutationError> {
        let request: RepairRequest = decode(params)?;
        let key = repair_key(&request)?;
        if let Some(body) = self.shared_request_cache(SET_NAMESPACE_OP, Some(key))? {
            return Ok(AgentPrecheck::Cached(body));
        }
        self.read_agent(|conn| {
            let (moves, unresolved) = plan_repair(conn)?;
            if let Some(refusal) = repair_refusal(conn, &moves, unresolved)? {
                return Err(refusal);
            }
            Ok(AgentPrecheck::Ready(repair_summary(&moves)))
        })
    }

    /// The correction's cached reply, after the same body and key checks the
    /// write applies.
    pub(crate) fn probe_repair(
        &self,
        params: Value,
    ) -> Result<Option<Vec<u8>>, AgentMutationError> {
        let request: RepairRequest = decode(params)?;
        let key = repair_key(&request)?;
        Ok(self.shared_request_cache(SET_NAMESPACE_OP, Some(key))?)
    }
}

/// `agent.preview_repair_workspaces`: what the correction would change, and
/// the refusal it would raise now (with the same detail), without writing.
/// `claim_from` and `claim_to` are `kind/key`, or null when the claim stays.
pub(crate) fn preview_repair(conn: &Connection) -> Result<Value, AgentMutationError> {
    let (moves, unresolved) = plan_repair(conn)?;
    let refusal = repair_refusal(conn, &moves, unresolved)?;
    let changes = moves
        .iter()
        .map(|planned| {
            let (from, to) = match (&planned.claim, &planned.target) {
                (Some(claim), Some(target)) => (
                    Some(format!("{}/{}", claim.namespace_kind, claim.namespace_key)),
                    Some(format!("workspace/{target}")),
                ),
                _ => (None, None),
            };
            json!({
                "agent_id": planned.row.agent_id,
                "name": planned.row.name,
                "role": planned.row.role,
                "project_id": planned.row.project_id,
                "clears_workspace": planned.clears_workspace(),
                "claim_from": from,
                "claim_to": to,
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "changes": changes,
        "refusal": refusal.map(|refusal| json!({
            "code": refusal.code,
            "message": refusal.message,
            "detail": refusal.detail,
        })),
    }))
}

/// `verify`'s agent placement mismatches, in the `kind:detail` form of its
/// other mismatches: `agent_workspace_stored:<agent_id>` for a head or hiree
/// that still stores a workspace, and `agent_claim_off_placement:<agent_id>`
/// for a live head or hiree whose active claim is not keyed on its project's
/// placement, including one whose project is unplaced. Both are expected on
/// an imported store until the correction runs.
pub(crate) fn placement_mismatches(conn: &Connection) -> rusqlite::Result<Vec<String>> {
    let mut mismatches = Vec::new();
    for (kind, sql) in [
        (
            "agent_workspace_stored",
            "SELECT agent_id FROM agent WHERE role IN ('head','hiree') AND workspace_id IS NOT NULL ORDER BY agent_id",
        ),
        (
            "agent_claim_off_placement",
            "SELECT a.agent_id FROM agent a
             LEFT JOIN project_workspace pw ON pw.project_id=a.project_id
             LEFT JOIN workspace w ON w.workspace_id=pw.workspace_id
             LEFT JOIN agent_name_claim c ON c.agent_id=a.agent_id AND c.released_at_ms IS NULL
             WHERE a.role IN ('head','hiree') AND a.terminal_reason IS NULL
               AND (w.workspace_id IS NULL OR c.claim_id IS NULL
                    OR c.namespace_kind<>'workspace' OR c.namespace_key<>w.workspace_id)
             ORDER BY a.agent_id",
        ),
    ] {
        let mut statement = conn.prepare(sql)?;
        for id in statement.query_map([], |r| r.get::<_, String>(0))? {
            mismatches.push(format!("{kind}:{}", id?));
        }
    }
    Ok(mismatches)
}

#[cfg(test)]
mod tests {
    use super::super::reads::tests::{call, create, fixture, import_core, placed, read, Fixture};
    use super::*;
    use crate::{
        shared_entry::ProjectEntry, AssignWorkspaceRequest, RegisterRequest, RegistryError,
    };

    const ADA: &str = "agent_0000000000000011";
    const GRACE: &str = "agent_0000000000000012";
    const LINUS: &str = "agent_0000000000000013";
    const BABBAGE: &str = "agent_0000000000000014";

    /// The imported shape core's rows came in, written as raw SQL into a core
    /// database and imported verbatim: Ada (live head of P, placed in W1) has
    /// its name claimed under its own project id, which is also its stored
    /// workspace; Grace (live head of Q, placed in W2) is claimed at its
    /// placement but still stores it; Linus (live hiree of P) is already
    /// correct; Babbage (retired head of P) stores a workspace and holds only
    /// a released claim.
    fn stale_store() -> Fixture {
        let f = fixture(false);
        placed(&f, "P", "W1");
        placed(&f, "Q", "W2");
        import_core(&f, &format!(
            "INSERT INTO agent(agent_id,name,tag,role,project_id,workspace_id,created_at_ms,updated_at_ms,terminal_reason,terminal_at_ms) VALUES
                ('{ADA}','Ada','tag','head','P','P',1,2,NULL,NULL),
                ('{GRACE}','Grace','tag','head','Q','W2',1,2,NULL,NULL),
                ('{LINUS}','Linus','tag','hiree','P',NULL,1,2,NULL,NULL),
                ('{BABBAGE}','Babbage','tag','head','P','P',1,2,'deleted',3);
             INSERT INTO agent_name_claim(agent_id,namespace_kind,namespace_key,normalized_name,display_name,claimed_at_ms,released_at_ms) VALUES
                ('{ADA}','workspace','P','ada','Ada',3,NULL),
                ('{GRACE}','workspace','W2','grace','Grace',3,NULL),
                ('{LINUS}','workspace','W1','linus','Linus',3,NULL),
                ('{BABBAGE}','workspace','P','babbage','Babbage',3,4);"
        ));
        f
    }

    fn generation(f: &Fixture) -> i64 {
        f.store.generation().unwrap()
    }

    fn journal_since(f: &Fixture, seq: i64) -> Vec<(String, Option<String>)> {
        f.store
            .read(|conn| {
                conn.prepare(
                    "SELECT op,request_key FROM registry_journal WHERE seq>?1 ORDER BY seq",
                )?
                .query_map([seq], |r| Ok((r.get(0)?, r.get(1)?)))?
                .collect()
            })
            .unwrap()
    }

    fn agent_mismatches(f: &Fixture) -> Vec<String> {
        f.store
            .verify()
            .unwrap()
            .mismatches
            .into_iter()
            .filter(|m| m.starts_with("agent_"))
            .collect()
    }

    /// Rebuild replays the journal into exactly the stored agent projection.
    fn clean_rebuild(f: &Fixture) {
        let before = serde_json::to_value(f.store.agent_snapshot().unwrap()).unwrap();
        let rebuilt = f.store.rebuild().unwrap();
        assert!(rebuilt.replay.ok, "{rebuilt:?}");
        assert_eq!(
            serde_json::to_value(f.store.agent_snapshot().unwrap()).unwrap(),
            before
        );
    }

    fn active_claim_of(f: &Fixture, id: &str) -> (String, String) {
        let claim = f
            .store
            .agent_claims(id)
            .unwrap()
            .into_iter()
            .find(|claim| claim.released_at_ms.is_none())
            .unwrap();
        (claim.namespace_kind, claim.namespace_key)
    }

    fn workspace(key: &str) -> (String, String) {
        ("workspace".into(), key.into())
    }

    fn refusal(error: AgentMutationError, code: &str, detail: Value) {
        assert_eq!(error.code, code, "{error:?}");
        assert_eq!(error.detail, Some(detail));
    }

    fn registry_refusal(error: RegistryError, code: &str, detail: Value) {
        match error {
            RegistryError::Domain {
                code: actual,
                detail: actual_detail,
                ..
            } => {
                assert_eq!(actual, code);
                assert_eq!(actual_detail, Some(detail));
            }
            other => panic!("expected {code}, got {other:?}"),
        }
    }

    fn assign(project: &str, workspace: &str, key: &str) -> AssignWorkspaceRequest {
        AssignWorkspaceRequest {
            project_id: project.into(),
            workspace_id: workspace.into(),
            request_key: Some(key.into()),
            ..Default::default()
        }
    }

    #[test]
    fn the_correction_previews_clears_and_moves_exactly_the_stale_agents_once() {
        let f = stale_store();
        assert_eq!(
            agent_mismatches(&f),
            vec![
                format!("agent_workspace_stored:{ADA}"),
                format!("agent_workspace_stored:{GRACE}"),
                format!("agent_workspace_stored:{BABBAGE}"),
                format!("agent_claim_off_placement:{ADA}"),
            ]
        );
        let babbage_claims = f.store.agent_claims(BABBAGE).unwrap();
        assert_eq!(
            read(&f, "agent.preview_repair_workspaces", json!({})),
            json!({"changes":[
                {"agent_id":ADA,"name":"Ada","role":"head","project_id":"P","clears_workspace":true,"claim_from":"workspace/P","claim_to":"workspace/W1"},
                {"agent_id":GRACE,"name":"Grace","role":"head","project_id":"Q","clears_workspace":true,"claim_from":null,"claim_to":null},
                {"agent_id":BABBAGE,"name":"Babbage","role":"head","project_id":"P","clears_workspace":true,"claim_from":null,"claim_to":null},
            ],"refusal":null})
        );
        match f
            .store
            .precheck_agent_confirmation(REPAIR_METHOD, json!({"request_key":"repair"}), 100)
            .unwrap()
        {
            AgentPrecheck::Ready(summary) => assert_eq!(
                summary,
                "repair agent workspaces: clear 3 stored workspaces, move 1 name claims"
            ),
            _ => panic!("the correction always asks the operator"),
        }
        let cursor = generation(&f);
        let reply = call(&f, REPAIR_METHOD, json!({"request_key":"repair"}));
        let head = generation(&f);
        assert_eq!(
            reply,
            json!({"cleared":3,"moved":1,"seq":head,"generation":head})
        );
        // One unkeyed row per changed agent in agent id order; the key and
        // the cached reply sit on the last row.
        assert_eq!(
            journal_since(&f, cursor),
            vec![
                ("agent.set_namespace".into(), None),
                ("agent.set_namespace".into(), None),
                ("agent.set_namespace".into(), Some("repair".into())),
            ]
        );
        let page = f.store.agent_changes(cursor, None).unwrap();
        assert_eq!(
            page.entries
                .iter()
                .map(|e| (
                    e.agent_id.as_str(),
                    e.claims.len(),
                    e.row.workspace_id.clone()
                ))
                .collect::<Vec<_>>(),
            vec![(ADA, 2, None), (GRACE, 0, None), (BABBAGE, 0, None)]
        );
        assert_eq!(active_claim_of(&f, ADA), workspace("W1"));
        assert_eq!(active_claim_of(&f, GRACE), workspace("W2"));
        // A retired head's claim never moves.
        assert_eq!(f.store.agent_claims(BABBAGE).unwrap(), babbage_claims);
        assert!(agent_mismatches(&f).is_empty());
        clean_rebuild(&f);
        // A same-key retry returns the committed reply without writing.
        assert_eq!(
            call(&f, REPAIR_METHOD, json!({"request_key":"repair"})),
            reply
        );
        assert_eq!(generation(&f), head);
        // A second run finds nothing to change and journals nothing.
        assert_eq!(
            call(&f, REPAIR_METHOD, json!({"request_key":"again"})),
            json!({"cleared":0,"moved":0,"seq":null,"generation":head})
        );
        assert_eq!(generation(&f), head);
        assert_eq!(
            read(&f, "agent.preview_repair_workspaces", json!({})),
            json!({"changes":[],"refusal":null})
        );
    }

    #[test]
    fn the_correction_refuses_unplaced_live_agents_and_colliding_names_writing_nothing() {
        // A live head whose project is unplaced has no workspace to move to.
        let f = fixture(false);
        f.store
            .register(RegisterRequest {
                project_id: Some("R".into()),
                name: "R".into(),
                ..Default::default()
            })
            .unwrap();
        import_core(&f, &format!(
            "INSERT INTO agent(agent_id,name,tag,role,project_id,workspace_id,created_at_ms,updated_at_ms) VALUES('{ADA}','Ada','tag','head','R','R',1,2);
             INSERT INTO agent_name_claim(agent_id,namespace_kind,namespace_key,normalized_name,display_name,claimed_at_ms) VALUES('{ADA}','workspace','R','ada','Ada',3);"
        ));
        assert_eq!(
            agent_mismatches(&f),
            vec![
                format!("agent_workspace_stored:{ADA}"),
                format!("agent_claim_off_placement:{ADA}"),
            ]
        );
        let unresolved = json!({"agent_id":ADA,"project_id":"R"});
        // Two moving names collide in one workspace, and a moving name is
        // already held there: each refusal names both agents and the target.
        let batch = fixture(false);
        placed(&batch, "P", "W1");
        placed(&batch, "Q", "W1");
        import_core(&batch, &format!(
            "INSERT INTO agent(agent_id,name,tag,role,project_id,workspace_id,created_at_ms,updated_at_ms) VALUES('{ADA}','Ada','tag','head','P','P',1,2),('{GRACE}','Ada','tag','head','Q','Q',1,2);
             INSERT INTO agent_name_claim(agent_id,namespace_kind,namespace_key,normalized_name,display_name,claimed_at_ms) VALUES('{ADA}','workspace','P','ada','Ada',3),('{GRACE}','workspace','Q','ada','Ada',3);"
        ));
        let colliding = json!({"agent_id":GRACE,"holder_agent_id":ADA,"workspace_id":"W1"});
        let stored = fixture(false);
        placed(&stored, "P", "W1");
        placed(&stored, "Q", "W1");
        import_core(&stored, &format!(
            "INSERT INTO agent(agent_id,name,tag,role,project_id,workspace_id,created_at_ms,updated_at_ms) VALUES('{ADA}','Ada','tag','head','P','P',1,2),('{GRACE}','Ada','tag','head','Q',NULL,1,2);
             INSERT INTO agent_name_claim(agent_id,namespace_kind,namespace_key,normalized_name,display_name,claimed_at_ms) VALUES('{ADA}','workspace','P','ada','Ada',3),('{GRACE}','workspace','W1','ada','Ada',3);"
        ));
        let held = json!({"agent_id":ADA,"holder_agent_id":GRACE,"workspace_id":"W1"});
        for (f, code, detail) in [
            (&f, "unresolved_workspace", unresolved),
            (&batch, "name_conflict", colliding),
            (&stored, "name_conflict", held),
        ] {
            let head = generation(f);
            let preview = read(f, "agent.preview_repair_workspaces", json!({}));
            assert_eq!(preview["refusal"]["code"], code);
            assert_eq!(preview["refusal"]["detail"], detail);
            let precheck = f.store.precheck_agent_confirmation(
                REPAIR_METHOD,
                json!({"request_key":"repair"}),
                100,
            );
            refusal(precheck.err().unwrap(), code, detail.clone());
            let error = f
                .store
                .agent_mutation(REPAIR_METHOD, json!({"request_key":"repair"}), 100)
                .unwrap_err();
            refusal(error, code, detail);
            assert_eq!(generation(f), head, "{code} wrote to the journal");
        }
    }

    #[test]
    fn the_correction_rechecks_its_approval_and_refuses_before_the_switchover() {
        let f = stale_store();
        let head = generation(&f);
        let stale = OperatorApproval::new(
            "repair agent workspaces: clear 0 stored workspaces, move 0 name claims".into(),
        );
        let error = f
            .store
            .with_principal("direct")
            .agent_mutation_with_approval(
                REPAIR_METHOD,
                json!({"request_key":"k"}),
                100,
                None,
                Some(&stale),
            )
            .unwrap_err();
        assert_eq!(error.code, "operator_approval_stale");
        assert_eq!(generation(&f), head);
        let approved = OperatorApproval::new(
            "repair agent workspaces: clear 3 stored workspaces, move 1 name claims".into(),
        );
        f.store
            .with_principal("direct")
            .agent_mutation_with_approval(
                REPAIR_METHOD,
                json!({"request_key":"k"}),
                100,
                None,
                Some(&approved),
            )
            .unwrap();
        let before = fixture(false);
        assert_eq!(
            read(&before, "agent.preview_repair_workspaces", json!({}))["refusal"]["code"],
            "authority_not_cut_over"
        );
        assert_eq!(
            before
                .store
                .agent_mutation(REPAIR_METHOD, json!({"request_key":"k"}), 100)
                .unwrap_err()
                .code,
            "authority_not_cut_over"
        );
        for body in [
            json!({}),
            json!({"request_key":null}),
            json!({"request_key":""}),
        ] {
            assert_eq!(
                f.store
                    .agent_mutation(REPAIR_METHOD, body, 100)
                    .unwrap_err()
                    .code,
                "request_key_required"
            );
        }
    }

    #[test]
    fn moving_a_project_carries_its_live_head_and_hiree_but_not_retired_claims() {
        let f = fixture(true);
        placed(&f, "P", "W1");
        let retired = create(&f, "Old", "head", Some("P"), None);
        call(
            &f,
            "agent.dispose",
            json!({"agent_id":retired,"request_key":"dispose"}),
        );
        let retired_claims = f.store.agent_claims(&retired).unwrap();
        let head = create(&f, "Ada", "head", Some("P"), None);
        let hiree = create(&f, "Linus", "hiree", Some("P"), None);
        let cursor = generation(&f);
        f.store
            .with_principal("reserved:prefrontal-core")
            .assign_workspace(assign("P", "W2", "move"))
            .unwrap();
        assert_eq!(
            journal_since(&f, cursor),
            vec![
                ("assign_workspace".into(), Some("move".into())),
                ("agent.set_namespace".into(), None),
                ("agent.set_namespace".into(), None),
            ]
        );
        let mut ids = vec![head.clone(), hiree.clone()];
        ids.sort();
        let page = f.store.agent_changes(cursor, None).unwrap();
        assert_eq!(
            page.entries
                .iter()
                .map(|e| e.agent_id.clone())
                .collect::<Vec<_>>(),
            ids
        );
        for entry in &page.entries {
            assert_eq!(entry.op, "agent.set_namespace");
            assert_eq!(entry.row.workspace_id, None);
            assert_eq!(entry.row.agent_generation, 2);
            assert_eq!(entry.claims.len(), 2);
            assert_eq!(entry.claims[0].namespace_key, "W1");
            assert!(entry.claims[0].released_at_ms.is_some());
            assert_eq!(entry.claims[1].namespace_key, "W2");
            assert_eq!(
                &f.store.agent_row(&entry.agent_id).unwrap().unwrap(),
                &entry.row
            );
        }
        assert_eq!(f.store.agent_claims(&retired).unwrap(), retired_claims);
        assert_eq!(
            read(&f, "agent.resolve_name", json!({"name":"w2/ada"}))["agent_id"],
            json!(head)
        );
        assert!(agent_mismatches(&f).is_empty());
        clean_rebuild(&f);
    }

    #[test]
    fn a_carry_on_an_imported_stale_store_fixes_what_it_moves_and_rebuilds_clean() {
        let f = stale_store();
        let cursor = generation(&f);
        f.store
            .with_principal("reserved:prefrontal-core")
            .assign_workspace(assign("P", "W3", "move"))
            .unwrap();
        // Ada's claim moves off its project id and Linus's off W1; both now
        // store no workspace. The retired head is not carried.
        let page = f.store.agent_changes(cursor, None).unwrap();
        assert_eq!(
            page.entries
                .iter()
                .map(|e| (e.agent_id.as_str(), e.claims.len()))
                .collect::<Vec<_>>(),
            vec![(ADA, 2), (LINUS, 2)]
        );
        for id in [ADA, LINUS] {
            assert_eq!(active_claim_of(&f, id), workspace("W3"));
            assert_eq!(f.store.agent_row(id).unwrap().unwrap().workspace_id, None);
        }
        // Only the rows this move did not touch are still reported.
        assert_eq!(
            agent_mismatches(&f),
            vec![
                format!("agent_workspace_stored:{GRACE}"),
                format!("agent_workspace_stored:{BABBAGE}"),
            ]
        );
        clean_rebuild(&f);
    }

    #[test]
    fn a_carry_into_a_taken_name_refuses_naming_both_agents_and_writes_nothing() {
        let f = fixture(true);
        placed(&f, "P", "W1");
        placed(&f, "Q", "W2");
        let mover = create(&f, "Ada", "head", Some("P"), None);
        let holder = create(&f, "ada", "head", Some("Q"), None);
        let head = generation(&f);
        let detail = json!({"agent_id":mover,"holder_agent_id":holder,"workspace_id":"W2"});
        let error = f
            .store
            .with_principal("reserved:prefrontal-core")
            .assign_workspace(assign("P", "W2", "move"))
            .unwrap_err();
        registry_refusal(error, "name_conflict", detail.clone());
        let precheck = f
            .store
            .precheck_agent_confirmation(
                "assign_workspace",
                json!({"projectId":"P","workspaceId":"W2","requestKey":"move"}),
                100,
            )
            .err()
            .unwrap();
        refusal(precheck, "name_conflict", detail);
        assert_eq!(generation(&f), head);
        assert_eq!(active_claim_of(&f, &mover), workspace("W1"));
    }

    #[test]
    fn a_carry_whose_moving_names_collide_with_each_other_refuses() {
        // Imported rows let a head and a hiree of one project hold the same
        // name in two different namespaces; moving the project would put both
        // in the target workspace, which the unique index forbids.
        let f = fixture(false);
        placed(&f, "P", "W1");
        import_core(&f, &format!(
            "INSERT INTO agent(agent_id,name,tag,role,project_id,workspace_id,created_at_ms,updated_at_ms) VALUES('{ADA}','Ada','tag','head','P','P',1,2),('{GRACE}','Ada','tag','hiree','P',NULL,1,2);
             INSERT INTO agent_name_claim(agent_id,namespace_kind,namespace_key,normalized_name,display_name,claimed_at_ms) VALUES('{ADA}','workspace','P','ada','Ada',3),('{GRACE}','workspace','W1','ada','Ada',3);"
        ));
        let head = generation(&f);
        let error = f
            .store
            .assign_workspace(assign("P", "W2", "move"))
            .unwrap_err();
        registry_refusal(
            error,
            "name_conflict",
            json!({"agent_id":GRACE,"holder_agent_id":ADA,"workspace_id":"W2"}),
        );
        assert_eq!(generation(&f), head);
    }

    #[test]
    fn placement_precheck_orders_cache_refusals_no_prompt_and_ready_and_the_write_rechecks() {
        let f = fixture(true);
        placed(&f, "P", "W1");
        placed(&f, "E", "W1");
        create(&f, "Ada", "head", Some("P"), None);
        create(&f, "Linus", "hiree", Some("P"), None);
        let precheck = |params: Value| {
            f.store
                .precheck_agent_confirmation("assign_workspace", params, 100)
        };
        assert_eq!(
            precheck(json!({"projectId":"missing","workspaceId":"W2"}))
                .err()
                .unwrap()
                .code,
            "not_found"
        );
        assert!(matches!(
            precheck(json!({"projectId":"E","workspaceId":"W2"})).unwrap(),
            AgentPrecheck::NoPrompt
        ));
        assert!(matches!(
            f.store
                .precheck_agent_confirmation(
                    "register",
                    json!({"projectId":"New","name":"New","workspaceId":"W2"}),
                    100
                )
                .unwrap(),
            AgentPrecheck::NoPrompt
        ));
        let summary = match precheck(
            json!({"projectId":"P","workspaceId":"W2","requestKey":"move"}),
        )
        .unwrap()
        {
            AgentPrecheck::Ready(summary) => summary,
            _ => panic!("a carrying move asks the operator"),
        };
        assert_eq!(summary, r#"move "P" to "W2": "Ada", "Linus""#);
        // The precheck wrote nothing.
        let head = generation(&f);
        assert_eq!(journal_since(&f, 0).len() as i64, head);
        let direct = |confirmation: PlacementConfirmation| {
            f.store
                .with_principal("direct")
                .with_confirmation(confirmation)
                .assign_workspace(assign("P", "W2", "move"))
        };
        for confirmation in [
            PlacementConfirmation::NoPrompt,
            PlacementConfirmation::Approved(OperatorApproval::new(
                "move \"P\" to \"W2\": \"Ada\"".into(),
            )),
        ] {
            match direct(confirmation).unwrap_err() {
                RegistryError::Domain { code, .. } => assert_eq!(code, "operator_approval_stale"),
                other => panic!("{other:?}"),
            }
            assert_eq!(generation(&f), head);
        }
        // A Direct placement that carries nothing needs no prompt.
        f.store
            .with_principal("direct")
            .with_confirmation(PlacementConfirmation::NoPrompt)
            .assign_workspace(assign("E", "W2", "move-empty"))
            .unwrap();
        let body = direct(PlacementConfirmation::Approved(OperatorApproval::new(
            summary,
        )))
        .unwrap();
        // A same-key retry returns the committed reply before anything else.
        match precheck(json!({"projectId":"P","workspaceId":"W2","requestKey":"move"})).unwrap() {
            AgentPrecheck::Cached(cached) => assert_eq!(cached, body),
            _ => panic!("a committed request key is served from the cache"),
        }
        assert!(agent_mismatches(&f).is_empty());
        clean_rebuild(&f);
    }

    #[test]
    fn a_carry_summary_lists_the_names_that_fit_and_counts_the_rest() {
        let f = fixture(true);
        placed(&f, "P", "W1");
        let head = create(&f, "Head", "head", Some("P"), None);
        let mut ids = vec![head];
        for n in 0..12 {
            ids.push(create(
                &f,
                &format!("Hiree number {n:02}"),
                "hiree",
                Some("P"),
                None,
            ));
        }
        ids.sort();
        let summary = match f
            .store
            .precheck_agent_confirmation(
                "assign_workspace",
                json!({"projectId":"P","workspaceId":"W2"}),
                100,
            )
            .unwrap()
        {
            AgentPrecheck::Ready(summary) => summary,
            _ => panic!("a carrying move asks the operator"),
        };
        assert!(
            summary.chars().count() <= OPERATOR_SUMMARY_LIMIT,
            "{summary}"
        );
        assert!(summary.starts_with(r#"move "P" to "W2": "#), "{summary}");
        let shown = summary.matches('"').count() / 2 - 2;
        assert!(
            summary.ends_with(&format!(" and {} more", 13 - shown)),
            "{summary}"
        );
    }

    #[test]
    fn carries_are_refused_before_the_switchover() {
        let f = fixture(true);
        placed(&f, "P", "W1");
        placed(&f, "E", "W1");
        create(&f, "Ada", "head", Some("P"), None);
        // Raw SQL takes away the switchover marker the fixture wrote, leaving
        // a live head in a store that has not cut over.
        f.store
            .db
            .with_conn_fenced(|tx| {
                tx.execute("DELETE FROM registry_journal WHERE op='agent.cutover'", [])
            })
            .unwrap();
        let head = generation(&f);
        match f
            .store
            .assign_workspace(assign("P", "W2", "move"))
            .unwrap_err()
        {
            RegistryError::Domain { code, .. } => assert_eq!(code, "authority_not_cut_over"),
            other => panic!("{other:?}"),
        }
        assert_eq!(generation(&f), head);
        // A placement that carries nothing is unaffected.
        f.store
            .assign_workspace(assign("E", "W2", "empty"))
            .unwrap();
    }

    fn enable(f: &Fixture) {
        f.store
            .apply_entry("identity_log.enable", "{}", "test", None, |tx| {
                tx.execute("UPDATE identity_log_state SET state='enabled'", [])?;
                Ok(())
            })
            .unwrap();
    }

    /// The shared rows appended after `seq`, as (seq, entry bytes).
    fn shared_since(f: &Fixture, seq: i64) -> Vec<(i64, Vec<u8>)> {
        f.store
            .read(|conn| {
                conn.prepare("SELECT seq,entry FROM registry_journal WHERE seq>?1 AND stream='shared' ORDER BY seq")?
                    .query_map([seq], |r| Ok((r.get(0)?, r.get(1)?)))?
                    .collect()
            })
            .unwrap()
    }

    #[test]
    fn a_carry_and_the_correction_each_post_one_shared_entry_another_node_applies() {
        let writer = stale_store();
        let reader = stale_store();
        for f in [&writer, &reader] {
            enable(f);
        }
        // The correction: one entry, on its last row, with both changed live
        // agents' after-images and the retired one's, and no project tables.
        let cursor = generation(&writer);
        call(&writer, REPAIR_METHOD, json!({"request_key":"repair"}));
        let corrected = shared_since(&writer, cursor);
        assert_eq!(corrected.len(), 1);
        assert_eq!(corrected[0].0, generation(&writer));
        let entry: ProjectEntry = serde_json::from_slice(&corrected[0].1).unwrap();
        assert_eq!(entry.op, "project.shared");
        assert!(entry.tables.is_empty());
        let page = writer.store.agent_changes(cursor, None).unwrap();
        assert_eq!(entry.agents.len(), 3);
        for (embedded, served) in entry.agents.iter().zip(&page.entries) {
            assert_eq!(
                (&embedded.row, &embedded.claims),
                (&served.row, &served.claims)
            );
        }
        // A carry: one entry, on the project op's row, holding the project
        // delta and one after-image per carried agent.
        let cursor = generation(&writer);
        writer
            .store
            .assign_workspace(assign("P", "W3", "move"))
            .unwrap();
        let carried = shared_since(&writer, cursor);
        assert_eq!(carried.len(), 1);
        assert_eq!(carried[0].0, cursor + 1);
        let entry: ProjectEntry = serde_json::from_slice(&carried[0].1).unwrap();
        assert!(entry.tables.contains_key("project_workspace"));
        assert_eq!(
            entry
                .agents
                .iter()
                .map(|a| a.agent_id.as_str())
                .collect::<Vec<_>>(),
            vec![ADA, LINUS]
        );
        assert!(agent_mismatches(&writer).is_empty());
        clean_rebuild(&writer);

        // Another node applies both entries into the same agent state and
        // serves the changes on its own feed.
        let start = generation(&reader);
        for (position, (_, bytes)) in corrected.iter().chain(&carried).enumerate() {
            reader
                .store
                .apply_remote_entry(
                    &crate::remote_apply::RemoteEntry {
                        position: position as i64 + 1,
                        entry_id: format!("writer-{position}"),
                        author: "01".repeat(16),
                        signer: "writer".into(),
                        key_id: "key".into(),
                        envelope_version: crate::remote_apply::ENVELOPE_VERSION,
                        kind: "change".into(),
                        entry: bytes.clone(),
                    },
                    None,
                )
                .unwrap();
        }
        assert_eq!(
            serde_json::to_value(reader.store.agent_snapshot().unwrap()).unwrap(),
            serde_json::to_value(writer.store.agent_snapshot().unwrap()).unwrap()
        );
        assert_eq!(
            journal_since(&reader, start)
                .into_iter()
                .map(|(op, _)| op)
                .collect::<Vec<_>>(),
            vec![
                "agent.set_namespace",
                "agent.set_namespace",
                "agent.set_namespace",
                "project.shared",
                "agent.set_namespace",
                "agent.set_namespace",
            ]
        );
        let served = reader.store.agent_changes(start, None).unwrap();
        assert_eq!(served.entries.len(), 5);
        assert!(agent_mismatches(&reader).is_empty());
        clean_rebuild(&reader);

        // A forged carry that moves another agent's claim is refused, and so
        // is a project entry carrying any agent change but `agent.set_namespace`.
        let mut forged: ProjectEntry = serde_json::from_slice(&carried[0].1).unwrap();
        let grace_claim = reader.store.agent_claims(GRACE).unwrap().remove(0);
        forged.agents[0].claims[0] = grace_claim;
        let mut relabeled: ProjectEntry = serde_json::from_slice(&carried[0].1).unwrap();
        relabeled.agents[0].op = "agent.rename".into();
        let head = generation(&reader);
        for (image, expected) in [
            (forged, "agent entry changes another agent's claim"),
            (relabeled, "unsupported project entry op"),
        ] {
            let error = reader
                .store
                .apply_remote_entry(
                    &crate::remote_apply::RemoteEntry {
                        position: 3,
                        entry_id: "forged".into(),
                        author: "01".repeat(16),
                        signer: "writer".into(),
                        key_id: "key".into(),
                        envelope_version: crate::remote_apply::ENVELOPE_VERSION,
                        kind: "change".into(),
                        entry: crate::shared_entry::canonical_bytes(&image),
                    },
                    None,
                )
                .unwrap_err()
                .to_string();
            assert!(error.contains(expected), "{error}");
            assert_eq!(generation(&reader), head);
        }
    }
}
