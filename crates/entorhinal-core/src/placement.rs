//! Shared placement history. Journal payloads retain exact rows, never a recipe
//! that would recalculate timestamps or ids during replay.
use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{params, Connection, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    mutations::{append, domain, Action},
    JournalWriter, RegistryError, RegistryStore,
};

pub(crate) const V21_PLACEMENT: &str = r#"
CREATE TABLE placement_fact (
    fact_id TEXT PRIMARY KEY,
    pos INTEGER NOT NULL UNIQUE,
    project_id TEXT NOT NULL,
    workspace_id TEXT NULL,
    effective_from_ms INTEGER NOT NULL,
    recorded_at_ms INTEGER NOT NULL,
    supersedes TEXT NULL
);
CREATE TABLE placement_state (
    id INTEGER PRIMARY KEY CHECK(id=1),
    backfilled_at_ms INTEGER NOT NULL,
    author TEXT NOT NULL
);
"#;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlacementFact {
    pub fact_id: String,
    pub supersedes: Option<String>,
    pub project_id: String,
    pub workspace_id: Option<String>,
    pub effective_from_ms: i64,
    pub recorded_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoredFact {
    pub pos: i64,
    #[serde(flatten)]
    pub fact: PlacementFact,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackfillRequest {
    #[serde(default, rename = "requestKey")]
    pub request_key: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadRequest {
    pub after: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Serialize)]
pub struct PlacementPage {
    pub facts: Vec<PlacementFact>,
    pub next: Option<String>,
    pub through: i64,
}

pub(crate) type Placements = BTreeMap<String, Option<String>>;

pub(crate) fn placements(conn: &Connection) -> rusqlite::Result<Placements> {
    conn.prepare("SELECT p.project_id,pw.workspace_id FROM project p LEFT JOIN project_workspace pw USING(project_id) ORDER BY p.project_id")?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?.collect()
}

pub(crate) fn active(conn: &Connection) -> rusqlite::Result<bool> {
    conn.query_row("SELECT EXISTS(SELECT 1 FROM placement_state)", [], |r| {
        r.get(0)
    })
}

fn changes(before: &Placements, after: &Placements) -> Vec<(String, Option<String>)> {
    before
        .keys()
        .chain(after.keys())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|id| before.get(*id) != after.get(*id))
        .map(|id| (id.clone(), after.get(id).cloned().flatten()))
        .collect()
}

fn insert_fact(
    tx: &Transaction<'_>,
    id: String,
    project: String,
    workspace: Option<String>,
    now: i64,
) -> rusqlite::Result<StoredFact> {
    let previous: Option<i64> = tx.query_row(
        "SELECT MAX(effective_from_ms) FROM placement_fact WHERE project_id=?1",
        [&project],
        |r| r.get(0),
    )?;
    let pos = tx.query_row(
        "SELECT COALESCE(MAX(pos),0)+1 FROM placement_fact",
        [],
        |r| r.get(0),
    )?;
    let row = StoredFact {
        pos,
        fact: PlacementFact {
            fact_id: id,
            supersedes: None,
            project_id: project,
            workspace_id: workspace,
            effective_from_ms: previous.map_or(now, |prev| prev.max(now)),
            recorded_at_ms: now,
        },
    };
    restore_fact(tx, &row)?;
    Ok(row)
}

fn restore_fact(tx: &Transaction<'_>, row: &StoredFact) -> rusqlite::Result<()> {
    let f = &row.fact;
    tx.execute("INSERT INTO placement_fact(fact_id,pos,project_id,workspace_id,effective_from_ms,recorded_at_ms,supersedes) VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(fact_id) DO UPDATE SET pos=excluded.pos,project_id=excluded.project_id,workspace_id=excluded.workspace_id,effective_from_ms=excluded.effective_from_ms,recorded_at_ms=excluded.recorded_at_ms,supersedes=excluded.supersedes", params![f.fact_id,row.pos,f.project_id,f.workspace_id,f.effective_from_ms,f.recorded_at_ms,f.supersedes])?;
    Ok(())
}

pub(crate) fn record_live(
    tx: &Transaction<'_>,
    before: &Placements,
    action: &mut Action,
) -> Result<(), RegistryError> {
    let Some(seq) = action.seq else {
        return Ok(());
    };
    let changed = changes(before, &placements(tx)?);
    if changed.is_empty() {
        return Ok(());
    }
    let entry = crate::mutations::pending::entry_id().ok_or_else(|| {
        domain(
            "placement_shared_attempt_required",
            "placement writes require a shared entry id",
        )
    })?;
    let now = tx.query_row(
        "SELECT created_at FROM registry_journal WHERE seq=?1",
        [seq],
        |r| r.get(0),
    )?;
    let rows = changed
        .into_iter()
        .map(|(project, workspace)| {
            insert_fact(tx, format!("e:{entry}:{project}"), project, workspace, now)
        })
        .collect::<rusqlite::Result<Vec<_>>>()?;
    action.payload["placement_facts"] = json!(rows);
    Ok(())
}

pub(crate) fn replay_facts(tx: &Transaction<'_>, payload: &Value) -> rusqlite::Result<()> {
    if let Some(rows) = payload.get("placement_facts") {
        let rows: Vec<StoredFact> =
            serde_json::from_value(rows.clone()).map_err(crate::shared_entry::decode_error)?;
        for row in rows {
            restore_fact(tx, &row)?;
        }
    }
    if let Some(state) = payload.get("placement_state") {
        tx.execute(
            "INSERT INTO placement_state(id,backfilled_at_ms,author) VALUES(1,?1,?2)",
            params![state["backfilled_at_ms"].as_i64(), state["author"].as_str()],
        )?;
    }
    Ok(())
}

impl RegistryStore {
    pub fn placement_read(
        &self,
        request: ReadRequest,
        incarnation: &str,
    ) -> Result<PlacementPage, RegistryError> {
        let limit = request.limit.unwrap_or(256);
        if !(1..=1024).contains(&limit) {
            return Err(domain(
                "invalid_request",
                "limit must be between 1 and 1024",
            ));
        }
        let after = match request.after {
            None => 0,
            Some(cursor) => {
                let bytes = cursor.as_bytes();
                let decoded = if bytes.len() % 2 == 0 {
                    bytes
                        .as_chunks::<2>()
                        .0
                        .iter()
                        .map(|pair| {
                            let hex = std::str::from_utf8(pair).ok()?;
                            u8::from_str_radix(hex, 16).ok()
                        })
                        .collect::<Option<Vec<_>>>()
                } else {
                    None
                };
                let (served_by, pos): (String, i64) = decoded
                    .and_then(|b| serde_json::from_slice(&b).ok())
                    .ok_or_else(|| domain("invalid_request", "invalid placement cursor"))?;
                if pos < 0 {
                    return Err(domain("invalid_request", "invalid placement cursor"));
                }
                if served_by == incarnation {
                    pos
                } else {
                    0
                }
            }
        };
        self.read(|conn| {
            let mut rows = conn.prepare("SELECT pos,fact_id,supersedes,project_id,workspace_id,effective_from_ms,recorded_at_ms FROM placement_fact WHERE pos>?1 ORDER BY pos LIMIT ?2")?
                .query_map(params![after,limit as i64+1], |r| Ok((r.get::<_,i64>(0)?, PlacementFact { fact_id:r.get(1)?,supersedes:r.get(2)?,project_id:r.get(3)?,workspace_id:r.get(4)?,effective_from_ms:r.get(5)?,recorded_at_ms:r.get(6)? })))?.collect::<rusqlite::Result<Vec<_>>>()?;
            let more = rows.len() > limit;
            rows.truncate(limit);
            let through = rows.last().map_or(after, |r| r.0);
            let next = more.then(|| serde_json::to_vec(&(incarnation,through)).unwrap().iter().map(|b| format!("{b:02x}")).collect());
            Ok(PlacementPage { facts: rows.into_iter().map(|r| r.1).collect(), next, through })
        })
    }
}

fn backfill_rows(tx: &Transaction<'_>, author: &str) -> Result<Vec<StoredFact>, RegistryError> {
    let copy = crate::read_connection::copy_snapshot(tx)?;
    let replay = copy.unchecked_transaction()?;
    let mut facts = Vec::new();
    crate::mutations::replay_with_observer(
        &replay,
        false,
        &mut |seq, op, origin, time, before, after| {
            // A snapshot seeds replay state; it is not the history it replaced.
            if op == "shared.snapshot" {
                return Ok(());
            }
            let changed = changes(before, after);
            if changed.is_empty() {
                return Ok(());
            }
            let now = time.filter(|n| origin != "log" || *n != 0).ok_or_else(|| {
                domain(
                    "placement_history_time_unknown",
                    format!("placement history time is unreadable at seq {seq}"),
                )
            })?;
            for (project, workspace) in changed {
                facts.push(insert_fact(
                    tx,
                    format!("b:{author}:{seq}:{project}"),
                    project,
                    workspace,
                    now,
                )?);
            }
            Ok(())
        },
    )?;
    Ok(facts)
}

impl JournalWriter<'_> {
    pub fn placement_backfill(
        &self,
        req: BackfillRequest,
        now: i64,
    ) -> Result<Vec<u8>, RegistryError> {
        let key = req.request_key.clone();
        self.mutation("placement.backfill", key.as_deref(), |tx| {
            validate_backfill(tx)?;
            if self.principal == "direct" {
                let summary = backfill_summary(tx)?;
                if !matches!(&self.confirmation, crate::agent::PlacementConfirmation::Approved(approval) if approval.approves(&summary)) {
                    return Err(domain("operator_approval_stale", "placement history changed after confirmation"));
                }
            }
            let author: String = tx.query_row("SELECT self_author FROM self_identity WHERE id=1", [], |r| r.get(0))?;
            let facts = backfill_rows(tx, &author)?;
            let payload = json!({"placement_facts":facts,"placement_state":{"backfilled_at_ms":now,"author":author}});
            let seq = append(tx,"placement.backfill", &payload,"operator",key.as_deref(),now,self.principal)?;
            tx.execute("INSERT INTO placement_state VALUES(1,?1,?2)",params![now,author])?;
            Ok(Action {changed:true,seq:Some(seq),value:json!({"facts":facts.len()}),payload})
        })
    }
}

fn validate_backfill(conn: &Connection) -> Result<(), RegistryError> {
    let enabled: bool = conn.query_row(
        "SELECT state='enabled' FROM identity_log_state WHERE id=1",
        [],
        |r| r.get(0),
    )?;
    if !enabled {
        return Err(domain(
            "sharing_not_enabled",
            "placement backfill requires sharing to be enabled",
        ));
    }
    if active(conn)? {
        return Err(domain(
            "placement_already_backfilled",
            "placement history is already backfilled",
        ));
    }
    let exists: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM placement_fact)", [], |r| {
        r.get(0)
    })?;
    if exists {
        return Err(domain(
            "placement_facts_exist",
            "placement facts already exist",
        ));
    }
    Ok(())
}

impl RegistryStore {
    /// Validate on a disposable read snapshot before asking for operator presence.
    pub fn placement_backfill_precheck(&self) -> Result<String, RegistryError> {
        let copy = self.read(crate::read_connection::copy_snapshot)?;
        let tx = copy.unchecked_transaction()?;
        validate_backfill(&tx)?;
        backfill_rows(&tx, "precheck")?;
        backfill_summary(&tx)
    }
}

fn backfill_summary(conn: &Connection) -> Result<String, RegistryError> {
    let generation: i64 = conn.query_row(
        "SELECT COALESCE(MAX(seq),0) FROM registry_journal",
        [],
        |r| r.get(0),
    )?;
    Ok(format!(
        "Share this machine's project placement history through journal generation {generation}?"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutations::tests::{code, Fixture};
    use crate::{AssignWorkspaceRequest, RegisterRequest, RemoveRequest, SharedSettlement};

    fn enable(f: &Fixture) {
        f.store
            .apply_entry("identity_log.enable", "{}", "operator", None, |tx| {
                tx.execute(
                    "UPDATE identity_log_state SET state='enabled' WHERE id=1",
                    [],
                )?;
                tx.execute(
                    "INSERT INTO self_identity VALUES(1,?1,1)",
                    ["ab".repeat(16)],
                )?;
                Ok(())
            })
            .unwrap();
    }
    fn attempt(f: &Fixture, id: &str, run: impl FnOnce() -> Result<Vec<u8>, RegistryError>) {
        let head = f.store.identity_log_status().unwrap().last_applied_position;
        f.store
            .shared_attempt(
                id.into(),
                head,
                move |_| SharedSettlement::Commit(head + 1),
                run,
            )
            .unwrap();
    }
    fn facts(f: &Fixture) -> Vec<PlacementFact> {
        f.store
            .placement_read(ReadRequest::default(), "test")
            .unwrap()
            .facts
    }
    fn backfill(f: &Fixture) {
        attempt(f, "backfill", || {
            f.store
                .with_principal("test")
                .placement_backfill(BackfillRequest::default(), 10)
        });
    }

    #[test]
    fn placement_facts_empty_activation_live_null_noop_removals_and_exact_replay() {
        let f = Fixture::new("placement-live");
        enable(&f);
        backfill(&f);
        assert!(facts(&f).is_empty());
        for (id, workspace) in [("P", None), ("Q", Some("W"))] {
            attempt(&f, id, || {
                f.store.with_principal("test").register_at(
                    RegisterRequest {
                        project_id: Some(id.into()),
                        name: id.into(),
                        workspace_id: workspace.map(str::to_owned),
                        ..Default::default()
                    },
                    20,
                )
            });
        }
        assert_eq!(
            facts(&f)[0],
            PlacementFact {
                fact_id: "e:P:P".into(),
                supersedes: None,
                project_id: "P".into(),
                workspace_id: None,
                effective_from_ms: 20,
                recorded_at_ms: 20
            }
        );
        let req = AssignWorkspaceRequest {
            project_id: "P".into(),
            workspace_id: "W".into(),
            ..Default::default()
        };
        attempt(&f, "assign", || {
            f.store
                .with_principal("test")
                .assign_workspace_at(req.clone(), 5)
        });
        attempt(&f, "noop", || {
            f.store.with_principal("test").assign_workspace_at(req, 25)
        });
        attempt(&f, "rename", || {
            f.store.with_principal("test").register_at(
                RegisterRequest {
                    project_id: Some("P".into()),
                    name: "Renamed".into(),
                    ..Default::default()
                },
                30,
            )
        });
        assert_eq!(facts(&f).len(), 3);
        assert_eq!(facts(&f)[2].effective_from_ms, 20);
        assert_eq!(facts(&f)[2].recorded_at_ms, 5);
        attempt(&f, "remove-workspace", || {
            f.store.with_principal("test").remove_at(
                RemoveRequest {
                    project_id: None,
                    workspace_id: Some("W".into()),
                    successor_project_id: None,
                    request_key: None,
                    actor: None,
                },
                40,
            )
        });
        assert_eq!(facts(&f).len(), 5);
        assert!(facts(&f)[3..].iter().all(|f| f.workspace_id.is_none()));
        attempt(&f, "remove-project", || {
            f.store.with_principal("test").remove_at(
                RemoveRequest {
                    project_id: Some("P".into()),
                    workspace_id: None,
                    successor_project_id: Some("Q".into()),
                    request_key: None,
                    actor: None,
                },
                50,
            )
        });
        let original = facts(&f);
        assert_eq!(original.len(), 6);
        assert!(f.store.verify().unwrap().replay.ok);
        f.store
            .db
            .with_conn_fenced(|tx| {
                tx.execute("DELETE FROM placement_fact", [])?;
                Ok(())
            })
            .unwrap();
        assert!(!f.store.verify().unwrap().replay.ok);
        f.store.rebuild().unwrap();
        assert_eq!(facts(&f), original);
        assert!(f.store.verify().unwrap().replay.ok);
    }

    #[test]
    fn placement_facts_backfill_replays_resolved_ids_noops_and_workspace_removal() {
        let f = Fixture::new("placement-history");
        let response = f
            .store
            .with_principal("test")
            .register_at(
                RegisterRequest {
                    name: "Minted".into(),
                    workspace_id: Some("W".into()),
                    ..Default::default()
                },
                50,
            )
            .unwrap();
        let id = serde_json::from_slice::<Value>(&response).unwrap()["result"]["projectId"]
            .as_str()
            .unwrap()
            .to_owned();
        f.store
            .with_principal("test")
            .register_at(
                RegisterRequest {
                    project_id: Some(id.clone()),
                    name: "Renamed".into(),
                    ..Default::default()
                },
                60,
            )
            .unwrap();
        f.store
            .assign_workspace(AssignWorkspaceRequest {
                project_id: id.clone(),
                workspace_id: "W".into(),
                ..Default::default()
            })
            .unwrap();
        f.store
            .with_principal("test")
            .assign_workspace_at(
                AssignWorkspaceRequest {
                    project_id: id.clone(),
                    workspace_id: "Z".into(),
                    ..Default::default()
                },
                10,
            )
            .unwrap();
        f.store
            .with_principal("test")
            .remove_at(
                RemoveRequest {
                    project_id: None,
                    workspace_id: Some("Z".into()),
                    successor_project_id: None,
                    request_key: None,
                    actor: None,
                },
                70,
            )
            .unwrap();
        assert!(facts(&f).is_empty());
        enable(&f);
        backfill(&f);
        let rows = facts(&f);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].fact_id, format!("b:{}:1:{id}", "ab".repeat(16)));
        assert_eq!(
            rows.iter()
                .map(|r| r.workspace_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("W"), Some("Z"), None]
        );
        assert_eq!(rows[1].effective_from_ms, 50);
        assert_eq!(rows[1].recorded_at_ms, 10);
        f.store.rebuild().unwrap();
        assert_eq!(facts(&f), rows);
    }

    #[test]
    fn placement_facts_diff_covers_upgrade_and_seed_live_and_backfill() {
        for shared in [false, true] {
            let f = Fixture::new(if shared {
                "placement-more-live"
            } else {
                "placement-more-history"
            });
            if shared {
                enable(&f);
                backfill(&f);
            }
            let root = f.dir("upgrade");
            let upgrade = || {
                f.store.with_principal("test").upgrade_implicit_at(
                    crate::UpgradeImplicitRequest {
                        implicit_id: crate::implicit_project_id(&root),
                        project_id: Some("upgraded".into()),
                        name: "Upgrade".into(),
                        root: Some(root.clone()),
                        workspace_id: Some("W".into()),
                        ..Default::default()
                    },
                    20,
                )
            };
            if shared {
                attempt(&f, "upgrade", upgrade);
            } else {
                upgrade().unwrap();
            }
            let seed_root = f.dir("seed");
            let seed = || {
                f.store.with_principal("test").seed_import_at(
                    crate::SeedImportRequest {
                        source: "mc".into(),
                        exclude_home_scoped: false,
                        payload: crate::SeedPayload {
                            pairs: vec![crate::SeedPair {
                                canonical_root: seed_root.clone(),
                                mc_identity: "seed-project".into(),
                                ..Default::default()
                            }],
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    30,
                )
            };
            if shared {
                attempt(&f, "seed", seed);
            } else {
                seed().unwrap();
                enable(&f);
                backfill(&f);
            }
            let rows = facts(&f);
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].project_id, "upgraded");
            assert_eq!(rows[0].workspace_id.as_deref(), Some("W"));
            assert_eq!(rows[0].recorded_at_ms, 20);
            assert_eq!(rows[1].recorded_at_ms, 30);
            assert!(rows[1].workspace_id.is_none());
            assert!(f.store.verify().unwrap().replay.ok);
            f.store.rebuild().unwrap();
            assert_eq!(facts(&f), rows);
        }
    }

    #[test]
    fn placement_facts_store_adoption_resets_shared_but_preserves_standalone_history() {
        for shared in [false, true] {
            let f = Fixture::new(if shared {
                "placement-adopt-shared"
            } else {
                "placement-adopt-local"
            });
            enable(&f);
            backfill(&f);
            attempt(&f, "register", || {
                f.store.with_principal("test").register_at(
                    RegisterRequest {
                        project_id: Some("P".into()),
                        name: "P".into(),
                        ..Default::default()
                    },
                    20,
                )
            });
            if !shared {
                f.store
                    .db
                    .with_conn_fenced(|tx| {
                        tx.execute("UPDATE identity_log_state SET state='disabled'", [])?;
                        Ok(())
                    })
                    .unwrap();
            }
            f.store.check_store_origin(Some("old-machine"), 30).unwrap();
            f.store.check_store_origin(Some("new-machine"), 31).unwrap();
            f.store.adopt_store("new-machine", false, 32).unwrap();
            assert_eq!(facts(&f).len(), usize::from(!shared));
            assert_eq!(f.store.read(active).unwrap(), !shared);
            assert!(f.store.verify().unwrap().replay.ok);
            f.store.rebuild().unwrap();
            assert_eq!(facts(&f).len(), usize::from(!shared));
        }
    }

    #[test]
    fn placement_facts_read_default_and_maximum_page_boundaries() {
        let f = Fixture::new("placement-pages");
        f.store
            .db
            .with_conn_fenced(|tx| {
                for pos in 1..=1025 {
                    tx.execute(
                        "INSERT INTO placement_fact VALUES(?1,?2,'P',NULL,10,10,NULL)",
                        params![format!("fact-{pos}"), pos],
                    )?;
                }
                Ok(())
            })
            .unwrap();
        let first = f
            .store
            .placement_read(ReadRequest::default(), "reader")
            .unwrap();
        assert_eq!(first.facts.len(), 256);
        assert_eq!(first.through, 256);
        assert!(first.next.is_some());
        let maximum = f
            .store
            .placement_read(
                ReadRequest {
                    limit: Some(1024),
                    after: None,
                },
                "reader",
            )
            .unwrap();
        assert_eq!(maximum.facts.len(), 1024);
        assert_eq!(maximum.through, 1024);
        let last = f
            .store
            .placement_read(
                ReadRequest {
                    limit: Some(1024),
                    after: maximum.next,
                },
                "reader",
            )
            .unwrap();
        assert_eq!(last.facts.len(), 1);
        assert_eq!(last.facts[0].fact_id, "fact-1025");
        assert_eq!(last.through, 1025);
        assert!(last.next.is_none());
    }

    #[test]
    fn placement_facts_backfill_refusals_are_named_and_atomic() {
        let f = Fixture::new("placement-refusals");
        code(
            f.store
                .with_principal("test")
                .placement_backfill(BackfillRequest::default(), 1)
                .unwrap_err(),
            "sharing_not_enabled",
        );
        enable(&f);
        f.store
            .db
            .with_conn_fenced(|tx| {
                tx.execute(
                    "INSERT INTO placement_fact VALUES('already',1,'P',NULL,1,1,NULL)",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        code(
            f.store
                .with_principal("test")
                .placement_backfill(BackfillRequest::default(), 1)
                .unwrap_err(),
            "placement_facts_exist",
        );
        f.store
            .db
            .with_conn_fenced(|tx| {
                tx.execute("DELETE FROM placement_fact", [])?;
                Ok(())
            })
            .unwrap();
        f.store
            .register(RegisterRequest {
                project_id: Some("P".into()),
                name: "P".into(),
                ..Default::default()
            })
            .unwrap();
        f.store
            .db
            .with_conn_fenced(|tx| {
                tx.execute(
                    "UPDATE registry_journal SET created_at='unreadable' WHERE op='register'",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        assert!(f.store.verify().is_err());
        let error = f
            .store
            .with_principal("test")
            .placement_backfill(BackfillRequest::default(), 1)
            .unwrap_err();
        assert!(error.to_string().contains("seq 2"));
        code(error, "placement_history_time_unknown");
        assert!(facts(&f).is_empty());
        f.store.db.with_conn_fenced(|tx| {tx.execute("UPDATE registry_journal SET created_at=0,origin='log',op='project.shared' WHERE op='register'",[])?; Ok(())}).unwrap();
        code(
            f.store
                .with_principal("test")
                .placement_backfill(BackfillRequest::default(), 1)
                .unwrap_err(),
            "placement_history_time_unknown",
        );
    }
}
