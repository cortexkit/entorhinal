//! Complete identity snapshots and journal-ordered after-images. Poll waiting
//! and incarnation checks belong to the serving process, not the stored data.

use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    change_ops_sql, claims::load_claims, reads::all_rows, AgentChangeEntry, AgentMutationError,
    AgentNameClaim, AgentRow,
};
use crate::RegistryStore;

/// The serving layer adds incarnation to this single, unpaged snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSnapshotReply {
    pub generation: i64,
    pub agents: Vec<AgentRow>,
    pub claims: Vec<AgentNameClaim>,
}

/// A full page advances only to its last identity entry. A partial page advances
/// to generation, including trailing project operations and the cutover marker.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentChangesReply {
    pub generation: i64,
    pub cursor: i64,
    pub entries: Vec<AgentChangeEntry>,
}

fn all_claims(conn: &Connection) -> rusqlite::Result<Vec<AgentNameClaim>> {
    let owners = conn
        .prepare("SELECT DISTINCT agent_id FROM agent_name_claim ORDER BY agent_id")?
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut claims = Vec::new();
    for owner in owners {
        claims.extend(load_claims(conn, &owner)?);
    }
    claims.sort_by_key(|claim| claim.claim_id);
    Ok(claims)
}

impl RegistryStore {
    /// Read all rows, all claim history and the journal head in one transaction.
    /// This head is a safe first cursor even if a writer commits during transfer.
    pub fn agent_snapshot(&self) -> Result<AgentSnapshotReply, AgentMutationError> {
        self.read_agent(|conn| {
            Ok(AgentSnapshotReply {
                generation: self.generation_from_connection(conn)?,
                agents: all_rows(conn)?,
                claims: all_claims(conn)?,
            })
        })
    }

    /// Scan the identity feed without waiting. The handler must check the
    /// requested incarnation before calling, and may wait only after an empty
    /// scan whose cursor has not advanced. A scan over project-only entries is
    /// immediately ready, even though its entries are empty.
    pub fn agent_changes(
        &self,
        cursor: i64,
        limit: Option<i64>,
    ) -> Result<AgentChangesReply, AgentMutationError> {
        let limit = limit.unwrap_or(500);
        if !(1..=1000).contains(&limit) {
            return Err(AgentMutationError::new(
                "invalid_request",
                "limit must be 1..=1000",
            ));
        }
        self.read_agent(|conn| {
            let generation = self.generation_from_connection(conn)?;
            if cursor > generation {
                return Err(AgentMutationError::new(
                    "snapshot_required",
                    "cursor is above the journal head",
                ));
            }
            let mut statement = conn.prepare(&format!(
                "SELECT seq,op,payload_json FROM registry_journal WHERE seq>?1 AND op IN ({})
                  ORDER BY seq LIMIT ?2",
                change_ops_sql()
            ))?;
            let entries = statement
                .query_map(rusqlite::params![cursor, limit], |r| {
                    let seq: i64 = r.get(0)?;
                    let op: String = r.get(1)?;
                    let payload: String = r.get(2)?;
                    let decode_error = |error: serde_json::Error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            2,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    };
                    let payload: Value = serde_json::from_str(&payload).map_err(decode_error)?;
                    let mut entry: AgentChangeEntry =
                        serde_json::from_value(payload.get("entry").cloned().unwrap_or(payload))
                            .map_err(decode_error)?;
                    // The journal owns ordering; a payload must not redirect a cursor.
                    entry.seq = seq;
                    entry.op = op;
                    Ok(entry)
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let cursor = if entries.len() == limit as usize {
                entries.last().expect("positive full page").seq
            } else {
                generation
            };
            Ok(AgentChangesReply {
                generation,
                cursor,
                entries,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::reads::tests::{call, create, fixture, import_core, placed};
    use super::*;
    use rusqlite::{params_from_iter, types::Value as SqlValue};
    use serde_json::json;
    use std::collections::BTreeMap;

    fn keys(value: &Value, expected: &[&str]) {
        let actual = value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(actual, expected.iter().copied().collect());
    }

    #[test]
    fn snapshot_and_every_change_op_pin_exact_row_claim_entry_key_sets() {
        let f = fixture(false);
        import_core(&f,"INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms,terminal_reason,terminal_at_ms) VALUES('agent_16013c86','Deleted','old','assistant',1,2,'deleted',3);");
        let a = create(&f, "First", "assistant", None, None);
        let b = create(&f, "Second", "assistant", None, None);
        placed(&f, "P", "W");
        create(&f, "Hire", "hiree", Some("P"), None);
        for (op, body) in [
            ("agent.rename", json!({"agent_id":a,"name":"Renamed"})),
            ("agent.update_tag", json!({"agent_id":a,"tag":"changed"})),
            (
                "agent.set_labels",
                json!({"agent_id":a,"labels":["one","two"]}),
            ),
            (
                "agent.set_avatar",
                json!({"agentId":a,"genome":"a".repeat(2048),"type":"creature.classic"}),
            ),
            (
                "agent.set_github_identity",
                json!({"agent_id":a,"github_identity":{"kind":"user_token","login":"login","credential_ref":"cred"}}),
            ),
            ("agent.dispose", json!({"agent_id":a})),
            (
                "agent.merge",
                json!({"agent_id":b,"into_agent_id":create(&f,"Target","assistant",None,None)}),
            ),
        ] {
            let mut body = body;
            body["request_key"] = json!(op);
            call(&f, op, body);
        }
        let snapshot = serde_json::to_value(f.store.agent_snapshot().unwrap()).unwrap();
        keys(&snapshot, &["generation", "agents", "claims"]);
        assert_eq!(snapshot["agents"].as_array().unwrap().len(), 5);
        assert_eq!(snapshot["claims"].as_array().unwrap().len(), 5);
        assert!(snapshot["agents"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["role"] == "hiree"));
        let feed = serde_json::to_value(f.store.agent_changes(0, None).unwrap()).unwrap();
        keys(&feed, &["generation", "cursor", "entries"]);
        let rows = snapshot["agents"].as_array().unwrap().iter().chain(
            feed["entries"]
                .as_array()
                .unwrap()
                .iter()
                .map(|e| &e["row"]),
        );
        for row in rows {
            keys(
                row,
                &[
                    "agent_id",
                    "name",
                    "name_version",
                    "name_normalization_version",
                    "tag",
                    "labels",
                    "role",
                    "project_id",
                    "workspace_id",
                    "avatar",
                    "github_identity",
                    "status",
                    "merged_into",
                    "supervisor_agent_id",
                    "request_key",
                    "created_at_ms",
                    "updated_at_ms",
                    "terminal_at_ms",
                    "agent_generation",
                ],
            );
            if !row["avatar"].is_null() {
                keys(&row["avatar"], &["genome", "type", "version"]);
                assert!(row["avatar"]["type"].is_string());
                assert!(row["avatar"]["version"].is_null());
            }
        }
        for claim in snapshot["claims"].as_array().unwrap().iter().chain(
            feed["entries"]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|e| e["claims"].as_array().unwrap()),
        ) {
            keys(
                claim,
                &[
                    "claim_id",
                    "agent_id",
                    "namespace_kind",
                    "namespace_key",
                    "normalized_name",
                    "name_normalization_version",
                    "display_name",
                    "claimed_at_ms",
                    "released_at_ms",
                ],
            );
        }
        let mut ops = std::collections::BTreeSet::new();
        for entry in feed["entries"].as_array().unwrap() {
            keys(
                entry,
                &[
                    "seq",
                    "op",
                    "agent_id",
                    "agent_generation",
                    "row",
                    "claims",
                    "old_display_name",
                    "new_display_name",
                    "status",
                    "merged_into",
                ],
            );
            ops.insert(entry["op"].as_str().unwrap());
            match entry["op"].as_str().unwrap() {
                "agent.rename" => {
                    assert_eq!(entry["old_display_name"], "First");
                    assert_eq!(entry["new_display_name"], "Renamed");
                }
                "agent.dispose" => {
                    assert_eq!(entry["status"], "retired");
                    assert!(entry["merged_into"].is_null());
                }
                "agent.merge" => {
                    assert_eq!(entry["status"], "merged");
                    assert!(entry["merged_into"].is_string());
                }
                _ => {
                    assert!(entry["old_display_name"].is_null());
                    assert!(entry["new_display_name"].is_null());
                    assert!(entry["status"].is_null());
                    assert!(entry["merged_into"].is_null());
                }
            }
        }
        assert_eq!(
            ops,
            [
                "agent.create",
                "agent.rename",
                "agent.update_tag",
                "agent.set_labels",
                "agent.set_avatar",
                "agent.set_github_identity",
                "agent.dispose",
                "agent.merge",
                "agent.import"
            ]
            .into_iter()
            .collect()
        );
    }

    #[test]
    fn changes_full_page_cursor_never_skips_identity_and_partial_pages_pass_nonidentity() {
        let f = fixture(true);
        let a = create(&f, "A", "assistant", None, None);
        f.store
            .apply_entry("approve_root", "{}", "test", None, |_| Ok(()))
            .unwrap();
        let b = create(&f, "B", "assistant", None, None);
        f.store
            .apply_entry("agent.cutover", "{}", "test", None, |_| Ok(()))
            .unwrap();
        let first = f.store.agent_changes(0, Some(1)).unwrap();
        assert_eq!(first.entries.len(), 1);
        assert_eq!(first.entries[0].agent_id, a);
        assert_eq!(first.cursor, 2);
        assert_eq!(first.generation, 5);
        let second = f.store.agent_changes(first.cursor, Some(1)).unwrap();
        assert_eq!(second.entries.len(), 1);
        assert_eq!(second.entries[0].agent_id, b);
        assert_eq!(second.cursor, 4);
        let trailing = f.store.agent_changes(second.cursor, Some(1)).unwrap();
        assert!(trailing.entries.is_empty());
        assert_eq!(trailing.cursor, 5);
        let partial = f.store.agent_changes(0, Some(3)).unwrap();
        assert_eq!(
            partial.entries.iter().map(|e| e.seq).collect::<Vec<_>>(),
            vec![2, 4]
        );
        assert_eq!(partial.cursor, 5);
        assert!(f.store.agent_changes(5, None).unwrap().entries.is_empty());
        assert_eq!(
            f.store.agent_changes(6, None).unwrap_err().code,
            "snapshot_required"
        );
        for limit in [-1, 0, 1001] {
            assert_eq!(
                f.store.agent_changes(0, Some(limit)).unwrap_err().code,
                "invalid_request"
            );
        }
        assert_eq!(
            f.store.agent_changes(0, Some(1000)).unwrap().entries.len(),
            2
        );
    }

    #[test]
    fn changes_omitted_limit_is_500_and_default_page_is_not_truncated_or_skipped() {
        let f = fixture(true);
        let a = create(&f, "A", "assistant", None, None);
        for n in 0..500 {
            call(
                &f,
                "agent.update_tag",
                json!({"agent_id":a,"tag":format!("tag-{n}"),"request_key":format!("tag-{n}")}),
            );
        }
        let first = f.store.agent_changes(0, None).unwrap();
        assert_eq!(first.entries.len(), 500);
        assert_eq!(first.cursor, 501);
        let last = f.store.agent_changes(first.cursor, None).unwrap();
        assert_eq!(last.entries.len(), 1);
        assert_eq!(last.entries[0].seq, 502);
        assert_eq!(last.cursor, 502);
    }

    #[test]
    fn journal_tail_returns_only_project_rows_and_advance_rule_passes_marker_and_agents() {
        let f = fixture(false);
        f.store
            .apply_entry("register", "{}", "test", None, |_| Ok(()))
            .unwrap();
        f.store
            .apply_entry("agent.cutover", "{}", "test", None, |_| Ok(()))
            .unwrap();
        f.store
            .apply_entry("approve_root", "{}", "test", None, |_| Ok(()))
            .unwrap();
        create(&f, "A", "assistant", None, None);
        let mut cursor = 0;
        let mut consumed = Vec::new();
        loop {
            let page = f.store.journal_tail(cursor, 1).unwrap();
            consumed.extend(page.entries.iter().map(|e| (e.seq, e.op.clone())));
            cursor = if page.entries.len() == 1 {
                page.entries[0].seq
            } else {
                page.generation
            };
            if cursor == page.generation {
                break;
            }
        }
        assert_eq!(
            consumed,
            vec![(1, "register".into()), (3, "approve_root".into())]
        );
        assert_eq!(cursor, 4);
        assert_eq!(f.store.journal_tail(0, 10).unwrap().entries.len(), 2);
    }

    fn dump(conn: &Connection, table: &str, order: &str) -> Vec<Vec<SqlValue>> {
        let mut statement = conn
            .prepare(&format!("SELECT * FROM {table} ORDER BY {order}"))
            .unwrap();
        let columns = statement.column_count();
        statement
            .query_map([], |r| (0..columns).map(|i| r.get(i)).collect())
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    }

    fn sql(value: &Value) -> SqlValue {
        match value {
            Value::Null => SqlValue::Null,
            Value::String(s) => SqlValue::Text(s.clone()),
            Value::Number(n) => SqlValue::Integer(n.as_i64().unwrap()),
            _ => SqlValue::Text(value.to_string()),
        }
    }

    fn wire(reply: impl Serialize) -> Value {
        serde_json::from_slice(&serde_json::to_vec(&reply).unwrap()).unwrap()
    }

    #[test]
    fn snapshot_generation_and_rows_share_a_transaction_during_concurrent_commits() {
        let f = fixture(true);
        let a = create(&f, "A", "assistant", None, None);
        let next = f.store.generation().unwrap() + 1;
        call(
            &f,
            "agent.update_tag",
            json!({"agent_id":a,"tag":next.to_string(),"request_key":"initial-tag"}),
        );
        let start = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            let start = &start;
            let path = f.root.join("store.db");
            let id = a.clone();
            scope.spawn(move || {
                let mut writer = Connection::open(path).unwrap();
                writer.busy_timeout(std::time::Duration::from_secs(5)).unwrap();
                start.wait();
                for _ in 0..128 {
                    let tx = writer.transaction().unwrap();
                    let mut row = super::super::store::load_row(&tx,&id).unwrap().unwrap();
                    tx.execute("INSERT INTO registry_journal(op,payload_json,actor,created_at,principal) VALUES('agent.update_tag','{}','test',100,'entorhinal')",[]).unwrap();
                    let seq = tx.last_insert_rowid();
                    row.tag = seq.to_string(); row.agent_generation += 1;
                    super::super::store::write_row(&tx,&row).unwrap();
                    let mut entry = AgentChangeEntry::new("agent.update_tag",row,vec![]); entry.seq = seq;
                    tx.execute("UPDATE registry_journal SET payload_json=?1 WHERE seq=?2",rusqlite::params![json!({"entry":entry}).to_string(),seq]).unwrap();
                    tx.commit().unwrap();
                    std::thread::yield_now();
                }
            });
            start.wait();
            for _ in 0..128 {
                let snapshot = f.store.agent_snapshot().unwrap();
                assert_eq!(snapshot.agents[0].tag, snapshot.generation.to_string());
                std::thread::yield_now();
            }
        });
        let final_snapshot = f.store.agent_snapshot().unwrap();
        assert_eq!(final_snapshot.generation, next + 128);
        assert_eq!(
            final_snapshot.agents[0].tag,
            final_snapshot.generation.to_string()
        );
    }

    // A consumer uses only decoded wire objects, not production row/claim
    // loaders or replay helpers. Compare its SQLite tables byte-for-byte with
    // the authority's tables to catch missing fields and lost historical claims.
    #[test]
    fn serialized_snapshot_and_concurrent_paged_changes_reproduce_both_tables_with_history() {
        let f = fixture(true);
        let a = create(&f, "Before", "assistant", None, None);
        call(
            &f,
            "agent.rename",
            json!({"agent_id":a,"name":"AtSnapshot","request_key":"old-history"}),
        );
        let snapshot = wire(f.store.agent_snapshot().unwrap());
        let mut rows = snapshot["agents"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| (r["agent_id"].as_str().unwrap().to_owned(), r.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut claims = snapshot["claims"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| (c["claim_id"].as_i64().unwrap(), c.clone()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(claims.len(), 2);
        assert!(claims[&1]["released_at_ms"].is_number());
        let mut cursor = snapshot["generation"].as_i64().unwrap();
        std::thread::scope(|scope| {
            let (ready_tx, ready_rx) = std::sync::mpsc::channel();
            let (go_tx, go_rx) = std::sync::mpsc::channel();
            let f = &f;
            let a = &a;
            scope.spawn(move || {
                for step in 0..4 {
                    go_rx.recv().unwrap();
                    match step {
                        0 => { call(f,"agent.rename",json!({"agent_id":a,"name":"AfterSnapshot","request_key":"new-history"})); create(f,"Concurrent","assistant",None,None); }
                        1 => { call(f,"agent.set_labels",json!({"agent_id":a,"labels":["new"],"request_key":"labels"})); }
                        2 => { call(f,"agent.set_avatar",json!({"agentId":a,"genome":"b".repeat(2048),"type":"creature.classic","version":2,"request_key":"avatar"})); }
                        _ => { call(f,"agent.dispose",json!({"agent_id":a,"request_key":"dispose"})); f.store.apply_entry("approve_root","{}","test",None, |_| Ok(())).unwrap(); }
                    }
                    ready_tx.send(()).unwrap();
                }
            });
            for _ in 0..4 {
                go_tx.send(()).unwrap();
                ready_rx.recv().unwrap();
                let page = wire(f.store.agent_changes(cursor, Some(1)).unwrap());
                apply_wire_page(&page, &mut rows, &mut claims);
                cursor = page["cursor"].as_i64().unwrap();
            }
        });
        while cursor < f.store.generation().unwrap() {
            let page = wire(f.store.agent_changes(cursor, Some(1)).unwrap());
            apply_wire_page(&page, &mut rows, &mut claims);
            cursor = page["cursor"].as_i64().unwrap();
        }
        let replica = Connection::open_in_memory().unwrap();
        replica
            .execute_batch(super::super::schema::V4_AGENT_IDENTITY)
            .unwrap();
        for row in rows.values() {
            let status = if row["status"] == "live" {
                Value::Null
            } else {
                row["status"].clone()
            };
            let values = [
                row["agent_id"].clone(),
                row["name"].clone(),
                row["name_version"].clone(),
                row["name_normalization_version"].clone(),
                row["tag"].clone(),
                Value::String(row["labels"].to_string()),
                row["role"].clone(),
                row["project_id"].clone(),
                row["workspace_id"].clone(),
                row["avatar"]["genome"].clone(),
                row["avatar"]["type"].clone(),
                row["avatar"]["version"].clone(),
                if row["github_identity"].is_null() {
                    Value::Null
                } else {
                    Value::String(row["github_identity"].to_string())
                },
                status,
                row["terminal_at_ms"].clone(),
                row["merged_into"].clone(),
                row["supervisor_agent_id"].clone(),
                row["request_key"].clone(),
                row["created_at_ms"].clone(),
                row["updated_at_ms"].clone(),
                row["agent_generation"].clone(),
            ];
            replica.execute("INSERT INTO agent VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21)",params_from_iter(values.iter().map(sql))).unwrap();
        }
        for claim in claims.values() {
            let values = [
                "claim_id",
                "agent_id",
                "namespace_kind",
                "namespace_key",
                "name_normalization_version",
                "normalized_name",
                "display_name",
                "claimed_at_ms",
                "released_at_ms",
            ]
            .map(|k| sql(&claim[k]));
            replica
                .execute(
                    "INSERT INTO agent_name_claim VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                    params_from_iter(values),
                )
                .unwrap();
        }
        let authority = Connection::open(f.root.join("store.db")).unwrap();
        assert_eq!(
            dump(&replica, "agent", "agent_id"),
            dump(&authority, "agent", "agent_id")
        );
        assert_eq!(
            dump(&replica, "agent_name_claim", "claim_id"),
            dump(&authority, "agent_name_claim", "claim_id")
        );
        assert_eq!(claims.len(), 4);
    }

    fn apply_wire_page(
        page: &Value,
        rows: &mut BTreeMap<String, Value>,
        claims: &mut BTreeMap<i64, Value>,
    ) {
        for entry in page["entries"].as_array().unwrap() {
            rows.insert(
                entry["agent_id"].as_str().unwrap().into(),
                entry["row"].clone(),
            );
            for claim in entry["claims"].as_array().unwrap() {
                claims.insert(claim["claim_id"].as_i64().unwrap(), claim.clone());
            }
        }
    }
}
