//! Deterministic shared after-images. Paths and execution authority never travel.

use std::collections::BTreeMap;

use rusqlite::{params_from_iter, types::ValueRef, Connection, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::agent::AgentChangeEntry;

pub type SharedRow = BTreeMap<String, Value>;

/// The machine-neutral tables a shared entry may change, parents before their
/// dependents. SQL built here takes table and column names only from this list
/// and the live schema, never from a received entry, so a malformed entry
/// can't inject SQL.
pub const PROJECT_SHARED_TABLES: &[&str] = &[
    "workspace",
    "project",
    "project_workspace",
    "project_alias",
    "project_root_key",
];
pub const AGENT_SHARED_TABLES: &[&str] = &["agent", "agent_name_claim"];

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableChanges {
    pub upsert: Vec<SharedRow>,
    pub delete: Vec<SharedRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectEntry {
    pub op: String,
    pub tables: BTreeMap<String, TableChanges>,
    /// Agents in a bootstrap snapshot, each in the same `AgentChangeEntry`
    /// format an agent write records, so one decoder serves both.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub agents: Vec<AgentChangeEntry>,
}

/// Captured inside the write transaction, on both sides of the operation.
/// Agent tables participate in classification even though their wire format is
/// the existing AgentChangeEntry, rather than the generic project format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedState(BTreeMap<String, Vec<SharedRow>>);

fn invalid(message: impl Into<String>) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message.into(),
    )))
}

pub(crate) fn decode_error(error: serde_json::Error) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(error))
}

fn columns(conn: &Connection, table: &str) -> rusqlite::Result<(Vec<String>, Vec<String>)> {
    let info = conn
        .prepare(&format!("PRAGMA table_info({table})"))?
        .query_map([], |r| Ok((r.get::<_, String>(1)?, r.get::<_, i64>(5)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let all = info.iter().map(|(name, _)| name.clone()).collect();
    let mut keys = info
        .into_iter()
        .filter(|(_, pk)| *pk > 0)
        .collect::<Vec<_>>();
    keys.sort_by_key(|(_, pk)| *pk);
    Ok((all, keys.into_iter().map(|(name, _)| name).collect()))
}

fn row_key(row: &SharedRow, keys: &[String]) -> SharedRow {
    keys.iter()
        .map(|key| (key.clone(), row[key].clone()))
        .collect()
}

impl SharedState {
    pub fn capture(conn: &Connection) -> rusqlite::Result<Self> {
        let mut tables = BTreeMap::new();
        for table in PROJECT_SHARED_TABLES.iter().chain(AGENT_SHARED_TABLES) {
            let (names, keys) = columns(conn, table)?;
            let filter = if *table == "project_alias" {
                " WHERE substr(old_id,1,13) <> 'pj-implicit1-'"
            } else {
                ""
            };
            let mut stmt = conn.prepare(&format!(
                "SELECT {} FROM {table}{filter} ORDER BY {}",
                names.join(","),
                keys.join(",")
            ))?;
            let rows = stmt
                .query_map([], |r| {
                    names
                        .iter()
                        .enumerate()
                        .map(|(i, name)| {
                            let value = match r.get_ref(i)? {
                                ValueRef::Null => Value::Null,
                                ValueRef::Integer(n) => Value::from(n),
                                ValueRef::Text(bytes) => Value::from(
                                    std::str::from_utf8(bytes)
                                        .map_err(|e| invalid(e.to_string()))?,
                                ),
                                _ => {
                                    return Err(invalid(format!(
                                        "unsupported shared storage class: {table}.{name}"
                                    )))
                                }
                            };
                            Ok((name.clone(), value))
                        })
                        .collect::<rusqlite::Result<SharedRow>>()
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            tables.insert((*table).into(), rows);
        }
        Ok(Self(tables))
    }

    pub fn project_delta(&self, after: &Self, conn: &Connection) -> rusqlite::Result<ProjectEntry> {
        let mut tables = BTreeMap::new();
        for table in PROJECT_SHARED_TABLES {
            let (_, keys) = columns(conn, table)?;
            let before = self.0[*table]
                .iter()
                .map(|row| (canonical_bytes(&row_key(row, &keys)), row))
                .collect::<BTreeMap<_, _>>();
            let later = after.0[*table]
                .iter()
                .map(|row| (canonical_bytes(&row_key(row, &keys)), row))
                .collect::<BTreeMap<_, _>>();
            let changes = TableChanges {
                upsert: later
                    .iter()
                    .filter(|(key, row)| before.get(*key) != Some(*row))
                    .map(|(_, row)| (*row).clone())
                    .collect(),
                delete: before
                    .iter()
                    .filter(|(key, _)| !later.contains_key(*key))
                    .map(|(_, row)| row_key(row, &keys))
                    .collect(),
            };
            if !changes.upsert.is_empty() || !changes.delete.is_empty() {
                tables.insert((*table).into(), changes);
            }
        }
        Ok(ProjectEntry {
            op: "project.shared".into(),
            tables,
            agents: vec![],
        })
    }

    /// Supply agents from the committed agent snapshot. Snapshot partitioning is
    /// the log coordinator's responsibility; no row is split by this layer.
    pub fn snapshot(&self, mut agents: Vec<AgentChangeEntry>) -> ProjectEntry {
        agents.sort_by(|a, b| a.agent_id.cmp(&b.agent_id));
        ProjectEntry {
            op: "shared.snapshot".into(),
            tables: PROJECT_SHARED_TABLES
                .iter()
                .map(|table| {
                    (
                        (*table).into(),
                        TableChanges {
                            upsert: self.0[*table].clone(),
                            delete: vec![],
                        },
                    )
                })
                .collect(),
            agents,
        }
    }
}

/// Recursively sort object keys, independently of serde_json's map feature set.
pub fn canonical_bytes(value: &impl Serialize) -> Vec<u8> {
    fn sorted(value: Value) -> Value {
        match value {
            Value::Object(map) => Value::Object(
                map.into_iter()
                    .map(|(k, v)| (k, sorted(v)))
                    .collect::<BTreeMap<_, _>>()
                    .into_iter()
                    .collect(),
            ),
            Value::Array(values) => Value::Array(values.into_iter().map(sorted).collect()),
            other => other,
        }
    }
    serde_json::to_vec(&sorted(
        serde_json::to_value(value).expect("shared values serialize"),
    ))
    .expect("shared values encode")
}

pub(crate) fn record_local_entry(
    tx: &Transaction<'_>,
    op: &str,
    seq: i64,
    payload: &str,
    before: &SharedState,
    after: &SharedState,
) -> rusqlite::Result<()> {
    if crate::log_schema::log_enabled(tx)?
        && before != after
        && !matches!(op, "agent.import" | "root_key.assign" | "root_key.backfill")
    {
        let body = if op.starts_with("agent.") {
            let payload: Value = serde_json::from_str(payload).map_err(decode_error)?;
            canonical_bytes(payload.get("entry").unwrap_or(&payload))
        } else {
            canonical_bytes(&before.project_delta(after, tx)?)
        };
        tx.execute(
            "UPDATE registry_journal SET stream='shared',entry=?1 WHERE seq=?2",
            rusqlite::params![body, seq],
        )?;
    }
    Ok(())
}

fn validate_row(row: &SharedRow, expected: &[String], table: &str) -> rusqlite::Result<()> {
    if row.len() != expected.len() || expected.iter().any(|name| !row.contains_key(name)) {
        return Err(invalid(format!("unknown or missing columns in {table}")));
    }
    for value in row.values() {
        sql_value(value)?;
    }
    if table == "project_alias"
        && row
            .get("old_id")
            .and_then(Value::as_str)
            .is_some_and(|id| id.starts_with("pj-implicit1-"))
    {
        return Err(invalid("implicit aliases are machine-local"));
    }
    Ok(())
}

fn sql_value(value: &Value) -> rusqlite::Result<rusqlite::types::Value> {
    use rusqlite::types::Value as Sql;
    match value {
        Value::Null => Ok(Sql::Null),
        Value::String(text) => Ok(Sql::Text(text.clone())),
        Value::Number(n) if n.as_i64().is_some() => Ok(Sql::Integer(n.as_i64().unwrap())),
        _ => Err(invalid(
            "shared cells must be null, text or signed integers",
        )),
    }
}

impl ProjectEntry {
    pub fn validate(&self, conn: &Connection) -> rusqlite::Result<()> {
        if !matches!(self.op.as_str(), "project.shared" | "shared.snapshot")
            || (self.op == "project.shared" && !self.agents.is_empty())
        {
            return Err(invalid("unsupported project entry op"));
        }
        for (table, changes) in &self.tables {
            if !PROJECT_SHARED_TABLES.contains(&table.as_str()) {
                return Err(invalid(format!("unknown shared table {table}")));
            }
            let (names, keys) = columns(conn, table)?;
            for row in &changes.upsert {
                validate_row(row, &names, table)?;
            }
            for row in &changes.delete {
                validate_row(row, &keys, table)?;
            }
        }
        for agent in &self.agents {
            if agent.op != "agent.import" {
                return Err(invalid("snapshot agents must be imports"));
            }
        }
        Ok(())
    }

    pub fn apply(&self, tx: &Transaction<'_>, seq: i64) -> rusqlite::Result<()> {
        self.restore(tx, seq, true)
    }

    pub(crate) fn restore(
        &self,
        tx: &Transaction<'_>,
        seq: i64,
        cascade: bool,
    ) -> rusqlite::Result<()> {
        self.validate(tx)?;
        // Defer foreign-key checks to the end of this transaction, so rows that
        // reference each other (for example an agent merged into another agent
        // in the same entry) can be installed in any order. The final state
        // must still satisfy every constraint.
        tx.execute_batch("PRAGMA defer_foreign_keys=ON;")?;
        // Install parent after-images before moving this machine's dependent rows.
        for table in ["workspace", "project"] {
            self.upsert_table(tx, table)?;
        }
        if cascade {
            self.cascade_local(tx, seq)?;
        }
        for table in PROJECT_SHARED_TABLES.iter().rev() {
            if let Some(changes) = self.tables.get(*table) {
                let (_, keys) = columns(tx, table)?;
                let predicate = keys
                    .iter()
                    .map(|name| format!("{name} IS ?"))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                for row in &changes.delete {
                    tx.execute(
                        &format!("DELETE FROM {table} WHERE {predicate}"),
                        params_from_iter(
                            keys.iter()
                                .map(|key| sql_value(&row[key]))
                                .collect::<rusqlite::Result<Vec<_>>>()?,
                        ),
                    )?;
                }
            }
        }
        for table in PROJECT_SHARED_TABLES.iter().skip(2) {
            self.upsert_table(tx, table)?;
        }
        for agent in &self.agents {
            crate::agent::replay_agent_entry(
                tx,
                &agent.op,
                &serde_json::to_value(agent).map_err(decode_error)?,
            )?;
        }
        // Which workspace a project belongs to is shared; `workspace_member` is
        // this machine's own index of it, so rebuild its local rows from the
        // shared placement. Rows of other kinds, written by older versions,
        // stay as they are.
        tx.execute("DELETE FROM workspace_member WHERE ref_kind='local'", [])?;
        tx.execute("INSERT INTO workspace_member(workspace_id,ref_kind,device_fingerprint,project_id) SELECT workspace_id,'local','',project_id FROM project_workspace", [])?;
        Ok(())
    }

    fn upsert_table(&self, tx: &Transaction<'_>, table: &str) -> rusqlite::Result<()> {
        let Some(changes) = self.tables.get(table) else {
            return Ok(());
        };
        let (names, keys) = columns(tx, table)?;
        let updates = names
            .iter()
            .filter(|name| !keys.contains(name))
            .map(|name| format!("{name}=excluded.{name}"))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "INSERT INTO {table}({}) VALUES({}) ON CONFLICT({}) DO UPDATE SET {updates}",
            names.join(","),
            vec!["?"; names.len()].join(","),
            keys.join(",")
        );
        for row in &changes.upsert {
            tx.execute(
                &sql,
                params_from_iter(
                    names
                        .iter()
                        .map(|name| sql_value(&row[name]))
                        .collect::<rusqlite::Result<Vec<_>>>()?,
                ),
            )?;
        }
        Ok(())
    }

    fn cascade_local(&self, tx: &Transaction<'_>, seq: i64) -> rusqlite::Result<()> {
        if let Some(changes) = self.tables.get("workspace") {
            for row in &changes.delete {
                let id = sql_value(&row["workspace_id"])?;
                tx.execute("DELETE FROM workspace_root WHERE workspace_id=?1", [&id])?;
                tx.execute("DELETE FROM workspace_member WHERE workspace_id=?1", [&id])?;
            }
        }
        if let Some(changes) = self.tables.get("project") {
            for row in &changes.delete {
                let id = row["project_id"]
                    .as_str()
                    .ok_or_else(|| invalid("project key must be text"))?;
                crate::binding::retire_project_bindings(tx, id, seq)?;
                let successor = self
                    .tables
                    .get("project_alias")
                    .and_then(|changes| {
                        changes
                            .upsert
                            .iter()
                            .find(|row| row["old_id"].as_str() == Some(id))
                    })
                    .and_then(|row| row["project_id"].as_str());
                if let Some(successor) = successor {
                    tx.execute(
                        "UPDATE project_root SET project_id=?1 WHERE project_id=?2",
                        [successor, id],
                    )?;
                    tx.execute(
                        "UPDATE derived_root_parent SET project_id=?1 WHERE project_id=?2",
                        [successor, id],
                    )?;
                    tx.execute("UPDATE project_alias SET project_id=?1 WHERE project_id=?2 AND substr(old_id,1,13)='pj-implicit1-'", [successor, id])?;
                } else {
                    tx.execute("DELETE FROM project_root WHERE project_id=?1", [id])?;
                    tx.execute("DELETE FROM derived_root_parent WHERE project_id=?1", [id])?;
                    tx.execute("DELETE FROM project_alias WHERE project_id=?1 AND substr(old_id,1,13)='pj-implicit1-'", [id])?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RootKeyMapping {
    pub canonical_root: String,
    pub kind: String,
    pub root_key: String,
}

/// Restores recorded root-key assignments. Which key a root got depended on its
/// git remotes at the time; the assignment is journaled, so replay restores it
/// as recorded and never reads git configuration, which may have changed since.
pub(crate) fn replay_root_keys(tx: &Transaction<'_>, value: Value) -> rusqlite::Result<()> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Mappings {
        mappings: Vec<RootKeyMapping>,
    }
    let mappings: Mappings = serde_json::from_value(value).map_err(decode_error)?;
    for mapping in mappings.mappings {
        if !matches!(mapping.kind.as_str(), "remote" | "label") {
            return Err(invalid("unknown root key kind"));
        }
        tx.execute(
            "UPDATE project_root SET root_key=?1,root_key_kind=?2 WHERE canonical_root=?3",
            [&mapping.root_key, &mapping.kind, &mapping.canonical_root],
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutations::tests::Fixture;
    use serde_json::json;
    use std::collections::BTreeSet;

    #[test]
    fn schema_shared_tables_are_all_compared() {
        let f = Fixture::new("shared-coverage");
        f.store
            .read(|conn| {
                // The complement is deliberately independent of the comparison's
                // list: any new application table must be classified explicitly.
                let local = [
                    "registry_journal",
                    "workspace_member",
                    "project_root",
                    "derived_root_parent",
                    "root_binding",
                    "retired_binding",
                    "root_approval",
                    "root_owned_remotes",
                    "workspace_root",
                    "identity_log_state",
                    "pending_entry",
                    "cortexkit_schema_version",
                    "cortexkit_fence",
                    "sqlite_sequence",
                ];
                let schema = conn
                    .prepare("SELECT name FROM sqlite_schema WHERE type='table'")?
                    .query_map([], |r| r.get::<_, String>(0))?
                    .collect::<rusqlite::Result<BTreeSet<_>>>()?;
                let shared = schema
                    .into_iter()
                    .filter(|name| !local.contains(&name.as_str()))
                    .collect::<BTreeSet<_>>();
                let compared = PROJECT_SHARED_TABLES
                    .iter()
                    .chain(AGENT_SHARED_TABLES)
                    .map(|name| name.to_string())
                    .collect::<BTreeSet<_>>();
                assert_eq!(
                    shared, compared,
                    "schema and shared comparison coverage differ"
                );
                let captured = SharedState::capture(conn)?;
                assert_eq!(
                    captured.0.keys().cloned().collect::<BTreeSet<_>>(),
                    compared
                );
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn comparison_records_full_upserts_keys_only_deletes_and_canonical_bytes() {
        let f = Fixture::new("shared-comparison");
        f.store.db.with_conn_fenced(|tx| {
            tx.execute_batch("INSERT INTO project VALUES('P','old',0,NULL,10,10); INSERT INTO workspace VALUES('W','old',10,10); INSERT INTO project_alias VALUES('old-alias','P',10);")?;
            let before = SharedState::capture(tx)?;
            tx.execute_batch("UPDATE project SET name='new',updated_at=20; UPDATE workspace SET name='new',updated_at=20; INSERT INTO project_workspace VALUES('P','W'); DELETE FROM project_alias; INSERT INTO project_alias VALUES('explicit','P',20); INSERT INTO project_alias VALUES('pj-implicit1-local','P',20); INSERT INTO workspace_root VALUES('W','/private',20); INSERT INTO project_root_key VALUES('P','remote','owner/repo',20);")?;
            let after = SharedState::capture(tx)?;
            let entry = before.project_delta(&after, tx)?;
            assert_eq!(entry.tables.len(), 5);
            assert_eq!(entry.tables["project"].upsert, vec![serde_json::from_value::<SharedRow>(json!({"project_id":"P","name":"new","implicit":0,"seed_identity":null,"created_at":10,"updated_at":20})).unwrap()]);
            assert_eq!(entry.tables["project_alias"].delete, vec![serde_json::from_value::<SharedRow>(json!({"old_id":"old-alias"})).unwrap()]);
            assert_eq!(entry.tables["project_alias"].upsert.len(), 1);
            let bytes = canonical_bytes(&entry);
            let text = String::from_utf8(bytes.clone()).unwrap();
            assert!(!text.contains("/private") && !text.contains("pj-implicit1-"));
            assert!(text.starts_with("{\"op\":\"project.shared\",\"tables\":{\"project\":{\"delete\":[],\"upsert\":[{\"created_at\":10,\"implicit\":0,\"name\":\"new\""));
            tx.execute_batch("DELETE FROM project_alias WHERE old_id='explicit'; INSERT INTO project_alias VALUES('explicit','P',20);")?;
            assert_eq!(bytes, canonical_bytes(&before.project_delta(&SharedState::capture(tx)?, tx)?));
            tx.execute_batch("UPDATE workspace_root SET root='/different'; INSERT INTO project_alias VALUES('pj-implicit1-other','P',99);")?;
            assert_eq!(after, SharedState::capture(tx)?);
            assert!(after.project_delta(&SharedState::capture(tx)?, tx)?.tables.is_empty());
            Ok(())
        }).unwrap();
    }
}
