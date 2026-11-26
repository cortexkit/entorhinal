//! Operator-confirmed workspace roots. Unlike project checkouts, these folders
//! need not contain git metadata, so approval is tied to their filesystem id.

use std::path::Path;

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::mutations::{append, canonical, set_workspace_root_projection, Action};
use crate::{
    domain, file_id, Approval, JournalWriter, RegistryError, RegistryStore, SetWorkspaceRootRequest,
};

pub(crate) const V29_WORKSPACE_ROOT_APPROVAL: &str = r#"
CREATE TABLE workspace_root_approval (
    workspace_id TEXT PRIMARY KEY NOT NULL REFERENCES workspace_root(workspace_id) ON DELETE CASCADE,
    root TEXT NOT NULL,
    file_id TEXT NOT NULL,
    journal_seq INTEGER NOT NULL REFERENCES registry_journal(seq),
    actor TEXT NOT NULL,
    approved_at INTEGER NOT NULL
);
CREATE TRIGGER clear_workspace_root_approval AFTER UPDATE OF root ON workspace_root
WHEN OLD.root IS NOT NEW.root
BEGIN
    DELETE FROM workspace_root_approval WHERE workspace_id=NEW.workspace_id;
END;
"#;

pub const APPROVE: &str = "approve_workspace_root";
pub const UNAPPROVE: &str = "unapprove_workspace_root";
const SET: &str = "set_workspace_root";
const FILE_ID_UNAVAILABLE: &str = "file_id_unavailable";

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceApprovalRequest {
    pub workspace_id: String,
}

#[derive(Clone)]
pub enum WorkspaceRootWrite {
    Set(SetWorkspaceRootRequest),
    Approve(WorkspaceApprovalRequest),
}

pub enum WorkspaceRootPrecheck {
    Cached(Vec<u8>),
    Ready(Box<WorkspaceRootPlan>),
}

#[derive(Clone, PartialEq, Eq)]
struct Workspace {
    name: String,
    created_at: i64,
    root: Option<String>,
    updated_at: Option<i64>,
    approved_seq: Option<i64>,
    approved_file: Option<String>,
}

fn workspace(conn: &Connection, id: &str) -> rusqlite::Result<Option<Workspace>> {
    conn.query_row(
        "SELECT w.name, w.created_at, r.root, r.updated_at, a.journal_seq, a.file_id
         FROM workspace w LEFT JOIN workspace_root r USING(workspace_id)
         LEFT JOIN workspace_root_approval a ON a.workspace_id=w.workspace_id AND a.root=r.root
         WHERE w.workspace_id=?1",
        [id],
        |r| {
            Ok(Workspace {
                name: r.get(0)?,
                created_at: r.get(1)?,
                root: r.get(2)?,
                updated_at: r.get(3)?,
                approved_seq: r.get(4)?,
                approved_file: r.get(5)?,
            })
        },
    )
    .optional()
}

#[derive(Clone)]
pub struct WorkspaceRootPlan {
    summary: String,
    workspace: Workspace,
    root: String,
    file_id: Option<String>,
}

impl WorkspaceRootPlan {
    pub fn summary(&self) -> &str {
        &self.summary
    }
}

pub(crate) fn validate_root(root: &str) -> Result<(), RegistryError> {
    let path = Path::new(root);
    if !path.is_absolute() {
        return Err(domain(
            "not_absolute",
            format!("workspace root must be an absolute path: {root}"),
        ));
    }
    canonical(root)?;
    if !path.is_dir() {
        return Err(domain(
            "not_a_directory",
            format!("workspace root is not a directory: {root}"),
        ));
    }
    Ok(())
}

pub(crate) fn current_file_id(root: &str) -> Option<String> {
    let path = Path::new(root);
    path.is_dir().then(|| file_id(path)).flatten()
}

pub(crate) fn approval(journal_seq: Option<i64>) -> Approval {
    Approval {
        state: if journal_seq.is_some() {
            "approved"
        } else {
            "unapproved"
        },
        journal_seq,
    }
}

impl WorkspaceRootWrite {
    fn op(&self) -> &'static str {
        match self {
            Self::Set(_) => SET,
            Self::Approve(_) => APPROVE,
        }
    }

    fn workspace_id(&self) -> &str {
        match self {
            Self::Set(req) => &req.workspace_id,
            Self::Approve(req) => &req.workspace_id,
        }
    }

    pub fn request_key(&self) -> Option<&str> {
        match self {
            Self::Set(req) => req.request_key.as_deref(),
            Self::Approve(_) => None,
        }
    }

    fn target<'a>(&'a self, before: &'a Workspace) -> Result<&'a str, RegistryError> {
        let root = match self {
            Self::Set(req) => req.root.as_deref(),
            Self::Approve(_) => before.root.as_deref(),
        }
        .ok_or_else(|| {
            domain(
                "workspace_root_not_set",
                "the workspace has no root to approve",
            )
        })?;
        validate_root(root)?;
        Ok(root)
    }

    pub fn precheck(&self, store: &RegistryStore) -> Result<WorkspaceRootPrecheck, RegistryError> {
        if let Some(body) = store.shared_request_cache(self.op(), self.request_key())? {
            return Ok(WorkspaceRootPrecheck::Cached(body));
        }
        let id = self.workspace_id();
        let before = store
            .read(|conn| workspace(conn, id))?
            .ok_or_else(|| domain("not_found", id))?;
        let root = self.target(&before)?.to_owned();
        let file_id = current_file_id(&root);
        let action = match (self, &file_id) {
            (Self::Set(_), Some(_)) => "Set and approve the root of",
            (Self::Set(_), None) => "Set the root (without approval) of",
            (Self::Approve(_), Some(_)) => "Approve the root of",
            (Self::Approve(_), None) => {
                return Err(domain(
                    FILE_ID_UNAVAILABLE,
                    "the workspace root's file identity could not be read",
                ))
            }
        };
        let summary = format!("{action} workspace {:?}: {root:?}", before.name);
        if summary.chars().count() > crate::agent::OPERATOR_SUMMARY_LIMIT {
            return Err(domain(
                "operator_summary_too_long",
                "workspace and folder do not fit in one presence prompt",
            ));
        }
        Ok(WorkspaceRootPrecheck::Ready(Box::new(WorkspaceRootPlan {
            summary,
            workspace: before,
            root,
            file_id,
        })))
    }

    pub fn commit(
        &self,
        writer: &JournalWriter<'_>,
        plan: &WorkspaceRootPlan,
        now: i64,
    ) -> Result<Vec<u8>, RegistryError> {
        let id = self.workspace_id();
        let key = self.request_key();
        let actor = match self {
            Self::Set(req) => req.actor.as_deref().unwrap_or("operator"),
            Self::Approve(_) => "operator",
        };
        writer.mutation(self.op(), key, |tx| {
            let stale = || domain("operator_approval_stale", "the workspace or folder changed after operator confirmation");
            let current = workspace(tx, id)?.ok_or_else(stale)?;
            if current != plan.workspace
                || self.target(&current).is_err()
                || current_file_id(&plan.root) != plan.file_id
            {
                return Err(stale());
            }
            let root_changed = current.root.as_deref() != Some(plan.root.as_str());
            let approved_seq = current.approved_seq.filter(|_| plan.file_id.is_some() && plan.file_id == current.approved_file);
            let changed = root_changed || if plan.file_id.is_some() {
                approved_seq.is_none()
            } else {
                current.approved_seq.is_some()
            };
            let payload = json!({
                "workspaceId": id,
                "root": plan.root,
                "workspaceApproval": {"rootChanged":root_changed, "fileId":plan.file_id, "actor":actor},
            });
            let seq = if changed {
                let seq = append(tx, self.op(), &payload, actor, key, now, writer.principal)?;
                replay(tx, seq, payload.clone(), now, crate::log_schema::log_enabled(tx)?)?;
                Some(seq)
            } else {
                None
            };
            let mut value = json!({
                "workspaceId": id, "root": plan.root,
                "approval": approval(plan.file_id.as_ref().and(seq.or(approved_seq))),
            });
            if plan.file_id.is_none() {
                value["unapprovedReason"] = json!(FILE_ID_UNAVAILABLE);
            }
            Ok(Action { changed, seq, value, payload })
        })
    }
}

impl JournalWriter<'_> {
    pub fn unapprove_workspace_root_at(
        &self,
        req: WorkspaceApprovalRequest,
        now: i64,
    ) -> Result<Vec<u8>, RegistryError> {
        let id = req.workspace_id;
        self.mutation(UNAPPROVE, None, |tx| {
            let current = workspace(tx, &id)?.ok_or_else(|| domain("not_found", &id))?;
            let payload = json!({
                "workspaceId": id, "root": current.root,
                "workspaceApproval": {"rootChanged":false, "fileId":null, "actor":"operator"},
            });
            let seq = if current.approved_seq.is_some() {
                let seq = append(
                    tx,
                    UNAPPROVE,
                    &payload,
                    "operator",
                    None,
                    now,
                    self.principal,
                )?;
                replay(
                    tx,
                    seq,
                    payload.clone(),
                    now,
                    crate::log_schema::log_enabled(tx)?,
                )?;
                Some(seq)
            } else {
                None
            };
            let mut value = json!({"workspaceId":id, "root":current.root});
            if current.root.is_some() {
                value["approval"] = json!(approval(None));
            }
            Ok(Action {
                changed: seq.is_some(),
                seq,
                value,
                payload,
            })
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApprovalChange {
    workspace_id: String,
    root: String,
    workspace_approval: AfterImage,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AfterImage {
    root_changed: bool,
    file_id: Option<String>,
    actor: String,
}

pub(crate) fn replay(
    tx: &Transaction<'_>,
    seq: i64,
    payload: Value,
    now: i64,
    enabled: bool,
) -> rusqlite::Result<()> {
    let ApprovalChange {
        workspace_id,
        root,
        workspace_approval: change,
    } = serde_json::from_value(payload).map_err(crate::shared_entry::decode_error)?;
    if change.root_changed {
        set_workspace_root_projection(tx, &workspace_id, Some(&root), now, enabled)?;
    }
    if let Some(file_id) = change.file_id {
        tx.execute(
            "INSERT INTO workspace_root_approval(workspace_id,root,file_id,journal_seq,actor,approved_at)
             VALUES(?1,?2,?3,?4,?5,?6)
             ON CONFLICT(workspace_id) DO UPDATE SET root=excluded.root,file_id=excluded.file_id,
                journal_seq=excluded.journal_seq,actor=excluded.actor,approved_at=excluded.approved_at",
            params![workspace_id, root, file_id, seq, change.actor, now],
        )?;
    } else {
        tx.execute(
            "DELETE FROM workspace_root_approval WHERE workspace_id=?1",
            [&workspace_id],
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutations::tests::{result, Fixture};
    use crate::CreateWorkspaceRequest;

    fn fixture() -> Fixture {
        let f = Fixture::new("ws-approval");
        f.store
            .with_principal("direct")
            .create_workspace_at(
                CreateWorkspaceRequest {
                    workspace_id: "studio".into(),
                    name: "Studio".into(),
                },
                1,
            )
            .unwrap();
        f
    }

    fn set_request(root: Option<String>) -> SetWorkspaceRootRequest {
        SetWorkspaceRootRequest {
            workspace_id: "studio".into(),
            root,
            ..Default::default()
        }
    }

    fn confirmed(store: &RegistryStore, write: WorkspaceRootWrite) -> Value {
        let WorkspaceRootPrecheck::Ready(plan) = write.precheck(store).unwrap() else {
            panic!("new request must be eligible for a prompt");
        };
        result(
            &write
                .commit(&store.with_principal("direct"), &plan, 42)
                .unwrap(),
        )
    }

    #[test]
    fn workspace_approval_verify_and_rebuild_restore_exact_record_even_after_replacement() {
        let f = fixture();
        let root = f.dir("folder");
        let original = current_file_id(&root).unwrap();
        let mut req = set_request(Some(root.clone()));
        req.actor = Some("Named operator".into());
        let reply = confirmed(&f.store, WorkspaceRootWrite::Set(req));
        let seq = reply["approval"]["journalSeq"].as_i64().unwrap();
        assert!(f.store.verify().unwrap().ok);

        std::fs::rename(&root, f.root.join("original-folder")).unwrap();
        std::fs::create_dir(&root).unwrap();
        assert_ne!(current_file_id(&root).as_deref(), Some(original.as_str()));
        assert_eq!(
            f.store.enumerate(None).unwrap().workspaces[0].approval,
            Some(approval(None))
        );
        f.store
            .db
            .with_conn_fenced(|tx| tx.execute("DELETE FROM workspace_root_approval", []))
            .unwrap();
        let drift = f.store.verify().unwrap();
        assert!(!drift.ok);
        assert_eq!(drift.replay.tables[0].table, "workspace_root_approval");
        assert_eq!(f.store.rebuild().unwrap().replay, drift.replay);
        let actual = f.store.read(|conn| conn.query_row(
            "SELECT root,file_id,journal_seq,actor,approved_at FROM workspace_root_approval WHERE workspace_id='studio'",
            [], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, i64>(2)?, row.get::<_, String>(3)?, row.get::<_, i64>(4)?)),
        )).unwrap();
        assert_eq!(actual, (root, original, seq, "Named operator".into(), 42));
        assert!(f.store.verify().unwrap().ok);
        assert_eq!(
            f.store.enumerate(Some("studio")).unwrap().workspaces[0].approval,
            Some(approval(None))
        );
    }

    #[test]
    fn workspace_approval_path_changes_clear_the_record_before_it_can_be_reused() {
        let f = fixture();
        let initial = f.dir("first");
        let other = f.dir("second");
        for root in [Some(other), None] {
            confirmed(
                &f.store,
                WorkspaceRootWrite::Set(set_request(Some(initial.clone()))),
            );
            f.store
                .set_workspace_root(set_request(root.clone()))
                .unwrap();
            let before = f
                .store
                .read(|conn| workspace(conn, "studio"))
                .unwrap()
                .unwrap();
            assert_eq!(before.root, root);
            let records: i64 = f
                .store
                .read(|conn| {
                    conn.query_row("SELECT COUNT(*) FROM workspace_root_approval", [], |row| {
                        row.get(0)
                    })
                })
                .unwrap();
            assert_eq!(
                records, 0,
                "a path change must delete the record, not just hide it"
            );
            f.store
                .set_workspace_root(set_request(Some(initial.clone())))
                .unwrap();
            assert_eq!(
                f.store.enumerate(None).unwrap().workspaces[0].approval,
                Some(approval(None))
            );
            assert!(f.store.verify().unwrap().ok);
        }
    }

    #[test]
    fn workspace_approval_never_enters_shared_log_or_agent_feeds() {
        let f = fixture();
        f.store
            .apply_entry("identity_log.enable", "{}", "fixture", None, |tx| {
                tx.execute("UPDATE identity_log_state SET state='enabled'", [])?;
                Ok(())
            })
            .unwrap();
        let before = f.store.generation().unwrap();
        let root = f.dir("folder");
        confirmed(&f.store, WorkspaceRootWrite::Set(set_request(Some(root))));
        let req = WorkspaceApprovalRequest {
            workspace_id: "studio".into(),
        };
        f.store
            .with_principal("direct")
            .unapprove_workspace_root_at(req.clone(), 43)
            .unwrap();
        confirmed(&f.store, WorkspaceRootWrite::Approve(req));

        let rows = f.store.read(|conn| {
            let mut stmt = conn.prepare("SELECT op, stream, entry IS NULL FROM registry_journal WHERE seq>?1 ORDER BY seq")?;
            let rows = stmt.query_map([before], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, bool>(2)?)))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
        }).unwrap();
        assert_eq!(
            rows,
            [SET, UNAPPROVE, APPROVE].map(|op| (op.into(), "local".into(), true))
        );
        let tail = f.store.journal_tail(before, 100).unwrap();
        assert!(tail.entries.is_empty());
        assert_eq!(tail.generation, before + 3);
        let changes = f.store.agent_changes(before, Some(100)).unwrap();
        assert!(changes.entries.is_empty());
        assert_eq!(changes.cursor, before + 3);
        assert!(f.store.verify().unwrap().ok);
    }
}
