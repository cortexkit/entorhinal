//! Ownership names are journaled; the repositories those names point to are
//! always read from the checkout's current git config.

use std::{collections::BTreeMap, path::Path};

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    binding::{github_remotes, repository_owner, GitRemote},
    canonical_query_path,
    mutations::{append, domain, Action},
    JournalWriter, RegistryError, RegistryStore,
};

/// No row means origin; an explicit empty JSON array means no ownership.
/// Cascading deletion keeps an override scoped to the root's registration.
pub const V6_OWNED_REMOTES: &str = r#"
CREATE TABLE root_owned_remotes (
    canonical_root TEXT PRIMARY KEY REFERENCES project_root(canonical_root) ON DELETE CASCADE,
    remotes_json TEXT NOT NULL
);
"#;

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SetOwnedRemotesRequest {
    pub root: String,
    pub remotes: Option<Vec<String>>,
    #[serde(default)]
    pub request_key: Option<String>,
    #[serde(default)]
    pub actor: Option<String>,
}

fn override_names(conn: &Connection, root: &str) -> rusqlite::Result<Option<Vec<String>>> {
    let stored: Option<String> = conn
        .query_row(
            "SELECT remotes_json FROM root_owned_remotes WHERE canonical_root = ?1",
            [root],
            |row| row.get(0),
        )
        .optional()?;
    stored
        .map(|text| {
            serde_json::from_str(&text)
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
        })
        .transpose()
}

pub(crate) fn effective_owned_names(
    conn: &Connection,
    root: &str,
) -> rusqlite::Result<Vec<String>> {
    Ok(override_names(conn, root)?.unwrap_or_else(|| vec!["origin".into()]))
}

pub(crate) fn root_remotes(conn: &Connection, root: &str) -> rusqlite::Result<Vec<GitRemote>> {
    let names = effective_owned_names(conn, root)?;
    let mut remotes = github_remotes(Path::new(root));
    for remote in &mut remotes {
        remote.owned = names.contains(&remote.name);
    }
    Ok(remotes)
}

fn apply_owned_remotes(
    tx: &Transaction<'_>,
    request: &SetOwnedRemotesRequest,
) -> rusqlite::Result<()> {
    match &request.remotes {
        Some(names) => {
            tx.execute(
                "INSERT INTO root_owned_remotes(canonical_root, remotes_json) VALUES(?1, ?2)
                 ON CONFLICT(canonical_root) DO UPDATE SET remotes_json=excluded.remotes_json",
                params![request.root, serde_json::to_string(names).unwrap()],
            )?;
        }
        None => {
            tx.execute(
                "DELETE FROM root_owned_remotes WHERE canonical_root=?1",
                [&request.root],
            )?;
        }
    }
    Ok(())
}

pub(crate) fn replay_ownership_op(
    tx: &Transaction<'_>,
    op: &str,
    value: &Value,
) -> rusqlite::Result<bool> {
    if op != "set_owned_remotes" {
        return Ok(false);
    }
    let request = serde_json::from_value(value.clone())
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    // Replay restores names, never rejudges history against today's git config.
    apply_owned_remotes(tx, &request)?;
    Ok(true)
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ResolveRemoteStatus {
    Found {
        #[serde(rename = "projectId")]
        project_id: String,
        root: String,
    },
    None,
    Ambiguous {
        #[serde(rename = "projectIds")]
        project_ids: Vec<String>,
    },
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ResolveRemoteReply {
    #[serde(flatten)]
    pub status: ResolveRemoteStatus,
    pub generation: i64,
}

impl RegistryStore {
    pub fn set_owned_remotes(
        &self,
        request: SetOwnedRemotesRequest,
    ) -> Result<Vec<u8>, RegistryError> {
        self.with_principal("entorhinal").set_owned_remotes(request)
    }

    pub fn resolve_remote(
        &self,
        owner: &str,
        repo: &str,
    ) -> Result<ResolveRemoteReply, RegistryError> {
        self.read(|conn| {
            let roots = conn
                .prepare("SELECT project_id, canonical_root FROM project_root ORDER BY project_id, canonical_root")?
                .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let mut projects = BTreeMap::new();
            for (project, root) in roots {
                if root_remotes(conn, &root)?.iter().any(|remote| {
                    remote.owned && remote.owner.eq_ignore_ascii_case(owner) && remote.repo.eq_ignore_ascii_case(repo)
                }) {
                    // Multiple roots of one project are one owner; retain its
                    // first matching root in canonical path order.
                    projects.entry(project).or_insert(root);
                }
            }
            let status = match projects.len() {
                0 => ResolveRemoteStatus::None,
                1 => {
                    let (project_id, root) = projects.into_iter().next().unwrap();
                    ResolveRemoteStatus::Found { project_id, root }
                }
                _ => ResolveRemoteStatus::Ambiguous { project_ids: projects.into_keys().collect() },
            };
            Ok(ResolveRemoteReply { status, generation: self.generation_from_connection(conn)? })
        })
    }
}

impl JournalWriter<'_> {
    pub fn set_owned_remotes(
        &self,
        mut request: SetOwnedRemotesRequest,
    ) -> Result<Vec<u8>, RegistryError> {
        // Use the same query canonicalization as remove_root: registered roots
        // remain addressable even when their checkout no longer exists.
        request.root = canonical_query_path(Path::new(&request.root))?.0;
        if let Some(names) = &mut request.remotes {
            if names.iter().any(|name| name.trim().is_empty()) {
                return Err(domain("invalid_params", "remote names must be non-empty"));
            }
            names.sort();
            names.dedup();
        }
        let key = request.request_key.clone();
        let actor = request.actor.clone().unwrap_or_else(|| "module".into());
        self.mutation("set_owned_remotes", key.as_deref(), |tx| {
            let project: String = tx
                .query_row(
                    "SELECT project_id FROM project_root WHERE canonical_root=?1",
                    [&request.root],
                    |row| row.get(0),
                )
                .optional()?
                .ok_or_else(|| {
                    domain(
                        "not_found",
                        format!("{} is not a registered root", request.root),
                    )
                })?;
            let names = request
                .remotes
                .clone()
                .unwrap_or_else(|| vec!["origin".into()]);
            let mut remotes = github_remotes(Path::new(&request.root));
            for remote in &mut remotes {
                remote.owned = names.contains(&remote.name);
            }
            if let Some((repository, owner)) = repository_owner(tx, &project, &remotes)? {
                return Err(domain(
                    "repository_owned",
                    format!("{repository} belongs to {owner}"),
                ));
            }
            let payload = serde_json::to_value(&request)
                .map_err(|error| domain("encode_failed", error.to_string()))?;
            let value = json!({"projectId": project, "root": request.root, "remotes": names});
            if override_names(tx, &request.root)? == request.remotes {
                return Ok(Action {
                    changed: false,
                    seq: None,
                    value,
                    payload,
                });
            }
            let seq = append(
                tx,
                "set_owned_remotes",
                &payload,
                &actor,
                key.as_deref(),
                crate::now_unix_millis(),
                self.principal,
            )?;
            apply_owned_remotes(tx, &request)?;
            Ok(Action {
                changed: true,
                seq: Some(seq),
                value,
                payload,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        mutations::tests::{register, result, Fixture},
        AddRootRequest, RegisterRequest, RemoveRequest, RemoveRootRequest,
    };
    use std::{fs, process::Command};

    fn fixture(label: &str) -> Fixture {
        let mut f = Fixture::new(label);
        f.store.set_root_records(true);
        f
    }

    fn git(root: &str, args: &[&str]) {
        let out = Command::new("git")
            .arg("-C")
            .arg(root)
            .args(args)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    fn repo(f: &Fixture, name: &str, origin: &str, upstream: &str) -> String {
        let root = f.dir(name);
        git(&root, &["init", "--quiet"]);
        for (name, url) in [("origin", origin), ("upstream", upstream)] {
            if !url.is_empty() {
                git(
                    &root,
                    &[
                        "remote",
                        "add",
                        name,
                        &format!("https://github.com/{url}.git"),
                    ],
                );
            }
        }
        root
    }

    fn set(f: &Fixture, root: &str, names: Option<&[&str]>) -> Result<Vec<u8>, RegistryError> {
        f.store.set_owned_remotes(SetOwnedRemotesRequest {
            root: root.into(),
            remotes: names.map(|n| n.iter().map(|s| s.to_string()).collect()),
            ..Default::default()
        })
    }

    fn add(f: &Fixture, id: &str, root: &str) -> Result<Vec<u8>, RegistryError> {
        f.store.add_root(AddRootRequest {
            project_id: id.into(),
            root: root.into(),
            actor: None,
            label: None,
        })
    }

    fn lookup(f: &Fixture, owner: &str, repo: &str) -> Value {
        serde_json::to_value(f.store.resolve_remote(owner, repo).unwrap()).unwrap()
    }

    fn found(f: &Fixture, owner: &str, repo: &str, id: &str, root: &str) {
        assert_eq!(
            lookup(f, owner, repo),
            json!({"status":"found", "projectId":id, "root":root, "generation":f.store.generation().unwrap()})
        );
    }

    fn flags(f: &Fixture, root: &str) -> Vec<(String, bool)> {
        let reply = serde_json::to_value(f.store.resolve(root).unwrap()).unwrap();
        reply["rootRecords"][0]["remotes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| {
                (
                    r["name"].as_str().unwrap().into(),
                    r["owned"].as_bool().unwrap(),
                )
            })
            .collect()
    }

    fn overrides(f: &Fixture) -> Vec<(String, String)> {
        f.store.read(|conn| conn.prepare("SELECT canonical_root, remotes_json FROM root_owned_remotes ORDER BY canonical_root")?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?.collect()).unwrap()
    }

    // Capture every table, not just a generation counter: refusals must roll
    // back both journal writes and all projections, including request caches.
    fn image(f: &Fixture) -> Vec<Vec<Vec<rusqlite::types::Value>>> {
        f.store
            .read(|conn| {
                let tables = conn
                    .prepare("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")?
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                tables
                    .iter()
                    .map(|table| {
                        let mut stmt =
                            conn.prepare(&format!("SELECT * FROM \"{table}\" ORDER BY rowid"))?;
                        let columns = stmt.column_count();
                        let rows = stmt
                            .query_map([], |row| {
                                (0..columns).map(|column| row.get(column)).collect()
                            })?
                            .collect();
                        rows
                    })
                    .collect()
            })
            .unwrap()
    }

    fn refusal(
        f: &Fixture,
        expected: &str,
        action: impl FnOnce() -> Result<Vec<u8>, RegistryError>,
    ) -> String {
        let before = image(f);
        let err = action().expect_err("must refuse");
        assert!(
            matches!(&err, RegistryError::Domain {code, ..} if code == expected),
            "{err}"
        );
        assert_eq!(image(f), before, "refusal wrote to the store");
        err.to_string()
    }

    #[test]
    fn ownership_default_marks_and_resolves_only_origin() {
        let f = fixture("ownership-default");
        let root = repo(&f, "root", "ualtinok/opencode", "public/upstream");
        register(&f, "p", root.clone());
        assert_eq!(
            flags(&f, &root),
            [("origin".into(), true), ("upstream".into(), false)]
        );
        let project =
            &serde_json::to_value(f.store.enumerate(None).unwrap()).unwrap()["projects"][0];
        assert_eq!(
            project["rootRecords"][0]["remotes"],
            json!([
                {"name":"origin", "owner":"ualtinok", "repo":"opencode", "owned":true},
                {"name":"upstream", "owner":"public", "repo":"upstream", "owned":false}
            ])
        );
        found(&f, "ualtinok", "opencode", "p", &root);
        assert_eq!(lookup(&f, "public", "upstream")["status"], "none");
    }

    #[test]
    fn ownership_case_insensitive_lookup() {
        let f = fixture("ownership-case");
        let root = repo(&f, "root", "ualtinok/opencode", "");
        register(&f, "p", root.clone());
        found(&f, "UALTINOK", "OpenCode", "p", &root);
    }

    #[test]
    fn ownership_none_and_live_ambiguity() {
        let f = fixture("ownership-live");
        let a = repo(&f, "a", "x/shared", "");
        let b = repo(&f, "b", "x/other", "");
        let clone = repo(&f, "clone", "X/SHARED", "");
        register(&f, "z", a.clone());
        add(&f, "z", &clone).unwrap();
        found(&f, "x", "shared", "z", &a);
        register(&f, "a", b.clone());
        let generation = f.store.generation().unwrap();
        assert_eq!(
            lookup(&f, "missing", "repo"),
            json!({"status":"none", "generation":generation})
        );
        git(
            &b,
            &["remote", "set-url", "origin", "git@github.com:X/Shared.git"],
        );
        assert_eq!(
            lookup(&f, "x", "shared"),
            json!({"status":"ambiguous", "projectIds":["a","z"], "generation":generation})
        );
    }

    #[test]
    fn ownership_forks_register_and_add() {
        let f = fixture("ownership-forks");
        let root = repo(&f, "original", "public/shared", "public/nonowned");
        register(&f, "original", root);
        let fork = repo(&f, "fork", "mine/fork", "PUBLIC/SHARED");
        register(&f, "fork", fork.clone());
        let extra = repo(&f, "extra", "mine/extra", "public/shared");
        add(&f, "fork", &extra).unwrap();
        // The other side of the comparison must ignore non-owned remotes too.
        let nonowned = repo(&f, "nonowned", "public/nonowned", "");
        register(&f, "nonowned", nonowned.clone());
        found(&f, "public", "nonowned", "nonowned", &nonowned);
        assert_eq!(f.store.resolve(&extra).unwrap().project_id, "fork");
        found(&f, "mine", "fork", "fork", &fork);
    }

    #[test]
    fn ownership_register_refuses_owned_clone_atomically() {
        let f = fixture("ownership-register-conflict");
        let root = repo(&f, "original", "ualtinok/opencode", "");
        register(&f, "original", root);
        let safe = repo(&f, "safe", "mine/safe", "");
        let clone = repo(&f, "clone", "UALTINOK/OpenCode", "");
        let err = refusal(&f, "repository_owned", || {
            f.store.register(RegisterRequest {
                project_id: Some("clone".into()),
                name: "Clone".into(),
                roots: vec![safe, clone],
                request_key: Some("refused".into()),
                ..Default::default()
            })
        });
        assert!(
            err.contains("UALTINOK/OpenCode belongs to original"),
            "{err}"
        );
    }

    #[test]
    fn ownership_add_root_refuses_owned_clone_atomically() {
        let f = fixture("ownership-add-conflict");
        register(
            &f,
            "original",
            repo(&f, "original", "ualtinok/opencode", ""),
        );
        register(&f, "other", repo(&f, "other", "mine/other", ""));
        let clone = repo(&f, "clone", "UALTINOK/OpenCode", "");
        let err = refusal(&f, "repository_owned", || add(&f, "other", &clone));
        assert!(
            err.contains("UALTINOK/OpenCode belongs to original"),
            "{err}"
        );
    }

    #[test]
    fn ownership_overrides_reset_and_future_names() {
        let f = fixture("ownership-overrides");
        let root = repo(&f, "root", "mine/fork", "public/upstream");
        register(&f, "p", root.clone());
        let out = result(&set(&f, &format!("{root}/."), Some(&["upstream", "upstream"])).unwrap());
        assert_eq!(out["remotes"], json!(["upstream"]));
        assert_eq!(out["root"], root);
        assert_eq!(
            flags(&f, &root),
            [("origin".into(), false), ("upstream".into(), true)]
        );
        found(&f, "public", "upstream", "p", &root);
        assert_eq!(lookup(&f, "mine", "fork")["status"], "none");
        let out = result(&set(&f, &root, None).unwrap());
        assert_eq!(out["remotes"], json!(["origin"]));
        assert!(overrides(&f).is_empty());
        found(&f, "mine", "fork", "p", &root);
        assert_eq!(
            flags(&f, &root),
            [("origin".into(), true), ("upstream".into(), false)]
        );
        set(&f, &root, Some(&["upstream", "mirror", "mirror"])).unwrap();
        assert_eq!(
            overrides(&f),
            [(root.clone(), "[\"mirror\",\"upstream\"]".into())]
        );
        git(
            &root,
            &["remote", "add", "mirror", "https://github.com/mine/mirror"],
        );
        found(&f, "mine", "mirror", "p", &root);
        set(&f, &root, Some(&[])).unwrap();
        assert_eq!(
            flags(&f, &root),
            [
                ("mirror".into(), false),
                ("origin".into(), false),
                ("upstream".into(), false)
            ]
        );
        assert_eq!(lookup(&f, "mine", "mirror")["status"], "none");
    }

    #[test]
    fn ownership_conflicting_override_and_reset_write_nothing() {
        let f = fixture("ownership-override-conflict");
        let root = repo(&f, "root", "mine/fork", "PUBLIC/SHARED");
        let other = repo(&f, "other", "public/shared", "");
        register(&f, "p", root.clone());
        register(&f, "other", other);
        let err = refusal(&f, "repository_owned", || {
            set(&f, &root, Some(&["upstream"]))
        });
        assert!(err.contains("PUBLIC/SHARED belongs to other"), "{err}");
        set(&f, &root, Some(&[])).unwrap();
        git(
            &root,
            &[
                "remote",
                "set-url",
                "origin",
                "https://github.com/Public/Shared",
            ],
        );
        refusal(&f, "repository_owned", || set(&f, &root, None));
    }

    #[test]
    fn ownership_request_key_cache_and_attribution() {
        let f = fixture("ownership-cache");
        let root = repo(&f, "root", "mine/fork", "public/upstream");
        register(&f, "p", root.clone());
        let request = SetOwnedRemotesRequest {
            root: root.clone(),
            remotes: Some(vec!["upstream".into()]),
            request_key: Some("K".into()),
            actor: Some("operator".into()),
        };
        let first = f
            .store
            .with_principal("direct")
            .set_owned_remotes(request.clone())
            .unwrap();
        let row = f.store.journal_tail(0, 100).unwrap().entries.pop().unwrap();
        assert_eq!(
            (
                row.op.as_str(),
                row.actor.as_str(),
                row.principal.as_deref()
            ),
            ("set_owned_remotes", "operator", Some("direct"))
        );
        set(&f, &root, Some(&[])).unwrap();
        let before = image(&f);
        let retry = SetOwnedRemotesRequest {
            remotes: None,
            ..request
        };
        assert_eq!(
            f.store
                .with_principal("reserved:prefrontal-core")
                .set_owned_remotes(retry)
                .unwrap(),
            first
        );
        assert_eq!(image(&f), before);
        assert_eq!(overrides(&f), [(root, "[]".into())]);
    }

    #[test]
    fn ownership_replay_restores_names_without_git_and_verify_detects_drift() {
        let f = fixture("ownership-replay");
        // Replay reaches this owner before add_root, so the later config edit
        // would conflict if add_root rechecked today's repositories.
        let other = repo(&f, "other", "public/other", "");
        register(&f, "other", other.clone());
        let root = repo(&f, "root", "mine/fork", "public/upstream");
        register(&f, "p", root.clone());
        let extra = repo(&f, "extra", "mine/extra", "");
        add(&f, "p", &extra).unwrap();
        set(&f, &root, Some(&["upstream"])).unwrap();
        set(&f, &extra, Some(&[])).unwrap();
        // A later config edit creates a conflict that would refuse a live
        // registration or ownership write. Replay must not consult that config.
        git(
            &other,
            &[
                "remote",
                "set-url",
                "origin",
                "https://github.com/public/upstream",
            ],
        );
        git(
            &extra,
            &[
                "remote",
                "set-url",
                "origin",
                "https://github.com/public/upstream",
            ],
        );
        fs::remove_dir_all(Path::new(&root).join(".git")).unwrap();
        let before = image(&f);
        assert!(f.store.verify().unwrap().replay.ok);
        assert_eq!(image(&f), before, "verify must roll back its replay");
        f.store.rebuild().unwrap();
        assert_eq!(image(&f), before);
        f.store.db.with_conn_fenced(|tx| {
            tx.execute("UPDATE root_owned_remotes SET remotes_json='[\"origin\"]' WHERE canonical_root=?1", [&root])?;
            Ok(())
        }).unwrap();
        let drift = f.store.verify().unwrap();
        assert!(!drift.replay.ok);
        assert_eq!(
            drift
                .replay
                .tables
                .iter()
                .map(|t| t.table.as_str())
                .collect::<Vec<_>>(),
            ["root_owned_remotes"]
        );
        f.store.rebuild().unwrap();
        assert_eq!(image(&f), before);
        assert!(f.store.verify().unwrap().replay.ok);
    }

    #[test]
    fn ownership_validation_and_root_removal() {
        let f = fixture("ownership-remove");
        let root = repo(&f, "root", "mine/root", "");
        let extra = repo(&f, "extra", "mine/extra", "");
        refusal(&f, "not_found", || set(&f, &root, Some(&[])));
        register(&f, "p", root.clone());
        add(&f, "p", &extra).unwrap();
        refusal(&f, "invalid_params", || set(&f, &root, Some(&[""])));
        set(&f, &root, Some(&[])).unwrap();
        set(&f, &extra, Some(&[])).unwrap();
        f.store
            .remove_root(RemoveRootRequest {
                project_id: "p".into(),
                root: extra.clone(),
                actor: None,
            })
            .unwrap();
        assert_eq!(overrides(&f), [(root.clone(), "[]".into())]);
        add(&f, "p", &extra).unwrap();
        found(&f, "mine", "extra", "p", &extra);
        f.store
            .remove(RemoveRequest {
                project_id: Some("p".into()),
                ..Default::default()
            })
            .unwrap();
        assert!(overrides(&f).is_empty());
        f.store.rebuild().unwrap();
        assert!(overrides(&f).is_empty());
        assert!(f.store.verify().unwrap().replay.ok);
    }

    #[test]
    fn ownership_migration_6_defaults_existing_roots() {
        use cortexkit_store::{Isolation, StorageBackend, StorageDescriptor};
        let f = fixture("ownership-migration");
        let root = repo(&f, "legacy", "mine/legacy", "public/upstream");
        let descriptor = StorageDescriptor {
            module_id: "ownership-legacy".into(),
            storage_namespace: "tests".into(),
            isolation: Isolation::Module,
            backend: StorageBackend::Sqlite {
                path: f.root.join("legacy.db").to_string_lossy().into_owned(),
            },
        };
        let old =
            RegistryStore::open_with_migrations(&descriptor, &crate::MIGRATIONS[..5]).unwrap();
        old.apply_entry("register", &serde_json::to_string(&RegisterRequest {
            project_id:Some("legacy".into()), name:"Legacy".into(), roots:vec![root.clone()], ..Default::default()
        }).unwrap(), "legacy actor", None, |tx| {
            tx.execute("INSERT INTO project(project_id,name,implicit,created_at,updated_at) VALUES('legacy','Legacy',0,1,1)", [])?;
            tx.execute("INSERT INTO project_root(canonical_root,project_id,added_at) VALUES(?1,'legacy',1)", [&root])?;
            Ok(())
        }).unwrap();
        let before = old.journal_tail(0, 100).unwrap();
        drop(old);
        let mut store = RegistryStore::open(&descriptor).unwrap();
        store.set_root_records(true);
        assert_eq!(
            serde_json::to_value(store.journal_tail(0, 100).unwrap()).unwrap(),
            serde_json::to_value(before).unwrap()
        );
        assert_eq!(
            store.resolve_remote("mine", "legacy").unwrap().status,
            ResolveRemoteStatus::Found {
                project_id: "legacy".into(),
                root: root.clone()
            }
        );
        let record = store
            .resolve(&root)
            .unwrap()
            .root_fields
            .unwrap()
            .root_records
            .remove(0);
        assert_eq!(
            record.remotes.iter().map(|r| r.owned).collect::<Vec<_>>(),
            [true, false]
        );
        assert_eq!(
            store
                .read(|conn| conn
                    .query_row("SELECT COUNT(*) FROM root_owned_remotes", [], |row| row
                        .get::<_, i64>(0)))
                .unwrap(),
            0
        );
    }
}
