//! Stable machine-neutral root names. A key is chosen only when a root is
//! inserted; the journal, not today's checkout configuration, owns its mapping.

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    mutations::{append, canonical, domain, Action},
    shared_entry::RootKeyMapping,
    GitRemote, JournalWriter, RegistryError, RegistryStore,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RootKey {
    pub kind: String,
    pub root_key: String,
}

/// Select only effective owned remotes. Secondary remotes intentionally do not
/// reserve another key: every root has at most one machine-neutral name.
pub(crate) fn choose(remotes: &[GitRemote], label: Option<&str>) -> Option<RootKey> {
    let remote = remotes
        .iter()
        .filter(|r| r.owned)
        .map(|r| format!("{}/{}", r.owner.to_lowercase(), r.repo.to_lowercase()))
        .min();
    remote
        .map(|root_key| RootKey {
            kind: "remote".into(),
            root_key,
        })
        .or_else(|| {
            label.map(|root_key| RootKey {
                kind: "label".into(),
                root_key: root_key.into(),
            })
        })
}

pub(crate) fn incoming(
    conn: &Connection,
    project: &str,
    root: &str,
    label: Option<&str>,
) -> Result<Option<RootKey>, RegistryError> {
    if !crate::log_schema::log_enabled(conn)? {
        return Ok(None);
    }
    // Re-registering an existing root must not re-key it after its remotes or
    // ownership names change. Backfill is a separate explicit operation.
    if conn
        .query_row(
            "SELECT 1 FROM project_root WHERE canonical_root=?1",
            [root],
            |r| r.get::<_, i64>(0),
        )
        .optional()?
        .is_some()
    {
        return Ok(None);
    }
    let key = choose(&crate::ownership::root_remotes(conn, root)?, label);
    if let Some(key) = &key {
        if key.kind == "remote" {
            if let Some(owner) = conn.query_row(
                "SELECT project_id FROM project_root_key WHERE kind='remote' AND root_key=?1 AND project_id<>?2",
                [&key.root_key, project], |r| r.get::<_, String>(0),
            ).optional()? {
                return Err(domain("root_key_exists", format!("{} belongs to project_id {owner}; use attach_root", key.root_key)));
            }
        }
    }
    Ok(key)
}

pub(crate) fn assign(
    tx: &Transaction<'_>,
    project: &str,
    root: &str,
    key: Option<RootKey>,
    now: i64,
    mappings: &mut Vec<RootKeyMapping>,
) -> rusqlite::Result<()> {
    if let Some(key) = key {
        tx.execute(
            "INSERT OR IGNORE INTO project_root_key(project_id,kind,root_key,created_at) VALUES(?1,?2,?3,?4)",
            params![project, key.kind, key.root_key, now],
        )?;
        tx.execute(
            "UPDATE project_root SET root_key_kind=?1,root_key=?2 WHERE canonical_root=?3 AND project_id=?4",
            [&key.kind, &key.root_key, root, project],
        )?;
        mappings.push(RootKeyMapping {
            canonical_root: root.into(),
            kind: key.kind,
            root_key: key.root_key,
        });
    }
    Ok(())
}

pub(crate) fn journal_assignments(
    tx: &Transaction<'_>,
    mappings: Vec<RootKeyMapping>,
    actor: &str,
    now: i64,
    principal: &str,
) -> Result<(), RegistryError> {
    if !mappings.is_empty() {
        append(
            tx,
            "root_key.assign",
            &json!({"mappings": mappings}),
            actor,
            None,
            now,
            principal,
        )?;
    }
    Ok(())
}

pub(crate) fn mapped_key(conn: &Connection, root: &str) -> rusqlite::Result<Option<RootKey>> {
    // Older-schema fixtures must keep their pre-log reply shape as well.
    if !crate::log_schema::log_enabled(conn)? {
        return Ok(None);
    }
    conn.query_row(
        "SELECT root_key_kind,root_key FROM project_root WHERE canonical_root=?1 AND root_key IS NOT NULL",
        [root], |r| Ok(RootKey { kind: r.get(0)?, root_key: r.get(1)? }),
    ).optional()
}

pub(crate) fn mismatch(key: &RootKey, remotes: &[GitRemote]) -> bool {
    choose(
        remotes,
        (key.kind == "label").then_some(key.root_key.as_str()),
    )
    .as_ref()
        != Some(key)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolveRootKeyRequest {
    pub project_id: String,
    pub kind: String,
    pub root_key: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ResolveRootKeyReply {
    pub roots: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachRootRequest {
    pub path: String,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub actor: Option<String>,
    #[serde(default)]
    pub request_key: Option<String>,
}

/// A read-only match, not a reservation: attachment checks again in its write
/// transaction because shared key ownership may change after a preview.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AttachRootPreview {
    pub project_id: String,
    pub root: String,
    pub root_key: RootKey,
}

fn attach_match(
    conn: &Connection,
    req: &AttachRootRequest,
) -> Result<AttachRootPreview, RegistryError> {
    // Check before touching the checkout, even when the supplied path is absent.
    if !crate::log_schema::log_enabled(conn)? {
        return Err(domain(
            "identity_log_disabled",
            "root keys require an enabled identity log",
        ));
    }
    let root = canonical(&req.path)?;
    let key = choose(
        &crate::ownership::root_remotes(conn, &root)?,
        req.label.as_deref(),
    )
    .ok_or_else(|| {
        domain(
            "root_key_not_found",
            "no owned remote; supply project_id and label",
        )
    })?;
    let project = if key.kind == "remote" {
        let owner = conn
            .query_row(
                "SELECT project_id FROM project_root_key WHERE kind='remote' AND root_key=?1",
                [&key.root_key],
                |r| r.get::<_, String>(0),
            )
            .optional()?
            .ok_or_else(|| domain("root_key_not_found", &key.root_key))?;
        if req.project_id.as_ref().is_some_and(|id| id != &owner) {
            return Err(domain(
                "root_key_conflict",
                format!("{} belongs to project_id {owner}", key.root_key),
            ));
        }
        owner
    } else {
        let project = req
            .project_id
            .clone()
            .ok_or_else(|| domain("invalid_params", "a label needs project_id"))?;
        if conn
            .query_row(
                "SELECT 1 FROM project_root_key WHERE project_id=?1 AND kind=?2 AND root_key=?3",
                [&project, &key.kind, &key.root_key],
                |r| r.get::<_, i64>(0),
            )
            .optional()?
            .is_none()
        {
            return Err(domain("root_key_not_found", &key.root_key));
        }
        project
    };
    if let Some(owner) = conn
        .query_row(
            "SELECT project_id FROM project_root WHERE canonical_root=?1",
            [&root],
            |r| r.get::<_, String>(0),
        )
        .optional()?
    {
        if owner != project || mapped_key(conn, &root)?.as_ref() != Some(&key) {
            return Err(domain(
                "root_conflict",
                format!("{root} is a root of {owner}"),
            ));
        }
    } else {
        crate::binding::check_root_location(conn, &root)?;
    }
    Ok(AttachRootPreview {
        project_id: project,
        root,
        root_key: key,
    })
}

impl RegistryStore {
    /// Match the same remote or label key used by `attach_root`, without minting
    /// binding tokens, changing database cells, or taking the writer lock.
    pub fn preview_attach_root(
        &self,
        req: AttachRootRequest,
    ) -> Result<AttachRootPreview, RegistryError> {
        self.read(|conn| Ok(attach_match(conn, &req)))?
    }

    pub fn resolve_root_key(
        &self,
        req: ResolveRootKeyRequest,
    ) -> Result<ResolveRootKeyReply, RegistryError> {
        self.read(|conn| {
            if !crate::log_schema::log_enabled(conn)? { return Ok(None); }
            let roots = conn.prepare(
                "SELECT canonical_root FROM project_root WHERE project_id=?1 AND root_key_kind=?2 AND root_key=?3 ORDER BY canonical_root",
            )?.query_map([&req.project_id, &req.kind, &req.root_key], |r| r.get(0))?.collect::<rusqlite::Result<Vec<String>>>()?;
            Ok(Some(ResolveRootKeyReply { roots }))
        })?.ok_or_else(|| domain("identity_log_disabled", "root keys require an enabled identity log"))
    }

    pub fn attach_root(&self, req: AttachRootRequest) -> Result<Vec<u8>, RegistryError> {
        self.with_principal("entorhinal").attach_root(req)
    }
}

impl JournalWriter<'_> {
    pub fn attach_root(&self, mut req: AttachRootRequest) -> Result<Vec<u8>, RegistryError> {
        // Refuse before path checks or filesystem writes, even for missing paths.
        if !self.read(crate::log_schema::log_enabled)? {
            return Err(domain(
                "identity_log_disabled",
                "root keys require an enabled identity log",
            ));
        }
        req.path = canonical(&req.path)?;
        let request_key = req.request_key.clone();
        let actor = req.actor.clone().unwrap_or_else(|| "module".into());
        self.mutation("attach_root", request_key.as_deref(), |tx| {
            let matched = attach_match(tx, &req)?;
            let project = matched.project_id;
            let key = matched.root_key;
            if tx
                .query_row(
                    "SELECT 1 FROM project_root WHERE canonical_root=?1",
                    [&req.path],
                    |r| r.get::<_, i64>(0),
                )
                .optional()?
                .is_some()
            {
                return Ok(Action {
                    changed: false,
                    seq: None,
                    value: json!({"projectId":project,"root":req.path,"rootKey":key}),
                    payload: json!({}),
                });
            }
            let binding = crate::binding::fresh_binding(&req.path, &project, &self.ids)?;
            let payload = json!({"root":req.path,"projectId":project,"binding":binding});
            let now = crate::now_unix_millis();
            let seq = append(
                tx,
                "attach_root",
                &payload,
                &actor,
                request_key.as_deref(),
                now,
                self.principal,
            )?;
            replay_attach(tx, seq, &payload, now)?;
            let mut mappings = Vec::new();
            assign(
                tx,
                &project,
                &req.path,
                Some(key.clone()),
                now,
                &mut mappings,
            )?;
            journal_assignments(tx, mappings, &actor, now, self.principal)?;
            Ok(Action {
                changed: true,
                seq: Some(seq),
                value: json!({"projectId":project,"root":req.path,"rootKey":key}),
                payload,
            })
        })
    }
}

pub(crate) fn replay_attach(
    tx: &Transaction<'_>,
    seq: i64,
    value: &Value,
    now: i64,
) -> rusqlite::Result<()> {
    crate::binding::replay_root_op(tx, seq, "add_root", value, now)?;
    if let Some(binding) = value.get("binding").filter(|v| !v.is_null()) {
        crate::binding::replay_binding_op(tx, seq, "bind_root", binding)?;
    }
    Ok(())
}

pub(crate) fn disk_mismatches(conn: &Connection) -> rusqlite::Result<Vec<String>> {
    if !crate::log_schema::log_enabled(conn)? {
        return Ok(Vec::new());
    }
    let roots = conn.prepare("SELECT canonical_root FROM project_root WHERE root_key IS NOT NULL ORDER BY canonical_root")?
        .query_map([], |r| r.get::<_, String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
    let mut errors = Vec::new();
    for root in roots {
        let key = mapped_key(conn, &root)?.expect("selected mapped root");
        if mismatch(&key, &crate::ownership::root_remotes(conn, &root)?) {
            errors.push(format!(
                "root_key_mismatch:{root}:{}:{}",
                key.kind, key.root_key
            ));
        }
    }
    Ok(errors)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        mutations::tests::{code, register, result, Fixture},
        shared_entry::{canonical_bytes, SharedState},
        AddRootRequest, RegisterRequest, RemoveRequest, RemoveRootRequest, SeedImportRequest,
        SeedPair, SeedPayload, SetOwnedRemotesRequest, UpgradeImplicitRequest,
    };
    use rusqlite::types::Value as Cell;
    use std::{collections::BTreeMap, fs, process::Command};

    fn enable(f: &Fixture) {
        f.store
            .apply_entry("identity_log.enable", "{}", "test", None, |tx| {
                tx.execute("UPDATE identity_log_state SET state='enabled'", [])?;
                Ok(())
            })
            .unwrap();
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

    fn repo(f: &Fixture, name: &str, remote: &str) -> String {
        let root = f.dir(name);
        git(&root, &["init", "--quiet"]);
        git(
            &root,
            &[
                "remote",
                "add",
                "origin",
                &format!("https://github.com/{remote}.git"),
            ],
        );
        root
    }

    // Compare actual cells, including cached responses, assignment tags, kind,
    // timestamps and binding epochs. Disk-dependent wire fields are not a proxy
    // for the journal or the persisted projection.
    fn cells(f: &Fixture) -> BTreeMap<String, Vec<Vec<Cell>>> {
        f.store.read(|conn| {
            let mut result = BTreeMap::new();
            let tables = conn.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name<>'cortexkit_fence' ORDER BY name")?
                .query_map([], |r| r.get::<_, String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
            for table in tables {
                let mut stmt = conn.prepare(&format!("SELECT * FROM {table}"))?;
                let count = stmt.column_count();
                let mut rows = stmt.query_map([], |r| (0..count).map(|i| r.get(i)).collect::<rusqlite::Result<Vec<Cell>>>())?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                rows.sort_by_key(|row| format!("{row:?}"));
                result.insert(table, rows);
            }
            Ok(result)
        }).unwrap()
    }

    fn lookup(f: &Fixture, project: &str, kind: &str, key: &str) -> Vec<String> {
        f.store
            .resolve_root_key(ResolveRootKeyRequest {
                project_id: project.into(),
                kind: kind.into(),
                root_key: key.into(),
            })
            .unwrap()
            .roots
    }

    #[test]
    fn attach_preview_is_read_only_and_matches_the_written_remote_or_label_key() {
        let f = Fixture::new("attach-preview");
        let original = repo(&f, "original", "preview/remote");
        let clone = repo(&f, "clone", "preview/remote");
        let labelled = f.dir("labelled");
        let label_clone = f.dir("label-clone");
        let disabled = cells(&f);
        code(
            f.store
                .preview_attach_root(AttachRootRequest {
                    path: clone.clone(),
                    ..Default::default()
                })
                .unwrap_err(),
            "identity_log_disabled",
        );
        assert_eq!(disabled, cells(&f));
        enable(&f);
        register(&f, "remote-project", original);
        f.store
            .register(RegisterRequest {
                project_id: Some("label-project".into()),
                name: "Label".into(),
                roots: vec![labelled],
                label: Some("preview/remote".into()),
                ..Default::default()
            })
            .unwrap();
        for (path, project, kind, label) in [
            (clone, "remote-project", "remote", None),
            (
                label_clone,
                "label-project",
                "label",
                Some("preview/remote"),
            ),
        ] {
            let req = AttachRootRequest {
                path: path.clone(),
                project_id: Some(project.into()),
                label: label.map(str::to_string),
                ..Default::default()
            };
            let before = cells(&f);
            let head = f.store.generation().unwrap();
            let preview = f.store.preview_attach_root(req.clone()).unwrap();
            assert_eq!(
                preview,
                AttachRootPreview {
                    project_id: project.into(),
                    root: path.clone(),
                    root_key: RootKey {
                        kind: kind.into(),
                        root_key: "preview/remote".into()
                    },
                }
            );
            assert_eq!(
                before,
                cells(&f),
                "preview must leave every database cell unchanged"
            );
            assert_eq!(head, f.store.generation().unwrap());
            assert!(
                !std::path::Path::new(&path)
                    .join(".git")
                    .join(crate::INCARNATION_FILE)
                    .exists(),
                "preview must not mint a binding token"
            );
            let written = result(&f.store.attach_root(req).unwrap());
            assert_eq!(written["projectId"], preview.project_id);
            assert_eq!(written["root"], preview.root);
            assert_eq!(
                written["rootKey"],
                serde_json::to_value(preview.root_key).unwrap()
            );
            assert!(f.store.generation().unwrap() > head);
        }
    }

    #[test]
    fn identity_log_status_reads_durable_progress_and_pending_count_without_writing() {
        let f = Fixture::new("log-status");
        assert_eq!(
            f.store.identity_log_status().unwrap(),
            crate::IdentityLogStatus {
                state: "disabled".into(),
                last_applied_position: 0,
                last_seen_head: 0,
                pending_write_count: 0,
            }
        );
        for (index, state) in ["enabling", "joining", "enabled"].into_iter().enumerate() {
            let applied = (index + 1) as i64;
            f.store.apply_entry("test.progress", "{}", "test", None, |tx| {
                tx.execute("UPDATE identity_log_state SET state=?1,last_applied_position=?2,last_seen_head=?3 WHERE id=1", params![state, applied, applied + 3])?;
                tx.execute("INSERT INTO pending_entry(entry_id,expected_head) VALUES(?1,?2)", params![format!("pending-{index}"), applied])?;
                Ok(())
            }).unwrap();
            let before = cells(&f);
            assert_eq!(
                f.store.identity_log_status().unwrap(),
                crate::IdentityLogStatus {
                    state: state.into(),
                    last_applied_position: applied,
                    last_seen_head: applied + 3,
                    pending_write_count: applied,
                }
            );
            assert_eq!(before, cells(&f));
        }
    }

    fn assignment_rows(f: &Fixture) -> Vec<Value> {
        f.store.read(|conn| conn.prepare("SELECT payload_json FROM registry_journal WHERE op='root_key.assign' ORDER BY seq")?
            .query_map([], |r| r.get::<_, String>(0))?.map(|r| Ok(serde_json::from_str(&r?).unwrap())).collect()).unwrap()
    }

    fn assert_assignment(f: &Fixture, root: &str, key: &str) {
        assert!(assignment_rows(f).iter().any(|row| row
            == &json!({"mappings":[{"canonical_root":root,"kind":"remote","root_key":key}]})));
        f.store.read(|conn| {
            assert_eq!(conn.query_row("SELECT COUNT(*) FROM registry_journal WHERE op='root_key.assign' AND (stream<>'local' OR origin<>'here' OR request_key IS NOT NULL OR entry IS NOT NULL)", [], |r| r.get::<_, i64>(0))?, 0);
            Ok(())
        }).unwrap();
    }

    fn rebuild_without_config(f: &Fixture, roots: &[&str]) {
        let before = cells(f);
        for root in roots {
            fs::remove_file(std::path::Path::new(root).join(".git/config")).unwrap();
        }
        // Verify may report a current remote mismatch, but replay itself must be
        // clean, and rebuilding must preserve every stored cell without git.
        assert!(f.store.verify().unwrap().replay.ok);
        assert!(f.store.rebuild().unwrap().replay.ok);
        assert_eq!(before, cells(f));
    }

    fn share(source: &Fixture, target: &Fixture) {
        let image = source
            .store
            .read(|conn| Ok(SharedState::capture(conn)?.snapshot(vec![])))
            .unwrap();
        let bytes = canonical_bytes(&image);
        target.store.apply_entry("shared.snapshot", "{}", "test", None, |tx| {
            image.apply(tx, 0)?;
            tx.execute("UPDATE registry_journal SET stream='shared',origin='log',entry=?1 WHERE seq=(SELECT MAX(seq) FROM registry_journal)", [&bytes])?;
            Ok(())
        }).unwrap();
    }

    fn insert(f: &Fixture, op: &str, root: &str) -> Result<Vec<u8>, RegistryError> {
        match op {
            "register" => f.store.register(RegisterRequest {
                project_id: Some("new".into()),
                name: "new".into(),
                roots: vec![root.into()],
                request_key: Some("write".into()),
                ..Default::default()
            }),
            "add_root" => f.store.add_root(AddRootRequest {
                project_id: "new".into(),
                root: root.into(),
                ..Default::default()
            }),
            "upgrade_implicit" => f.store.upgrade_implicit(UpgradeImplicitRequest {
                project_id: Some("new".into()),
                name: "new".into(),
                implicit_id: crate::implicit_project_id(root),
                roots: vec![root.into()],
                request_key: Some("write".into()),
                ..Default::default()
            }),
            "seed_import" => f.store.seed_import(SeedImportRequest {
                source: "mc".into(),
                request_key: Some("write".into()),
                exclude_home_scoped: false,
                payload: SeedPayload {
                    pairs: vec![SeedPair {
                        canonical_root: root.into(),
                        mc_identity: "git:new".into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                ..Default::default()
            }),
            _ => unreachable!(),
        }
    }

    fn root_inserter(op: &str) {
        let f = Fixture::new(op);
        enable(&f);
        if op == "add_root" {
            register(&f, "new", f.dir("original"));
        }
        let root = repo(&f, "checkout", "owned/new");
        let reply = insert(&f, op, &root).unwrap();
        let project = f.store.resolve(&root).unwrap().project_id;
        assert_eq!(
            lookup(&f, &project, "remote", "owned/new"),
            vec![root.clone()]
        );
        assert_assignment(&f, &root, "owned/new");
        assert_eq!(result(&reply)["generation"], f.store.generation().unwrap());
        rebuild_without_config(&f, &[&root]);

        let owner = Fixture::new(&format!("{op}-owner"));
        enable(&owner);
        register(&owner, "owner", repo(&owner, "original", "taken/key"));
        let loser = Fixture::new(&format!("{op}-loser"));
        enable(&loser);
        share(&owner, &loser);
        if op == "add_root" {
            register(&loser, "new", loser.dir("local"));
        }
        let conflicting = repo(&loser, "conflicting", "taken/key");
        let before = cells(&loser);
        let error = insert(&loser, op, &conflicting).unwrap_err();
        assert!(
            error.to_string().contains("owner")
                && error.to_string().contains("taken/key")
                && error.to_string().contains("attach_root")
        );
        code(error, "root_key_exists");
        assert_eq!(before, cells(&loser));
    }

    #[test]
    fn key_choice_uses_owned_minimum_then_label_then_none() {
        let remote = |name: &str, owner: &str, repo: &str, owned| GitRemote {
            name: name.into(),
            owner: owner.into(),
            repo: repo.into(),
            owned,
        };
        let remotes = vec![
            remote("origin", "Z", "Z", true),
            remote("upstream", "0", "0", false),
            remote("other", "A", "A", true),
        ];
        assert_eq!(
            choose(&remotes[..1], Some("ignored")),
            Some(RootKey {
                kind: "remote".into(),
                root_key: "z/z".into()
            })
        );
        assert_eq!(
            choose(&remotes, Some("ignored")),
            Some(RootKey {
                kind: "remote".into(),
                root_key: "a/a".into()
            })
        );
        assert_eq!(
            choose(&remotes[1..2], Some("a/a")),
            Some(RootKey {
                kind: "label".into(),
                root_key: "a/a".into()
            })
        );
        assert_eq!(choose(&[], None), None);
    }

    #[test]
    fn register_assigns_and_refuses_shared_remote_conflicts() {
        root_inserter("register");
    }
    #[test]
    fn add_root_assigns_and_refuses_shared_remote_conflicts() {
        root_inserter("add_root");
    }
    #[test]
    fn upgrade_implicit_assigns_and_refuses_shared_remote_conflicts() {
        root_inserter("upgrade_implicit");
    }
    #[test]
    fn seed_import_assigns_and_refuses_shared_remote_conflicts() {
        root_inserter("seed_import");
    }

    #[test]
    fn kind_and_text_distinguish_labels_and_remotes_through_rebuild() {
        let f = Fixture::new("typed-keys");
        enable(&f);
        let z = repo(&f, "z-clone", "equal/text");
        let a = repo(&f, "a-clone", "equal/text");
        register(&f, "P", z.clone());
        f.store
            .add_root(AddRootRequest {
                project_id: "P".into(),
                root: a.clone(),
                ..Default::default()
            })
            .unwrap();
        let labeled = f.dir("label");
        f.store
            .add_root(AddRootRequest {
                project_id: "P".into(),
                root: labeled.clone(),
                label: Some("equal/text".into()),
                ..Default::default()
            })
            .unwrap();
        let unlabeled = f.dir("local-only");
        f.store
            .add_root(AddRootRequest {
                project_id: "P".into(),
                root: unlabeled.clone(),
                ..Default::default()
            })
            .unwrap();
        f.store
            .register(RegisterRequest {
                project_id: Some("Q".into()),
                name: "Q".into(),
                roots: vec![f.dir("other-label")],
                label: Some("equal/text".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            lookup(&f, "P", "remote", "equal/text"),
            vec![a.clone(), z.clone()]
        );
        assert_eq!(
            lookup(&f, "P", "label", "equal/text"),
            vec![labeled.clone()]
        );
        assert_eq!(lookup(&f, "P", "remote", "missing"), Vec::<String>::new());
        assert!(f
            .store
            .read(|conn| mapped_key(conn, &unlabeled))
            .unwrap()
            .is_none());
        rebuild_without_config(&f, &[&a, &z]);
        assert_eq!(lookup(&f, "P", "remote", "equal/text"), vec![a, z]);
        assert_eq!(lookup(&f, "P", "label", "equal/text"), vec![labeled]);
    }

    #[test]
    fn local_key_columns_require_a_valid_kind_and_paired_text() {
        let f = Fixture::new("paired-key-columns");
        register(&f, "P", f.dir("root"));
        let before = cells(&f);
        for sql in [
            "UPDATE project_root SET root_key='only-text'",
            "UPDATE project_root SET root_key_kind='label'",
            "UPDATE project_root SET root_key_kind='bogus',root_key='text'",
        ] {
            assert!(
                f.store
                    .db
                    .with_conn_fenced(|tx| tx.execute(sql, []))
                    .is_err(),
                "{sql}"
            );
            assert_eq!(before, cells(&f));
        }
    }

    #[test]
    fn attach_uses_existing_keys_never_mints_and_starts_unapproved() {
        let a = Fixture::new("attach-source");
        enable(&a);
        register(&a, "P", repo(&a, "root", "attach/key"));
        a.store
            .register(RegisterRequest {
                project_id: Some("P".into()),
                name: "P".into(),
                roots: vec![a.dir("label")],
                label: Some("operator-label".into()),
                ..Default::default()
            })
            .unwrap();
        let b = Fixture::new("attach-target");
        enable(&b);
        share(&a, &b);
        let clone = repo(&b, "clone", "attach/key");
        let before = cells(&b);
        code(
            b.store
                .attach_root(AttachRootRequest {
                    path: clone.clone(),
                    project_id: Some("wrong".into()),
                    ..Default::default()
                })
                .unwrap_err(),
            "root_key_conflict",
        );
        assert_eq!(before, cells(&b));
        let reply = b
            .store
            .attach_root(AttachRootRequest {
                path: clone.clone(),
                request_key: Some("attach".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(result(&reply)["projectId"], "P");
        assert_eq!(
            result(&reply)["rootKey"],
            json!({"kind":"remote","rootKey":"attach/key"})
        );
        let records = b
            .store
            .trust(&clone)
            .unwrap()
            .root_fields
            .unwrap()
            .root_records;
        assert_eq!(records[0].identity, "bound");
        assert_eq!(records[0].approval.state, "unapproved");
        assert!(records[0].registration_epoch.is_some());
        assert_assignment(&b, &clone, "attach/key");
        assert_eq!(lookup(&b, "P", "remote", "attach/key"), vec![clone.clone()]);
        let label_root = b.dir("label-checkout");
        b.store
            .attach_root(AttachRootRequest {
                path: label_root.clone(),
                project_id: Some("P".into()),
                label: Some("operator-label".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(lookup(&b, "P", "label", "operator-label"), vec![label_root]);
        let unknown = repo(&b, "unknown", "not/registered");
        let no_key = b.dir("no-key");
        for req in [
            AttachRootRequest {
                path: unknown,
                ..Default::default()
            },
            AttachRootRequest {
                path: no_key.clone(),
                ..Default::default()
            },
            AttachRootRequest {
                path: no_key,
                project_id: Some("P".into()),
                label: Some("missing".into()),
                ..Default::default()
            },
        ] {
            let before = cells(&b);
            code(b.store.attach_root(req).unwrap_err(), "root_key_not_found");
            assert_eq!(before, cells(&b));
        }
        let before = cells(&b);
        assert_eq!(
            b.store
                .attach_root(AttachRootRequest {
                    path: clone.clone(),
                    request_key: Some("attach".into()),
                    ..Default::default()
                })
                .unwrap(),
            reply
        );
        assert_eq!(before, cells(&b));
        rebuild_without_config(&b, &[&clone]);
    }

    #[test]
    fn never_enabled_key_ops_refuse_all_inputs_without_journal_or_projection_writes() {
        let f = Fixture::new("disabled-root-keys");
        let remote = repo(&f, "remote", "disabled/key");
        register(&f, "P", remote.clone());
        let label = f.dir("label");
        f.store
            .register(RegisterRequest {
                project_id: Some("Q".into()),
                name: "Q".into(),
                roots: vec![label.clone()],
                label: Some("operator".into()),
                ..Default::default()
            })
            .unwrap();
        let before = cells(&f);
        for (path, project, kind, root_key) in [
            (&remote, "P", "remote", "disabled/key"),
            (&label, "Q", "label", "operator"),
            (
                &f.root.join("missing").to_string_lossy().into_owned(),
                "missing",
                "remote",
                "absent/key",
            ),
        ] {
            code(
                f.store
                    .resolve_root_key(ResolveRootKeyRequest {
                        project_id: project.into(),
                        kind: kind.into(),
                        root_key: root_key.into(),
                    })
                    .unwrap_err(),
                "identity_log_disabled",
            );
            code(
                f.store
                    .attach_root(AttachRootRequest {
                        path: path.clone(),
                        project_id: Some(project.into()),
                        label: Some(root_key.into()),
                        ..Default::default()
                    })
                    .unwrap_err(),
                "identity_log_disabled",
            );
            assert_eq!(before, cells(&f));
        }
        assert!(assignment_rows(&f).is_empty());
        assert!(f.store.verify().unwrap().ok);
        assert!(f.store.rebuild().unwrap().replay.ok);
        assert_eq!(before, cells(&f));
    }

    #[test]
    fn minted_ids_refuse_remote_projects_only_when_enabled() {
        for op in ["register", "upgrade_implicit"] {
            for enabled in [false, true] {
                let a = Fixture::new("mint-source");
                let b = Fixture::new("mint-target");
                if enabled {
                    enable(&a);
                    enable(&b);
                }
                let root = repo(&a, "same-path", "first/repository");
                let write = |store: &RegistryStore| match op {
                    "register" => store.register(RegisterRequest {
                        name: "same name".into(),
                        roots: vec![root.clone()],
                        ..Default::default()
                    }),
                    _ => store.upgrade_implicit(UpgradeImplicitRequest {
                        name: "same name".into(),
                        roots: vec![root.clone()],
                        implicit_id: crate::implicit_project_id(&root),
                        ..Default::default()
                    }),
                };
                let id = result(&write(&a.store).unwrap())["projectId"]
                    .as_str()
                    .unwrap()
                    .to_owned();
                share(&a, &b);
                git(
                    &root,
                    &[
                        "remote",
                        "set-url",
                        "origin",
                        "https://github.com/second/repository.git",
                    ],
                );
                let before = cells(&b);
                if enabled {
                    let error = write(&b.store).unwrap_err();
                    assert!(error.to_string().contains(&id));
                    code(error, "project_id_occupied");
                    assert_eq!(before, cells(&b));
                    assert!(lookup(&b, &id, "remote", "second/repository").is_empty());
                } else {
                    assert_eq!(result(&write(&b.store).unwrap())["projectId"], id);
                    assert_eq!(b.store.resolve(&root).unwrap().project_id, id);
                }
            }
        }
    }

    #[test]
    fn last_local_root_removal_preserves_shared_identity_only_when_enabled() {
        for enabled in [false, true] {
            let f = Fixture::new("last-root");
            if enabled {
                enable(&f);
            }
            let root = repo(&f, "root", "last/key");
            register(&f, "P", root.clone());
            let before = cells(&f);
            let removal = f.store.remove_root(RemoveRootRequest {
                project_id: "P".into(),
                root: root.clone(),
                ..Default::default()
            });
            if enabled {
                removal.unwrap();
                assert!(lookup(&f, "P", "remote", "last/key").is_empty());
                assert_eq!(
                    f.store
                        .read(|conn| conn.query_row(
                            "SELECT COUNT(*) FROM project_root_key WHERE project_id='P'",
                            [],
                            |r| r.get::<_, i64>(0)
                        ))
                        .unwrap(),
                    1
                );
                assert_eq!(f.store.resolve_project_id("P").unwrap().via, "current");
                assert!(f.store.verify().unwrap().ok);
                let after = cells(&f);
                assert!(f.store.rebuild().unwrap().replay.ok);
                assert_eq!(after, cells(&f));
            } else {
                code(removal.unwrap_err(), "last_root");
                assert_eq!(before, cells(&f));
            }
        }
    }

    #[test]
    fn changed_owned_remotes_report_mismatch_without_rekeying() {
        let f = Fixture::new("stable-root-key");
        enable(&f);
        let root = repo(&f, "root", "original/key");
        git(
            &root,
            &[
                "remote",
                "add",
                "other",
                "https://github.com/changed/key.git",
            ],
        );
        register(&f, "P", root.clone());
        assert!(f.store.verify().unwrap().ok);
        f.store
            .set_owned_remotes(SetOwnedRemotesRequest {
                root: root.clone(),
                remotes: Some(vec!["other".into()]),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            lookup(&f, "P", "remote", "original/key"),
            vec![root.clone()]
        );
        assert!(lookup(&f, "P", "remote", "changed/key").is_empty());
        let records = f
            .store
            .trust(&root)
            .unwrap()
            .root_fields
            .unwrap()
            .root_records;
        assert_eq!(
            records[0].root_key.as_ref().unwrap().root_key,
            "original/key"
        );
        assert_eq!(records[0].root_key_mismatch, Some(true));
        let verify = f.store.verify().unwrap();
        assert!(!verify.ok && verify.replay.ok);
        assert!(verify
            .mismatches
            .iter()
            .any(|m| m.starts_with("root_key_mismatch:")));
        let before = cells(&f);
        f.store.rebuild().unwrap();
        assert_eq!(before, cells(&f));
    }

    #[test]
    fn secondary_remote_does_not_reserve_a_shared_key() {
        let a = Fixture::new("secondary-source");
        let root = repo(&a, "root", "z/z");
        git(
            &root,
            &["remote", "add", "other", "https://github.com/a/a.git"],
        );
        register(&a, "P", root.clone());
        a.store
            .set_owned_remotes(SetOwnedRemotesRequest {
                root: root.clone(),
                remotes: Some(vec!["origin".into(), "other".into()]),
                ..Default::default()
            })
            .unwrap();
        enable(&a);
        // Install a recorded backfill for an existing root with two owned
        // remotes. A shared snapshot carries only its chosen key, not the
        // checkout's secondary remotes or any of its local paths.
        a.store.apply_entry("root_key.backfill", &json!({"mappings":[{"canonical_root":root,"kind":"remote","root_key":"a/a"}]}).to_string(), "test", None, |tx| {
            tx.execute("INSERT INTO project_root_key VALUES('P','remote','a/a',1)", [])?;
            crate::shared_entry::replay_root_keys(tx, json!({"mappings":[{"canonical_root":root,"kind":"remote","root_key":"a/a"}]}))?;
            Ok(())
        }).unwrap();
        assert_eq!(
            choose(
                &a.store
                    .read(|conn| crate::ownership::root_remotes(conn, &root))
                    .unwrap(),
                None
            )
            .unwrap()
            .root_key,
            "a/a"
        );
        let b = Fixture::new("secondary-target");
        enable(&b);
        share(&a, &b);
        let secondary = repo(&b, "root", "z/z");
        register(&b, "Q", secondary.clone());
        assert_eq!(lookup(&b, "Q", "remote", "z/z"), vec![secondary]);
        assert_eq!(
            b.store
                .read(
                    |conn| conn.query_row("SELECT COUNT(*) FROM project_root_key", [], |r| r
                        .get::<_, i64>(0))
                )
                .unwrap(),
            2
        );
    }

    #[test]
    fn successor_moves_remote_keys_and_collapses_duplicate_labels() {
        let f = Fixture::new("successor-keys");
        enable(&f);
        let source = repo(&f, "source", "source/key");
        let target = repo(&f, "target", "target/key");
        register(&f, "P", source.clone());
        register(&f, "Q", target.clone());
        let source_label = f.dir("source-label");
        let target_label = f.dir("target-label");
        for (project, root) in [("P", &source_label), ("Q", &target_label)] {
            f.store
                .add_root(AddRootRequest {
                    project_id: project.into(),
                    root: root.clone(),
                    label: Some("same-label".into()),
                    ..Default::default()
                })
                .unwrap();
        }
        f.store
            .remove(RemoveRequest {
                project_id: Some("P".into()),
                successor_project_id: Some("Q".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            lookup(&f, "Q", "remote", "source/key"),
            vec![source.clone()]
        );
        assert_eq!(
            lookup(&f, "Q", "remote", "target/key"),
            vec![target.clone()]
        );
        assert_eq!(
            lookup(&f, "Q", "label", "same-label"),
            vec![source_label, target_label]
        );
        assert_eq!(
            f.store
                .read(|conn| conn.query_row(
                    "SELECT COUNT(*) FROM project_root_key WHERE project_id='Q' AND kind='label'",
                    [],
                    |r| r.get::<_, i64>(0)
                ))
                .unwrap(),
            1
        );
        assert!(lookup(&f, "P", "remote", "source/key").is_empty());
        rebuild_without_config(&f, &[&source, &target]);
    }
}
