//! Operator-confirmed project folder writes and checkout proofs.

use std::cell::RefCell;
use std::path::Path;

use rusqlite::{Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::binding::{self, BindingRow, DiskIdentity, PreparedBinding};
use crate::mutations::{canonical, wire_value, Action};
use crate::{
    domain, AddRootRequest, AttachRootRequest, JournalWriter, RegisterRequest, RegistryError,
    RegistryStore,
};

#[derive(Clone)]
pub enum FolderWrite {
    Register(RegisterRequest),
    Attach(AttachRootRequest),
    Add(AddRootRequest),
    Approve {
        path: String,
        actor: String,
        project: bool,
    },
}

pub enum FolderPrecheck {
    Cached(Vec<u8>),
    Ready(Box<FolderPlan>),
}

#[derive(Clone)]
pub struct FolderPlan {
    summary: String,
    target: Target,
    project_before: Option<Value>,
    roots: Vec<RootSnapshot>,
    parent_files: Vec<Option<String>>,
    placement: Option<String>,
}

impl FolderPlan {
    pub fn summary(&self) -> &str {
        &self.summary
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Checkout {
    root_file: Option<String>,
    git_dir: Option<(String, String)>,
    token: Option<String>,
    reason: Option<String>,
}

impl Checkout {
    pub(crate) fn read(root: &str) -> Self {
        let path = Path::new(root);
        let root_file = crate::file_id(path);
        let mut checkout = Self {
            root_file,
            git_dir: None,
            token: None,
            reason: None,
        };
        let common = match binding::git_common_dir(path) {
            Ok(common) => common,
            Err(reason) => {
                checkout.reason = Some(reason);
                return checkout;
            }
        };
        checkout.git_dir =
            crate::file_id(&common).map(|id| (common.to_string_lossy().into_owned(), id));
        match binding::read_token(&common) {
            DiskIdentity::Token(token) => checkout.token = Some(token),
            DiskIdentity::NoToken => {}
            DiskIdentity::Unverifiable(reason) => checkout.reason = Some(reason),
        }
        if checkout.root_file.is_none() || checkout.git_dir.is_none() {
            checkout.reason = Some("checkout file identity is unavailable".into());
        }
        checkout
    }

    pub(crate) fn can_approve(&self) -> bool {
        self.reason.is_none()
    }

    // A binding attempt may have created the first marker between checks.
    // Only its absence may change; the root and Git directory must be the
    // same directories, and an existing marker must still match exactly.
    pub(crate) fn matches(&self, current: &Self) -> bool {
        self.root_file == current.root_file
            && self.git_dir == current.git_dir
            && self.reason == current.reason
            && (self.token.is_none() || self.token == current.token)
    }
}

#[derive(Clone)]
struct RootSnapshot {
    root: String,
    owner: Option<(String, i64)>,
    binding: Option<BindingRow>,
    approved: Option<i64>,
    checkout: Checkout,
}

impl RootSnapshot {
    fn read(conn: &Connection, root: &str) -> Result<Self, RegistryError> {
        let owner = conn
            .query_row(
                "SELECT project_id,added_at FROM project_root WHERE canonical_root=?1",
                [root],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let binding = binding::active_binding(conn, root)?;
        let approved = match &binding {
            Some(bound) => binding::approval_for(conn, &bound.epoch)?.journal_seq,
            None => None,
        };
        Ok(Self {
            root: root.into(),
            owner,
            binding,
            approved,
            checkout: Checkout::read(root),
        })
    }

    fn matches(&self, current: &Self) -> bool {
        self.root == current.root
            && self.owner == current.owner
            && self.binding == current.binding
            && self.approved == current.approved
            && self.checkout.matches(&current.checkout)
    }

    fn bound(&self, op: &str) -> Result<&BindingRow, RegistryError> {
        let replaced = self.binding.is_some() && self.checkout.can_approve();
        self.binding
            .as_ref()
            .filter(|bound| self.checkout.token.as_ref() == Some(&bound.incarnation))
            .ok_or_else(|| {
                domain(
                    if replaced && op == "approve_root" {
                        "root_replaced"
                    } else {
                        "root_not_bound"
                    },
                    format!(
                        "{} is {}; nothing is approved",
                        self.root,
                        if replaced { "replaced" } else { "not bound" }
                    ),
                )
            })
    }
}

#[derive(Clone, PartialEq, Eq)]
struct Target {
    project: String,
    name: String,
    roots: Vec<String>,
    keys: Vec<Option<crate::RootKey>>,
    parents: Vec<String>,
}

fn project_state(conn: &Connection, project: &str) -> Result<Option<Value>, RegistryError> {
    Ok(conn.query_row(
        "SELECT name,domain,workspace_id,created_at FROM project LEFT JOIN project_workspace USING(project_id) WHERE project_id=?1",
        [project], |r| Ok(json!([r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?, r.get::<_, Option<String>>(2)?, r.get::<_, i64>(3)?])),
    ).optional()?)
}

fn stale() -> RegistryError {
    domain(
        "operator_approval_stale",
        "the project or checkout changed after operator confirmation",
    )
}

fn stale_on_domain(error: RegistryError) -> RegistryError {
    if matches!(error, RegistryError::Domain { .. }) {
        stale()
    } else {
        error
    }
}

// Quote a folder for the presence prompt as the person would type it. Debug
// formatting would double every Windows backslash, so only quotes and
// control characters are escaped.
fn quote_folder(root: &str) -> String {
    let mut quoted = String::with_capacity(root.len() + 2);
    quoted.push('"');
    for c in root.chars() {
        match c {
            '"' => quoted.push_str("\\\""),
            c if c.is_control() => quoted.extend(c.escape_default()),
            c => quoted.push(c),
        }
    }
    quoted.push('"');
    quoted
}

fn folder_names(roots: &[String]) -> String {
    let quoted = quote_folder;
    let full = roots
        .iter()
        .map(|root| quoted(root))
        .collect::<Vec<_>>()
        .join(", ");
    // State the shared ancestor once rather than repeating a long absolute
    // path in the prompt. Each suffix still names its whole path below it.
    if let [first, _, ..] = roots {
        if let Some(parent) = Path::new(first).ancestors().skip(1).find(|parent| {
            roots
                .iter()
                .all(|root| Path::new(root).starts_with(parent) && Path::new(root) != *parent)
        }) {
            let suffixes = roots
                .iter()
                .map(|root| {
                    quoted(
                        &Path::new(root)
                            .strip_prefix(parent)
                            .unwrap()
                            .to_string_lossy(),
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            let short = format!("{suffixes} in {}", quote_folder(&parent.to_string_lossy()));
            if short.chars().count() < full.chars().count() {
                return short;
            }
        }
    }
    full
}

impl FolderWrite {
    pub fn op(&self) -> &'static str {
        match self {
            Self::Register(_) => "register",
            Self::Attach(_) => "attach_root",
            Self::Add(_) => "add_root",
            Self::Approve { project: true, .. } => "approve_project",
            Self::Approve { project: false, .. } => "approve_root",
        }
    }

    pub fn request_key(&self) -> Option<&str> {
        match self {
            Self::Register(req) => req.request_key.as_deref(),
            Self::Attach(req) => req.request_key.as_deref(),
            _ => None,
        }
    }

    pub fn label(&self) -> Option<&str> {
        match self {
            Self::Register(req) => req.label.as_deref(),
            Self::Attach(req) => req.label.as_deref(),
            Self::Add(req) => req.label.as_deref(),
            Self::Approve { .. } => None,
        }
    }

    fn actor(&self) -> &str {
        match self {
            Self::Register(req) => req.actor.as_deref(),
            Self::Attach(req) => req.actor.as_deref(),
            Self::Add(req) => req.actor.as_deref(),
            Self::Approve { actor, .. } => Some(actor.as_str()),
        }
        .unwrap_or("module")
    }

    fn normalize(&mut self) -> Result<(), RegistryError> {
        match self {
            Self::Register(req) => req.normalize()?,
            Self::Add(req) => req.root = canonical(&req.root)?,
            // attach_match retains its disabled-log refusal before path checks.
            Self::Attach(_) | Self::Approve { .. } => {}
        }
        Ok(())
    }

    fn roots(
        &self,
        store: &RegistryStore,
        conn: &Connection,
    ) -> Result<Vec<String>, RegistryError> {
        match self {
            Self::Register(req) => Ok(req.roots.clone()),
            Self::Add(req) => Ok(vec![req.root.clone()]),
            Self::Attach(req) => Ok(vec![crate::root_keys::attach_match(conn, req)?.root]),
            Self::Approve {
                project: false,
                path,
                ..
            } => Ok(vec![crate::canonical_query_path(Path::new(path))?.0]),
            Self::Approve {
                project: true,
                path,
                ..
            } => {
                let (path_input, checkouts) = crate::ResolvePath::read(path, None, false)?;
                let reply = store.resolve_walk_in(conn, &path_input, None, &checkouts)?;
                if !matches!(reply.via.as_str(), "root" | "walk") {
                    return Err(domain(
                        "not_found",
                        format!("{path} is not inside a registered root"),
                    ));
                }
                Ok(reply
                    .root_fields
                    .expect("walk includes root fields")
                    .root_records
                    .into_iter()
                    .map(|r| r.root)
                    .collect())
            }
        }
    }

    fn run_in(
        &self,
        writer: &JournalWriter<'_>,
        tx: &Transaction<'_>,
        roots: &[RootSnapshot],
        now: i64,
    ) -> Result<Action, RegistryError> {
        match self {
            Self::Register(req) => writer.register_in(tx, req, now, None),
            Self::Add(req) => writer.add_root_in(tx, req, now),
            Self::Attach(req) => {
                let mut req = req.clone();
                req.path.clone_from(&roots[0].root);
                writer.attach_root_in(tx, &req, now, false)
            }
            Self::Approve { project, .. } => {
                let op = self.op();
                let bindings = roots
                    .iter()
                    .map(|root| root.bound(op))
                    .collect::<Result<Vec<_>, _>>()?;
                let mut results = Vec::new();
                let mut last = None;
                for (root, bound) in roots.iter().zip(&bindings) {
                    let written = binding::approve_in(
                        tx,
                        &root.root,
                        &bound.epoch,
                        self.actor(),
                        writer.principal,
                        now,
                    )?;
                    results.push(json!({"root":root.root,"registrationEpoch":bound.epoch,"noop":written.is_none()}));
                    if written.is_some() {
                        last = written;
                    }
                }
                let project_id = &bindings[0].project_id;
                let value = if *project {
                    json!({"projectId":project_id,"roots":results})
                } else {
                    json!({"canonicalRoot":roots[0].root,"registrationEpoch":bindings[0].epoch})
                };
                let (seq, payload) =
                    last.map_or((None, Value::Null), |(seq, payload)| (Some(seq), payload));
                Ok(Action {
                    changed: seq.is_some(),
                    seq,
                    value,
                    payload,
                })
            }
        }
    }

    fn target(
        &self,
        conn: &Connection,
        roots: Vec<String>,
        action: &Action,
    ) -> Result<Target, RegistryError> {
        let project = if matches!(self, Self::Approve { project: false, .. }) {
            binding::active_binding(conn, &roots[0])?
                .expect("approved binding")
                .project_id
        } else {
            action.value["projectId"]
                .as_str()
                .expect("project write returns projectId")
                .into()
        };
        let name = conn.query_row(
            "SELECT name FROM project WHERE project_id=?1",
            [&project],
            |row| row.get(0),
        )?;
        let keys = roots
            .iter()
            .map(|root| crate::root_keys::mapped_key(conn, root))
            .collect::<Result<_, _>>()?;
        let parents = match self {
            Self::Register(req) => req.derived_root_parents.clone(),
            _ => Vec::new(),
        };
        Ok(Target {
            project,
            name,
            roots,
            keys,
            parents,
        })
    }

    pub fn precheck(
        &self,
        store: &RegistryStore,
        now: i64,
    ) -> Result<FolderPrecheck, RegistryError> {
        if let Some(body) = store.shared_request_cache(self.op(), self.request_key())? {
            return Ok(FolderPrecheck::Cached(body));
        }
        let mut request = self.clone();
        request.normalize()?;
        let mut copy = store.read(crate::read_connection::copy_snapshot)?;
        let tx = copy.transaction()?;
        let paths = request.roots(store, &tx)?;
        let roots = paths
            .iter()
            .map(|root| RootSnapshot::read(&tx, root))
            .collect::<Result<Vec<_>, _>>()?;
        let placement = RefCell::new(None);
        let mut writer = store.with_principal("direct");
        writer.probe = Some(&placement);
        let action = request.run_in(&writer, &tx, &roots, now)?;
        let target = request.target(&tx, paths, &action)?;
        let parent_files = target
            .parents
            .iter()
            .map(|path| crate::file_id(Path::new(path)))
            .collect();
        tx.rollback()?;
        let project_before = project_state(&copy, &target.project)?;
        let placement = placement.into_inner();
        let approval = roots.iter().any(|root| root.checkout.can_approve());
        let action = match (&request, approval) {
            (Self::Register(_), true) => "Register and approve",
            (Self::Register(_), false) => "Register",
            (Self::Add(_), true) => "Add and approve for",
            (Self::Add(_), false) => "Add to",
            (Self::Attach(_), true) => "Attach and approve for",
            (Self::Attach(_), false) => "Attach to",
            (Self::Approve { .. }, _) => "Approve for",
        };
        let mut summary = format!("{action} project {:?}", target.name);
        if !target.roots.is_empty() {
            summary.push_str(": ");
            summary.push_str(&folder_names(&target.roots));
        }
        for parent in &target.parents {
            summary.push_str(&format!(" (worker container {})", quote_folder(parent)));
        }
        if let Some(placement) = &placement {
            summary.push_str(&format!("; {placement}"));
        }
        if summary.chars().count() > crate::agent::OPERATOR_SUMMARY_LIMIT {
            return Err(domain(
                "operator_summary_too_long",
                "project and folders do not fit in one presence prompt",
            ));
        }
        Ok(FolderPrecheck::Ready(Box::new(FolderPlan {
            summary,
            target,
            project_before,
            roots,
            parent_files,
            placement,
        })))
    }

    pub fn commit(
        &self,
        writer: &JournalWriter<'_>,
        plan: &FolderPlan,
        now: i64,
    ) -> Result<Vec<u8>, RegistryError> {
        let mut request = self.clone();
        request.normalize().map_err(stale_on_domain)?;
        let op = request.op();
        let body = writer.mutation(op, request.request_key(), |tx| {
            let paths = request.roots(writer, tx).map_err(stale_on_domain)?;
            if paths != plan.target.roots
                || project_state(tx, &plan.target.project)? != plan.project_before
            {
                return Err(stale());
            }
            let roots = paths
                .iter()
                .map(|root| RootSnapshot::read(tx, root))
                .collect::<Result<Vec<_>, _>>()?;
            if !plan
                .roots
                .iter()
                .zip(&roots)
                .all(|(old, current)| old.matches(current))
            {
                return Err(stale());
            }
            let placement = RefCell::new(None);
            let mut check = writer.store.with_principal(writer.principal);
            check.probe = Some(&placement);
            let mut action = request
                .run_in(&check, tx, &roots, now)
                .map_err(stale_on_domain)?;
            if request.target(tx, paths, &action)? != plan.target
                || *placement.borrow() != plan.placement
                || !plan
                    .target
                    .parents
                    .iter()
                    .map(|path| crate::file_id(Path::new(path)))
                    .eq(plan.parent_files.iter().cloned())
            {
                return Err(stale());
            }
            if !matches!(request, Self::Approve { .. }) {
                approve_registered(
                    writer,
                    tx,
                    &mut action,
                    roots.iter().map(|r| (r.root.as_str(), &r.checkout)),
                    request.actor(),
                    now,
                )?;
            }
            Ok(action)
        })?;
        if op == "approve_project" {
            let response: Value = serde_json::from_slice(&body)
                .map_err(|e| domain("encode_failed", e.to_string()))?;
            return wire_value(
                json!({"projectId":response["result"]["projectId"],"roots":response["result"]["roots"]}),
            );
        }
        Ok(body)
    }
}

pub(crate) fn approve_registered<'a>(
    writer: &JournalWriter<'_>,
    tx: &Transaction<'_>,
    action: &mut Action,
    roots: impl IntoIterator<Item = (&'a str, &'a Checkout)>,
    actor: &str,
    now: i64,
) -> Result<(), RegistryError> {
    let project = action.value["projectId"]
        .as_str()
        .expect("registration returns projectId");
    let mut outcomes = Vec::new();
    let mut hub_approvals = Vec::new();
    for (root, approved) in roots {
        if !approved.matches(&Checkout::read(root)) {
            return Err(stale());
        }
        let before = binding::active_binding(tx, root)?;
        let approved_before = approval_sequence(tx, before.as_ref())?;
        let prepared = match &approved.reason {
            Some(reason) => Err(reason.clone()),
            None => match writer.prepare_hub_binding(root) {
                Ok(prepared) => Ok(prepared),
                Err(RegistryError::Domain { code, message, .. })
                    if code == "root_identity_unverifiable" =>
                {
                    Err(message)
                }
                Err(error) => return Err(error),
            },
        };
        let prepared = match prepared {
            Ok(binding) => {
                if !binding.still_present() || !approved.matches(&Checkout::read(root)) {
                    return Err(stale());
                }
                binding.write_at(tx, project, actor, writer.principal, now)?;
                let bound = binding::active_binding(tx, root)?.expect("binding was written");
                if let Some((seq, payload)) =
                    binding::approve_in(tx, root, &bound.epoch, actor, writer.principal, now)?
                {
                    if !action.changed {
                        action.changed = true;
                        action.seq = Some(seq);
                        action.payload = payload;
                    }
                }
                outcomes.push(json!({"root":root,"state":"approved"}));
                Some(binding)
            }
            Err(reason) => {
                outcomes.push(json!({"root":root,"state":"unapproved","reason":reason}));
                None
            }
        };
        hub_approvals.push(PendingFolder {
            root: root.into(),
            checkout: approved.clone(),
            before,
            approved_before,
            prepared,
        });
    }
    crate::pending::stage_folder_approvals(HubFolderApprovals {
        folders: hub_approvals,
        actor: actor.into(),
        principal: writer.principal.into(),
    });
    action.value["folderApprovals"] = json!(outcomes);
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingFolder {
    root: String,
    checkout: Checkout,
    before: Option<BindingRow>,
    approved_before: Option<i64>,
    prepared: Option<PreparedBinding>,
}

fn approval_sequence(
    conn: &Connection,
    previous: Option<&BindingRow>,
) -> Result<Option<i64>, RegistryError> {
    match previous {
        Some(bound) => Ok(binding::approval_for(conn, &bound.epoch)?.journal_seq),
        None => Ok(None),
    }
}

/// Saved on this machine until the shared project write lands in the log.
/// Checkout identities and approval never travel to other machines.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HubFolderApprovals {
    folders: Vec<PendingFolder>,
    actor: String,
    principal: String,
}

impl HubFolderApprovals {
    pub(crate) fn apply(
        &self,
        tx: &Transaction<'_>,
        project: &str,
        now: i64,
        seq: &mut i64,
    ) -> Result<(), RegistryError> {
        for folder in &self.folders {
            let current = binding::active_binding(tx, &folder.root)?;
            if !folder.checkout.matches(&Checkout::read(&folder.root))
                || current != folder.before
                || approval_sequence(tx, current.as_ref())? != folder.approved_before
                || folder
                    .prepared
                    .as_ref()
                    .is_some_and(|binding| !binding.still_present())
            {
                return Err(stale());
            }
        }
        for folder in &self.folders {
            let Some(binding) = &folder.prepared else {
                continue;
            };
            if let Some((bound, _)) =
                binding.write_at(tx, project, &self.actor, &self.principal, now)?
            {
                *seq = bound;
            }
            let bound = binding::active_binding(tx, &folder.root)?.expect("binding was written");
            if let Some((approved, _)) = crate::binding::approve_in(
                tx,
                &folder.root,
                &bound.epoch,
                &self.actor,
                &self.principal,
                now,
            )? {
                *seq = approved;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hub_store::{HubAttemptSend, HubAttemptWrite};
    use crate::mutations::tests::{code, Fixture};
    use crate::remote_apply::RemoteEntry;
    use crate::{HubContinuation, HubRoots};
    use std::rc::Rc;

    #[test]
    fn folder_presence_prompt_quotes_folders_as_typed() {
        // A Windows path must read with single backslashes, on every OS.
        assert_eq!(
            folder_names(&[r"C:\Users\a\repo".into()]),
            r#""C:\Users\a\repo""#
        );
        assert_eq!(quote_folder("a\"b\nc"), r#""a\"b\nc""#);
    }

    #[test]
    fn folder_presence_hub_projects_share_identity_but_keep_approval_local() {
        let local = Fixture::new("folder-hub-local");
        let follower = Fixture::new("folder-hub-follower");
        let author = "ab".repeat(16);
        for f in [&local, &follower] {
            f.store
                .apply_entry("identity_log.enable", "{}", "fixture", None, |tx| {
                    tx.execute(
                        "UPDATE identity_log_state SET state='enabled',hub_log_base=0 WHERE id=1",
                        [],
                    )?;
                    tx.execute("INSERT INTO self_identity VALUES(1,?1,1)", [&author])?;
                    Ok(())
                })
                .unwrap();
        }
        let root = local.dir("repo");
        let git = Path::new(&root).join(".git");
        std::fs::create_dir(&git).unwrap();
        std::fs::write(
            git.join("config"),
            "[remote \"origin\"]\nurl = https://github.com/test/presence.git\n",
        )
        .unwrap();
        let request = FolderWrite::Register(RegisterRequest {
            project_id: Some("P".into()),
            name: "Project".into(),
            roots: vec![root.clone()],
            ..Default::default()
        });
        let FolderPrecheck::Ready(plan) = request.precheck(&local.store, 70).unwrap() else {
            panic!("expected a prompt")
        };
        let captured = Rc::new(RefCell::new(None));
        let output = captured.clone();
        let id = "cd".repeat(16);
        let rolled_back = local.store.preview_hub_write_with_approvals(
            id.clone(),
            move |body, reply, approvals| {
                *output.borrow_mut() = Some((body, reply, approvals));
            },
            || request.commit(&local.store.with_principal("direct"), &plan, 70),
        );
        code(rolled_back.unwrap_err(), "hub_write_preview");
        assert_eq!(local.store.root_count("P").unwrap(), 0);
        let (bytes, reply, approvals) = captured.take().unwrap();
        assert!(approvals.is_some());
        let shared = std::str::from_utf8(&bytes).unwrap();
        for local_only in [
            &*root,
            "folderApprovals",
            "incarnation",
            "registrationEpoch",
        ] {
            assert!(
                !shared.contains(local_only),
                "shared entry contains {local_only}"
            );
        }
        let continuation = HubContinuation::prepare_with_approvals(
            &local.store,
            &id,
            70,
            reply,
            Some(HubRoots {
                project: "P".into(),
                roots: vec![root.clone()],
                parents: Vec::new(),
            }),
            None,
            None,
            approvals,
        )
        .unwrap();
        let write = HubAttemptWrite {
            request_key: "register".into(),
            params_digest: "digest".into(),
            key: "ck.fleet.idlog".into(),
            expected: 0,
            value: bytes.clone(),
            step: "fixture".into(),
        };
        let send = HubAttemptSend {
            op_id: id.clone(),
            sent_at_ms: 70,
            process: "fixture".into(),
        };
        assert!(local
            .store
            .persist_hub_attempt_with_local(&write, &send, None, Some(&continuation))
            .unwrap());
        let entry = RemoteEntry {
            position: 1,
            entry_id: id,
            author,
            signer: "fixture".into(),
            key_id: "fixture".into(),
            envelope_version: 1,
            kind: "change".into(),
            entry: bytes,
        };
        local.store.apply_hub_remote_entry(&entry, 0).unwrap();
        follower.store.apply_hub_remote_entry(&entry, 0).unwrap();
        assert_eq!(follower.store.root_count("P").unwrap(), 0);
        let state = || {
            local
                .store
                .trust(&root)
                .unwrap()
                .root_fields
                .unwrap()
                .root_records[0]
                .approval
                .state
        };
        assert_eq!(state(), "approved");
        assert!(local.store.verify().unwrap().ok);
        local.store.rebuild().unwrap();
        assert_eq!(state(), "approved");
    }
}
