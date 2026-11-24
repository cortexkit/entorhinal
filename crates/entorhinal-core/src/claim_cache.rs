//! Claim views come from the hub, or from local sharing state before the hub
//! can supply a claim view.

use crate::{agent, RegistryError, RegistryStore};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::Serialize;
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CachedClaim {
    pub holder: Option<String>,
    pub epoch: i64,
    pub cut_id: Option<String>,
    pub stale: bool,
    pub integrity_failed: bool,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct NodeSelf {
    pub author: Option<String>,
    pub display_name: Option<String>,
    pub sharing: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct AgentWhere {
    pub known: bool,
    pub status: &'static str,
    pub holder: Option<String>,
    pub holder_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub holder_box: Option<String>,
    pub epoch: i64,
    pub cut_id: Option<String>,
    pub read_at_ms: Option<i64>,
    pub stale: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub moving: Option<Value>,
    #[serde(rename = "moveErrors", skip_serializing_if = "Vec::is_empty")]
    pub move_errors: Vec<Value>,
    #[serde(rename = "moveWarnings", skip_serializing_if = "Vec::is_empty")]
    pub move_warnings: Vec<Value>,
    #[serde(rename = "moveDiagnostics", skip_serializing_if = "Vec::is_empty")]
    pub move_diagnostics: Vec<Value>,
}

pub(crate) fn self_author(conn: &Connection) -> rusqlite::Result<Option<String>> {
    conn.query_row(
        "SELECT self_author FROM self_identity WHERE id=1",
        [],
        |row| row.get(0),
    )
    .optional()
}

/// Returns false on conflict without replacing the persisted identity. The
/// caller records that conflict in health, independently of identity-log state.
pub(crate) fn learn_self_author(
    tx: &Transaction<'_>,
    author: &str,
    now: i64,
) -> rusqlite::Result<bool> {
    if let Some(known) = self_author(tx)? {
        return Ok(known == author);
    }
    tx.execute(
        "INSERT INTO self_identity(id,self_author,learned_at_ms) VALUES(1,?1,?2)",
        params![author, now],
    )?;
    Ok(true)
}

pub(crate) fn holder_name(
    conn: &Connection,
    holder: Option<&str>,
) -> rusqlite::Result<Option<String>> {
    conn.query_row(
        "SELECT display_name FROM member_name WHERE author=?1",
        [holder],
        |row| row.get(0),
    )
    .optional()
}

fn refusal(code: &str, detail: Option<serde_json::Value>) -> agent::AgentMutationError {
    agent::AgentMutationError {
        code: code.into(),
        message: code.into(),
        detail,
    }
}

pub(crate) fn check_hub_registration(
    tx: &Transaction<'_>,
    id: &str,
    context: &HubClaimContext<'_>,
) -> Result<(), agent::AgentMutationError> {
    let view = hub_claim_view(tx, id, context)?;
    if view.claim.integrity_failed {
        return Err(refusal("claim_integrity_failed", None));
    }
    if view.claim.stale || view.claim.epoch == 0 {
        return Err(refusal("claim_unavailable", None));
    }
    if view.status() != "held_here" {
        return Err(refusal(
            "held_elsewhere",
            Some(serde_json::json!({
                "holder":view.claim.holder,"holder_name":view.holder_name(tx)?,"epoch":view.claim.epoch
            })),
        ));
    }
    Ok(())
}

fn unclaimed(stale: bool, integrity_failed: bool) -> CachedClaim {
    CachedClaim {
        holder: None,
        epoch: 0,
        cut_id: None,
        stale,
        integrity_failed,
    }
}

impl RegistryStore {
    pub fn persisted_self_author(&self) -> Result<Option<String>, RegistryError> {
        self.read(self_author)
    }

    /// Read this machine's identity and name from the same snapshot, using
    /// the member-name lookup that supplies `agent.where`'s holder name.
    pub fn node_self(&self) -> Result<NodeSelf, RegistryError> {
        self.read(|conn| {
            let author = self_author(conn)?;
            let display_name = holder_name(conn, author.as_deref())?;
            let sharing = conn.query_row(
                "SELECT state FROM identity_log_state WHERE id=1",
                [],
                |row| row.get(0),
            )?;
            Ok(NodeSelf {
                author,
                display_name,
                sharing,
            })
        })
    }

    /// Whereabouts based on local sharing state when no hub claim view is available.
    pub fn agent_where(&self, id: &str) -> Result<AgentWhere, agent::AgentMutationError> {
        self.agent_where_with(id, |conn| load_claim_view(conn, self.is_foreign()))
    }

    pub fn hub_agent_where(
        &self,
        id: &str,
        context: &HubClaimContext<'_>,
    ) -> Result<AgentWhere, agent::AgentMutationError> {
        self.agent_where_with(id, |conn| hub_claim_view(conn, id, context))
    }

    fn agent_where_with(
        &self,
        id: &str,
        view: impl FnOnce(&Connection) -> rusqlite::Result<ClaimView>,
    ) -> Result<AgentWhere, agent::AgentMutationError> {
        self.read(|conn| Ok(agent_where_with(conn, id, view)))
            .map_err(|e| {
                refusal(
                    "storage_error",
                    Some(serde_json::json!({"message":e.to_string()})),
                )
            })?
    }
}

pub(crate) struct ClaimView {
    pub known: bool,
    pub claim: CachedClaim,
    pub own: Option<String>,
    pub read_at_ms: Option<i64>,
    holder_box: Option<String>,
    hub: bool,
}

pub struct HubClaimContext<'a> {
    pub local_box: &'a str,
    pub fresh: &'a dyn Fn(&str, Option<u64>) -> bool,
    pub holder_current: &'a dyn Fn(&str, u64, i64) -> bool,
    pub seed_expected: &'a dyn Fn(&str) -> bool,
}

pub(crate) fn hub_claim_view(
    conn: &Connection,
    id: &str,
    context: &HubClaimContext<'_>,
) -> rusqlite::Result<ClaimView> {
    let cached = crate::hub_store::kv_cached(conn, &format!("residence.{id}"))?;
    let body = cached.as_ref().and_then(|row| row.body.as_ref());
    let holder = body.and_then(|body| body["holder"].as_str());
    if context.local_box.is_empty() && holder.is_some() {
        return Ok(ClaimView {
            known: false,
            claim: unclaimed(true, false),
            own: None,
            read_at_ms: None,
            holder_box: None,
            hub: true,
        });
    }
    let revision = cached.as_ref().map(|row| row.revision);
    let integrity = cached.as_ref().is_some_and(|row| row.integrity_failed);
    let pending = body.is_none()
        && (crate::hub_store::agent_creator(conn, id, i64::MAX)?.is_some()
            || (context.seed_expected)(id));
    let mut claim = unclaimed(pending || !(context.fresh)(id, revision), integrity);
    let mut holder_box = None;
    if let Some(body) = body {
        claim.holder = holder.map(str::to_owned);
        claim.epoch = body["gen"].as_i64().unwrap_or_default();
        claim.cut_id = body["cut_id"].as_str().map(str::to_owned);
        // The local box knows its own ID even before the roster loads.
        holder_box = if holder == Some(context.local_box) {
            Some(context.local_box.to_owned())
        } else {
            let applied = conn.query_row(
                "SELECT last_applied_position FROM identity_log_state WHERE id=1",
                [],
                |row| row.get(0),
            )?;
            holder
                .filter(|holder| {
                    (context.holder_current)(
                        holder,
                        body["holder_ep"].as_u64().unwrap_or_default(),
                        applied,
                    )
                })
                .map(str::to_owned)
        };
    }
    Ok(ClaimView {
        known: cached.is_some() || !claim.stale,
        own: if body.is_none() || holder_box.is_some() {
            Some(context.local_box.to_owned())
        } else {
            None
        },
        claim,
        read_at_ms: cached.and_then(|row| row.read_at_ms),
        holder_box,
        hub: true,
    })
}

impl ClaimView {
    pub fn status(&self) -> &'static str {
        if self.claim.epoch == 0 {
            "unclaimed"
        } else if self.claim.holder.is_some() && self.claim.holder == self.own {
            "held_here"
        } else {
            "held_elsewhere"
        }
    }

    pub fn holder_name(&self, conn: &Connection) -> rusqlite::Result<Option<String>> {
        if self.own.is_some() {
            holder_name(conn, self.claim.holder.as_deref())
        } else {
            Ok(None)
        }
    }
}

/// `agent.where` and `agent.session_residence` share the sharing fallback and holder comparison.
pub(crate) fn load_claim_view(conn: &Connection, foreign: bool) -> rusqlite::Result<ClaimView> {
    // Only a local, disabled store can rule out a holder without a hub watch.
    let sharing_off = !foreign
        && conn
            .query_row(
                "SELECT state='disabled' FROM identity_log_state WHERE id=1",
                [],
                |r| r.get::<_, bool>(0),
            )
            .optional()?
            .unwrap_or(false);
    Ok(ClaimView {
        known: sharing_off,
        claim: unclaimed(!sharing_off, false),
        own: None,
        read_at_ms: None,
        holder_box: None,
        hub: false,
    })
}

pub(crate) fn agent_where_in(
    conn: &Connection,
    id: &str,
    foreign: bool,
) -> Result<AgentWhere, agent::AgentMutationError> {
    agent_where_with(conn, id, |conn| load_claim_view(conn, foreign))
}

fn agent_where_with(
    conn: &Connection,
    id: &str,
    view: impl FnOnce(&Connection) -> rusqlite::Result<ClaimView>,
) -> Result<AgentWhere, agent::AgentMutationError> {
    let row = agent::load_row(conn, id)?.ok_or_else(|| refusal("unknown_agent", None))?;
    if let Some(gone) = row.gone() {
        return Err(refusal("gone", Some(gone)));
    }
    let view = view(conn)?;
    if view.claim.integrity_failed {
        return Err(refusal("claim_integrity_failed", None));
    }
    let status = view.status();
    let holder_name = view.holder_name(conn)?;
    let claim = view.claim;
    let (moving, move_errors, move_warnings, move_diagnostics) =
        crate::move_store::where_moves(conn, id)?;
    Ok(AgentWhere {
        known: view.known,
        status,
        holder_name,
        holder_box: if view.hub {
            view.holder_box
        } else {
            crate::member_box::lookup(conn, claim.holder.as_deref())?
        },
        holder: claim.holder,
        epoch: claim.epoch,
        cut_id: claim.cut_id,
        read_at_ms: view.read_at_ms,
        stale: claim.stale,
        moving,
        move_errors,
        move_warnings,
        move_diagnostics,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SessionRegistration;
    use std::sync::atomic::{AtomicU64, Ordering};
    const ID: &str = "agent_0123456789abcdef";
    const SELF: &str = "01010101010101010101010101010101";
    const OTHER: &str = "02020202020202020202020202020202";
    static NEXT: AtomicU64 = AtomicU64::new(0);

    struct Fixture {
        store: RegistryStore,
        descriptor: cortexkit_store::StorageDescriptor,
        _cleanup: crate::scratch_cleanup::ScratchCleanup,
    }
    impl Fixture {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!(
                "claim-cache-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let descriptor = cortexkit_store::StorageDescriptor {
                module_id: "entorhinal-test".into(),
                storage_namespace: "claims".into(),
                isolation: cortexkit_store::Isolation::Module,
                backend: cortexkit_store::StorageBackend::Sqlite {
                    path: root.join("store.db").to_string_lossy().into_owned(),
                },
            };
            let store = RegistryStore::open(&descriptor).unwrap();
            store.db.with_conn_fenced(|tx| {
                tx.execute("INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms) VALUES(?1,'Ada','test','assistant',1,1)", [ID])?;
                tx.execute("INSERT INTO registry_journal(op,payload_json) VALUES(?1,'{}')", [agent::IDENTITY_MARKER_OP])?;
                Ok(())
            }).unwrap();
            Self {
                store,
                descriptor,
                _cleanup: crate::scratch_cleanup::ScratchCleanup(root),
            }
        }
        fn observe(&self, revision: u64, holder: &str) {
            let body =
                serde_json::json!({"holder":holder,"holder_ep":1,"gen":revision,"cut_id":"cut"});
            self.store
                .record_hub_kv(
                    &format!("residence.{ID}"),
                    revision,
                    Some((b"value", Some(&body))),
                    9,
                )
                .unwrap();
        }

        fn register(&self) -> Result<crate::SessionBinding, agent::AgentMutationError> {
            self.store
                .register_session_with_claim(&registration(), Some(&ready_context()))
        }
    }

    fn ready_context() -> HubClaimContext<'static> {
        HubClaimContext {
            local_box: SELF,
            fresh: &|_, _| true,
            holder_current: &|_, episode, _| episode == 1,
            seed_expected: &|_| false,
        }
    }
    fn registration() -> SessionRegistration<'static> {
        SessionRegistration {
            role: None,
            agent_id: ID,
            ref_kind: "test",
            session_ref: "ref",
            harness: "pi",
            session_id: "session",
            project_root: Some("/project"),
            registered_by: "host",
            runner: Some("host"),
            network: None,
            now_ms: 10,
        }
    }
    fn hub_startup_where(holder: &str, local_box: &str) -> Value {
        let f = Fixture::new();
        let body = serde_json::json!({"holder":holder,"holder_ep":1,"gen":1,"cut_id":"cut"});
        f.store
            .record_hub_kv(
                &format!("residence.{ID}"),
                1,
                Some((b"value", Some(&body))),
                9,
            )
            .unwrap();
        let context = HubClaimContext {
            local_box,
            fresh: &|_, _| false,
            holder_current: &|_, _, _| false,
            seed_expected: &|_| false,
        };
        serde_json::to_value(f.store.hub_agent_where(ID, &context).unwrap()).unwrap()
    }

    #[test]
    fn hub_where_own_holder_before_membership_is_held_here_but_stale() {
        assert_eq!(
            hub_startup_where(SELF, SELF),
            serde_json::json!({"known":true,"status":"held_here","holder":SELF,"holder_name":null,
                "holder_box":SELF,"epoch":1,"cut_id":"cut","read_at_ms":9,"stale":true})
        );
    }

    #[test]
    fn hub_where_holder_with_unknown_local_box_is_unclaimed_and_stale() {
        assert_eq!(
            hub_startup_where(SELF, ""),
            serde_json::json!({"known":false,"status":"unclaimed","holder":null,"holder_name":null,
                "epoch":0,"cut_id":null,"read_at_ms":null,"stale":true})
        );
    }

    #[test]
    fn hub_where_other_holder_before_membership_is_held_elsewhere_without_holder_box() {
        assert_eq!(
            hub_startup_where(OTHER, SELF),
            serde_json::json!({"known":true,"status":"held_elsewhere","holder":OTHER,"holder_name":null,
                "epoch":1,"cut_id":"cut","read_at_ms":9,"stale":true})
        );
    }

    #[test]
    fn self_author_conflict_never_overwrites_identity_or_journals() {
        let f = Fixture::new();
        let generation = f.store.generation().unwrap();
        assert!(f.store.learn_verified_self_author(SELF, 9).unwrap());
        assert!(!f.store.learn_verified_self_author(OTHER, 9).unwrap());
        assert_eq!(
            f.store.persisted_self_author().unwrap().as_deref(),
            Some(SELF)
        );
        assert_eq!(f.store.generation().unwrap(), generation);
    }
    #[test]
    fn where_wire_nulls_known_status_integrity_and_restart() {
        let f = Fixture::new();
        // With sharing never turned on, no other machine can hold the agent, so an
        // absent claim is a verified "unclaimed". Core's head registration
        // accepts exactly this shape.
        assert_eq!(serde_json::to_string(&f.store.agent_where(ID).unwrap()).unwrap(),"{\"known\":true,\"status\":\"unclaimed\",\"holder\":null,\"holder_name\":null,\"epoch\":0,\"cut_id\":null,\"read_at_ms\":null,\"stale\":false}");
        // Without a selected backend, an absent claim is unknown while
        // enabling or after the hub snapshot is installed.
        for (state, enable_id, base) in [
            ("enabling", Some("native-plan"), None),
            ("enabled", None, Some(0)),
        ] {
            f.store
                .db
                .with_conn_fenced(|tx| {
                    tx.execute(
                        "UPDATE identity_log_state SET state=?1,enable_id=?2,hub_log_base=?3 WHERE id=1",
                        rusqlite::params![state, enable_id, base],
                    )?;
                    Ok(())
                })
                .unwrap();
            assert_eq!(serde_json::to_string(&f.store.agent_where(ID).unwrap()).unwrap(),"{\"known\":false,\"status\":\"unclaimed\",\"holder\":null,\"holder_name\":null,\"epoch\":0,\"cut_id\":null,\"read_at_ms\":null,\"stale\":true}", "{state}");
        }
        let key = format!("residence.{ID}");
        let observe = |revision, holder| {
            let body =
                serde_json::json!({"holder":holder,"holder_ep":1,"gen":revision,"cut_id":"cut"});
            f.store
                .record_hub_kv(&key, revision, Some((b"value", Some(&body))), 9)
                .unwrap();
        };
        let context = HubClaimContext {
            local_box: SELF,
            fresh: &|_, _| true,
            holder_current: &|holder, episode, _| holder == OTHER && episode == 1,
            seed_expected: &|_| false,
        };
        observe(1, OTHER);
        f.store
            .db
            .with_conn_fenced(|tx| {
                tx.execute("INSERT INTO member_name VALUES(?1,'Studio')", [OTHER])?;
                Ok(())
            })
            .unwrap();
        let row = f.store.hub_agent_where(ID, &context).unwrap();
        assert_eq!(row.status, "held_elsewhere");
        assert_eq!(row.holder_name.as_deref(), Some("Studio"));
        observe(2, SELF);
        let descriptor = f.descriptor.clone();
        drop(f.store);
        let store = RegistryStore::open(&descriptor).unwrap();
        assert_eq!(
            store.hub_agent_where(ID, &context).unwrap().status,
            "held_here"
        );
        store.record_hub_kv(&key, 3, None, 11).unwrap();
        assert_eq!(
            store.hub_agent_where(ID, &context).unwrap_err().code,
            "claim_integrity_failed"
        );
        drop(store);
    }
    #[test]
    fn where_fallback_ignores_residence_until_selected() {
        let f = Fixture::new();
        f.observe(1, SELF);
        assert_eq!(
            f.store
                .hub_agent_where(ID, &ready_context())
                .unwrap()
                .status,
            "held_here"
        );
        for (state, base, known) in [("disabled", None, true), ("enabled", Some(0), false)] {
            f.store
                .db
                .with_conn_fenced(|tx| {
                    tx.execute(
                        "UPDATE identity_log_state SET state=?1,hub_log_base=?2 WHERE id=1",
                        params![state, base],
                    )?;
                    Ok(())
                })
                .unwrap();
            let row = f.store.agent_where(ID).unwrap();
            assert_eq!(
                (row.status, row.epoch, row.known, row.stale),
                ("unclaimed", 0, known, !known)
            );
            assert_eq!(
                (row.holder, row.holder_name, row.cut_id, row.read_at_ms),
                (None, None, None, None)
            );
        }
    }

    #[test]
    fn register_claim_admission_checks_integrity_before_holder_and_binding() {
        let f = Fixture::new();
        assert_eq!(f.register().unwrap_err().code, "claim_unavailable");
        f.observe(1, SELF);
        f.register().unwrap();
        f.observe(2, OTHER);
        let error = f.register().unwrap_err();
        assert_eq!(error.code, "held_elsewhere");
        assert_eq!(
            error.detail,
            Some(serde_json::json!({"holder":OTHER,"holder_name":null,"epoch":2}))
        );
        f.store
            .record_hub_kv(&format!("residence.{ID}"), 3, None, 10)
            .unwrap();
        assert_eq!(f.register().unwrap_err().code, "claim_integrity_failed");
        f.observe(4, SELF);
        f.register().unwrap();
    }
    #[test]
    fn registration_does_not_assume_a_holder_without_local_box_is_here() {
        let f = Fixture::new();
        f.observe(1, SELF);
        let context = HubClaimContext {
            local_box: "",
            ..ready_context()
        };
        assert_eq!(
            f.store
                .register_session_with_claim(&registration(), Some(&context))
                .unwrap_err()
                .code,
            "claim_unavailable"
        );
        assert_eq!(
            f.store
                .agent_where("agent_ffffffffffffffff")
                .unwrap_err()
                .code,
            "unknown_agent"
        );
        f.store
            .db
            .with_conn_fenced(|tx| {
                tx.execute(
                    "UPDATE agent SET terminal_reason='retired',terminal_at_ms=2 WHERE agent_id=?1",
                    [ID],
                )?;
                Ok(())
            })
            .unwrap();
        assert_eq!(f.store.agent_where(ID).unwrap_err().code, "gone");
        assert_eq!(f.register().unwrap_err().code, "gone");
    }
    #[test]
    fn registration_snapshot_excludes_a_cache_write_started_after_claim_read() {
        let f = Fixture::new();
        f.observe(1, SELF);
        let writer = f.store.clone();
        let (start, started) = std::sync::mpsc::channel();
        let (done, completed) = std::sync::mpsc::channel();

        // Start the competing hub observation while registration checks the
        // already-read claim. It must wait for registration's transaction.
        let thread = f
            .store
            .db
            .with_conn_fenced(|tx| {
                let thread = std::thread::spawn(move || {
                    start.send(()).unwrap();
                    let body = serde_json::json!({"holder":OTHER,"holder_ep":1,"gen":2});
                    writer
                        .record_hub_kv(
                            &format!("residence.{ID}"),
                            2,
                            Some((b"new value", Some(&body))),
                            20,
                        )
                        .unwrap();
                    done.send(()).unwrap();
                });
                let fresh = |_: &str, revision| {
                    assert_eq!(revision, Some(1));
                    started.recv().unwrap();
                    assert!(matches!(
                        completed.try_recv(),
                        Err(std::sync::mpsc::TryRecvError::Empty)
                    ));
                    true
                };
                let context = HubClaimContext {
                    fresh: &fresh,
                    ..ready_context()
                };
                assert!(RegistryStore::register_session_in(
                    tx,
                    &registration(),
                    &mut Vec::new(),
                    Some(&context)
                )
                .is_ok());
                Ok(thread)
            })
            .unwrap();
        thread.join().unwrap();
        assert_eq!(f.register().unwrap_err().code, "held_elsewhere");
        assert_eq!(
            f.store
                .session_binding_for("pi", "session")
                .unwrap()
                .unwrap()
                .agent_id,
            ID
        );
    }
}
