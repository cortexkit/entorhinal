//! Enable the shared identity log and recover interrupted attempts. Save each
//! snapshot part's entry id and encoded bytes before sending it, then reuse both
//! on retries so engram recognises a duplicate instead of appending it again.
//! A reply or connection can fail after engram accepts an append, so retain the
//! saved data until the log confirms whether that append was accepted.

use super::{
    log_client::{self, AppendRequest, EntryKind, LogClient, LogError},
    *,
};
use entorhinal_core::enable_state::EnablePart;
use tokio::time::{timeout_at, Instant};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EnableParams {
    #[serde(default)]
    without_agents: bool,
}

pub(super) fn refresh(health: &HealthGauges, store: &RegistryStore) {
    if let Ok(status) = store.identity_log_status() {
        refresh_status(health, &status);
    }
}

pub(super) fn refresh_status(health: &HealthGauges, status: &entorhinal_core::IdentityLogStatus) {
    health.log_mode.store(
        match status.state.as_str() {
            "enabling" => 1,
            "joining" => 2,
            "enabled" => 3,
            _ => 0,
        },
        Ordering::Relaxed,
    );
    health
        .log_applied
        .store(status.last_applied_position, Ordering::Relaxed);
    health
        .log_head
        .store(status.last_seen_head, Ordering::Relaxed);
    health
        .log_pending
        .store(status.pending_write_count, Ordering::Relaxed);
}

pub(super) fn status_state(health: &HealthGauges) -> &'static str {
    let mode = health.log_mode.load(Ordering::Relaxed);
    if mode == 1 {
        return "enabling";
    }
    if mode == 2 {
        return "joining";
    }
    let error = health.log_state.load(Ordering::Relaxed);
    if error == 8 {
        return "unavailable";
    }
    if error != 0 {
        return shared_write::health_state(error);
    }
    if mode == 3 {
        "enabled"
    } else {
        "disabled"
    }
}

fn error_name(code: u64) -> Option<&'static str> {
    match code {
        0 => None,
        8 => Some("engram_unavailable"),
        9 => Some("engram_outcome_unknown"),
        _ => Some(shared_write::health_state(code)),
    }
}
pub(super) fn last_error(health: &HealthGauges) -> Option<&'static str> {
    error_name(health.log_error.load(Ordering::Relaxed))
}
pub(super) fn last_error_at(health: &HealthGauges) -> Option<i64> {
    let time = health.log_error_at.load(Ordering::Relaxed);
    (time != 0).then_some(time)
}
pub(super) fn record_error(health: &HealthGauges, error: &HandlerError) {
    let code = match error.code.as_str() {
        "engram_key_unavailable" => 1,
        "identity_log_not_member" => 2,
        "identity_log_invariant" => 3,
        "identity_log_stalled" => health.log_state.load(Ordering::Relaxed),
        "engram_unavailable" => {
            let state = health.log_state.load(Ordering::Relaxed);
            if (1..=7).contains(&state) {
                state
            } else {
                health.log_state.store(8, Ordering::Relaxed);
                8
            }
        }
        "engram_outcome_unknown" => 9,
        _ => return,
    };
    health.log_error.store(code, Ordering::Relaxed);
    health.log_error_at.store(unix_millis(), Ordering::Relaxed);
}

fn log_error(error: LogError, health: &HealthGauges) -> HandlerError {
    match error {
        LogError::NotMember => {
            health.log_state.store(2, Ordering::Relaxed);
            HandlerError::new("identity_log_not_member", "device is not a log member")
        }
        LogError::KeyUnavailable => {
            health.log_state.store(1, Ordering::Relaxed);
            HandlerError::new("engram_key_unavailable", "engram key unavailable")
        }
        LogError::IdReused => {
            health.log_state.store(3, Ordering::Relaxed);
            HandlerError::new("identity_log_invariant", "stored snapshot id was reused")
        }
        _ => HandlerError::new("engram_unavailable", "engram unavailable during enable"),
    }
}

fn draw_id() -> Result<[u8; 16], HandlerError> {
    let mut id = [0; 16];
    getrandom::getrandom(&mut id).map_err(|e| HandlerError::new("storage_error", e.to_string()))?;
    Ok(id)
}

/// With the writer lock held, read confirmed progress and resend only missing
/// saved snapshot parts using their original ids and bytes. Keep the state enabling, which refuses shared
/// mutations, until the log confirms every part at its original position or a
/// read finds a different entry at base + 1 and permits returning to disabled.
/// A missing or failed reply alone is not a reason to discard the saved data.
pub(super) async fn resume(
    store: &RegistryStore,
    log: &LogClient,
    health: &HealthGauges,
    deadline: Instant,
    principal: &str,
) -> Result<(), HandlerError> {
    let parts = store.enable_parts()?;
    let base = store.enable_base()?;
    if parts.is_empty()
        || base
            .checked_add(parts.len() as u64)
            .is_none_or(|p| p > i64::MAX as u64)
    {
        return Err(HandlerError::new(
            "identity_log_invariant",
            "invalid saved enable range",
        ));
    }
    // Receipts can arrive too slowly to fit all parts into one request. A read
    // accumulates durable progress across retries without spending the deadline
    // resending parts that already occupy their unique expected positions.
    let mut confirmed = 0;
    loop {
        let after = base + confirmed as u64;
        let page = timeout_at(deadline, log.read(after, parts.len() - confirmed))
            .await
            .map_err(|_| {
                HandlerError::new(
                    "engram_unavailable",
                    "enable recovery read deadline expired",
                )
            })?
            .map_err(|e| log_error(e, health))?;
        if page.head < after || (page.entries.is_empty() && page.head > after) {
            return Err(HandlerError::new(
                "identity_log_invariant",
                "enable recovery log range is missing",
            ));
        }
        for entry in page.entries {
            if entry.position != base + confirmed as u64 + 1 {
                return Err(HandlerError::new(
                    "identity_log_invariant",
                    "enable recovery log gap",
                ));
            }
            if entry.entry_id != parts[confirmed].entry_id {
                if confirmed == 0 {
                    return abandon_contended(store, base, page.head);
                }
                return Err(HandlerError::new(
                    "identity_log_invariant",
                    "snapshot parts are not consecutive",
                ));
            }
            confirmed += 1;
        }
        if confirmed == parts.len() || base + confirmed as u64 == page.head {
            break;
        }
        // A byte-capped page may contain fewer than the requested parts. Read
        // the rest before deciding which parts need another append.
    }
    for (i, part) in parts.iter().enumerate().skip(confirmed) {
        let request = AppendRequest {
            expected_head: base + i as u64,
            entry_id: part.entry_id,
            kind: EntryKind::Snapshot,
            data: part.data.clone(),
        };
        let reply = timeout_at(deadline, log.append(&request))
            .await
            .map_err(|_| {
                HandlerError::new("engram_outcome_unknown", "snapshot append deadline expired")
            })?;
        match reply {
            Ok(reply) if reply.position == base + i as u64 + 1 => {}
            Err(LogError::HeadMoved { .. }) if i == 0 => {
                let page = timeout_at(deadline, log.read(base, 1))
                    .await
                    .map_err(|_| {
                        HandlerError::new(
                            "engram_unavailable",
                            "enable recovery read deadline expired",
                        )
                    })?
                    .map_err(|e| log_error(e, health))?;
                if let Some(first) = page.entries.first().filter(|e| e.position == base + 1) {
                    if first.entry_id != part.entry_id {
                        return abandon_contended(store, base, page.head);
                    }
                }
                return Err(HandlerError::new(
                    "identity_log_invariant",
                    "snapshot receipt conflicts with first part position",
                ));
            }
            Ok(_) | Err(LogError::HeadMoved { .. }) => {
                return Err(HandlerError::new(
                    "identity_log_invariant",
                    "snapshot parts are not consecutive",
                ))
            }
            Err(error) => {
                if matches!(error, LogError::IdReused) {
                    tracing::warn!(target: "identity_log", entry_id = log_client::encode_hex(&part.entry_id), "snapshot id reused");
                }
                return Err(log_error(error, health));
            }
        }
    }
    store.finish_enable(unix_millis(), principal)?;
    health.log_state.store(0, Ordering::Relaxed);
    refresh(health, store);
    Ok(())
}

fn abandon_contended(store: &RegistryStore, base: u64, head: u64) -> Result<(), HandlerError> {
    store.abandon_enable()?;
    if base > 0 {
        return Err(HandlerError::new(
            "identity_log_contended",
            "another entry won the first snapshot position; retry enable",
        ));
    }
    // Keep the existing bootstrap refusal: a populated store cannot join the
    // winning snapshot, while an empty store can retry as a joiner.
    store.check_enable_preconditions(head)?;
    Err(HandlerError::new(
        "engram_unavailable",
        "another machine supplied the bootstrap; retry to join",
    ))
}

/// Read precisely the head that was observed before enable. No durable state is
/// updated: every author check and writer refusal precedes begin_enable.
async fn own_prefix(
    log: &LogClient,
    health: &HealthGauges,
    mut page: log_client::ReadReply,
    deadline: Instant,
) -> Result<Vec<entorhinal_core::remote_apply::RemoteEntry>, HandlerError> {
    let head = page.head;
    let mut entries = vec![];
    let mut after = 0;
    let mut own = true;
    loop {
        if page.head < head || page.entries.is_empty() {
            return Err(HandlerError::new(
                "identity_log_invariant",
                format!("log prefix missing at position {}", after + 1),
            ));
        }
        for entry in page.entries {
            if entry.position != after + 1 || entry.position > head {
                return Err(HandlerError::new(
                    "identity_log_invariant",
                    format!("log prefix gap at position {}", after + 1),
                ));
            }
            own &= entry.signed_by_self;
            after = entry.position;
            entries.push(entorhinal_core::remote_apply::RemoteEntry {
                position: i64::try_from(entry.position).map_err(|_| {
                    HandlerError::new("identity_log_invariant", "log position exceeds local range")
                })?,
                entry_id: log_client::encode_hex(&entry.entry_id),
                signer: log_client::encode_hex(&entry.signer),
                key_id: json!({"family":entry.key_id.family,"epoch":entry.key_id.epoch})
                    .to_string(),
                envelope_version: i64::try_from(entry.envelope_version).unwrap_or(-1),
                kind: entry.kind,
                entry: entry.entry,
            });
        }
        if after == head {
            break;
        }
        page = timeout_at(deadline, log.read(after, (head - after).min(128) as usize))
            .await
            .map_err(|_| {
                HandlerError::new("engram_unavailable", "enable prefix read deadline expired")
            })?
            .map_err(|e| log_error(e, health))?;
    }
    // An empty result identifies a foreign prefix; a nonzero head has at least
    // one entry. The caller supplies the counts in the existing join refusal.
    if !own {
        entries.clear();
    }
    Ok(entries)
}

impl ProjectsHandler {
    pub(super) async fn enable_log(
        &self,
        params: Value,
        key: RouteKey,
        received: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        let admission = self.admit("identity_log.enable", key)?;
        let params: EnableParams = serde_json::from_value(params)
            .map_err(|error| HandlerError::new("invalid_request", error.to_string()))?;
        let deadline = received + Duration::from_secs(25);
        let _writer = timeout_at(deadline, self.writer.lock())
            .await
            .map_err(|_| {
                HandlerError::new("identity_log_contended", "writer lock deadline expired")
            })?;
        let store = self.with_store(|s| Ok(s.clone()))?;
        let result = self
            .enable_locked(
                &store,
                params.without_agents,
                deadline,
                &principal_label(admission.principal.as_ref()),
            )
            .await;
        refresh(&self.health, &store);
        self.health
            .generation
            .fetch_max(store.generation()?, Ordering::Relaxed);
        self.commits.notify_waiters();
        if let Err(error) = &result {
            record_error(&self.health, error);
        }
        result?;
        self.identity_log_status()
    }

    async fn enable_locked(
        &self,
        store: &RegistryStore,
        without_agents: bool,
        deadline: Instant,
        principal: &str,
    ) -> Result<(), HandlerError> {
        match store.identity_log_status()?.state.as_str() {
            "enabled" => return Ok(()),
            "enabling" => {
                return resume(store, &self.log_client, &self.health, deadline, principal).await
            }
            "joining" => return self.catch_up(store, deadline).await,
            _ => {}
        }
        let page = timeout_at(deadline, self.log_client.read(0, 1))
            .await
            .map_err(|_| HandlerError::new("engram_unavailable", "enable head deadline expired"))?
            .map_err(|e| log_error(e, &self.health))?;
        store.check_enable_preconditions(0)?;
        let base = page.head;
        if base > 0 && store.enable_registry_is_empty()? {
            return self.catch_up(store, deadline).await;
        }
        let history = if base > 0 {
            let entries = own_prefix(&self.log_client, &self.health, page, deadline).await?;
            if entries.is_empty() {
                store.check_enable_preconditions(base)?;
            }
            entries
        } else {
            vec![]
        };
        store.check_agent_import_preconditions(without_agents)?;
        let snapshot_id = log_client::encode_hex(&draw_id()?);
        let plan = if base > 0 {
            store.prepare_supersede(&snapshot_id, (self.clock)(), base, &history)?
        } else {
            store.prepare_enable(&snapshot_id, (self.clock)())?
        };
        let parts = plan
            .bodies
            .into_iter()
            .map(|data| {
                Ok(EnablePart {
                    entry_id: draw_id()?,
                    data,
                })
            })
            .collect::<Result<Vec<_>, HandlerError>>()?;
        if Instant::now() >= deadline {
            return Err(HandlerError::new(
                "engram_unavailable",
                "enable deadline expired before append",
            ));
        }
        store.begin_enable(&parts, &plan.backfill)?;
        refresh(&self.health, store);
        resume(store, &self.log_client, &self.health, deadline, principal).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        fake_log::{Action, FakeLog},
        log_client::{LogConnector, LogTransport, TransportError},
    };
    use async_trait::async_trait;
    use entorhinal_core::{agent::AgentChangeEntry, shared_entry::SharedState};
    use rusqlite::{types::Value as Cell, Connection, OpenFlags};
    use std::path::Path;
    use std::sync::atomic::AtomicBool;
    use subc_client_rs::ModuleHandler;

    const DIRECT: RouteKey = (131, 1);
    const CORE: RouteKey = (132, 1);

    struct Fixture {
        dir: PathBuf,
        descriptor: StorageDescriptor,
        handler: ProjectsHandler,
    }
    impl Fixture {
        fn new(label: &str, log: Arc<dyn LogConnector>) -> Self {
            let (dir, descriptor) = crate::tests::scratch_descriptor(label);
            let dir = dir.canonicalize().unwrap();
            let handler = Self::handler(&descriptor, log);
            Self {
                dir,
                descriptor,
                handler,
            }
        }
        fn handler(descriptor: &StorageDescriptor, log: Arc<dyn LogConnector>) -> ProjectsHandler {
            let handler = ProjectsHandler::with_log_connector("enable-test".into(), || 700, log);
            *handler.store.lock().unwrap() = Some(RegistryStore::open(descriptor).unwrap());
            handler.health.store_ready.store(true, Ordering::Relaxed);
            for (key, principal) in [
                (DIRECT, Principal::Direct),
                (
                    CORE,
                    Principal::Reserved {
                        module_id: WRITER_MODULE.into(),
                    },
                ),
            ] {
                handler.route_admissions().insert(
                    key,
                    RouteAdmission {
                        principal: Some(principal),
                        flow_id: None,
                    },
                );
            }
            handler
        }
        fn restart(&mut self, log: Arc<dyn LogConnector>) {
            self.handler.store.lock().unwrap().take();
            self.handler = Self::handler(&self.descriptor, log);
        }
        fn fork(&self, label: &str, log: Arc<dyn LogConnector>) -> Self {
            let (dir, descriptor) = crate::tests::scratch_descriptor(label);
            let StorageBackend::Sqlite { path } = &descriptor.backend else {
                unreachable!()
            };
            self.conn().execute("VACUUM INTO ?1", [path]).unwrap();
            let handler = Self::handler(&descriptor, log);
            Self {
                dir: dir.canonicalize().unwrap(),
                descriptor,
                handler,
            }
        }
        fn store(&self) -> RegistryStore {
            self.handler.with_store(|s| Ok(s.clone())).unwrap()
        }
        fn conn(&self) -> Connection {
            let StorageBackend::Sqlite { path } = &self.descriptor.backend else {
                unreachable!()
            };
            Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap()
        }
        fn journal(&self) -> Vec<Vec<Cell>> {
            self.rows("registry_journal")
        }
        fn rows(&self, table: &str) -> Vec<Vec<Cell>> {
            let conn = self.conn();
            let mut stmt = conn
                .prepare(&format!("SELECT * FROM {table} ORDER BY 1"))
                .unwrap();
            let n = stmt.column_count();
            stmt.query_map([], |r| (0..n).map(|i| r.get(i)).collect())
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap()
        }
        fn shared(&self) -> SharedState {
            SharedState::capture(&self.conn()).unwrap()
        }
        async fn call(&self, method: &str, params: Value) -> Result<Value, String> {
            let route = if method.starts_with("agent.") {
                CORE
            } else {
                DIRECT
            };
            let body = serde_json::to_vec(&json!({"method":method,"params":params})).unwrap();
            match self.handler.handle_served_request(&body, route).await {
                HandlerOutcome::Response(bytes) => {
                    Ok(serde_json::from_slice::<Value>(&bytes).unwrap()["result"].clone())
                }
                HandlerOutcome::Error { code, .. }
                | HandlerOutcome::ErrorWithDetail { code, .. } => Err(code),
                HandlerOutcome::Streamed => panic!("unexpected streamed enable response"),
            }
        }
        async fn enable(&self) -> Result<Value, String> {
            self.call("identity_log.enable", json!({})).await
        }
        async fn enable_without_agents(&self) -> Result<Value, String> {
            self.call("identity_log.enable", json!({"without_agents":true}))
                .await
        }
        fn marker(&self) {
            self.store()
                .apply_entry("agent.cutover", "{}", "test", None, |_| Ok(()))
                .unwrap();
        }
        fn agents(&self, count: usize, label_size: usize) {
            self.marker();
            for i in 0..count {
                let id = format!("agent_{i:016x}");
                self.store().with_principal("reserved:prefrontal-core").agent_mutation_with_id("agent.create", json!({"role":"assistant","name":format!("Agent {i}"),"tag":"ok","request_key":format!("create-{i}")}), 10, Some(&id)).unwrap();
                if label_size > 0 {
                    let mut row = self.store().agent_row(&id).unwrap().unwrap();
                    row.labels = vec!["x".repeat(label_size)];
                    let entry = AgentChangeEntry::new(
                        "agent.import",
                        row,
                        self.store().agent_claims(&id).unwrap(),
                    );
                    let payload = json!({"entry":entry});
                    self.store()
                        .apply_entry("agent.import", &payload.to_string(), "test", None, |tx| {
                            entorhinal_core::agent::replay_agent_entry(tx, "agent.import", &payload)
                                .map(|_| ())
                        })
                        .unwrap();
                }
            }
        }
        fn project(&self, id: &str, root: &Path, name: &str, key: Option<&str>) {
            self.store()
                .register(RegisterRequest {
                    project_id: Some(id.into()),
                    roots: vec![root.to_string_lossy().into()],
                    name: name.into(),
                    request_key: key.map(Into::into),
                    ..Default::default()
                })
                .unwrap();
        }
        fn empty_import(&self) {
            let path = self.dir.join("import.db");
            Connection::open(&path)
                .unwrap()
                .execute_batch("CREATE TABLE empty_snapshot(id INTEGER)")
                .unwrap();
            self.store()
                .agent_import(
                    json!({"snapshot_path":path,"request_key":"after-refusal"}),
                    100,
                )
                .unwrap();
        }
        fn marker_count(&self) -> i64 {
            self.conn()
                .query_row(
                    "SELECT COUNT(*) FROM registry_journal WHERE op='agent.cutover'",
                    [],
                    |r| r.get(0),
                )
                .unwrap()
        }
        async fn refuse_unwritten(&self, log: &FakeLog, params: Value, code: &str) {
            let journal = self.journal();
            let state = self.rows("identity_log_state");
            let head = log.head();
            let calls = log.calls();
            assert_eq!(
                self.call("identity_log.enable", params).await.unwrap_err(),
                code
            );
            assert_eq!(self.journal(), journal);
            assert_eq!(self.rows("identity_log_state"), state);
            assert_eq!(
                self.store().identity_log_status().unwrap().state,
                "disabled"
            );
            assert_eq!(log.head(), head);
            assert_eq!(log.calls(), calls + 1, "refusal must only read the head");
        }
        async fn import_agent(&self) {
            let path = self.dir.join("agent-snapshot.db");
            let source = Connection::open(&path).unwrap();
            // Use the frozen core schema fixtures also used by the importer tests,
            // so this exercises a real agent import rather than inserting a marker.
            for migration in [
                include_str!("../../entorhinal-core/tests/fixtures/core-agent-migrations/076_agent_registry.sql"),
                include_str!("../../entorhinal-core/tests/fixtures/core-agent-migrations/080_agent_github_identity.sql"),
                include_str!("../../entorhinal-core/tests/fixtures/core-agent-migrations/082_wake_delivery.sql"),
                include_str!("../../entorhinal-core/tests/fixtures/core-agent-migrations/109_agent_generation.sql"),
                include_str!("../../entorhinal-core/tests/fixtures/core-agent-migrations/112_agent_avatar.sql"),
                include_str!("../../entorhinal-core/tests/fixtures/core-agent-migrations/126_agent_labels.sql"),
            ] { source.execute_batch(migration).unwrap(); }
            source.execute_batch("INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms) VALUES('agent_0000000000000001','Imported','ok','assistant',10,10);
                INSERT INTO agent_name_claim(agent_id,namespace_kind,namespace_key,normalized_name,display_name,claimed_at_ms) VALUES('agent_0000000000000001','assistant','global','imported','Imported',10);").unwrap();
            drop(source);
            self.call(
                "agent.import",
                json!({"snapshot_path":path,"request_key":"import-before-enable"}),
            )
            .await
            .unwrap();
            assert_eq!(self.store().agent_snapshot().unwrap().agents.len(), 1);
            assert_eq!(self.marker_count(), 1);
        }
        fn clean_rebuild(&self) {
            let before = self.shared();
            let journal = self.journal();
            let local = [
                "project_root",
                "derived_root_parent",
                "root_binding",
                "retired_binding",
                "root_approval",
                "root_owned_remotes",
                "workspace_root",
                "workspace_member",
            ];
            let cells: Vec<_> = local.iter().map(|t| self.rows(t)).collect();
            assert!(self.store().verify().unwrap().replay.ok);
            assert!(self.store().rebuild().unwrap().replay.ok);
            assert_eq!(self.shared(), before);
            assert_eq!(self.journal(), journal);
            assert_eq!(
                local.iter().map(|t| self.rows(t)).collect::<Vec<_>>(),
                cells
            );
        }
        async fn refuse_supersede(&self, log: &FakeLog, params: Value, code: &str) {
            let journal = self.journal();
            let state = self.rows("identity_log_state");
            let shared = self.shared();
            let before = log.client().read(0, 128).await.unwrap();
            assert_eq!(
                self.call("identity_log.enable", params).await.unwrap_err(),
                code
            );
            assert_eq!(self.journal(), journal);
            assert_eq!(self.rows("identity_log_state"), state);
            assert_eq!(self.shared(), shared);
            let after = log.client().read(0, 128).await.unwrap();
            assert_eq!(after.head, before.head);
            assert_eq!(after.entries, before.entries);
            assert_eq!(
                self.store().identity_log_status().unwrap().state,
                "disabled"
            );
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            self.handler.store.lock().unwrap().take();
            std::fs::remove_dir_all(&self.dir).unwrap();
        }
    }
    fn git(root: &Path, args: &[&str]) {
        let output = std::process::Command::new("git")
            .current_dir(root)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{:?}: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    fn repo(root: &Path, remote: Option<&str>) {
        std::fs::create_dir_all(root).unwrap();
        git(root, &["init", "-q"]);
        if let Some(remote) = remote {
            git(root, &["remote", "add", "origin", remote]);
        }
    }
    async fn finish<F: std::future::Future>(future: F) -> F::Output {
        tokio::pin!(future);
        loop {
            tokio::select! { result = &mut future => return result, () = tokio::task::yield_now() => {} }
        }
    }

    #[derive(Clone)]
    struct SupersedeTap {
        log: FakeLog,
        calls: Arc<Mutex<Vec<(String, Value)>>>,
        race: Arc<Mutex<Option<(FakeLog, AppendRequest)>>>,
        own_race: Arc<AtomicBool>,
    }
    impl SupersedeTap {
        fn new(log: &FakeLog) -> Self {
            Self {
                log: log.clone(),
                calls: Arc::new(Mutex::new(vec![])),
                race: Arc::new(Mutex::new(None)),
                own_race: Arc::new(AtomicBool::new(false)),
            }
        }
        fn appends(&self) -> Vec<Value> {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|(method, _)| method == log_client::APPEND)
                .map(|(_, params)| params.clone())
                .collect()
        }
    }
    #[async_trait]
    impl LogConnector for SupersedeTap {
        async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
            Ok(Arc::new(self.clone()))
        }
    }
    #[async_trait]
    impl LogTransport for SupersedeTap {
        async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
            self.calls
                .lock()
                .unwrap()
                .push((method.into(), params.clone()));
            if method == log_client::APPEND {
                let race = self.race.lock().unwrap().take();
                if let Some((foreign, request)) = race {
                    foreign.client().append(&request).await.unwrap();
                }
                if self.own_race.swap(false, Ordering::SeqCst) {
                    self.log.call(method, params.clone()).await?;
                    return Err(TransportError::Refusal {
                        code: log_client::HEAD_MOVED.into(),
                        detail: Some(json!({"head":self.log.head()})),
                    });
                }
            }
            self.log.call(method, params).await
        }
    }

    async fn supersede_setup(label: &str) -> (Arc<FakeLog>, Fixture, Fixture) {
        let log = Arc::new(FakeLog::default());
        let a = Fixture::new(&format!("{label}-a"), log.clone());
        let root = a.dir.join("root");
        std::fs::create_dir_all(&root).unwrap();
        a.project("P", &root, "Before", None);
        let restored = a.fork(&format!("{label}-restored"), log.clone());
        a.enable_without_agents().await.unwrap();
        restored.agents(3, 100_000);
        (log, a, restored)
    }

    async fn append_empty_change(log: &FakeLog, id: u64) {
        log.client()
            .append(&AppendRequest {
                expected_head: log.head(),
                entry_id: id.to_le_bytes().repeat(2).try_into().unwrap(),
                kind: EntryKind::Change,
                data: serde_json::to_vec(&json!({"op":"project.shared","tables":{}})).unwrap(),
            })
            .await
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn writer_supersede_requires_every_entry_signed_by_self_before_guard() {
        for imported in [false, true] {
            let log = Arc::new(FakeLog::default());
            let a = Fixture::new("writer-auth-a", log.clone());
            let root = a.dir.join("root");
            std::fs::create_dir_all(&root).unwrap();
            a.project("P", &root, "Before", None);
            let restored = a.fork("writer-auth-restored", log.clone());
            a.enable_without_agents().await.unwrap();
            for id in 1..=130 {
                append_empty_change(&log, id).await;
            }
            append_empty_change(&log.with_author([0xcc; 16]), 131).await;
            if imported {
                restored.agents(1, 0);
            }
            for params in [json!({}), json!({"without_agents":true})] {
                restored
                    .refuse_supersede(&log, params, "join_requires_empty_registry")
                    .await;
            }
            assert_eq!(log.head(), 132);
        }
        // An equally long own prefix is permitted, after the guarded branch's
        // import/flag checks. A joiner must not take this writer branch.
        let (log, _a, restored) = supersede_setup("writer-own-paged").await;
        for id in 1..=130 {
            append_empty_change(&log, id).await;
        }
        restored
            .refuse_supersede(&log, json!({"without_agents":true}), "invalid_request")
            .await;
        restored.enable().await.unwrap();
        assert_eq!(
            restored
                .store()
                .identity_log_status()
                .unwrap()
                .last_applied_position,
            133
        );
        let body: Value =
            serde_json::from_slice(&log.client().read(131, 1).await.unwrap().entries[0].entry)
                .unwrap();
        assert_eq!(body["supersedes_through"], 131);

        let log = Arc::new(FakeLog::default());
        let a = Fixture::new("writer-guard-a", log.clone());
        let root = a.dir.join("root");
        std::fs::create_dir_all(&root).unwrap();
        a.project("P", &root, "Before", None);
        let restored = a.fork("writer-guard-restored", log.clone());
        a.enable_without_agents().await.unwrap();
        restored
            .refuse_supersede(&log, json!({}), "agents_not_imported")
            .await;
        restored.enable_without_agents().await.unwrap();
        assert_eq!(restored.marker_count(), 1);
        assert_eq!(log.head(), 2);
    }

    fn rewrite_agent(f: &Fixture, mut row: entorhinal_core::agent::AgentRow) {
        row.agent_generation += 1;
        let entry = AgentChangeEntry::new(
            "agent.import",
            row.clone(),
            f.store().agent_claims(&row.agent_id).unwrap(),
        );
        let payload = json!({"entry":entry});
        f.store()
            .apply_entry("agent.import", &payload.to_string(), "test", None, |tx| {
                entorhinal_core::agent::replay_agent_entry(tx, "agent.import", &payload).map(|_| ())
            })
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn writer_supersede_refusals_leave_journal_state_and_log_unchanged() {
        const X: &str = "agent_0000000000000000";
        for case in [
            "drop",
            "regress",
            "fork-twice",
            "fork-equal",
            "name-transfer",
            "head-transfer",
            "incomplete",
        ] {
            let log = Arc::new(FakeLog::default());
            let a = Fixture::new(&format!("writer-refuse-{case}-a"), log.clone());
            a.agents(
                if case == "head-transfer" {
                    2
                } else if case == "incomplete" {
                    3
                } else {
                    1
                },
                if case == "incomplete" { 100_000 } else { 0 },
            );
            if case == "head-transfer" {
                let mut row = a.store().agent_row(X).unwrap().unwrap();
                row.role = "head".into();
                row.project_id = Some("P".into());
                rewrite_agent(&a, row);
            }
            let restored = a.fork(&format!("writer-refuse-{case}-restored"), log.clone());
            if case == "incomplete" {
                log.on_append(Action::DropReply);
            }
            let enabled = a.enable().await;
            if case == "incomplete" {
                assert_eq!(enabled.unwrap_err(), "engram_unavailable");
            } else {
                enabled.unwrap();
            }
            let code = match case {
                "drop" => {
                    finish(a.call(
                        "agent.create",
                        json!({"role":"assistant","name":"Extra","tag":"ok","request_key":"extra"}),
                    ))
                    .await
                    .unwrap();
                    "supersede_drops_agents"
                }
                "regress" => {
                    finish(a.call(
                        "agent.rename",
                        json!({"agent_id":X,"name":"Grace","request_key":"grace"}),
                    ))
                    .await
                    .unwrap();
                    "supersede_regresses_agents"
                }
                "fork-twice" | "fork-equal" => {
                    finish(a.call(
                        "agent.rename",
                        json!({"agent_id":X,"name":"Grace","request_key":"grace"}),
                    ))
                    .await
                    .unwrap();
                    finish(restored.call(
                        "agent.rename",
                        json!({"agent_id":X,"name":"Bob","request_key":"bob"}),
                    ))
                    .await
                    .unwrap();
                    if case == "fork-twice" {
                        finish(restored.call(
                            "agent.rename",
                            json!({"agent_id":X,"name":"Carol","request_key":"carol"}),
                        ))
                        .await
                        .unwrap();
                    }
                    // The last update carries no claims. Losing accumulated
                    // rename claims would incorrectly admit the higher fork.
                    if case == "fork-twice" {
                        finish(a.call(
                            "agent.update_tag",
                            json!({"agent_id":X,"tag":"changed","request_key":"tag"}),
                        ))
                        .await
                        .unwrap();
                        finish(restored.call(
                            "agent.update_tag",
                            json!({"agent_id":X,"tag":"local","request_key":"tag"}),
                        ))
                        .await
                        .unwrap();
                    }
                    "supersede_forks_agents"
                }
                "name-transfer" => {
                    finish(restored.call(
                        "agent.dispose",
                        json!({"agent_id":X,"request_key":"dispose"}),
                    ))
                    .await
                    .unwrap();
                    finish(restored.call("agent.create", json!({"role":"assistant","name":"Agent 0","tag":"ok","request_key":"reuse"}))).await.unwrap();
                    "supersede_forks_agents"
                }
                "head-transfer" => {
                    let mut row = restored.store().agent_row(X).unwrap().unwrap();
                    row.role = "hiree".into();
                    rewrite_agent(&restored, row);
                    let mut row = restored
                        .store()
                        .agent_row("agent_0000000000000001")
                        .unwrap()
                        .unwrap();
                    row.role = "head".into();
                    row.project_id = Some("P".into());
                    rewrite_agent(&restored, row);
                    "supersede_forks_agents"
                }
                "incomplete" => "identity_log_invariant",
                _ => unreachable!(),
            };
            restored.refuse_supersede(&log, json!({}), code).await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn writer_supersede_parts_use_base_and_finish_at_base_plus_parts() {
        let (log, _a, mut restored) = supersede_setup("writer-parts").await;
        let tap = Arc::new(SupersedeTap::new(&log));
        restored.restart(tap.clone());
        restored.enable().await.unwrap();
        let requests = tap.appends();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests
                .iter()
                .map(|r| r["expected_head"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            [1, 2]
        );
        let page = log.client().read(1, 128).await.unwrap();
        assert_eq!(
            page.entries.iter().map(|e| e.position).collect::<Vec<_>>(),
            [2, 3]
        );
        for (i, entry) in page.entries.iter().enumerate() {
            let body: Value = serde_json::from_slice(&entry.entry).unwrap();
            assert_eq!(body["supersedes_through"], 1);
            assert_eq!(body["part"], i + 1);
            assert_eq!(body["parts"], 2);
            assert!(entry.entry.len() <= 200 * 1024);
            assert_eq!(
                entry.entry,
                log_client::decode_hex(requests[i]["entry"]["data"].as_str().unwrap()).unwrap()
            );
        }
        let status = restored.store().identity_log_status().unwrap();
        assert_eq!(status.last_applied_position, 3);
        assert_eq!(status.last_seen_head, 3);
        assert_eq!(status.state, "enabled");
        let payload: String = restored
            .conn()
            .query_row(
                "SELECT payload_json FROM registry_journal WHERE op='identity_log.enable'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&payload).unwrap(),
            json!({"supersedes_through":1})
        );
        restored.clean_rebuild();
    }

    #[tokio::test(start_paused = true)]
    async fn writer_supersede_lost_race_disables_and_refuses_foreign_retry() {
        let (log, _a, mut restored) = supersede_setup("writer-race").await;
        let tap = Arc::new(SupersedeTap::new(&log));
        *tap.race.lock().unwrap() = Some((
            log.with_author([0xcc; 16]),
            AppendRequest {
                expected_head: 1,
                entry_id: [0xcc; 16],
                kind: EntryKind::Change,
                data: serde_json::to_vec(&json!({"op":"project.shared","tables":{}})).unwrap(),
            },
        ));
        restored.restart(tap.clone());
        let journal = restored.journal();
        let shared = restored.shared();
        assert_eq!(
            restored.enable().await.unwrap_err(),
            "identity_log_contended"
        );
        assert_eq!(
            restored.store().identity_log_status().unwrap().state,
            "disabled"
        );
        assert!(restored.store().enable_parts().is_err());
        assert_eq!(restored.journal(), journal);
        assert_eq!(restored.shared(), shared);
        assert_eq!(log.head(), 2);
        assert_eq!(tap.appends().len(), 1, "no blind retry");
        restored
            .refuse_supersede(&log, json!({}), "join_requires_empty_registry")
            .await;
        assert_eq!(tap.appends().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn writer_supersede_own_part_one_after_head_moved_keeps_plan() {
        let (log, _a, mut restored) = supersede_setup("writer-own-race").await;
        let tap = Arc::new(SupersedeTap::new(&log));
        tap.own_race.store(true, Ordering::SeqCst);
        restored.restart(tap.clone());
        let journal = restored.journal();
        assert_eq!(
            restored.enable().await.unwrap_err(),
            "identity_log_invariant"
        );
        assert_eq!(
            restored.store().identity_log_status().unwrap().state,
            "enabling"
        );
        assert_eq!(restored.store().enable_base().unwrap(), 1);
        let parts = restored.store().enable_parts().unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(
            log.client().read(1, 1).await.unwrap().entries[0].entry_id,
            parts[0].entry_id
        );
        assert_eq!(restored.journal(), journal);
        restored.restart(tap.clone());
        restored.enable().await.unwrap();
        assert_eq!(log.head(), 3);
        assert_eq!(tap.appends().len(), 2);
        assert_eq!(
            tap.appends()[1]["entry_id"],
            log_client::encode_hex(&parts[1].entry_id)
        );
        restored.clean_rebuild();
    }

    #[tokio::test(start_paused = true)]
    async fn writer_supersede_resume_from_base_resends_only_unconfirmed_parts() {
        let (log, _a, mut restored) = supersede_setup("writer-resume").await;
        let tap = Arc::new(SupersedeTap::new(&log));
        restored.restart(tap.clone());
        let journal = restored.journal();
        log.on_append(Action::Hold);
        {
            let enable = restored.enable();
            tokio::pin!(enable);
            let held = log.wait_for_calls(log.calls() + 3);
            tokio::pin!(held);
            loop {
                tokio::select! { biased; () = &mut held => break, _ = &mut enable => panic!("enable did not park"), () = tokio::task::yield_now() => {} }
            }
            assert_eq!(log.release_next().unwrap(), 2);
            // Cancel before the first receipt is polled: part 1 landed, but
            // part 2 and the local completion transaction have not run.
        }
        assert_eq!(log.head(), 2);
        assert_eq!(
            restored.store().identity_log_status().unwrap().state,
            "enabling"
        );
        assert_eq!(restored.journal(), journal);
        let parts = restored.store().enable_parts().unwrap();
        restored.restart(tap.clone());
        tap.calls.lock().unwrap().clear();
        restored.enable().await.unwrap();
        let calls = tap.calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 2);
        assert_eq!(
            calls[0],
            (log_client::READ.into(), json!({"after":1,"limit":2}))
        );
        assert_eq!(calls[1].1["expected_head"], 2);
        assert_eq!(
            calls[1].1["entry_id"],
            log_client::encode_hex(&parts[1].entry_id)
        );
        assert_eq!(
            log.client()
                .read(1, 128)
                .await
                .unwrap()
                .entries
                .iter()
                .map(|e| e.entry.clone())
                .collect::<Vec<_>>(),
            parts.iter().map(|p| p.data.clone()).collect::<Vec<_>>()
        );
        assert_eq!(&restored.journal()[..journal.len()], journal);
        let completed = restored.journal();
        restored.enable().await.unwrap();
        assert_eq!(restored.journal(), completed);
        assert_eq!(restored.marker_count(), 1);
        assert_eq!(
            restored
                .store()
                .identity_log_status()
                .unwrap()
                .last_applied_position,
            3
        );
        restored.clean_rebuild();

        // More than one decoded-byte page is already confirmed. None of those
        // parts may be resent just because the first recovery read hit its cap.
        let (log, _a, mut restored) = supersede_setup("writer-resume-paged").await;
        restored.agents(13, 100_000);
        log.on_append(Action::DropReply);
        assert_eq!(restored.enable().await.unwrap_err(), "engram_unavailable");
        let parts = restored.store().enable_parts().unwrap();
        assert_eq!(parts.len(), 7);
        for (i, part) in parts.iter().enumerate().take(6).skip(1) {
            log.client()
                .append(&AppendRequest {
                    expected_head: 1 + i as u64,
                    entry_id: part.entry_id,
                    kind: EntryKind::Snapshot,
                    data: part.data.clone(),
                })
                .await
                .unwrap();
        }
        assert_eq!(log.head(), 7);
        let tap = Arc::new(SupersedeTap::new(&log));
        restored.restart(tap.clone());
        restored.enable().await.unwrap();
        let calls = tap.calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 3);
        assert_eq!(
            calls[0],
            (log_client::READ.into(), json!({"after":1,"limit":7}))
        );
        assert_eq!(
            calls[1],
            (log_client::READ.into(), json!({"after":6,"limit":2}))
        );
        assert_eq!(calls[2].1["expected_head"], 7);
        assert_eq!(tap.appends().len(), 1);
        assert_eq!(
            tap.appends()[0]["entry_id"],
            log_client::encode_hex(&parts[6].entry_id)
        );
        assert_eq!(
            restored
                .store()
                .identity_log_status()
                .unwrap()
                .last_applied_position,
            8
        );
        restored.clean_rebuild();
    }

    #[tokio::test(start_paused = true)]
    async fn writer_supersede_legacy_plan_resumes_base_zero_without_duplicates() {
        let log = Arc::new(FakeLog::default());
        let mut f = Fixture::new("writer-legacy-plan", log.clone());
        f.agents(3, 100_000);
        log.on_append(Action::DropReply);
        assert_eq!(f.enable().await.unwrap_err(), "engram_unavailable");
        let parts = f.store().enable_parts().unwrap();
        assert_eq!(parts.len(), 2);
        // Restore exactly the older persisted backfill shape, not a fresh plan
        // re-encoded by today's writer. Saved part ids and bytes stay untouched.
        let StorageBackend::Sqlite { path } = &f.descriptor.backend else {
            unreachable!()
        };
        let conn = Connection::open(path).unwrap();
        let text: String = conn
            .query_row("SELECT enable_backfill FROM identity_log_state", [], |r| {
                r.get(0)
            })
            .unwrap();
        let mut saved: Value = serde_json::from_str(&text).unwrap();
        saved.as_object_mut().unwrap().remove("base");
        conn.execute(
            "UPDATE identity_log_state SET enable_backfill=?1",
            [saved.to_string()],
        )
        .unwrap();
        drop(conn);
        let journal = f.journal();
        let tap = Arc::new(SupersedeTap::new(&log));
        f.restart(tap.clone());
        assert_eq!(f.store().enable_base().unwrap(), 0);
        f.enable().await.unwrap();
        let calls = tap.calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 2);
        assert_eq!(
            calls[0],
            (log_client::READ.into(), json!({"after":0,"limit":2}))
        );
        assert_eq!(calls[1].1["expected_head"], 1);
        assert_eq!(
            calls[1].1["entry_id"],
            log_client::encode_hex(&parts[1].entry_id)
        );
        let page = log.client().read(0, 128).await.unwrap();
        assert_eq!(page.head, 2);
        for (entry, part) in page.entries.iter().zip(&parts) {
            assert_eq!(entry.entry_id, part.entry_id);
            assert_eq!(entry.entry, part.data);
            assert!(serde_json::from_slice::<Value>(&entry.entry)
                .unwrap()
                .get("supersedes_through")
                .is_none());
        }
        assert_eq!(&f.journal()[..journal.len()], journal);
        let completed = f.journal();
        f.enable().await.unwrap();
        assert_eq!(f.journal(), completed);
        assert_eq!(f.marker_count(), 1);
        let status = f.store().identity_log_status().unwrap();
        assert_eq!(status.last_applied_position, 2);
        assert_eq!(status.last_seen_head, 2);
        f.clean_rebuild();
    }

    #[tokio::test(start_paused = true)]
    async fn import_consent_order_and_flag_rules() {
        // A store with projects but no agents and no marker is refused, and the
        // refusal must write nothing: `agent.import` still works afterwards,
        // and enable then succeeds.
        let log = Arc::new(FakeLog::default());
        let f = Fixture::new("guard-project", log.clone());
        let root = f.dir.join("root");
        std::fs::create_dir_all(&root).unwrap();
        f.project("P", &root, "Project", None);
        f.refuse_unwritten(&log, json!({}), "agents_not_imported")
            .await;
        let message = f
            .store()
            .check_agent_import_preconditions(false)
            .unwrap_err()
            .to_string();
        assert!(
            message.contains("agent import") && message.contains("--without-agents"),
            "{message}"
        );
        f.import_agent().await;
        f.enable().await.unwrap();
        assert_eq!(f.marker_count(), 1);
        assert_eq!(log.head(), 1);
        f.clean_rebuild();

        let log = Arc::new(FakeLog::default());
        let a = Fixture::new("guard-empty", Arc::new(log.with_author([1; 16])));
        a.refuse_unwritten(&log, json!({}), "agents_not_imported")
            .await;
        assert_eq!(a.marker_count(), 0);
        a.enable_without_agents().await.unwrap();
        assert_eq!(a.marker_count(), 1);
        let before = a.journal();
        a.enable_without_agents().await.unwrap();
        assert_eq!(a.journal(), before, "enabled ignores the flag");

        // Joining a log that already has entries skips the import check: the
        // joiner gets its cutover marker from the first machine's snapshot. So
        // --without-agents is ignored on a join, even on a store that already
        // holds a marker from an earlier import of zero agents.
        for imported in [false, true] {
            let b = Fixture::new("guard-join", Arc::new(log.with_author([2; 16])));
            if imported {
                b.empty_import();
            }
            if imported {
                b.enable_without_agents().await.unwrap();
            } else {
                b.enable().await.unwrap();
            }
            assert_eq!(b.marker_count(), 1);
            assert_eq!(
                b.store()
                    .identity_log_status()
                    .unwrap()
                    .last_applied_position,
                1
            );
            assert_eq!(b.shared(), a.shared());
            b.clean_rebuild();
        }
        assert_eq!(log.head(), 1, "join must not append");

        let log = Arc::new(FakeLog::default());
        let f = Fixture::new("guard-imported-empty", log.clone());
        f.empty_import();
        f.refuse_unwritten(&log, json!({"without_agents":true}), "invalid_request")
            .await;
        f.enable().await.unwrap();
        assert_eq!(f.marker_count(), 1);

        let log = Arc::new(FakeLog::default());
        let f = Fixture::new("guard-imported-agent", log.clone());
        f.agents(1, 0);
        f.refuse_unwritten(&log, json!({"without_agents":true}), "invalid_request")
            .await;
        f.store()
            .apply_entry("test.legacy_agents", "{}", "test", None, |tx| {
                tx.execute("DELETE FROM registry_journal WHERE op='agent.cutover'", [])?;
                Ok(())
            })
            .unwrap();
        for params in [json!({}), json!({"without_agents":true})] {
            f.refuse_unwritten(&log, params, "authority_not_cut_over")
                .await;
        }

        // While an enable is resuming a saved plan (state `enabling`) or a join
        // is catching up (state `joining`), the import check already happened
        // or doesn't apply, so the flag is ignored, even when the store now
        // holds imported agents and a marker.
        let log = Arc::new(FakeLog::default());
        let a = Fixture::new("guard-enabling-imported", log.clone());
        a.agents(1, 0);
        log.on_append(Action::DropReply);
        assert_eq!(a.enable().await.unwrap_err(), "engram_unavailable");
        assert_eq!(a.store().identity_log_status().unwrap().state, "enabling");
        a.enable_without_agents().await.unwrap();
        finish(a.call("agent.rename", json!({"agent_id":"agent_0000000000000000","name":"After bootstrap","request_key":"after-bootstrap"}))).await.unwrap();
        let b = Fixture::new("guard-joining-imported", Arc::new(log.with_author([2; 16])));
        let page = log.client().read(0, 1).await.unwrap();
        let entry = &page.entries[0];
        b.store()
            .apply_join_snapshot(
                &[entorhinal_core::remote_apply::RemoteEntry {
                    position: 1,
                    entry_id: log_client::encode_hex(&entry.entry_id),
                    signer: "test".into(),
                    key_id: "test".into(),
                    envelope_version: 1,
                    kind: entry.kind.clone(),
                    entry: entry.entry.clone(),
                }],
                page.head as i64,
            )
            .unwrap();
        assert_eq!(b.store().identity_log_status().unwrap().state, "joining");
        b.enable_without_agents().await.unwrap();
        assert_eq!(b.shared(), a.shared());
        assert_eq!(b.marker_count(), 1);
        b.clean_rebuild();
    }

    #[tokio::test(start_paused = true)]
    async fn import_consent_enable_params_fail_closed() {
        let log = Arc::new(FakeLog::default());
        let f = Fixture::new("guard-params", log.clone());
        let state = f.rows("identity_log_state");
        for params in [
            json!({"without_agent":true}),
            json!({"without_agents":true,"unknown":false}),
            json!({"without_agents":"true"}),
            json!({"without_agents":1}),
            json!({"without_agents":null}),
            json!({"without_agents":[]}),
        ] {
            assert_eq!(
                f.call("identity_log.enable", params.clone())
                    .await
                    .unwrap_err(),
                "invalid_request",
                "{params}"
            );
            assert!(f.journal().is_empty());
            assert_eq!(f.rows("identity_log_state"), state);
            assert_eq!(log.calls(), 0, "invalid params must not contact engram");
        }
        f.refuse_unwritten(&log, json!({"without_agents":false}), "agents_not_imported")
            .await;
    }

    async fn interrupt_without_agents(f: &Fixture, log: &FakeLog) -> Vec<EnablePart> {
        for i in 0..3 {
            f.store()
                .register(RegisterRequest {
                    project_id: Some(format!("P{i}")),
                    name: "x".repeat(80_000),
                    ..Default::default()
                })
                .unwrap();
        }
        log.on_append(Action::Hold);
        // Enable makes three log calls before it parks: it reads the head, reads
        // again to check for its own earlier parts, then sends part 1 of 2.
        let calls = log.calls() + 3;
        {
            let enable = f.enable_without_agents();
            tokio::pin!(enable);
            let held = log.wait_for_calls(calls);
            tokio::pin!(held);
            loop {
                tokio::select! {
                    biased;
                    () = &mut held => break,
                    _ = &mut enable => panic!("enable did not park"),
                    () = tokio::task::yield_now() => {}
                }
            }
            assert_eq!(log.held_count(), 1);
            assert_eq!(log.release_next().unwrap(), 1);
            // Dropping the enable future here kills the request. The first of
            // the two snapshot parts is in the log, but the second was never
            // sent and `finish_enable` (which writes the marker) never ran.
        }
        let parts = f.store().enable_parts().unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(log.head(), 1);
        assert_eq!(f.store().identity_log_status().unwrap().state, "enabling");
        assert_eq!(f.marker_count(), 0);
        parts
    }

    async fn assert_without_agents_finished(f: &Fixture, log: &FakeLog, parts: &[EnablePart]) {
        assert_eq!(f.store().identity_log_status().unwrap().state, "enabled");
        assert_eq!(
            f.store()
                .identity_log_status()
                .unwrap()
                .last_applied_position,
            2
        );
        assert_eq!(f.marker_count(), 1);
        assert_eq!(log.head(), 2);
        let page = log.client().read(0, 128).await.unwrap();
        for (entry, part) in page.entries.iter().zip(parts) {
            assert_eq!(entry.entry_id, part.entry_id);
            assert_eq!(entry.entry, part.data);
        }
        let journal = f.journal();
        f.enable_without_agents().await.unwrap();
        assert_eq!(f.journal(), journal);
        assert_eq!(f.marker_count(), 1);
        f.clean_rebuild();
    }

    #[tokio::test(start_paused = true)]
    async fn import_consent_interrupted_without_agents_reruns_without_flag() {
        let log = Arc::new(FakeLog::default());
        let mut f = Fixture::new("guard-resume-request", log.clone());
        let parts = interrupt_without_agents(&f, &log).await;
        let journal = f.journal();
        f.restart(log.clone());
        let calls = log.calls();
        f.enable().await.unwrap();
        assert_eq!(log.calls(), calls + 2, "resume only reads and sends part 2");
        assert_eq!(&f.journal()[..journal.len()], journal);
        assert_without_agents_finished(&f, &log, &parts).await;
    }

    #[tokio::test(start_paused = true)]
    async fn import_consent_interrupted_without_agents_background_resumes() {
        let log = Arc::new(FakeLog::default());
        let mut f = Fixture::new("guard-resume-background", log.clone());
        let parts = interrupt_without_agents(&f, &log).await;
        let journal = f.journal();
        f.restart(log.clone());
        let calls = log.calls();
        f.handler.start_catch_up();
        tokio::task::yield_now().await;
        assert_eq!(
            log.calls(),
            calls + 2,
            "background only reads and sends part 2"
        );
        assert_eq!(&f.journal()[..journal.len()], journal);
        assert_without_agents_finished(&f, &log, &parts).await;
    }

    #[tokio::test(start_paused = true)]
    async fn empty_enable_is_idempotent_admits_create_and_reports_status() {
        let log = Arc::new(FakeLog::default());
        let f = Fixture::new("enable-empty", log.clone());
        let status = f.enable_without_agents().await.unwrap();
        assert_eq!(status["state"], "enabled");
        assert_eq!(status["lastAppliedPosition"], 1);
        assert_eq!(status["pendingWriteCount"], 0);
        assert_eq!(status["lastError"], Value::Null);
        assert_eq!(log.head(), 1);
        let page = log.client().read(0, 128).await.unwrap();
        assert_eq!(page.entries[0].kind, "snapshot");
        let ops: Vec<String> = f
            .conn()
            .prepare("SELECT op FROM registry_journal ORDER BY seq")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            ops,
            ["root_key.backfill", "agent.cutover", "identity_log.enable"]
        );
        let before = f.journal();
        assert_eq!(f.enable().await.unwrap(), status);
        assert_eq!(f.journal(), before);
        let metrics = f.handler.health().await.metrics.unwrap();
        for field in [
            "lastAppliedPosition",
            "lastSeenHead",
            "pendingWriteCount",
            "lastError",
            "lastErrorAt",
        ] {
            assert_eq!(metrics[field], status[field]);
        }
        assert_eq!(metrics["identityLogState"], status["state"]);
        finish(f.call(
            "agent.create",
            json!({"role":"assistant","name":"Later","tag":"ok","request_key":"later"}),
        ))
        .await
        .unwrap();
        assert_eq!(f.store().agent_snapshot().unwrap().agents.len(), 1);
        f.clean_rebuild();
    }

    #[tokio::test(start_paused = true)]
    async fn reachable_member_and_cutover_preconditions_leave_disabled_and_unwritten() {
        for refusal in ["unavailable", "not_member", "key_unavailable"] {
            let log = Arc::new(FakeLog::default());
            let f = Fixture::new("enable-preconditions", log.clone());
            log.on_read(Action::Refuse {
                code: refusal.into(),
                detail: None,
            });
            let expected = match refusal {
                "not_member" => "identity_log_not_member",
                "key_unavailable" => "engram_key_unavailable",
                _ => "engram_unavailable",
            };
            assert_eq!(f.enable().await.unwrap_err(), expected);
            assert_eq!(f.store().identity_log_status().unwrap().state, "disabled");
            assert!(f.journal().is_empty());
            assert_eq!(log.head(), 0);
            f.empty_import();
        }
        let log = Arc::new(FakeLog::default());
        let f = Fixture::new("enable-cutover", log.clone());
        f.agents(1, 0);
        f.store()
            .apply_entry("test.legacy_agents", "{}", "test", None, |tx| {
                tx.execute("DELETE FROM registry_journal WHERE op='agent.cutover'", [])?;
                Ok(())
            })
            .unwrap();
        let before = f.journal();
        assert_eq!(f.enable().await.unwrap_err(), "authority_not_cut_over");
        assert_eq!(f.journal(), before);
        assert_eq!(f.store().identity_log_status().unwrap().state, "disabled");
        assert_eq!(log.head(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn backfill_conflict_refuses_before_writing_and_import_still_works() {
        let log = Arc::new(FakeLog::default());
        let f = Fixture::new("enable-conflict", log.clone());
        for id in ["P", "Q"] {
            let root = f.dir.join(id);
            repo(&root, None);
            f.project(id, &root, id, None);
            git(
                &root,
                &[
                    "remote",
                    "add",
                    "origin",
                    "https://github.com/owner/same.git",
                ],
            );
        }
        let before = f.journal();
        assert_eq!(
            f.enable_without_agents().await.unwrap_err(),
            "root_key_exists"
        );
        assert_eq!(f.journal(), before);
        assert!(f.rows("project_root_key").is_empty());
        assert_eq!(f.store().identity_log_status().unwrap().state, "disabled");
        assert_eq!(log.head(), 0);
        f.empty_import();
    }

    #[tokio::test(start_paused = true)]
    async fn oversized_agent_row_refuses_enable_without_appending_or_writing() {
        let log = Arc::new(FakeLog::default());
        let f = Fixture::new("enable-oversize", log.clone());
        f.agents(1, 201 * 1024);
        let before = f.journal();
        assert_eq!(f.enable().await.unwrap_err(), "shared_entry_too_large");
        assert_eq!(f.journal(), before);
        assert_eq!(log.head(), 0);
        assert_eq!(f.store().identity_log_status().unwrap().state, "disabled");
    }

    #[tokio::test(start_paused = true)]
    async fn populated_enable_restart_does_not_reimport_its_own_bootstrap() {
        let log = Arc::new(FakeLog::default());
        let mut f = Fixture::new("enable-restart", log.clone());
        f.agents(2, 0);
        f.enable().await.unwrap();
        let cursor = f.store().generation().unwrap();
        let journal = f.journal();
        f.restart(log.clone());
        f.handler
            .catch_up(&f.store(), Instant::now() + Duration::from_secs(25))
            .await
            .unwrap();
        assert_eq!(f.journal(), journal);
        assert!(f
            .store()
            .agent_changes(cursor, None)
            .unwrap()
            .entries
            .is_empty());
        assert_eq!(log.head(), 1);
        f.clean_rebuild();
    }

    #[tokio::test(start_paused = true)]
    async fn multipart_snapshot_round_trips_shared_tables_and_bootstrap_feed() {
        let log = Arc::new(FakeLog::default());
        let a = Fixture::new("enable-multipart-a", log.clone());
        let b = Fixture::new("enable-multipart-b", log.clone());
        a.agents(5, 100_000);
        let root = a.dir.join("checkout");
        std::fs::create_dir_all(&root).unwrap();
        a.project("P", &root, "Project", None);
        a.enable().await.unwrap();
        assert!(log.head() > 1);
        let page = log.client().read(0, 128).await.unwrap();
        for (i, part) in page.entries.iter().enumerate() {
            assert_eq!(part.position, i as u64 + 1);
            assert_eq!(part.kind, "snapshot");
            assert!(part.entry.len() <= 200 * 1024);
        }
        finish(a.call("agent.rename", json!({"agent_id":"agent_0000000000000000","name":"Before join","request_key":"before-join"}))).await.unwrap();
        let cursor = b.store().agent_snapshot().unwrap().generation;
        let body = serde_json::to_vec(&json!({"method":"agent.changes","params":{"incarnation":"enable-test","cursor":cursor,"wait":true}})).unwrap();
        let waiter = b.handler.handle_request_wait(&body, CORE);
        tokio::pin!(waiter);
        use std::task::{Context, Poll, Waker};
        assert!(matches!(
            std::future::Future::poll(waiter.as_mut(), &mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        b.enable().await.unwrap();
        let HandlerOutcome::Response(bytes) = waiter.await else {
            panic!("bootstrap waiter failed")
        };
        assert_eq!(
            serde_json::from_slice::<Value>(&bytes).unwrap()["result"]["entries"]
                .as_array()
                .unwrap()
                .len(),
            6
        );
        assert_eq!(b.shared(), a.shared());
        let feed = b.store().agent_changes(cursor, None).unwrap();
        assert_eq!(feed.entries.len(), 6);
        assert!(feed.entries[..5]
            .iter()
            .all(|entry| entry.op == "agent.import"));
        assert_eq!(feed.entries[5].op, "agent.rename");
        let ops: Vec<String> = b
            .conn()
            .prepare("SELECT op FROM registry_journal ORDER BY seq")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            &ops[..6],
            [
                "agent.import",
                "agent.import",
                "agent.import",
                "agent.import",
                "agent.import",
                "agent.cutover"
            ]
        );
        assert_eq!(ops[6], "shared.snapshot");
        a.clean_rebuild();
        b.clean_rebuild();
        finish(b.call("agent.rename", json!({"agent_id":"agent_0000000000000000","name":"Joined name","request_key":"rename"}))).await.unwrap();
        assert_eq!(
            b.store()
                .agent_row("agent_0000000000000000")
                .unwrap()
                .unwrap()
                .name,
            "Joined name"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn interrupted_append_resumes_enable_with_consecutive_parts_and_import_fence() {
        let log = Arc::new(FakeLog::default());
        let mut f = Fixture::new("enable-interrupt", log.clone());
        f.agents(5, 100_000);
        log.on_append(Action::DropReply);
        assert_eq!(f.enable().await.unwrap_err(), "engram_unavailable");
        assert_eq!(log.head(), 1);
        assert_eq!(f.store().identity_log_status().unwrap().state, "enabling");
        let status = f.call("identity_log.status", json!({})).await.unwrap();
        let health = f.handler.health().await.metrics.unwrap();
        assert_eq!(status["lastError"], "engram_unavailable");
        assert!(status["lastErrorAt"].is_i64());
        assert_eq!(health["lastError"], status["lastError"]);
        assert_eq!(health["lastErrorAt"], status["lastErrorAt"]);
        assert_eq!(health["identityLogState"], "enabling");
        let parts = f.store().enable_parts().unwrap();
        for (method, params) in [
            (
                "register",
                json!({"name":"Fenced","roots":[],"projectId":"fenced"}),
            ),
            ("remove", json!({"projectId":"missing"})),
            (
                "agent.rename",
                json!({"agent_id":"agent_0000000000000000","name":"Nope","request_key":"nope"}),
            ),
            (
                "agent.import",
                json!({"snapshot_path":"missing","request_key":"import"}),
            ),
            (
                "set_workspace_root",
                json!({"workspaceId":"missing","root":null}),
            ),
        ] {
            assert_eq!(
                f.call(method, params).await.unwrap_err(),
                "identity_log_enabling",
                "{method}"
            );
        }
        assert_eq!(
            f.store().agent_import(json!({}), 100).unwrap_err().code,
            "identity_log_enabling"
        );
        f.restart(log.clone());
        f.enable().await.unwrap();
        assert_eq!(log.head(), parts.len() as u64);
        let page = log.client().read(0, 128).await.unwrap();
        for (i, entry) in page.entries.iter().enumerate() {
            assert_eq!(entry.entry_id, parts[i].entry_id);
            assert_eq!(entry.entry, parts[i].data);
            assert_eq!(entry.position, i as u64 + 1);
            assert_eq!(entry.kind, "snapshot");
        }
        assert!(f.store().enable_parts().is_err());
        f.clean_rebuild();
    }

    #[tokio::test(start_paused = true)]
    async fn landed_multipart_enable_finishes_after_restart_without_resending_receipts() {
        struct SlowReceipts(FakeLog);
        #[async_trait]
        impl LogConnector for SlowReceipts {
            async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
                Ok(Arc::new(SlowReceipts(self.0.clone())))
            }
        }
        #[async_trait]
        impl LogTransport for SlowReceipts {
            async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
                let reply = self.0.call(method, params).await;
                if method == log_client::APPEND {
                    // Each append reply arrives 15 seconds after the entry lands.
                    // Two parts take 30 seconds, so the second part is in the log
                    // but its reply misses enable's 25-second deadline.
                    tokio::time::sleep(Duration::from_secs(15)).await;
                }
                reply
            }
        }
        let log = FakeLog::default();
        let connector = Arc::new(SlowReceipts(log.clone()));
        let mut f = Fixture::new("enable-slow-receipts", connector.clone());
        f.agents(3, 100_000);
        assert_eq!(f.enable().await.unwrap_err(), "engram_outcome_unknown");
        let parts = f.store().enable_parts().unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(log.head(), 2);
        f.restart(connector);
        let calls = log.calls();
        let resumed = Instant::now();
        f.enable().await.unwrap();
        assert_eq!(
            Instant::now(),
            resumed,
            "recovery waited for redundant receipts"
        );
        assert_eq!(
            log.calls(),
            calls + 1,
            "recovery should only read the saved part range"
        );
        assert_eq!(f.store().identity_log_status().unwrap().state, "enabled");
        assert_eq!(
            f.store()
                .identity_log_status()
                .unwrap()
                .last_applied_position,
            2
        );
        f.clean_rebuild();
    }

    #[tokio::test(start_paused = true)]
    async fn background_enable_completion_publishes_generation_and_wakes_parked_feed() {
        use std::{
            future::Future,
            task::{Context, Poll, Waker},
        };
        let log = Arc::new(FakeLog::default());
        let f = Fixture::new("enable-background-feed", log.clone());
        log.on_append(Action::DropReply);
        assert_eq!(
            f.enable_without_agents().await.unwrap_err(),
            "engram_unavailable"
        );
        let cursor = f.store().generation().unwrap();
        let body = serde_json::to_vec(&json!({"method":"agent.changes","params":{"incarnation":"enable-test","cursor":cursor,"wait":true}})).unwrap();
        let waiter = f.handler.handle_request_wait(&body, CORE);
        tokio::pin!(waiter);
        assert!(waiter
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        let before = Instant::now();
        f.handler.start_catch_up();
        tokio::task::yield_now().await;
        assert_eq!(f.store().identity_log_status().unwrap().state, "enabled");
        assert_eq!(
            Instant::now(),
            before,
            "completion must precede the feed timeout"
        );
        let Poll::Ready(HandlerOutcome::Response(bytes)) = waiter
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("parked feed was not notified after background enable committed")
        };
        let reply: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(reply["result"]["cursor"], f.store().generation().unwrap());
        assert_eq!(
            f.handler.health.generation.load(Ordering::Relaxed),
            f.store().generation().unwrap()
        );
        assert!(f.store().generation().unwrap() > cursor);
    }

    #[tokio::test(start_paused = true)]
    async fn nonempty_join_refuses_with_counts_without_marker_and_allows_import() {
        let log = Arc::new(FakeLog::default());
        let a = Fixture::new("enable-nonempty-a", Arc::new(log.with_author([1; 16])));
        a.enable_without_agents().await.unwrap();
        let b = Fixture::new("enable-nonempty-b", Arc::new(log.with_author([2; 16])));
        let root = b.dir.join("root");
        std::fs::create_dir_all(&root).unwrap();
        b.project("P", &root, "Project", None);
        let before = b.journal();
        assert_eq!(
            b.enable().await.unwrap_err(),
            "join_requires_empty_registry"
        );
        let error = b
            .store()
            .check_enable_preconditions(1)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("projects=1, workspaces=0, agents=0"),
            "{error}"
        );
        assert_eq!(b.journal(), before);
        assert_eq!(b.store().identity_log_status().unwrap().state, "disabled");
        assert_eq!(
            b.conn()
                .query_row(
                    "SELECT COUNT(*) FROM registry_journal WHERE op='agent.cutover'",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
        b.empty_import();
    }

    #[tokio::test(start_paused = true)]
    async fn parked_append_publishes_pending_count_before_health_and_status_without_waiting() {
        use std::{
            future::Future,
            task::{Context, Poll, Waker},
        };
        fn ready<F: Future>(future: F) -> F::Output {
            let mut future = std::pin::pin!(future);
            match future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
            {
                Poll::Ready(value) => value,
                Poll::Pending => panic!("health or status waited behind a parked writer"),
            }
        }
        let log = Arc::new(FakeLog::default());
        let f = Fixture::new("enable-pending-health", log.clone());
        f.agents(1, 0);
        f.enable().await.unwrap();
        log.on_append(Action::Hold);
        let expected_calls = log.calls() + 2;
        let write = f.call(
            "agent.rename",
            json!({"agent_id":"agent_0000000000000000","name":"Parked","request_key":"parked"}),
        );
        tokio::pin!(write);
        let barrier = log.wait_for_calls(expected_calls);
        tokio::pin!(barrier);
        loop {
            tokio::select! { biased; () = &mut barrier => break, _ = &mut write => panic!("write did not park"), () = tokio::task::yield_now() => {} }
        }
        assert_eq!(log.held_count(), 1);
        let health = ready(f.handler.health()).metrics.unwrap();
        assert_eq!(health["pendingWriteCount"], 1);
        let status = ready(f.call("identity_log.status", json!({}))).unwrap();
        assert_eq!(status["pendingWriteCount"], 1);
        assert_eq!(status["lastAppliedPosition"], health["lastAppliedPosition"]);
        assert!(write
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        log.release_next().unwrap();
        finish(write).await.unwrap();
        assert_eq!(
            f.handler.health().await.metrics.unwrap()["pendingWriteCount"],
            0
        );
    }

    #[tokio::test(start_paused = true)]
    async fn background_reader_resumes_enabling_and_completes_joining() {
        let log = Arc::new(FakeLog::default());
        let a = Fixture::new("enable-background-a", log.clone());
        log.on_append(Action::DropReply);
        assert_eq!(
            a.enable_without_agents().await.unwrap_err(),
            "engram_unavailable"
        );
        a.handler.start_catch_up();
        tokio::task::yield_now().await;
        assert_eq!(a.store().identity_log_status().unwrap().state, "enabled");
        assert_eq!(log.head(), 1);
        a.handler
            .catch_up_task
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .abort();
        finish(a.call(
            "agent.create",
            json!({"role":"assistant","name":"Background","tag":"ok","request_key":"background"}),
        ))
        .await
        .unwrap();
        let b = Fixture::new("enable-background-b", log.clone());
        let page = log.client().read(0, 1).await.unwrap();
        let entry = &page.entries[0];
        let remote = entorhinal_core::remote_apply::RemoteEntry {
            position: 1,
            entry_id: log_client::encode_hex(&entry.entry_id),
            signer: "test".into(),
            key_id: "test".into(),
            envelope_version: 1,
            kind: entry.kind.clone(),
            entry: entry.entry.clone(),
        };
        b.store()
            .apply_join_snapshot(&[remote], page.head as i64)
            .unwrap();
        assert_eq!(b.store().identity_log_status().unwrap().state, "joining");
        b.handler.start_catch_up();
        tokio::task::yield_now().await;
        assert_eq!(b.store().identity_log_status().unwrap().state, "enabled");
        assert_eq!(b.shared(), a.shared());
    }

    #[tokio::test(start_paused = true)]
    async fn losing_position_one_restores_disabled_without_a_marker_or_backfill() {
        let log = Arc::new(FakeLog::default());
        let a = Fixture::new("enable-loser", log.clone());
        let root = a.dir.join("local");
        std::fs::create_dir_all(&root).unwrap();
        a.project("Local", &root, "Local", None);
        let before = a.journal();
        log.on_append(Action::Refuse {
            code: "unavailable".into(),
            detail: None,
        });
        assert_eq!(
            a.enable_without_agents().await.unwrap_err(),
            "engram_unavailable"
        );
        let b = Fixture::new("enable-winner", log.clone());
        b.enable_without_agents().await.unwrap();
        assert_eq!(
            a.enable().await.unwrap_err(),
            "join_requires_empty_registry"
        );
        assert_eq!(a.store().identity_log_status().unwrap().state, "disabled");
        assert_eq!(a.journal(), before);
        assert!(a.store().enable_parts().is_err());
        a.empty_import();
    }

    #[derive(Clone)]
    struct StaleBootstrapRead {
        log: Arc<FakeLog>,
        stale_once: Arc<AtomicBool>,
    }
    #[async_trait]
    impl LogConnector for StaleBootstrapRead {
        async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
            Ok(Arc::new(self.clone()))
        }
    }
    #[async_trait]
    impl LogTransport for StaleBootstrapRead {
        async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
            if method == log_client::READ && self.stale_once.swap(false, Ordering::SeqCst) {
                return Ok(serde_json::to_vec(&json!({"result":{"head":0,"entries":[]}})).unwrap());
            }
            self.log.call(method, params).await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn own_bootstrap_found_after_head_moved_preserves_the_enable_plan() {
        let log = Arc::new(FakeLog::default());
        let mut f = Fixture::new("enable-own-race", log.clone());
        let root = f.dir.join("local");
        std::fs::create_dir_all(&root).unwrap();
        f.project("Local", &root, "Local", None);
        let journal = f.journal();
        let shared = f.shared();
        log.on_append(Action::DropReply);
        assert_eq!(
            f.enable_without_agents().await.unwrap_err(),
            "engram_unavailable"
        );
        let saved = f.rows("identity_log_state");
        assert_eq!(log.head(), 1);
        // A stale read hasn't seen this machine's first snapshot part, which has
        // in fact landed at position 1. The resend is refused, so enable reads
        // position 1 again and finds its own entry id there: its own snapshot is
        // in the log, not another machine's. Enable must not give up, so the
        // saved snapshot parts and the block on agent imports must both stay.
        log.on_append(Action::Refuse {
            code: log_client::HEAD_MOVED.into(),
            detail: Some(json!({"head":1})),
        });
        f.restart(Arc::new(StaleBootstrapRead {
            log: log.clone(),
            stale_once: Arc::new(AtomicBool::new(true)),
        }));
        assert_eq!(f.enable().await.unwrap_err(), "identity_log_invariant");
        assert_eq!(f.store().identity_log_status().unwrap().state, "enabling");
        assert_eq!(f.rows("identity_log_state"), saved);
        assert_eq!(f.journal(), journal);
        assert_eq!(f.shared(), shared);
        assert_eq!(f.store().enable_parts().unwrap().len(), 1);
        f.enable().await.unwrap();
        assert_eq!(f.store().identity_log_status().unwrap().state, "enabled");
        assert_eq!(log.head(), 1);
        f.clean_rebuild();
    }

    struct BootstrapOnly {
        log: Arc<FakeLog>,
        once: AtomicBool,
    }
    #[async_trait]
    impl LogConnector for BootstrapOnly {
        async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
            Ok(Arc::new(BootstrapTransport {
                log: self.log.clone(),
                once: self.once.swap(false, Ordering::SeqCst),
            }))
        }
    }
    struct BootstrapTransport {
        log: Arc<FakeLog>,
        once: bool,
    }
    #[async_trait]
    impl LogTransport for BootstrapTransport {
        async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
            let bytes = self.log.call(method, params.clone()).await?;
            if self.once && method == log_client::READ && params["limit"] == 128 {
                let mut value: Value = serde_json::from_slice(&bytes).unwrap();
                value["result"]["entries"]
                    .as_array_mut()
                    .unwrap()
                    .retain(|e| e["kind"] == "snapshot");
                self.log.on_read(Action::Refuse {
                    code: "unavailable".into(),
                    detail: None,
                });
                return Ok(serde_json::to_vec(&value).unwrap());
            }
            Ok(bytes)
        }
    }

    #[tokio::test(start_paused = true)]
    async fn join_outage_preserves_bootstrap_fence_and_resume_has_no_duplicates() {
        let log = Arc::new(FakeLog::default());
        let a = Fixture::new("join-outage-a", log.clone());
        a.agents(1, 0);
        a.enable().await.unwrap();
        finish(a.call("agent.rename", json!({"agent_id":"agent_0000000000000000","name":"Remote rename","request_key":"remote"}))).await.unwrap();
        let mut b = Fixture::new("join-outage-b", log.clone());
        // Make B receive only the initial snapshot on its first entry read, then
        // fail its next read. The reported final log position still includes A's
        // later rename, so B must remain joining until it can fetch that change.
        let connector = Arc::new(BootstrapOnly {
            log: log.clone(),
            once: AtomicBool::new(false),
        });
        b.handler.log_client = LogClient::new(connector.clone());
        // Check B's registry using the fake log's current final position, then
        // enable the one-time reply change and call catch-up directly. Skipping
        // enable's separate network check makes the simulated outage occur after
        // the snapshot is installed but before the rename is fetched.
        b.store().check_enable_preconditions(log.head()).unwrap();
        connector.once.store(true, Ordering::SeqCst);
        assert_eq!(
            b.handler
                .catch_up(&b.store(), Instant::now() + Duration::from_secs(25))
                .await
                .unwrap_err()
                .code,
            "engram_unavailable"
        );
        assert_eq!(b.store().identity_log_status().unwrap().state, "joining");
        assert_eq!(
            b.store()
                .identity_log_status()
                .unwrap()
                .last_applied_position,
            1
        );
        assert_eq!(
            b.call("agent.import", json!({})).await.unwrap_err(),
            "identity_log_enabling"
        );
        let before = b.journal();
        b.restart(log.clone());
        b.enable().await.unwrap();
        assert_eq!(&b.journal()[..before.len()], before);
        assert_eq!(b.shared(), a.shared());
        assert_eq!(b.store().agent_changes(0, None).unwrap().entries.len(), 2);
        let journal = b.journal();
        b.enable().await.unwrap();
        assert_eq!(b.journal(), journal);
        b.clean_rebuild();
    }

    #[tokio::test(start_paused = true)]
    async fn join_keeps_local_history_cached_replies_retired_bindings_and_enabled_local_rules() {
        let log = Arc::new(FakeLog::default());
        let a = Fixture::new("join-history-a", log.clone());
        let root = a.dir.join("root");
        repo(&root, Some("https://github.com/owner/remote.git"));
        a.project("Shared", &root, "Shared", None);
        a.store()
            .assign_workspace(AssignWorkspaceRequest {
                project_id: "Shared".into(),
                workspace_id: "W".into(),
                workspace_name: Some("Work".into()),
                ..Default::default()
            })
            .unwrap();
        a.enable_without_agents().await.unwrap();
        let b = Fixture::new("join-history-b", log.clone());
        let old = b.dir.join("old");
        repo(&old, None);
        b.project("Old", &old, "Old", Some("old-cache"));
        b.store().bind_root(old.to_str().unwrap(), "test").unwrap();
        b.store()
            .approve_root(old.to_str().unwrap(), "test")
            .unwrap();
        b.store()
            .remove(RemoveRequest {
                project_id: Some("Old".into()),
                ..Default::default()
            })
            .unwrap();
        let prior = b.journal();
        let retired = b.rows("retired_binding");
        assert!(!retired.is_empty());
        let cached = b
            .store()
            .shared_request_cache("register", Some("old-cache"))
            .unwrap()
            .unwrap();
        b.enable().await.unwrap();
        assert_eq!(&b.journal()[..prior.len()], prior);
        assert_eq!(b.rows("retired_binding"), retired);
        assert_eq!(
            b.store()
                .shared_request_cache("register", Some("old-cache"))
                .unwrap()
                .unwrap(),
            cached
        );
        assert_eq!(b.shared(), a.shared());
        let checkout = b.dir.join("checkout");
        repo(&checkout, Some("https://github.com/owner/remote.git"));
        finish(b.call("attach_root", json!({"path":checkout})))
            .await
            .unwrap();
        for path in [&b.dir, &checkout] {
            b.call("set_workspace_root", json!({"workspaceId":"W","root":path}))
                .await
                .unwrap();
        }
        b.call("remove_root", json!({"projectId":"Shared","root":checkout}))
            .await
            .unwrap();
        assert!(b.rows("project_root").is_empty());
        assert_eq!(b.rows("project_root_key").len(), 1);
        b.clean_rebuild();
        assert!(retired
            .iter()
            .all(|row| b.rows("retired_binding").contains(row)));
        assert_eq!(
            b.store()
                .shared_request_cache("register", Some("old-cache"))
                .unwrap()
                .unwrap(),
            cached
        );
    }
}
