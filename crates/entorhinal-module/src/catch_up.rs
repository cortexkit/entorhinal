//! Reads the identity log under the shared writer lock. Incomplete snapshots
//! stay uncommitted, so a restart can fetch their parts again from durable progress.

use super::{log_client, HandlerError, HealthGauges, ProjectsHandler};
use entorhinal_core::{remote_apply::RemoteEntry, RegistryError, RegistryStore};
use std::sync::{atomic::Ordering, Arc, Mutex};
use std::time::Duration;
use tokio::time::{timeout_at, Instant};

const INTERVAL: Duration = Duration::from_secs(30);
const READ_DEADLINE: Duration = Duration::from_secs(25);

struct Worker {
    store: Arc<Mutex<Option<RegistryStore>>>,
    health: Arc<HealthGauges>,
    log: log_client::LogClient,
    writer: Arc<tokio::sync::Mutex<()>>,
    commits: Arc<tokio::sync::Notify>,
}

impl ProjectsHandler {
    fn catch_up_worker(&self) -> Worker {
        Worker {
            store: self.store.clone(),
            health: self.health.clone(),
            log: self.log_client.clone(),
            writer: self.writer.clone(),
            commits: self.commits.clone(),
        }
    }

    pub(super) fn start_catch_up(&self) {
        let mut task = self.catch_up_task.lock().unwrap();
        if task.is_none() {
            let worker = self.catch_up_worker();
            *task = Some(tokio::spawn(async move { worker.background().await }));
        }
    }

    /// The caller already holds the writer lock. Shared writes and the periodic
    /// reader use this same implementation, including the request's deadline.
    pub(super) async fn catch_up(
        &self,
        store: &RegistryStore,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let result = self.catch_up_worker().run(store, deadline).await;
        if let Err(error) = &result {
            super::enable::record_error(&self.health, error);
        }
        result
    }
}

impl Drop for ProjectsHandler {
    fn drop(&mut self) {
        if let Some(task) = self.catch_up_task.get_mut().unwrap().take() {
            task.abort();
        }
    }
}

fn storage(error: RegistryError) -> HandlerError {
    HandlerError::new("storage_error", error.to_string())
}

fn remote(entry: log_client::LogEntry) -> Result<RemoteEntry, HandlerError> {
    Ok(RemoteEntry {
        position: i64::try_from(entry.position).map_err(|_| {
            HandlerError::new("identity_log_stalled", "log position exceeds local range")
        })?,
        entry_id: log_client::encode_hex(&entry.entry_id),
        signer: log_client::encode_hex(&entry.signer),
        key_id: serde_json::json!({"family":entry.key_id.family,"epoch":entry.key_id.epoch})
            .to_string(),
        envelope_version: i64::try_from(entry.envelope_version).unwrap_or(-1),
        kind: entry.kind,
        entry: entry.entry,
    })
}

impl Worker {
    async fn background(self) {
        let mut ticks = tokio::time::interval(INTERVAL);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticks.tick().await;
            let deadline = Instant::now() + READ_DEADLINE;
            let Ok(_writer) = timeout_at(deadline, self.writer.lock()).await else {
                continue;
            };
            let store = self.store.lock().unwrap().clone();
            if let Some(store) = store {
                // Disabled stores never contact engram. An enabling store has
                // snapshot parts saved in SQLite with their entry ids and bytes:
                // retry those appends rather than import the log as if this
                // machine had not started writing it. Joining and enabled stores
                // read the remaining entries from their saved applied position.
                if store
                    .identity_log_status()
                    .is_ok_and(|s| matches!(s.state.as_str(), "enabled" | "joining" | "enabling"))
                {
                    let result = if store
                        .identity_log_status()
                        .is_ok_and(|s| s.state == "enabling")
                    {
                        let result = super::enable::resume(
                            &store,
                            &self.log,
                            &self.health,
                            deadline,
                            "entorhinal",
                        )
                        .await;
                        result.and_then(|()| self.applied(&store))
                    } else {
                        self.run(&store, deadline).await
                    };
                    super::enable::refresh(&self.health, &store);
                    if let Err(error) = result {
                        super::enable::record_error(&self.health, &error);
                        tracing::warn!(target: "identity_log", "catch-up: {}: {}", error.code, error.message);
                    }
                }
            }
        }
    }

    fn stalled(&self, state: u64, position: u64, message: impl std::fmt::Display) -> HandlerError {
        self.health.log_state.store(state, Ordering::Relaxed);
        let message = format!("at log position {position}: {message}");
        tracing::warn!(target: "identity_log", "{message}");
        HandlerError::new("identity_log_stalled", message)
    }

    async fn read(
        &self,
        after: u64,
        deadline: Instant,
    ) -> Result<log_client::ReadReply, HandlerError> {
        timeout_at(deadline, self.log.read(after, 128))
            .await
            .map_err(|_| {
                HandlerError::new(
                    "engram_unavailable",
                    "connecting or catch-up deadline expired",
                )
            })?
            .map_err(|error| match error {
                log_client::LogError::KeyUnavailable => {
                    self.health.log_state.store(1, Ordering::Relaxed);
                    HandlerError::new("engram_key_unavailable", "engram key unavailable")
                }
                log_client::LogError::NotMember => {
                    self.health.log_state.store(2, Ordering::Relaxed);
                    HandlerError::new("identity_log_not_member", "device is not a log member")
                }
                log_client::LogError::VerifyFailed { position } => {
                    self.stalled(5, position.unwrap_or(after + 1), "log verification failed")
                }
                log_client::LogError::Protocol(message) => self.stalled(6, after + 1, message),
                _ => HandlerError::new("engram_unavailable", "engram unavailable during catch-up"),
            })
    }

    async fn check_supersede_authors(
        &self,
        through: u64,
        author: &str,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        // Re-read entries 1..through and compare each one's author with the
        // snapshot's. Authors are never stored, so a member that applied those
        // entries earlier, possibly before a restart, has no record of them. A
        // joiner that just read them re-reads too, so there is one code path.
        let mut after = 0;
        while after < through {
            let page = self.read(after, deadline).await?;
            if page.head < through {
                self.health.log_state.store(7, Ordering::Relaxed);
                return Err(HandlerError::new(
                    "engram_unavailable",
                    "log head regressed",
                ));
            }
            let start = after;
            for entry in page.entries {
                if entry.position > through {
                    break;
                }
                if entry.position != after + 1 {
                    return Err(self.stalled(4, after + 1, "log gap"));
                }
                if entry.author != author {
                    return Err(self.stalled(6, through + 1, format!("supersede invariant: replaced entry has a different author at position {}", entry.position)));
                }
                after = entry.position;
            }
            if after == start {
                return Err(self.stalled(4, after + 1, "log page omitted the next position"));
            }
        }
        Ok(())
    }

    async fn run(&self, store: &RegistryStore, deadline: Instant) -> Result<(), HandlerError> {
        if matches!(self.health.log_state.load(Ordering::Relaxed), 4..=6)
            && store.identity_log_status().map_err(storage)?.state == "enabled"
        {
            return Err(HandlerError::new(
                "identity_log_stalled",
                "identity log catch-up is stalled",
            ));
        }
        let mut after = store
            .identity_log_status()
            .map_err(storage)?
            .last_applied_position as u64;
        let mut parts = Vec::new();
        let mut authors = Vec::new();
        let mut snapshot = None;
        loop {
            let page = self.read(after, deadline).await?;
            if page.head < after {
                self.health.log_state.store(7, Ordering::Relaxed);
                return Err(HandlerError::new(
                    "engram_unavailable",
                    "log head regressed",
                ));
            }
            let head = i64::try_from(page.head)
                .map_err(|_| self.stalled(6, after + 1, "log head exceeds local range"))?;
            let mut joining = store.identity_log_status().map_err(storage)?.state == "disabled";
            if !joining {
                store.observe_log_head(head).map_err(storage)?;
            }
            let page_start = after;
            for entry in page.entries {
                if entry.position != after + 1 || entry.position > page.head {
                    return Err(self.stalled(4, after + 1, "log gap"));
                }
                let author = entry.author.clone();
                let entry = remote(entry).map_err(|e| self.stalled(6, after + 1, e.message))?;
                let position = entry.position as u64;
                if entry.kind == "snapshot" || !parts.is_empty() {
                    let info = entorhinal_core::remote_apply::snapshot_part_info(&entry)
                        .map_err(|e| self.stalled(6, position, e))?;
                    let expected = snapshot.get_or_insert_with(|| (info.0.clone(), info.2));
                    if info.0 != expected.0
                        || info.2 != expected.1
                        || info.1 != parts.len() as u64 + 1
                    {
                        return Err(self.stalled(6, position, "snapshot parts are not in order"));
                    }
                    parts.push(entry);
                    authors.push(author);
                    if info.1 == info.2 {
                        if let Some(through) =
                            entorhinal_core::remote_apply::snapshot_supersedes_through(&parts)
                                .map_err(|e| self.stalled(6, position, e))?
                        {
                            if authors.iter().any(|author| author != &authors[0]) {
                                return Err(self.stalled(6, position, format!("supersede invariant: snapshot parts have different authors at position {position}")));
                            }
                            self.check_supersede_authors(through, &authors[0], deadline)
                                .await?;
                        }
                        (if joining {
                            store.apply_join_snapshot(&parts, head)
                        } else {
                            store.apply_remote_snapshot(&parts)
                        })
                        .map_err(|e| self.stalled(6, position, e))?;
                        joining = false;
                        parts.clear();
                        authors.clear();
                        snapshot = None;
                        self.applied(store)?;
                    }
                } else {
                    if joining {
                        return Err(self.stalled(
                            6,
                            position,
                            "join requires a bootstrap snapshot",
                        ));
                    }
                    let own = store.is_pending_entry(&entry.entry_id).map_err(storage)?;
                    store
                        .apply_remote_entry(&entry)
                        .map_err(|e| self.stalled(6, position, e))?;
                    if own {
                        self.health
                            .log_own_entries_applied
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    self.applied(store)?;
                }
                after = position;
            }
            if after == page.head {
                if !parts.is_empty() {
                    // Reaching head without the remaining parts is not a gap,
                    // nor proof that an unresolved append can never land.
                    return Err(HandlerError::new(
                        "engram_unavailable",
                        "snapshot incomplete; waiting for remaining parts",
                    ));
                }
                self.health.log_state.store(0, Ordering::Relaxed);
                store.finish_join(head).map_err(storage)?;
                super::enable::refresh(&self.health, store);
                self.applied(store)?;
                return Ok(());
            }
            if after == page_start {
                return Err(self.stalled(4, after + 1, "log page omitted the next position"));
            }
        }
    }

    fn applied(&self, store: &RegistryStore) -> Result<(), HandlerError> {
        super::enable::refresh(&self.health, store);
        self.health
            .generation
            .fetch_max(store.generation().map_err(storage)?, Ordering::Relaxed);
        self.commits.notify_waiters();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        fake_log::{Action, FailConnector, FakeLog},
        log_client::{AppendRequest, EntryKind, LogConnector, LogTransport, TransportError},
        RouteAdmission, WireRequest, WRITER_MODULE,
    };
    use async_trait::async_trait;
    use cortexkit_store_types::StorageDescriptor;
    use serde_json::{json, Value};
    use std::{
        future::Future,
        io::{BufRead, Write},
        path::PathBuf,
        pin::Pin,
        task::{Context, Poll, Waker},
    };
    use subc_client_rs::ModuleHandler;

    const ROUTE: super::super::RouteKey = (98, 1);
    const TICK: Duration = Duration::from_millis(1);

    #[derive(Default)]
    struct Tap {
        fake: FakeLog,
        reads: Mutex<Vec<Value>>,
        override_head: Mutex<Option<u64>>,
        reread_author: Mutex<Option<u64>>,
        reread_action: Mutex<Option<Action>>,
    }
    struct TapConnector(Arc<Tap>);
    #[async_trait]
    impl LogConnector for TapConnector {
        async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
            Ok(self.0.clone())
        }
    }
    #[async_trait]
    impl LogTransport for Tap {
        async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
            let reread = method == log_client::READ
                && params["after"] == 0
                && self
                    .reads
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|read| read["after"] == 0);
            if method == log_client::READ {
                self.reads.lock().unwrap().push(params.clone());
            }
            if reread {
                if let Some(action) = self.reread_action.lock().unwrap().take() {
                    self.fake.on_read(action);
                }
            }
            let reply = self.fake.call(method, params).await?;
            if method == log_client::READ {
                if reread {
                    if let Some(position) = *self.reread_author.lock().unwrap() {
                        let mut value: Value = serde_json::from_slice(&reply).unwrap();
                        for entry in value["result"]["entries"].as_array_mut().unwrap() {
                            if entry["position"] == position {
                                entry["author"] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into();
                            }
                        }
                        return Ok(serde_json::to_vec(&value).unwrap());
                    }
                }
                if let Some(head) = *self.override_head.lock().unwrap() {
                    let mut value: Value = serde_json::from_slice(&reply).unwrap();
                    value["result"]["head"] = head.into();
                    return Ok(serde_json::to_vec(&value).unwrap());
                }
            }
            Ok(reply)
        }
    }

    struct Fixture {
        dir: PathBuf,
        descriptor: StorageDescriptor,
        handler: ProjectsHandler,
        tap: Arc<Tap>,
    }
    impl Fixture {
        fn handler(
            descriptor: &StorageDescriptor,
            connector: Arc<dyn LogConnector>,
        ) -> ProjectsHandler {
            let handler =
                ProjectsHandler::with_log_connector("0123456789abcdef".into(), || 700, connector);
            *handler.store.lock().unwrap() = Some(RegistryStore::open(descriptor).unwrap());
            handler.health.store_ready.store(true, Ordering::Relaxed);
            handler.route_admissions().insert(
                ROUTE,
                RouteAdmission {
                    principal: Some(subc_protocol::Principal::Reserved {
                        module_id: WRITER_MODULE.into(),
                    }),
                    flow_id: None,
                    handle: None,
                },
            );
            handler
        }
        fn new(label: &str) -> Self {
            let (dir, descriptor) = crate::tests::scratch_descriptor(label);
            let tap = Arc::new(Tap::default());
            let handler = Self::handler(&descriptor, Arc::new(TapConnector(tap.clone())));
            handler
                .with_store(|s| {
                    s.apply_entry("identity_log.enable", "{}", "test", None, |tx| {
                        tx.execute("UPDATE identity_log_state SET state='enabled'", [])?;
                        Ok(())
                    })
                })
                .unwrap();
            handler
                .with_store(|s| s.apply_entry("agent.cutover", "{}", "test", None, |_| Ok(())))
                .unwrap();
            Self {
                dir,
                descriptor,
                handler,
                tap,
            }
        }
        fn store(&self) -> RegistryStore {
            self.handler.with_store(|s| Ok(s.clone())).unwrap()
        }
        fn restart(&mut self, connector: Arc<dyn LogConnector>) {
            self.handler.store.lock().unwrap().take();
            self.handler = Self::handler(&self.descriptor, connector);
        }
        fn position(&self) -> i64 {
            self.store()
                .identity_log_status()
                .unwrap()
                .last_applied_position
        }
        fn pending(&self) -> i64 {
            self.store()
                .identity_log_status()
                .unwrap()
                .pending_write_count
        }
        fn state(&self) -> &'static str {
            crate::shared_write::health_state(self.handler.health.log_state.load(Ordering::Relaxed))
        }
        async fn catch(&self) -> Result<(), HandlerError> {
            self.handler
                .catch_up(&self.store(), Instant::now() + READ_DEADLINE)
                .await
        }
        async fn append(&self, value: Value, snapshot: bool) {
            let head = self.tap.fake.head();
            let mut id = [0; 16];
            id[..8].copy_from_slice(&(head + 1).to_le_bytes());
            self.tap
                .fake
                .client()
                .append(&AppendRequest {
                    expected_head: head,
                    entry_id: id,
                    kind: if snapshot {
                        EntryKind::Snapshot
                    } else {
                        EntryKind::Change
                    },
                    data: serde_json::to_vec(&value).unwrap(),
                })
                .await
                .unwrap();
        }
        async fn write(&self, id: &str) -> Result<Vec<u8>, HandlerError> {
            self.handler
                .write_wait(
                    WireRequest {
                        method: "register".into(),
                        params: json!({"projectId":id,"name":id,"roots":[],"requestKey":id}),
                    },
                    ROUTE,
                    Instant::now(),
                )
                .await
        }
        fn own_count(&self) -> u64 {
            self.handler
                .health
                .log_own_entries_applied
                .load(Ordering::Relaxed)
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            self.handler.store.lock().unwrap().take();
            std::fs::remove_dir_all(&self.dir).unwrap();
        }
    }
    fn empty() -> Value {
        json!({"op":"project.shared","tables":{}})
    }
    fn project(id: &str) -> Value {
        json!({"op":"project.shared","tables":{"project":{"upsert":[{"project_id":id,"name":id,"implicit":0,"seed_identity":null,"created_at":1,"updated_at":1}],"delete":[]}}})
    }
    fn part(number: u64, count: u64, id: &str) -> Value {
        let mut value = project(id);
        value["op"] = "shared.snapshot".into();
        value["snapshot_id"] = "bootstrap".into();
        value["part"] = number.into();
        value["parts"] = count.into();
        value
    }

    fn agent(id: char, name: &str, claim_id: i64) -> entorhinal_core::agent::AgentChangeEntry {
        let id = format!("agent_{}", id.to_string().repeat(16));
        serde_json::from_value(json!({"seq":0,"op":"agent.import","agent_id":id,"agent_generation":1,
            "row":{"agent_id":id,"name":name,"name_version":1,"name_normalization_version":1,"tag":"test","labels":[],"role":"assistant","project_id":null,"workspace_id":null,"avatar":null,"github_identity":null,"status":"live","merged_into":null,"supervisor_agent_id":null,"request_key":null,"created_at_ms":1,"updated_at_ms":1,"terminal_at_ms":null,"agent_generation":1},
            "claims":[{"claim_id":claim_id,"agent_id":id,"namespace_kind":"assistant","namespace_key":"","normalized_name":name.to_lowercase(),"name_normalization_version":1,"display_name":name,"claimed_at_ms":1,"released_at_ms":null}],
            "old_display_name":null,"new_display_name":null,"status":null,"merged_into":null})).unwrap()
    }

    fn rename(
        mut agent: entorhinal_core::agent::AgentChangeEntry,
        name: &str,
        claim_id: i64,
    ) -> entorhinal_core::agent::AgentChangeEntry {
        agent.agent_generation += 1;
        agent.row.agent_generation += 1;
        agent.row.name_version += 1;
        agent.row.name = name.into();
        agent.row.updated_at_ms += 1;
        let mut claim = agent.claims.last().unwrap().clone();
        agent.claims.last_mut().unwrap().released_at_ms = Some(agent.row.updated_at_ms);
        claim.claim_id = claim_id;
        claim.normalized_name = name.to_lowercase();
        claim.display_name = name.into();
        claim.claimed_at_ms = agent.row.updated_at_ms;
        agent.claims.push(claim);
        agent
    }

    fn agents_part(
        agents: &[entorhinal_core::agent::AgentChangeEntry],
        through: Option<u64>,
    ) -> Value {
        let mut value = part(1, 1, "P");
        value["agents"] = serde_json::to_value(agents).unwrap();
        if let Some(through) = through {
            value["supersedes_through"] = through.into();
        }
        value
    }

    fn joiner(f: &Fixture) {
        f.store()
            .apply_entry("test-joining", "{}", "test", None, |tx| {
                tx.execute("UPDATE identity_log_state SET state='disabled'", [])?;
                Ok(())
            })
            .unwrap();
    }

    async fn assert_supersede_stalled(
        f: &mut Fixture,
        position: i64,
        bad_position: u64,
        joining: bool,
    ) {
        for _ in 0..2 {
            let error = f.catch().await.unwrap_err();
            assert_eq!(error.code, "identity_log_stalled");
            assert!(error.message.contains("supersede invariant"), "{error:?}");
            assert!(
                error.message.contains(&format!("position {bad_position}")),
                "{error:?}"
            );
            assert_eq!(f.handler.health.log_state.load(Ordering::Relaxed), 6);
            assert_eq!(f.state(), "apply_failed");
            assert_eq!(
                super::super::enable::last_error(&f.handler.health),
                Some("apply_failed")
            );
            assert_eq!(f.position(), position);
            let generation = f.store().generation().unwrap();
            let before = serde_json::to_value(f.store().agent_snapshot().unwrap()).unwrap();
            assert_eq!(
                f.write("blocked").await.unwrap_err().code,
                if joining {
                    "identity_log_enabling"
                } else {
                    "identity_log_stalled"
                }
            );
            assert_eq!(f.store().generation().unwrap(), generation);
            assert_eq!(
                serde_json::to_value(f.store().agent_snapshot().unwrap()).unwrap(),
                before
            );
            assert!(audit(&f.store()).iter().all(|row| row.2 <= position));
            f.restart(Arc::new(TapConnector(f.tap.clone())));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn forged_or_malformed_supersedes_stall_joiners_members_and_restarted_members() {
        for mode in ["joiner", "member", "restarted"] {
            for bad in [
                "foreign",
                "zero",
                "mixed",
                "partial",
                "reread",
                "part-author",
            ] {
                let mut f = Fixture::new(&format!("supersede-{mode}-{bad}"));
                if mode == "joiner" {
                    joiner(&f);
                }
                f.append(part(1, 1, "Before"), true).await;
                if bad == "foreign" {
                    f.tap
                        .fake
                        .with_author([7; 16])
                        .client()
                        .append(&AppendRequest {
                            expected_head: 1,
                            entry_id: [9; 16],
                            kind: EntryKind::Change,
                            data: serde_json::to_vec(&empty()).unwrap(),
                        })
                        .await
                        .unwrap();
                } else {
                    f.append(empty(), false).await;
                }
                if mode != "joiner" {
                    f.catch().await.unwrap();
                    if mode == "restarted" {
                        f.restart(Arc::new(TapConnector(f.tap.clone())));
                    }
                }
                let mut first = part(1, 2, "After1");
                first["supersedes_through"] = if bad == "zero" { 0 } else { 2 }.into();
                let mut second = part(2, 2, "After2");
                if bad != "partial" {
                    second["supersedes_through"] = if bad == "mixed" {
                        1
                    } else if bad == "zero" {
                        0
                    } else {
                        2
                    }
                    .into();
                }
                f.append(first, true).await;
                if bad == "part-author" {
                    f.tap
                        .fake
                        .with_author([7; 16])
                        .client()
                        .append(&AppendRequest {
                            expected_head: 3,
                            entry_id: [8; 16],
                            kind: EntryKind::Snapshot,
                            data: serde_json::to_vec(&second).unwrap(),
                        })
                        .await
                        .unwrap();
                } else {
                    f.append(second, true).await;
                }
                f.append(project("PastBadSnapshot"), false).await;
                if bad == "reread" {
                    *f.tap.reread_author.lock().unwrap() = Some(2);
                }
                let bad_position = match bad {
                    "foreign" | "reread" => 2,
                    "zero" => 3,
                    _ => 4,
                };
                assert_supersede_stalled(&mut f, 2, bad_position, mode == "joiner").await;
                let projects = f.store().enumerate(None).unwrap().projects;
                assert_eq!(projects.len(), 1);
                assert_eq!(projects[0].project_id, "Before");
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn restarted_member_supersede_rereads_paged_prefix_then_replaces_state() {
        let mut f = Fixture::new("supersede-paged");
        f.append(part(1, 1, "Before"), true).await;
        for _ in 1..130 {
            f.append(empty(), false).await;
        }
        f.catch().await.unwrap();
        f.restart(Arc::new(TapConnector(f.tap.clone())));
        f.tap.reads.lock().unwrap().clear();
        f.append(agents_part(&[], Some(130)), true).await;
        f.catch().await.unwrap();
        assert_eq!(f.position(), 131);
        assert_eq!(
            *f.tap.reads.lock().unwrap(),
            vec![
                json!({"after":130,"limit":128}),
                json!({"after":0,"limit":128}),
                json!({"after":128,"limit":128})
            ]
        );
        let projects = f.store().enumerate(None).unwrap().projects;
        assert_eq!(projects.len(), 1);
        assert_eq!(projects[0].project_id, "P");
        assert!(f.store().verify().unwrap().ok);
        f.store().rebuild().unwrap();
        assert_eq!(
            f.store().enumerate(None).unwrap().projects[0].project_id,
            "P"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn supersede_reread_failures_use_main_read_health_and_never_install() {
        for (action, code, state) in [
            (
                Action::Refuse {
                    code: log_client::UNAVAILABLE.into(),
                    detail: None,
                },
                "engram_unavailable",
                8,
            ),
            (
                Action::Refuse {
                    code: log_client::VERIFY_FAILED.into(),
                    detail: Some(json!({"position":1})),
                },
                "identity_log_stalled",
                5,
            ),
            (
                Action::Refuse {
                    code: log_client::KEY_UNAVAILABLE.into(),
                    detail: None,
                },
                "engram_key_unavailable",
                1,
            ),
            (Action::Hang, "engram_unavailable", 8),
        ] {
            let f = Fixture::new("supersede-reread-errors");
            f.append(part(1, 1, "Before"), true).await;
            f.catch().await.unwrap();
            let before = f.store().generation().unwrap();
            f.append(agents_part(&[], Some(1)), true).await;
            *f.tap.reread_action.lock().unwrap() = Some(action);
            assert_eq!(f.catch().await.unwrap_err().code, code);
            assert_eq!(f.handler.health.log_state.load(Ordering::Relaxed), state);
            assert_eq!(f.position(), 1);
            assert_eq!(f.store().generation().unwrap(), before);
            assert_eq!(
                f.store().enumerate(None).unwrap().projects[0].project_id,
                "Before"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn supersede_valid_rename_sorts_claims_and_wakes_import_feed_without_duplicates() {
        let f = Fixture::new("supersede-rename");
        let old = agent('b', "Ada", 1);
        let unchanged = agent('c', "Unchanged", 3);
        f.append(agents_part(&[old.clone(), unchanged.clone()], None), true)
            .await;
        f.catch().await.unwrap();
        let cursor = f.store().generation().unwrap();
        let body = serde_json::to_vec(&json!({"method":"agent.changes","params":{"incarnation":f.handler.incarnation,"cursor":cursor,"wait":true}})).unwrap();
        let waiter = f.handler.handle_request_wait(&body, ROUTE);
        tokio::pin!(waiter);
        poll_pending(waiter.as_mut());
        let mut renamed = rename(old, "Grace", 2);
        // Send the claims newest first. The reader must sort them by claim_id
        // itself, releasing the old name before writing the new active one, or
        // the unique index on active claims refuses the write.
        renamed.claims.reverse();
        f.append(agents_part(&[unchanged, renamed.clone()], Some(1)), true)
            .await;
        f.catch().await.unwrap();
        let Poll::Ready(subc_client_rs::HandlerOutcome::Response(body)) = waiter
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("import feed waiter was not woken");
        };
        let value: Value = serde_json::from_slice(&body).unwrap();
        let entries = value["result"]["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["op"], "agent.import");
        assert_eq!(entries[0]["agent_id"], renamed.agent_id);
        renamed.claims.sort_by_key(|claim| claim.claim_id);
        assert_eq!(
            f.store().agent_claims(&renamed.agent_id).unwrap(),
            renamed.claims
        );
        assert_eq!(
            entries[0]["claims"],
            serde_json::to_value(&renamed.claims).unwrap()
        );
        assert!(f.store().verify().unwrap().ok);
        f.store().rebuild().unwrap();
        assert_eq!(
            f.store().agent_claims(&renamed.agent_id).unwrap(),
            renamed.claims
        );
    }

    #[tokio::test(start_paused = true)]
    async fn supersede_valid_head_replacement_writes_terminal_agents_before_live_heads() {
        let f = Fixture::new("supersede-head");
        let mut old = agent('f', "Old", 1);
        old.row.role = "head".into();
        old.row.project_id = Some("P".into());
        f.append(agents_part(&[old.clone()], None), true).await;
        f.catch().await.unwrap();
        let cursor = f.store().generation().unwrap();
        old.row.status = "retired".into();
        old.row.terminal_at_ms = Some(2);
        old.row.updated_at_ms = 2;
        old.row.agent_generation = 2;
        old.agent_generation = 2;
        old.claims[0].released_at_ms = Some(2);
        let mut new = agent('a', "New", 2);
        new.row.role = "head".into();
        new.row.project_id = Some("P".into());
        f.append(agents_part(&[new.clone(), old.clone()], Some(1)), true)
            .await;
        f.catch().await.unwrap();
        let entries = f.store().agent_changes(cursor, None).unwrap().entries;
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.agent_id.as_str())
                .collect::<Vec<_>>(),
            vec![old.agent_id.as_str(), new.agent_id.as_str()]
        );
        assert!(entries.iter().all(|entry| entry.op == "agent.import"));
        let before = serde_json::to_value(f.store().agent_snapshot().unwrap()).unwrap();
        assert!(f.store().verify().unwrap().ok);
        f.store().rebuild().unwrap();
        assert_eq!(
            serde_json::to_value(f.store().agent_snapshot().unwrap()).unwrap(),
            before
        );
    }

    #[tokio::test(start_paused = true)]
    async fn supersede_agent_forks_drops_and_regressions_stall_without_changing_tables() {
        for bad in [
            "missing",
            "generation",
            "version",
            "equal-row",
            "fork-rename",
            "claim-agent",
            "claim-namespace-kind",
            "claim-namespace-key",
            "claim-normalized",
            "claim-display",
            "claim-version",
            "claim-time",
            "claim-release",
            "claim-missing",
            "active-name",
            "head",
        ] {
            let mut f = Fixture::new(&format!("supersede-agent-{bad}"));
            let mut x = rename(agent('b', "Ada", 1), "Grace", 2);
            if bad == "head" {
                x.row.role = "head".into();
                x.row.project_id = Some("P".into());
            }
            let y = agent('c', "Other", 3);
            f.append(agents_part(&[x.clone(), y.clone()], None), true)
                .await;
            f.catch().await.unwrap();
            let before = serde_json::to_value(f.store().agent_snapshot().unwrap()).unwrap();
            let mut incoming = vec![x.clone(), y];
            match bad {
                "missing" => {
                    incoming.remove(0);
                }
                "generation" => {
                    incoming[0].agent_generation -= 1;
                    incoming[0].row.agent_generation -= 1;
                }
                "version" => incoming[0].row.name_version -= 1,
                "equal-row" => incoming[0].row.tag = "fork".into(),
                "fork-rename" => {
                    incoming[0] = rename(rename(agent('b', "Ada", 1), "Bob", 2), "Carol", 4)
                }
                "active-name" => {
                    incoming[0] = rename(x.clone(), "Freed", 4);
                    incoming[1] = rename(incoming[1].clone(), "Grace", 5);
                }
                "head" => {
                    incoming[0].row.role = "hiree".into();
                    incoming[0].row.agent_generation += 1;
                    incoming[0].agent_generation += 1;
                    incoming[1].row.role = "head".into();
                    incoming[1].row.project_id = Some("P".into());
                    incoming[1].row.agent_generation += 1;
                    incoming[1].agent_generation += 1;
                }
                "claim-missing" => {
                    incoming[0].claims.remove(0);
                }
                _ => {
                    let claim = &mut incoming[0].claims[0];
                    match bad {
                        "claim-agent" => claim.agent_id = format!("agent_{}", "c".repeat(16)),
                        "claim-namespace-kind" => claim.namespace_kind = "workspace".into(),
                        "claim-namespace-key" => claim.namespace_key = "W".into(),
                        "claim-normalized" => claim.normalized_name = "fork".into(),
                        "claim-display" => claim.display_name = "Fork".into(),
                        "claim-version" => claim.name_normalization_version = 2,
                        "claim-time" => claim.claimed_at_ms = 10,
                        "claim-release" => claim.released_at_ms = Some(10),
                        _ => unreachable!(),
                    }
                }
            }
            f.append(agents_part(&incoming, Some(1)), true).await;
            f.append(project("PastBadSnapshot"), false).await;
            assert_supersede_stalled(&mut f, 1, 2, false).await;
            assert_eq!(
                serde_json::to_value(f.store().agent_snapshot().unwrap()).unwrap(),
                before,
                "{bad}"
            );
        }
    }
    // Reads journal columns that are only reachable inside a write transaction.
    // `apply_entry` inserts a throwaway journal row to open one; the closure
    // collects the columns, then runs a deliberately invalid SELECT so the
    // transaction fails and rolls that row back. The store is left unchanged.
    fn audit(store: &RegistryStore) -> Vec<(String, String, i64, String, String)> {
        let rows = Mutex::new(None);
        let result = store.apply_entry("inspection", "{}", "test", None, |tx| {
            let found = tx.prepare("SELECT op,origin,log_position,entry_id,payload_json FROM registry_journal WHERE stream='shared' ORDER BY seq")?
                .query_map([], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)))?
                .collect::<Result<Vec<_>,_>>()?;
            *rows.lock().unwrap() = Some(found);
            tx.execute_batch("SELECT catch_up_inspection_rollback")?;
            Ok(())
        });
        assert!(result.is_err());
        rows.into_inner().unwrap().unwrap()
    }
    fn poll_pending<F: Future>(future: Pin<&mut F>) {
        assert!(future
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
    }
    async fn finish<F: Future>(mut future: Pin<&mut F>) -> F::Output {
        let now = Instant::now();
        let output = loop {
            tokio::select! { result = &mut future => break result, () = tokio::task::yield_now() => {} }
        };
        assert_eq!(Instant::now(), now, "database work consumed paused time");
        output
    }
    async fn reach<F: Future>(mut future: Pin<&mut F>, fake: &FakeLog, calls: usize) {
        let now = Instant::now();
        let barrier = fake.wait_for_calls(calls);
        tokio::pin!(barrier);
        loop {
            tokio::select! { biased; () = &mut barrier => break, _ = &mut future => panic!("completed before log barrier"), () = tokio::task::yield_now() => {} }
        }
        assert_eq!(Instant::now(), now);
    }

    #[tokio::test(start_paused = true)]
    async fn background_reads_every_30_seconds_under_writer_lock_and_disabled_never_connects() {
        let f = Fixture::new("catch-background");
        // Exercise the real startup hook, not just the worker constructor.
        f.handler.store.lock().unwrap().take();
        let ack = subc_protocol::ModuleHelloAckBody {
            negotiated_ver: 1,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: Some(serde_json::to_value(&f.descriptor).unwrap()),
            machine_id: None,
        };
        f.handler.on_hello_ack(&ack).await;
        f.tap.fake.wait_for_calls(1).await;
        assert_eq!(f.tap.reads.lock().unwrap().len(), 1);
        let lock = f.handler.writer.lock().await;
        f.append(project("remote"), false).await;
        tokio::time::advance(Duration::from_secs(30) - TICK).await;
        tokio::task::yield_now().await;
        assert_eq!(f.tap.reads.lock().unwrap().len(), 1);
        tokio::time::advance(TICK).await;
        tokio::task::yield_now().await;
        assert_eq!(f.position(), 0, "background bypassed writer lock");
        drop(lock);
        let due = Instant::now();
        f.tap.fake.wait_for_calls(3).await;
        assert_eq!(Instant::now(), due, "the 30-second tick was missed");
        assert_eq!(f.position(), 1);
        assert_eq!(
            f.tap.reads.lock().unwrap()[1],
            json!({"after":0,"limit":128})
        );

        let fail = Arc::new(FailConnector::default());
        let mut disabled = Fixture::new("catch-disabled");
        disabled
            .store()
            .apply_entry("test-disable", "{}", "test", None, |tx| {
                tx.execute("UPDATE identity_log_state SET state='disabled'", [])?;
                Ok(())
            })
            .unwrap();
        disabled.restart(fail.clone());
        disabled.handler.start_catch_up();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(60)).await;
        tokio::task::yield_now().await;
        assert_eq!(fail.calls(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn paging_journals_authorship_and_wakes_agent_changes_waiters() {
        let f = Fixture::new("catch-pages");
        for _ in 0..129 {
            f.append(empty(), false).await;
        }
        f.catch().await.unwrap();
        assert_eq!(f.position(), 129);
        let reads = f.tap.reads.lock().unwrap().clone();
        assert_eq!(
            reads,
            vec![
                json!({"after":0,"limit":128}),
                json!({"after":128,"limit":128})
            ]
        );
        let rows = audit(&f.store());
        assert_eq!(rows.len(), 129);
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(
                (&row.0, &row.1, row.2),
                (&"project.shared".into(), &"log".into(), i as i64 + 1)
            );
            assert!(!row.3.is_empty());
            let payload: Value = serde_json::from_str(&row.4).unwrap();
            assert_eq!(payload["signer"], log_client::encode_hex(&[0x51; 32]));
            assert_eq!(
                payload["key_id"],
                json!({"family":"bmk","epoch":0}).to_string()
            );
        }
        let a = Fixture::new("catch-agent-source");
        let write = a.handler.write_wait(WireRequest { method: "agent.create".into(), params: json!({"role":"assistant","name":"Ada","tag":"helper","request_key":"agent"}) }, ROUTE, Instant::now());
        tokio::pin!(write);
        finish(write.as_mut()).await.unwrap();
        let entry = a
            .tap
            .fake
            .client()
            .read(0, 128)
            .await
            .unwrap()
            .entries
            .remove(0);
        let cursor = f.store().generation().unwrap();
        let body = serde_json::to_vec(&json!({"method":"agent.changes","params":{"incarnation":f.handler.incarnation,"cursor":cursor,"wait":true}})).unwrap();
        let waiter = f.handler.handle_request_wait(&body, ROUTE);
        tokio::pin!(waiter);
        poll_pending(waiter.as_mut());
        f.append(serde_json::from_slice(&entry.entry).unwrap(), false)
            .await;
        f.catch().await.unwrap();
        let outcome = waiter
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()));
        let Poll::Ready(subc_client_rs::HandlerOutcome::Response(body)) = outcome else {
            panic!("feed waiter did not wake on remote commit")
        };
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["result"]["entries"][0]["op"], "agent.create");
        assert_eq!(
            f.handler.health.generation.load(Ordering::Relaxed),
            f.store().generation().unwrap()
        );
        assert!(f.store().verify().unwrap().ok);
    }

    #[tokio::test(start_paused = true)]
    async fn committed_own_entry_only_advances_position_without_reapplying() {
        let f = Fixture::new("catch-own");
        let write = f.write("own");
        tokio::pin!(write);
        finish(write.as_mut()).await.unwrap();
        let before = f.store().generation().unwrap();
        f.store()
            .apply_entry("test-rewind", "{}", "test", None, |tx| {
                tx.execute("UPDATE identity_log_state SET last_applied_position=0", [])?;
                Ok(())
            })
            .unwrap();
        f.catch().await.unwrap();
        assert_eq!(f.position(), 1);
        assert_eq!(f.store().generation().unwrap(), before + 1);
        assert_eq!(f.own_count(), 0);
        let rows = audit(&f.store());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1, "here");
    }

    #[tokio::test(start_paused = true)]
    async fn gap_stops_at_next_position_and_shared_writes_refuse_stalled() {
        let f = Fixture::new("catch-gap");
        for id in ["before", "missing", "after"] {
            f.append(project(id), false).await;
        }
        f.tap.fake.skip_position(2);
        assert_eq!(f.catch().await.unwrap_err().code, "identity_log_stalled");
        assert_eq!(f.state(), "log_gap");
        assert_eq!(f.position(), 1);
        assert_eq!(f.store().enumerate(None).unwrap().projects.len(), 1);
        assert_eq!(audit(&f.store()).len(), 1);
        let calls = f.tap.fake.calls();
        assert_eq!(
            f.write("blocked").await.unwrap_err().code,
            "identity_log_stalled"
        );
        assert_eq!(f.tap.fake.calls(), calls);
        assert_eq!(f.pending(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn verification_failure_stalls_before_any_projection_or_position_changes() {
        let f = Fixture::new("catch-verification");
        f.append(project("unverified"), false).await;
        f.tap.fake.on_read(Action::Refuse {
            code: log_client::VERIFY_FAILED.into(),
            detail: Some(json!({"position":1})),
        });
        let before = f.store().generation().unwrap();
        assert_eq!(f.catch().await.unwrap_err().code, "identity_log_stalled");
        assert_eq!(f.state(), "log_verify_failed");
        assert_eq!(f.position(), 0);
        assert_eq!(f.store().generation().unwrap(), before);
        assert!(f.store().enumerate(None).unwrap().projects.is_empty());
        assert_eq!(
            f.write("blocked").await.unwrap_err().code,
            "identity_log_stalled"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn unsupported_operations_and_constraint_images_fail_closed() {
        let mut unsupported = empty();
        unsupported["op"] = "future.operation".into();
        let constraint = json!({"op":"project.shared","tables":{"project_workspace":{"upsert":[{"project_id":"absent","workspace_id":"absent"}],"delete":[]}}});
        for (label, invalid) in [("unknown", unsupported), ("constraint", constraint)] {
            let f = Fixture::new(&format!("catch-{label}"));
            f.append(invalid, false).await;
            f.append(project("later"), false).await;
            let before = f.store().generation().unwrap();
            let error = f.catch().await.unwrap_err();
            assert_eq!(error.code, "identity_log_stalled");
            assert!(error.message.contains(if label == "constraint" {
                "foreign key"
            } else {
                "unknown entry op"
            }));
            assert_eq!(f.state(), "apply_failed");
            assert_eq!(f.position(), 0);
            assert_eq!(f.store().generation().unwrap(), before);
            assert!(audit(&f.store()).is_empty());
            assert!(f.store().enumerate(None).unwrap().projects.is_empty());
            assert_eq!(
                f.write("blocked").await.unwrap_err().code,
                "identity_log_stalled"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn regressed_head_is_ignored_without_moving_applied_position() {
        let f = Fixture::new("catch-regressed");
        f.append(project("kept"), false).await;
        f.catch().await.unwrap();
        let before = f.store().generation().unwrap();
        *f.tap.override_head.lock().unwrap() = Some(0);
        assert_eq!(f.catch().await.unwrap_err().code, "engram_unavailable");
        assert_eq!(f.state(), "log_head_regressed");
        assert_eq!(f.position(), 1);
        assert_eq!(f.store().identity_log_status().unwrap().last_seen_head, 1);
        assert_eq!(f.store().generation().unwrap(), before);
        *f.tap.override_head.lock().unwrap() = None;
        f.catch().await.unwrap();
        assert_eq!(f.state(), "ok");
    }

    #[tokio::test(start_paused = true)]
    async fn read_key_unavailable_has_distinct_health_and_shared_write_refusal() {
        let f = Fixture::new("catch-key");
        for _ in 0..2 {
            f.tap.fake.on_read(Action::Refuse {
                code: log_client::KEY_UNAVAILABLE.into(),
                detail: None,
            });
        }
        assert_eq!(f.catch().await.unwrap_err().code, "engram_key_unavailable");
        assert_eq!(f.state(), "engram_key_unavailable");
        assert_eq!(
            f.write("blocked").await.unwrap_err().code,
            "engram_key_unavailable"
        );
        assert_eq!(f.pending(), 0);
        assert_eq!(f.tap.fake.head(), 0);
        f.tap.fake.on_read(Action::Refuse {
            code: log_client::UNAVAILABLE.into(),
            detail: None,
        });
        assert_eq!(
            f.write("absent").await.unwrap_err().code,
            "engram_unavailable"
        );
        f.catch().await.unwrap();
        assert_eq!(f.state(), "ok");
    }

    #[tokio::test(start_paused = true)]
    async fn snapshot_waits_across_restart_then_installs_once_with_all_part_audits() {
        let mut f = Fixture::new("catch-snapshot");
        f.append(part(1, 129, "P1"), true).await;
        let before = f.store().generation().unwrap();
        assert_eq!(f.catch().await.unwrap_err().code, "engram_unavailable");
        assert_eq!(f.position(), 0);
        assert_eq!(f.store().generation().unwrap(), before);
        assert!(f.store().enumerate(None).unwrap().projects.is_empty());
        f.restart(Arc::new(TapConnector(f.tap.clone())));
        for i in 2..=129 {
            f.append(part(i, 129, &format!("P{i}")), true).await;
        }
        f.catch().await.unwrap();
        assert_eq!(f.position(), 129);
        assert_eq!(f.store().enumerate(None).unwrap().projects.len(), 129);
        let rows = audit(&f.store());
        assert_eq!(rows.len(), 129);
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row.0, "shared.snapshot");
            assert_eq!(row.1, "log");
            assert_eq!(row.2, i as i64 + 1);
            let payload: Value = serde_json::from_str(&row.4).unwrap();
            assert_eq!(payload["snapshot_part"]["part"], i as u64 + 1);
            assert!(payload["signer"].is_string());
            assert!(payload["key_id"].is_string());
        }
        assert!(f.store().verify().unwrap().ok);
        f.store().rebuild().unwrap();
        assert_eq!(f.store().enumerate(None).unwrap().projects.len(), 129);
        let committed = f.store().generation().unwrap();
        f.restart(Arc::new(TapConnector(f.tap.clone())));
        f.catch().await.unwrap();
        assert_eq!(f.store().generation().unwrap(), committed);
    }

    #[tokio::test(start_paused = true)]
    async fn snapshot_validation_and_fault_at_final_position_roll_back_the_whole_install() {
        let f = Fixture::new("catch-snapshot-atomic");
        f.append(part(1, 2, "A"), true).await;
        f.append(part(2, 2, "B"), true).await;
        let entries = f
            .tap
            .fake
            .client()
            .read(0, 128)
            .await
            .unwrap()
            .entries
            .into_iter()
            .map(remote)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let before = f.store().generation().unwrap();
        for bad in 0..4 {
            let mut wrong = entries
                .iter()
                .map(|e| RemoteEntry {
                    position: e.position,
                    entry_id: e.entry_id.clone(),
                    signer: e.signer.clone(),
                    key_id: e.key_id.clone(),
                    envelope_version: e.envelope_version,
                    kind: e.kind.clone(),
                    entry: e.entry.clone(),
                })
                .collect::<Vec<_>>();
            let mut value: Value = serde_json::from_slice(&wrong[1].entry).unwrap();
            match bad {
                0 => wrong[1].position = 3,
                1 => value["snapshot_id"] = "other".into(),
                2 => value["part"] = 1.into(),
                _ => value["parts"] = 3.into(),
            }
            wrong[1].entry = serde_json::to_vec(&value).unwrap();
            assert!(f.store().apply_remote_snapshot(&wrong).is_err());
            assert_eq!(f.position(), 0);
            assert_eq!(f.store().generation().unwrap(), before);
        }
        f.store().apply_entry("test-fault", "{}", "test", None, |tx| { tx.execute_batch("CREATE TEMP TRIGGER fail_snapshot_commit BEFORE UPDATE OF last_applied_position ON identity_log_state WHEN NEW.last_applied_position=2 BEGIN SELECT RAISE(ABORT,'final position fault'); END;")?; Ok(()) }).unwrap();
        let before = f.store().generation().unwrap();
        let error = f.store().apply_remote_snapshot(&entries).unwrap_err();
        assert!(error.to_string().contains("final position fault"));
        assert_eq!(f.position(), 0);
        assert_eq!(f.store().generation().unwrap(), before);
        assert!(audit(&f.store()).is_empty());
        assert!(f.store().enumerate(None).unwrap().projects.is_empty());
        f.store()
            .apply_entry("test-clear-fault", "{}", "test", None, |tx| {
                tx.execute_batch("DROP TRIGGER fail_snapshot_commit")?;
                Ok(())
            })
            .unwrap();
        f.catch().await.unwrap();
        assert_eq!(f.position(), 2);
        assert_eq!(f.store().enumerate(None).unwrap().projects.len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn held_append_and_refused_resend_settle_by_position_across_restart() {
        for refusal in [
            None,
            Some("unavailable"),
            Some("not_member"),
            Some("key_unavailable"),
        ] {
            for landed in [true, false] {
                let mut f = Fixture::new("catch-held");
                f.tap.fake.on_append(Action::Hold);
                for _ in 0..30 {
                    f.tap.fake.on_append(
                        refusal
                            .map(|code| Action::Refuse {
                                code: code.into(),
                                detail: None,
                            })
                            .unwrap_or(Action::Hang),
                    );
                }
                let result = {
                    let received = Instant::now() - Duration::from_secs(18);
                    let write = f.handler.write_wait(WireRequest {method:"register".into(),params:json!({"projectId":"X","name":"X","roots":[],"requestKey":"held"})},ROUTE,received);
                    tokio::pin!(write);
                    reach(write.as_mut(), &f.tap.fake, 2).await;
                    assert_eq!(f.tap.fake.held_count(), 1);
                    tokio::time::advance(Duration::from_secs(1) + TICK).await;
                    poll_pending(write.as_mut());
                    tokio::time::advance(Duration::from_millis(100) + TICK).await;
                    reach(write.as_mut(), &f.tap.fake, 3).await;
                    tokio::time::advance(Duration::from_secs(1)).await;
                    let result = finish(write.as_mut()).await;
                    result
                };
                assert_eq!(result.unwrap_err().code, "engram_outcome_unknown");
                assert_eq!(f.pending(), 1);
                f.restart(Arc::new(TapConnector(f.tap.clone())));
                f.catch().await.unwrap();
                assert_eq!(f.pending(), 1);
                assert_eq!(f.position(), 0);
                if landed {
                    assert_eq!(f.tap.fake.release_next().unwrap(), 1);
                } else {
                    // The original append is still held open. Another device
                    // now appends at position 1, the one the original expected
                    // to take. This goes straight to the fake log, skipping the
                    // refusals scripted for this machine's resends, but still
                    // through the log's check that the head is where expected.
                    let params = json!({"expected_head":0,"entry_id":log_client::encode_hex(&[0xfe;16]),"entry":{"kind":"change","data":log_client::encode_hex(&serde_json::to_vec(&project("other")).unwrap())}});
                    // Poll until the fake accepts the other device's append, so
                    // position 1 is taken before the held original is released.
                    for _ in 0..30 {
                        let future = f.tap.fake.call(log_client::APPEND, params.clone());
                        tokio::pin!(future);
                        match future
                            .as_mut()
                            .poll(&mut Context::from_waker(Waker::noop()))
                        {
                            Poll::Ready(Ok(_)) => break,
                            Poll::Ready(Err(_)) | Poll::Pending => {}
                        }
                    }
                    assert_eq!(f.tap.fake.head(), 1);
                }
                f.catch().await.unwrap();
                assert_eq!(f.pending(), 0);
                assert_eq!(f.position(), 1);
                assert_eq!(f.own_count(), u64::from(landed));
                let projects = f.store().enumerate(None).unwrap().projects;
                assert_eq!(projects.len(), 1);
                assert_eq!(projects[0].project_id, if landed { "X" } else { "other" });
                f.catch().await.unwrap();
                assert_eq!(f.own_count(), u64::from(landed));
                assert!(f.store().verify().unwrap().ok);
                if !landed {
                    assert!(f.tap.fake.release_next().is_err());
                }
            }
        }
    }

    struct CrashTransport {
        fake: FakeLog,
        receipt: PathBuf,
        before_commit: bool,
    }
    struct CrashConnector(Arc<CrashTransport>);
    #[async_trait]
    impl LogConnector for CrashConnector {
        async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
            Ok(self.0.clone())
        }
    }
    #[async_trait]
    impl LogTransport for CrashTransport {
        async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
            let result = self.fake.call(method, params.clone()).await?;
            if method == log_client::APPEND {
                std::fs::write(&self.receipt, serde_json::to_vec(&params).unwrap()).unwrap();
                if self.before_commit {
                    println!("CRASH_BARRIER appended");
                    std::io::stdout().flush().unwrap();
                    // Keep paused time still until the parent kills this process.
                    loop {
                        tokio::task::yield_now().await;
                    }
                }
            }
            Ok(result)
        }
    }

    #[tokio::test(start_paused = true)]
    async fn process_killed_before_and_after_local_commit_recovers_without_duplicates() {
        if let Ok(config) = std::env::var("ENTORHINAL_CATCH_UP_CRASH_TEST") {
            let config: Value = serde_json::from_str(&config).unwrap();
            let descriptor = serde_json::from_value(config["descriptor"].clone()).unwrap();
            let transport = Arc::new(CrashTransport {
                fake: FakeLog::default(),
                receipt: config["receipt"].as_str().unwrap().into(),
                before_commit: config["before"].as_bool().unwrap(),
            });
            let handler = Fixture::handler(&descriptor, Arc::new(CrashConnector(transport)));
            let write = handler.write_wait(WireRequest {method:"register".into(),params:json!({"projectId":"crashed","name":"crashed","roots":[],"requestKey":"crash"})},ROUTE,Instant::now());
            tokio::pin!(write);
            finish(write.as_mut()).await.unwrap();
            println!("CRASH_BARRIER committed");
            std::io::stdout().flush().unwrap();
            loop {
                tokio::task::yield_now().await;
            }
        }
        for before_commit in [true, false] {
            let mut f = Fixture::new("catch-crash");
            f.handler.store.lock().unwrap().take();
            let receipt = f.dir.join("accepted.json");
            let config =
                json!({"descriptor":f.descriptor,"receipt":receipt,"before":before_commit});
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact","catch_up::tests::process_killed_before_and_after_local_commit_recovers_without_duplicates","--nocapture"])
                .env("ENTORHINAL_CATCH_UP_CRASH_TEST",config.to_string())
                .stdout(std::process::Stdio::piped()).spawn().unwrap();
            let output = child.stdout.take().unwrap();
            let mut lines = std::io::BufReader::new(output).lines();
            loop {
                let line = lines
                    .next()
                    .expect("child exited before crash barrier")
                    .unwrap();
                if line.contains("CRASH_BARRIER") {
                    assert!(line.contains(if before_commit {
                        "appended"
                    } else {
                        "committed"
                    }));
                    break;
                }
            }
            child.kill().unwrap();
            assert!(!child.wait().unwrap().success());
            let fail = Arc::new(FailConnector::default());
            f.restart(if before_commit {
                Arc::new(TapConnector(f.tap.clone()))
            } else {
                fail.clone()
            });
            assert_eq!(f.pending(), i64::from(before_commit));
            if before_commit {
                let params: Value =
                    serde_json::from_slice(&std::fs::read(&receipt).unwrap()).unwrap();
                f.tap.fake.call(log_client::APPEND, params).await.unwrap();
                f.catch().await.unwrap();
                assert_eq!(f.pending(), 0);
                assert_eq!(f.own_count(), 1);
                let generation = f.store().generation().unwrap();
                f.catch().await.unwrap();
                assert_eq!(f.store().generation().unwrap(), generation);
            } else {
                assert_eq!(f.position(), 1);
                assert_eq!(f.own_count(), 0);
                let generation = f.store().generation().unwrap();
                let retry = f.handler.write_wait(WireRequest {method:"register".into(),params:json!({"projectId":"crashed","name":"crashed","roots":[],"requestKey":"crash"})},ROUTE,Instant::now());
                tokio::pin!(retry);
                finish(retry.as_mut()).await.unwrap();
                assert_eq!(f.store().generation().unwrap(), generation);
                assert_eq!(fail.calls(), 0);
            }
            assert_eq!(f.store().enumerate(None).unwrap().projects.len(), 1);
            assert_eq!(audit(&f.store()).len(), 1);
            assert!(f.store().verify().unwrap().ok);
        }
    }
}
