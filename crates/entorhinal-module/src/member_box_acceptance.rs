use super::*;
use crate::{
    fake_record::{FakeRecord, FakeRecordConnector},
    log_client::{encode_hex, AppendRequest, EntryKind},
    record_client::RecordClient,
};

const A_BOX: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const B_BOX: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn daemon(machine: &Machine, box_id: &str) {
    machine
        .handler
        .health
        .origin
        .set_daemon(Some(box_id.into()));
    machine
        .handler
        .health
        .origin
        .check(&machine.store())
        .unwrap();
}

async fn barrier(log: &FakeLog, machine: &Machine, calls: usize) {
    drive(log.wait_for_calls(calls)).await;
    drop(drive(machine.handler.writer.lock()).await);
}

#[tokio::test(start_paused = true)]
async fn member_box_startup_writes_once_and_replicates_box() {
    let log = FakeLog::default();
    let a_log = Arc::new(log.with_author([0x11; 16]));
    let b_log = Arc::new(log.with_author([0x22; 16]));
    let mut a = Machine::new("member-box-holder", a_log.clone());
    let b = Machine::new("member-box-reader", b_log);
    a.enable_without_agents().await;
    b.enable_without_agents().await;

    a.restart_legacy(a_log.clone()).await;
    daemon(&a, A_BOX);
    let (calls, head) = (log.calls(), log.head());
    a.handler.start_catch_up();
    barrier(&log, &a, calls + 1).await;
    assert_eq!(log.head(), head + 1);
    assert_eq!(a.store().own_member_box().unwrap().as_deref(), Some(A_BOX));
    b.catch_up().await;
    let calls = log.calls();
    a.handler.catch_up_wake.notify_one();
    barrier(&log, &a, calls + 1).await;
    assert_eq!(log.head(), head + 1, "unchanged box appended again");
    assert_eq!(a.rows("member_box"), b.rows("member_box"));
    a.clean_rebuild().await;
    b.clean_rebuild().await;

    // Same author and store, but a different verified daemon id after adoption.
    a.restart_legacy(a_log).await;
    a.store().adopt_store(B_BOX, true, 800).unwrap();
    daemon(&a, B_BOX);
    let calls = log.calls();
    a.handler.start_catch_up();
    barrier(&log, &a, calls + 1).await;
    assert_eq!(log.head(), head + 2);
    assert_eq!(a.store().own_member_box().unwrap().as_deref(), Some(B_BOX));
    crate::tests::stop_store_tasks(&a.handler).await;
    crate::tests::stop_store_tasks(&b.handler).await;
}

#[tokio::test(start_paused = true)]
async fn member_box_empty_join_learns_author_and_retries_failed_read() {
    let log = FakeLog::default();
    let a = Machine::new(
        "member-box-bootstrap",
        Arc::new(log.with_author([0x11; 16])),
    );
    a.enable_without_agents().await;
    let b_records = FakeRecord::new([0x22; 16]);
    b_records.offline.store(true, Ordering::Relaxed);
    let b = Machine::new_with_record(
        "member-box-empty-join",
        Arc::new(log.with_author([0x22; 16])),
        RecordClient::new(Arc::new(FakeRecordConnector(b_records.clone()))),
    );
    daemon(&b, B_BOX);
    b.handler.start_catch_up();
    b.enable_without_agents().await;
    // Wait for the join wake-up pass to complete before turning records back on.
    drive(b_records.wait_for_calls(1)).await;
    drop(drive(b.handler.writer.lock()).await);
    assert!(b.rows("member_box").is_empty());
    b_records.offline.store(false, Ordering::Relaxed);
    let calls = log.calls();
    tokio::time::advance(Duration::from_secs(30)).await;
    barrier(&log, &b, calls + 1).await;
    assert_eq!(
        b.store().persisted_self_author().unwrap(),
        Some("22".repeat(16))
    );
    assert_eq!(b.store().own_member_box().unwrap().as_deref(), Some(B_BOX));
    assert!(b_records.cas_attempts.lock().unwrap().is_empty());
    b.call("member.set_name", json!({"display_name":"Joiner"}))
        .await
        .unwrap();
    a.catch_up().await;
    assert_eq!(a.rows("member_box"), b.rows("member_box"));
    crate::tests::stop_store_tasks(&a.handler).await;
    crate::tests::stop_store_tasks(&b.handler).await;
}

#[tokio::test(start_paused = true)]
async fn member_box_never_writes_for_disabled_or_foreign_store() {
    let log = Arc::new(FakeLog::default());
    let machine = Machine::new("member-box-skip", log.clone());
    daemon(&machine, A_BOX);
    let store = machine.store();
    for state in ["disabled", "enabled"] {
        if state == "enabled" {
            machine.enable_without_agents().await;
            store.check_store_origin(Some(B_BOX), 800).unwrap();
            assert!(store.is_foreign());
        }
        let calls = log.calls();
        machine
            .handler
            .catch_up_worker()
            .publish_member_box(
                &store,
                machine.handler.writer.clone().lock_owned().await,
                tokio::time::Instant::now() + Duration::from_secs(5),
            )
            .await
            .unwrap();
        assert!(
            machine.rows("member_box").is_empty(),
            "published for {state}"
        );
        assert_eq!(log.calls(), calls);
    }
    crate::tests::stop_store_tasks(&machine.handler).await;
}

async fn append_box_change(log: &FakeLog, author: &str, box_id: &str, id: u8) -> u64 {
    log.client()
        .append(&AppendRequest {
            expected_head: log.head(),
            entry_id: [id; 16],
            kind: EntryKind::Change,
            data: serde_json::to_vec(
                &json!({"op":"member.set_box","author":author,"box_id":box_id}),
            )
            .unwrap(),
        })
        .await
        .unwrap()
        .position
}

async fn catch_result(machine: &Machine) -> Result<(), HandlerError> {
    let _writer = machine.handler.writer.lock().await;
    machine
        .handler
        .catch_up(
            &machine.store(),
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await
}

#[tokio::test(start_paused = true)]
async fn member_box_refuses_forged_author_and_malformed_ids() {
    for (author, box_id, error) in [
        ("22".repeat(16), B_BOX.to_owned(), "differs from log author"),
        ("11".repeat(16), "AB".repeat(16), "invalid member box"),
        ("11".repeat(15), A_BOX.to_owned(), "invalid member box"),
    ] {
        let log = FakeLog::default();
        let a_log = Arc::new(log.with_author([0x11; 16]));
        let a = Machine::new("member-box-invalid-a", a_log.clone());
        let b = Machine::new(
            "member-box-invalid-b",
            Arc::new(log.with_author([0x22; 16])),
        );
        a.enable_without_agents().await;
        b.enable_without_agents().await;
        let before = log.head() as i64;
        let position = append_box_change(&a_log, &author, &box_id, 3).await;
        let refusal = catch_result(&b).await.unwrap_err();
        assert_eq!(refusal.code, "identity_log_stalled");
        assert!(refusal.message.contains(error), "{}", refusal.message);
        assert!(refusal.message.contains(&format!("position {position}")));
        assert_eq!(
            b.store()
                .identity_log_status()
                .unwrap()
                .last_applied_position,
            before
        );
        assert!(b.rows("member_box").is_empty());
        crate::tests::stop_store_tasks(&a.handler).await;
        crate::tests::stop_store_tasks(&b.handler).await;
    }
}

// Emulate an older decoder's unknown-op failure without changing the stored log.
struct OldReader {
    log: FakeLog,
    position: u64,
}

#[async_trait::async_trait]
impl LogConnector for OldReader {
    async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
        Ok(Arc::new(Self {
            log: self.log.clone(),
            position: self.position,
        }))
    }
}

#[async_trait::async_trait]
impl LogTransport for OldReader {
    async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
        let bytes = self.log.connect().await?.call(method, params).await?;
        if method != crate::log_client::READ {
            return Ok(bytes);
        }
        let mut reply: Value = serde_json::from_slice(&bytes).unwrap();
        for entry in reply["result"]["entries"].as_array_mut().unwrap() {
            if entry["position"] == self.position {
                entry["entry"] = json!(encode_hex(br#"{"op":"future.member_box"}"#));
            }
        }
        Ok(serde_json::to_vec(&reply).unwrap())
    }
}

#[tokio::test(start_paused = true)]
async fn member_box_unknown_operation_retries_same_position_after_restart() {
    let log = FakeLog::default();
    let a_log = Arc::new(log.with_author([0x11; 16]));
    let b_log = log.with_author([0x22; 16]);
    let a = Machine::new("member-box-upgrade-a", a_log.clone());
    a.enable_without_agents().await;
    let blocked = log.head() + 1;
    let mut b = Machine::new(
        "member-box-upgrade-b",
        Arc::new(OldReader {
            log: b_log.clone(),
            position: blocked,
        }),
    );
    b.enable_without_agents().await;
    let author = "11".repeat(16);
    assert_eq!(append_box_change(&a_log, &author, A_BOX, 4).await, blocked);
    assert_eq!(
        append_box_change(&a_log, &author, B_BOX, 5).await,
        blocked + 1
    );

    let error = catch_result(&b).await.unwrap_err();
    assert!(error.message.contains("unknown entry op"));
    assert_eq!(
        b.store()
            .identity_log_status()
            .unwrap()
            .last_applied_position,
        blocked as i64 - 1
    );
    assert!(b.rows("member_box").is_empty());
    let calls = log.calls();
    assert_eq!(
        catch_result(&b).await.unwrap_err().code,
        "identity_log_stalled"
    );
    assert_eq!(log.calls(), calls, "the old process must remain stalled");

    b.restart_legacy(Arc::new(b_log)).await;
    b.catch_up().await;
    assert_eq!(
        b.store()
            .identity_log_status()
            .unwrap()
            .last_applied_position,
        blocked as i64 + 1
    );
    assert_eq!(b.query("SELECT log_position FROM registry_journal WHERE op='member.set_box' ORDER BY log_position"), vec![vec![Cell::Integer(blocked as i64)], vec![Cell::Integer(blocked as i64 + 1)]]);
    assert_eq!(
        b.rows("member_box"),
        vec![vec![Cell::Text(author), Cell::Text(B_BOX.into())]]
    );
    b.clean_rebuild().await;
    crate::tests::stop_store_tasks(&a.handler).await;
    crate::tests::stop_store_tasks(&b.handler).await;
}
