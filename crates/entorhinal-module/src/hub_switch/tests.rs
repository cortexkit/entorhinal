use super::*;
use crate::{
    fake_log::{Action, FakeLog},
    fake_record::{FakeRecord, FakeRecordConnector},
    log_client::encode_hex,
};
use cortexkit_store_types::StorageDescriptor;
use std::{path::PathBuf, time::Duration};

mod drain;

const A: &str = "11111111111111111111111111111111";
const B: &str = "22222222222222222222222222222222";
const BOX_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const BOX_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const MIGRATION: &str = "cccccccccccccccccccccccccccccccc";

struct EmptyHub;

#[async_trait::async_trait]
impl Probe for EmptyHub {
    fn activate(&self) {}
    async fn head(&self, _: Instant) -> Result<u64, HandlerError> {
        Ok(0)
    }
    async fn local_box(&self, _: Instant) -> Result<String, HandlerError> {
        Ok(BOX_A.into())
    }
    async fn hub_cuts(&self, _: Instant) -> Result<bool, HandlerError> {
        Ok(true)
    }
}

struct Fixture {
    slot: Arc<Mutex<Option<RegistryStore>>>,
    descriptor: StorageDescriptor,
    dir: PathBuf,
    records: Arc<FakeRecord>,
    log: FakeLog,
    switch: Switch,
}

impl Fixture {
    fn new(member_b: bool) -> Self {
        Self::for_author(member_b, [0x11; 16])
    }

    fn for_author(member_b: bool, author: [u8; 16]) -> Self {
        let (dir, descriptor) = crate::tests::scratch_descriptor("hub-switch");
        let store = RegistryStore::open(&descriptor).unwrap();
        store
            .apply_entry("identity_log.enable", "{}", "test", None, |tx| {
                tx.execute("UPDATE identity_log_state SET state='enabled'", [])?;
                tx.execute("INSERT INTO member_box VALUES(?1,?2)", [A, BOX_A])?;
                if member_b {
                    tx.execute("INSERT INTO member_box VALUES(?1,?2)", [B, BOX_B])?;
                }
                Ok(())
            })
            .unwrap();
        assert!(store
            .learn_verified_self_author(&encode_hex(&author), 1)
            .unwrap());
        store.role_read("connector_home").unwrap();
        Self {
            dir,
            descriptor,
            slot: Arc::new(Mutex::new(Some(store))),
            records: FakeRecord::new(author),
            log: FakeLog::default().with_author(author),
            switch: Switch {
                probe: Arc::new(EmptyHub),
                moves: Arc::default(),
                drain: Default::default(),
            },
        }
    }
    fn store(&self) -> RegistryStore {
        self.slot.lock().unwrap().clone().unwrap()
    }
    fn restart(&self) {
        self.slot.lock().unwrap().take();
        *self.slot.lock().unwrap() = Some(RegistryStore::open(&self.descriptor).unwrap());
    }
    fn record_client(&self) -> RecordClient {
        RecordClient::new(Arc::new(FakeRecordConnector(self.records.clone())))
            .fenced(LegacyFence::new(&self.slot))
    }
    async fn tick(&self) -> Result<Option<MigrationImage>, HandlerError> {
        self.tick_with(&self.record_client()).await
    }
    async fn tick_with(
        &self,
        records: &RecordClient,
    ) -> Result<Option<MigrationImage>, HandlerError> {
        self.switch
            .advance(
                &self.store(),
                &self.log.client().fenced(LegacyFence::new(&self.slot)),
                records,
                Instant::now() + Duration::from_secs(2),
            )
            .await
    }
    fn put(&self, subject: &str, version: u64, value: Value, author: &str) {
        self.records.put(
            subject,
            version,
            serde_json::to_vec(&value).unwrap(),
            author,
        );
    }
    fn propose(&self) {
        self.put(
            PROPOSAL,
            1,
            json!({"v":1,"migration_id":MIGRATION,"excluded":[]}),
            A,
        );
    }

    fn apply_verified(&self, author: &str, body: Value) {
        let store = self.store();
        let position = store.identity_log_status().unwrap().last_applied_position + 1;
        store
            .apply_remote_entry(
                &entorhinal_core::remote_apply::RemoteEntry {
                    position,
                    entry_id: format!("{position:032x}"),
                    author: author.into(),
                    signer: A.into(),
                    key_id: "test".into(),
                    envelope_version: 1,
                    kind: "change".into(),
                    entry: serde_json::to_vec(&body).unwrap(),
                },
                Some(&self.records.author),
            )
            .unwrap();
    }

    fn freeze_at_marker(&self, author: &str, roles: &[&str], excluded: &[&str]) {
        self.apply_verified(
            author,
            json!({
                "op":"log.migrated", "hub":true, "migration_id":MIGRATION,
                "roles":roles, "excluded":excluded,
                "tables_digest":self.store().migration_tables_digest().unwrap(),
            }),
        );
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.slot.lock().unwrap().take();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[tokio::test]
async fn hub_switch_requires_every_verified_readiness_and_current_quiet() {
    let f = Fixture::new(true);
    f.propose();
    let node =
        |version, cuts, id| json!({"v":1,"box":id,"entorhinal_hub":version,"engram_hub_cuts":cuts});
    let subject = format!("node:{BOX_B}");
    let cases = [
        None,
        Some((node(0, true, BOX_B), B)),
        Some((node(1, false, BOX_B), B)),
        Some((node(1, true, BOX_A), B)),
        Some((node(1, true, BOX_B), A)),
    ];
    for (i, row) in cases.into_iter().enumerate() {
        if let Some((body, author)) = row {
            f.put(&subject, i as u64, body, author);
        }
        let error = f.tick().await.unwrap_err();
        assert_eq!(error.code, "hub_switch_members_not_ready");
        assert_eq!(error.detail, Some(json!({"boxes":[BOX_B]})));
        assert_eq!(f.log.head(), 0);
        assert!(!f.store().legacy_moves_paused().unwrap());
    }
    f.put(&subject, 5, node(1, true, BOX_B), B);
    f.tick().await.unwrap();
    assert_eq!(f.log.head(), 0, "missing quiet must hold the marker");
    assert!(f.store().legacy_moves_paused().unwrap());
    f.put(
        &format!("hubquiet:{B}"),
        1,
        json!({"v":1,"migration_id":BOX_B,"roles":["other"]}),
        B,
    );
    f.tick().await.unwrap();
    assert_eq!(f.log.head(), 0, "an old proposal's quiet must not count");
    let mut peer = Fixture::for_author(true, [0x22; 16]);
    peer.records = f.records.for_author([0x22; 16]);
    peer.store().role_read("other").unwrap();
    let peer_roles = peer.store().role_subjects().unwrap();
    assert!(!f
        .store()
        .role_subjects()
        .unwrap()
        .iter()
        .any(|name| name == "other"));
    f.put(
        &format!("hubquiet:{B}"),
        2,
        json!({"v":1,"migration_id":MIGRATION,"roles":peer_roles}),
        B,
    );
    f.tick().await.unwrap();
    let page = f.log.client().read(0, 1).await.unwrap();
    let marker: MigrationMarker = serde_json::from_slice(&page.entries[0].entry).unwrap();
    assert_eq!(marker.roles, ["connector_home", "other"]);
    assert_eq!(page.head, 1);
    // The follower can drain and image even if the marker writer stops here.
    let marker = serde_json::to_value(marker).unwrap();
    peer.apply_verified(A, marker.clone());
    let follower_image = peer.tick().await.unwrap().unwrap();
    assert!(
        f.record_client()
            .read(&format!("hubdrained:{A}"))
            .await
            .unwrap()
            .absent
    );
    assert_eq!(
        f.record_client().read("role:other").await.unwrap().version,
        1
    );
    f.apply_verified(A, marker);
    let writer_image = f.tick().await.unwrap().unwrap();
    assert_eq!(writer_image, follower_image);
    assert_eq!(writer_image.image.roles[1].role, "other");
    assert_eq!(
        f.record_client().read("role:other").await.unwrap().version,
        2
    );
}

#[tokio::test]
async fn hub_switch_nonterminal_move_blocks_quiet_and_marker() {
    let f = Fixture::new(false);
    f.propose();
    f.store().apply_entry("test.move", "{}", "test", None, |tx| {
        tx.execute_batch("INSERT INTO agent_move(cut_id,agent_id,side,peer,step,plan,record_version,seal_sent,attempts,created_at_ms,updated_at_ms)
            VALUES ('cut','agent','source','peer','preparing','{}',0,0,0,1,1)")
    }).unwrap();
    f.tick().await.unwrap();
    assert!(f.store().legacy_moves_paused().unwrap());
    assert!(
        f.record_client()
            .read(&format!("hubquiet:{A}"))
            .await
            .unwrap()
            .absent
    );
    assert_eq!(f.log.head(), 0);
    f.store()
        .apply_entry("test.move", "{}", "test", None, |tx| {
            tx.execute_batch("UPDATE agent_move SET step='aborted'")
        })
        .unwrap();
    f.tick().await.unwrap();
    assert_eq!(f.log.head(), 1);
}

#[tokio::test]
async fn hub_switch_lost_marker_reply_fences_claim_after_restart() {
    let f = Fixture::new(false);
    f.propose();
    f.log.on_append(Action::DropReply);
    f.tick().await.unwrap();
    assert_eq!(f.log.head(), 1);
    assert!(matches!(
        f.store().engram_freeze().unwrap(),
        Some(EngramFreeze::Pending { .. })
    ));
    f.restart();
    let claim = CasRequest {
        subject: "claim:agent_0000000000000000".into(),
        expected_version: 0,
        body: b"{}".to_vec(),
        write_id: [7; 16],
    };
    assert_eq!(
        f.record_client().cas(&claim).await.unwrap_err().code(),
        PENDING
    );
    assert!(f.record_client().read(&claim.subject).await.unwrap().absent);
    let remote = f
        .log
        .client()
        .read(0, 1)
        .await
        .unwrap()
        .entries
        .pop()
        .unwrap();
    f.store()
        .apply_remote_entry(
            &entorhinal_core::remote_apply::RemoteEntry {
                position: 1,
                entry_id: encode_hex(&remote.entry_id),
                author: remote.author,
                signer: encode_hex(&remote.signer),
                key_id: "test".into(),
                envelope_version: 1,
                kind: remote.kind,
                entry: remote.entry,
            },
            Some(A),
        )
        .unwrap();
    assert!(matches!(
        f.store().engram_freeze().unwrap(),
        Some(EngramFreeze::Applied { position: 1, .. })
    ));
    assert_eq!(
        f.store()
            .identity_log_status()
            .unwrap()
            .last_applied_position,
        1
    );
    assert_eq!(
        f.record_client().cas(&claim).await.unwrap_err().code(),
        PENDING
    );
}

#[tokio::test]
async fn hub_switch_startup_uses_durable_base_and_verified_history() {
    let f = Fixture::new(false);
    let store = f.store();
    assert_eq!(select(&store, HubHistory::Empty).unwrap(), Startup::Migrate);
    assert_eq!(
        select(&store, HubHistory::Complete).unwrap(),
        Startup::Follow
    );
    assert_eq!(
        select(
            &store,
            HubHistory::Incomplete {
                engram_through: 1,
                enable_id: None
            }
        )
        .unwrap(),
        Startup::ResumeMigration
    );
    assert_eq!(
        select(&store, HubHistory::Unverified).unwrap_err().code,
        "hub_switch_unverified_log"
    );
    store
        .apply_entry("test.state", "{}", "test", None, |tx| {
            tx.execute_batch("UPDATE identity_log_state SET state='disabled',enable_id='mine'")
        })
        .unwrap();
    assert_eq!(
        select(&store, HubHistory::Empty).unwrap(),
        Startup::NoHistory
    );
    assert_eq!(
        select(
            &store,
            HubHistory::Incomplete {
                engram_through: 0,
                enable_id: Some("foreign")
            }
        )
        .unwrap(),
        Startup::Wait
    );
    assert_eq!(
        select(
            &store,
            HubHistory::Incomplete {
                engram_through: 0,
                enable_id: Some("mine")
            }
        )
        .unwrap(),
        Startup::ResumeEnable
    );
    store
        .apply_entry("test.state", "{}", "test", None, |tx| {
            tx.execute_batch("UPDATE identity_log_state SET hub_log_base=0")
        })
        .unwrap();
    assert_eq!(
        select(&store, HubHistory::Unverified).unwrap(),
        Startup::Hub
    );
    let calls = f.log.calls();
    let error = f
        .log
        .client()
        .fenced(LegacyFence::new(&f.slot))
        .read(0, 1)
        .await
        .unwrap_err();
    assert!(matches!(error, LogError::Refused { code, .. } if code == PENDING));
    assert_eq!(f.log.calls(), calls);
    assert_eq!(
        f.record_client().read(PROPOSAL).await.unwrap_err().code(),
        PENDING
    );
}
