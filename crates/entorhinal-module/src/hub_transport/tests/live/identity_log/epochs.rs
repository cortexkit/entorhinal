use super::*;
use crate::{
    fleet_record::{
        vault::{EpochControl, EpochStatus, EpochVault, Imported, Minted, MintedWrap, PendingMint},
        SealedBody,
    },
    hub_epoch::Controller,
    hub_membership::{tests::anchor, PeerAnchor, PeerKeysRead},
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use std::{
    collections::BTreeSet,
    sync::atomic::{AtomicBool, AtomicU64},
};

mod joining;
mod snapshot_enable;

const D: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

struct Peers {
    own: String,
    current: Arc<StdMutex<Vec<PeerAnchor>>>,
}

#[async_trait::async_trait]
impl PeerKeysRead for Peers {
    async fn peer_keys_read(&self) -> Result<Vec<PeerAnchor>, HandlerError> {
        Ok(self.current.lock().unwrap().clone())
    }

    async fn own_fingerprint(&self) -> Result<String, HandlerError> {
        Ok(self.own.clone())
    }
}

#[derive(Default)]
struct KeyState {
    held: BTreeSet<u64>,
    pending: Option<PendingMint>,
    discards: Vec<String>,
    imports: Vec<u64>,
    commits: Vec<u64>,
    mint_exclusions: Vec<Vec<String>>,
    joins_served: Vec<String>,
    fail_next_join_wrap: bool,
    join_wrap_attempts: usize,
    joins_imported: Vec<Vec<u64>>,
}

#[derive(Default)]
struct Fleet {
    next: AtomicU64,
    current: Arc<StdMutex<Vec<PeerAnchor>>>,
    supports_excluded: AtomicBool,
}

struct Vault {
    own: String,
    fleet: Arc<Fleet>,
    state: StdMutex<KeyState>,
}

#[async_trait::async_trait]
impl EpochControl for Vault {
    async fn status(&self) -> Result<EpochStatus, HandlerError> {
        let state = self.state.lock().unwrap();
        Ok(EpochStatus {
            current: state.held.last().copied(),
            held: state.held.iter().copied().collect(),
            pending: state.pending.clone(),
            sealing_blocked_by: vec![],
        })
    }

    async fn mint(&self, members: &[String], excluded: &[String]) -> Result<Minted, HandlerError> {
        let mut state = self.state.lock().unwrap();
        state.mint_exclusions.push(excluded.to_vec());
        if !excluded.is_empty() && !self.fleet.supports_excluded.load(Ordering::Acquire) {
            return Err(HandlerError::new(
                "invalid_params",
                "legacy vault rejects excluded",
            ));
        }
        let mut expected: BTreeSet<_> = self
            .fleet
            .current
            .lock()
            .unwrap()
            .iter()
            .map(|peer| format!("fed-computer:{}", peer.fingerprint))
            .collect();
        for member in excluded {
            assert_ne!(member, &format!("fed-computer:{}", self.own));
            assert!(expected.remove(member));
        }
        if members.iter().cloned().collect::<BTreeSet<_>>() != expected {
            return Err(HandlerError::new(
                "fleet_members_mismatch",
                "unexpected stand-in members",
            ));
        }
        assert!(
            state.pending.is_none(),
            "must discard a pending vault mint first"
        );
        let epoch = state.held.last().copied().unwrap_or(0) + 1;
        let mint_id = URL_SAFE_NO_PAD
            .encode(u128::from(self.fleet.next.fetch_add(1, Ordering::Relaxed)).to_be_bytes());
        state.pending = Some(PendingMint {
            epoch,
            mint_id: mint_id.clone(),
        });
        let wraps = members
            .iter()
            .filter(|member| member.strip_prefix("fed-computer:") != Some(&self.own))
            .map(|member| MintedWrap {
                member_id: member.clone(),
                wrap: json!({"fixture_epoch":epoch,"fixture_mint":mint_id,"to":member}),
            })
            .collect();
        Ok(Minted {
            epoch,
            mint_id,
            wraps,
        })
    }

    async fn commit(&self, mint: &PendingMint) -> Result<(), HandlerError> {
        let mut state = self.state.lock().unwrap();
        if !state.held.contains(&mint.epoch) {
            assert_eq!(state.pending.take().unwrap().mint_id, mint.mint_id);
            state.held.insert(mint.epoch);
            state.commits.push(mint.epoch);
        }
        Ok(())
    }

    async fn discard(&self, mint: &PendingMint) -> Result<(), HandlerError> {
        let mut state = self.state.lock().unwrap();
        if !state.discards.contains(&mint.mint_id) {
            assert_eq!(state.pending.take().unwrap().mint_id, mint.mint_id);
            state.discards.push(mint.mint_id.clone());
        }
        Ok(())
    }

    async fn import(&self, wrap: &Value) -> Result<Imported, HandlerError> {
        let mut state = self.state.lock().unwrap();
        assert!(
            state.pending.is_none(),
            "discard before importing the winner"
        );
        assert_eq!(wrap["to"], format!("fed-computer:{}", self.own));
        let epochs = if let Some(ct) = wrap["ct"].as_str() {
            let bytes = URL_SAFE_NO_PAD.decode(ct).unwrap();
            let keys: Vec<(u64, String)> = serde_json::from_slice(&bytes).unwrap();
            let epochs: Vec<_> = keys.into_iter().map(|(epoch, _)| epoch).collect();
            state.joins_imported.push(epochs.clone());
            epochs
        } else {
            vec![wrap["fixture_epoch"].as_u64().unwrap()]
        };
        let epochs: Vec<_> = epochs
            .into_iter()
            .filter(|e| state.held.insert(*e))
            .collect();
        state.imports.extend(&epochs);
        Ok(Imported {
            already_held: epochs.is_empty(),
            epochs,
        })
    }

    async fn wrap_for(&self, member_id: &str) -> Result<Value, HandlerError> {
        assert_ne!(member_id, format!("fed-computer:{}", self.own));
        assert!(self
            .fleet
            .current
            .lock()
            .unwrap()
            .iter()
            .any(|peer| { member_id == format!("fed-computer:{}", peer.fingerprint) }));
        let mut state = self.state.lock().unwrap();
        state.join_wrap_attempts += 1;
        if std::mem::take(&mut state.fail_next_join_wrap) {
            return Err(HandlerError::new(
                "fixture_wrap_unavailable",
                "try the wrap again",
            ));
        }
        state.joins_served.push(member_id.into());
        let keys: Vec<_> = state.held.iter().map(|e| (*e, "x".repeat(96))).collect();
        let ct = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&keys).unwrap());
        Ok(json!({"v":1,"ct":ct,"enc":"fixture","from":self.own,"to":member_id}))
    }
}

#[async_trait::async_trait]
impl EpochVault for Vault {
    fn control(&self) -> Option<&dyn EpochControl> {
        Some(self)
    }

    async fn seal(&self, aad: &[u8], plaintext: &[u8]) -> Result<SealedBody, HandlerError> {
        let epoch = *self.state.lock().unwrap().held.last().unwrap();
        let mut ciphertext = sha2::Sha256::digest(aad)[..16].to_vec();
        ciphertext.extend_from_slice(plaintext);
        Ok(serde_json::from_value(json!({"epoch":epoch,"nonce":URL_SAFE_NO_PAD.encode([0;24]),"ciphertext":URL_SAFE_NO_PAD.encode(ciphertext)})).unwrap())
    }

    async fn open(&self, aad: &[u8], body: &SealedBody) -> Result<Vec<u8>, HandlerError> {
        let (epoch, ciphertext) = body.fixture_ciphertext();
        assert!(self.state.lock().unwrap().held.contains(&epoch));
        let bytes = URL_SAFE_NO_PAD.decode(ciphertext).unwrap();
        assert_eq!(bytes[..16], sha2::Sha256::digest(aad)[..16]);
        Ok(bytes[16..].to_vec())
    }
}

use sha2::Digest;

struct EpochNode {
    node: Node,
    vault: Arc<Vault>,
    controller: Arc<Controller<Leaf, TestKeys>>,
}

impl EpochNode {
    fn epoch_view(&self) -> Value {
        serde_json::from_slice::<Value>(&self.node.handler.fleet_epoch_current(json!({})).unwrap())
            .unwrap()["result"]
            .clone()
    }

    async fn new(
        rig: &IdentityRig,
        name: &str,
        leaf: Arc<Leaf>,
        fleet: Arc<Fleet>,
        vault: Option<Arc<Vault>>,
    ) -> Self {
        let node = Self::start(rig, name, leaf, fleet, vault);
        within(async {
            while !node.node.log.membership_ready() {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        node
    }

    fn start(
        rig: &IdentityRig,
        name: &str,
        leaf: Arc<Leaf>,
        fleet: Arc<Fleet>,
        vault: Option<Arc<Vault>>,
    ) -> Self {
        let id = leaf.box_id.to_string();
        let handler = handler(store(&rig._processes, name), &id);
        let own = id;
        if let Some(vault) = &vault {
            // A cold join's verified join wrap brings all retained epochs.
            handler
                .with_store(|store| {
                    store.record_hub_held_epochs(
                        entorhinal_core::hub_store::HubEpochSource::Join("cold-rebuild"),
                        &vault
                            .state
                            .lock()
                            .unwrap()
                            .held
                            .iter()
                            .copied()
                            .collect::<Vec<_>>(),
                    )
                })
                .unwrap();
        }
        let vault = vault.unwrap_or_else(|| {
            Arc::new(Vault {
                own: own.clone(),
                fleet: fleet.clone(),
                state: StdMutex::default(),
            })
        });
        let log = Self::activate(rig, &handler, &leaf, &vault);
        let controller = Controller::new(log.clone()).unwrap();
        Self {
            node: Node { handler, leaf, log },
            vault,
            controller,
        }
    }

    fn activate(
        rig: &IdentityRig,
        handler: &ProjectsHandler,
        leaf: &Arc<Leaf>,
        vault: &Arc<Vault>,
    ) -> Arc<IdentityLog<Leaf, TestKeys>> {
        Arc::new(
            handler
                .activate_hub_identity_log(
                    leaf.box_id.clone(),
                    Link {
                        transport: leaf.clone(),
                        trust: rig.keys.clone(),
                        signer: Arc::new(ClaustrumCredentialResolver::with_target(
                            rig.keys.clone(),
                            0,
                        )),
                        vault: vault.clone(),
                        peers: Arc::new(Peers {
                            own: vault.own.clone(),
                            current: vault.fleet.current.clone(),
                        }),
                    },
                )
                .unwrap(),
        )
    }

    fn watch(&self) -> WatchTask {
        let leaf = self.node.leaf.clone();
        let feed = Feed::epoch(self.node.handler.health.clone(), move |_| {
            let leaf = leaf.clone();
            async move {
                Binding {
                    session: leaf.session(),
                    changes: None,
                }
                .epoch_watch(&leaf.box_id.to_string())
                .await
            }
        })
        .with_durable_cursor();
        let controller = self.controller.clone();
        WatchTask(tokio::spawn(async move {
            controller.watch_for(feed, Duration::from_millis(750)).await;
        }))
    }

    async fn fence(&self) -> u64 {
        self.node
            .log
            .epoch
            .as_ref()
            .unwrap()
            .fence(deadline())
            .await
            .unwrap()
    }
}

struct WatchTask(tokio::task::JoinHandle<()>);
impl Drop for WatchTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(15)
}

fn roster(fleet: &Fleet, rig: &IdentityRig, boxes: &[&str]) {
    *fleet.current.lock().unwrap() = boxes
        .iter()
        .map(|id| {
            let mut peer = anchor(id, &rig.keys);
            peer.fingerprint = (*id).to_owned();
            peer
        })
        .collect();
}

async fn devices(rig: &IdentityRig) -> (Arc<Fleet>, EpochNode, EpochNode) {
    for node in [&rig.a, &rig.b] {
        node.handler
            .hub_tail_task
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .abort();
    }
    let fleet = Arc::new(Fleet::default());
    roster(&fleet, rig, &[A, B, C, D]);
    let a = EpochNode::new(rig, "epoch-a", rig.a.leaf.clone(), fleet.clone(), None).await;
    let b = EpochNode::new(rig, "epoch-b", rig.b.leaf.clone(), fleet.clone(), None).await;
    a.controller.bootstrap(deadline()).await.unwrap();
    assert_eq!(b.fence().await, 1);
    (fleet, a, b)
}

fn event(id: &str, removal: &str) -> Vec<u8> {
    json!({"box_id":id,"fingerprint":id,"removal_id":removal})
        .to_string()
        .into_bytes()
}

async fn register(rig: &IdentityRig) -> Value {
    let entry = rig.kv.get("epoch").await.unwrap().unwrap();
    UnverifiedRecord::parse("epoch", &entry)
        .unwrap()
        .unsealed()
        .unwrap()
        .clone()
}

async fn history(rig: &IdentityRig) -> Vec<Value> {
    let leaf = &rig.a.leaf;
    let mut removed = Vec::new();
    for sequence in 1..=leaf.log_head(deadline()).await.unwrap() {
        let bytes = leaf.log_entry(sequence, deadline()).await.unwrap().unwrap();
        let record = UnverifiedRecord::parse(LOG_SUBJECT, &bytes).unwrap();
        if record.header().kind == "member.removed" {
            removed.push(record.unsealed().unwrap().clone());
        }
    }
    removed
}

async fn admit(node: &EpochNode, box_id: &str, removal: &str) {
    let head = node.node.log.applied_sequence(deadline()).await.unwrap();
    let body = json!({"box_id": box_id,"after_removal_id":removal,"request_id":"join-episode"});
    let landed = node
        .node
        .log
        .write_epoch_step(
            "test-member.admitted",
            "digest",
            &hub_write::Step {
                name: "member.admitted".into(),
                key: LOG_SUBJECT.into(),
                expected: head,
                payload: hub_write::Payload::Record {
                    kind: "member.admitted".into(),
                    body,
                    snapshot_id: None,
                },
            },
            deadline(),
        )
        .await
        .unwrap();
    within(through(&node.node.handler, landed.position.unwrap())).await;
}

#[tokio::test]
async fn hub_epoch_two_minters_discard_and_import_and_duplicate_after_readmission_is_inert() {
    let Some(rig) = IdentityRig::new().await else {
        return;
    };
    let (fleet, a, b) = devices(&rig).await;
    roster(&fleet, &rig, &[A, B, D]);
    let race = Arc::new(Barrier::new(2));
    for leaf in [&a.node.leaf, &b.node.leaf] {
        *leaf.epoch_gate.lock().unwrap() = Some(("epoch".into(), Gate::Race(race.clone())));
    }
    let bytes = event(C, "100:permanent:7");
    let (a_mint, b_mint) = within(async {
        tokio::join!(
            a.controller.deliver(&bytes, deadline()),
            b.controller.deliver(&bytes, deadline())
        )
    })
    .await;
    assert!(a_mint.is_ok() || b_mint.is_ok(), "{a_mint:?}; {b_mint:?}");
    let active = register(&rig).await;
    let (winner, loser) = if active["minter"] == A {
        (&a, &b)
    } else {
        (&b, &a)
    };
    assert_eq!(active["e"], 2);
    assert_eq!(active["state"], "committed");
    assert_eq!(loser.fence().await, 2);
    assert_eq!(loser.vault.state.lock().unwrap().discards.len(), 1);
    assert_eq!(loser.vault.state.lock().unwrap().imports.last(), Some(&2));
    assert_eq!(winner.vault.state.lock().unwrap().commits.last(), Some(&2));
    assert_eq!(history(&rig).await.len(), 1);
    roster(&fleet, &rig, &[A, B, C, D]);
    admit(winner, C, "100:permanent:7").await;
    let mut joined = active;
    joined["recipients"].as_array_mut().unwrap().push(C.into());
    let revision = rig.kv.entry("epoch").await.unwrap().unwrap().revision;
    winner
        .node
        .log
        .write_epoch_step(
            "test-join-epoch",
            "digest",
            &hub_write::Step {
                name: "epoch".into(),
                key: "epoch".into(),
                expected: revision,
                payload: hub_write::Payload::Record {
                    kind: "epoch".into(),
                    body: joined.clone(),
                    snapshot_id: None,
                },
            },
            deadline(),
        )
        .await
        .unwrap();
    for node in [&a, &b] {
        node.controller.deliver(&bytes, deadline()).await.unwrap();
    }
    assert_eq!(register(&rig).await, joined);
    assert_eq!(history(&rig).await.len(), 1);
}

fn stop_after(leaf: &Leaf, key: &str) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
    let (reached, reached_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    *leaf.epoch_gate.lock().unwrap() = Some((key.into(), Gate::LoseAck(reached, release_rx)));
    (reached_rx, release)
}

async fn kill_minter_at(node: &EpochNode, key: &str, removal: Vec<u8>) {
    let (reached, _release) = stop_after(&node.node.leaf, key);
    let controller = node.controller.clone();
    let worker = tokio::spawn(async move { controller.deliver(&removal, deadline()).await });
    within(reached).await.unwrap();
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn hub_epoch_incomplete_minter_is_superseded_and_cleans_only_its_wraps() {
    let Some(rig) = IdentityRig::new().await else {
        return;
    };
    let (fleet, a, b) = devices(&rig).await;
    assert_eq!(b.epoch_view(), json!({"current":1,"highest_held":1}));
    let before_pending = b.node.handler.health.fleet_epoch.generation();
    roster(&fleet, &rig, &[A, B, D]);
    // The stand-in returns wraps in box order, so D's wrap lands first.
    kill_minter_at(&a, "wrap.2.", event(C, "200:9:8")).await;
    let old = register(&rig).await;
    let old_mint = old["mint_id"].as_str().unwrap();
    assert_eq!(old["state"], "pending");
    assert!(rig
        .kv
        .get(format!("wrap.2.{old_mint}.{D}"))
        .await
        .unwrap()
        .is_some());
    assert!(rig
        .kv
        .get(format!("wrap.2.{old_mint}.{B}"))
        .await
        .unwrap()
        .is_none());
    assert_eq!(b.fence().await, 1);
    assert_eq!(b.epoch_view(), json!({"current":1,"highest_held":1}));
    assert_eq!(
        b.node.handler.health.fleet_epoch.generation(),
        before_pending
    );
    let cold = EpochNode::new(&rig, "no-base", rig.b.leaf.clone(), fleet.clone(), None).await;
    assert_eq!(
        cold.node
            .log
            .epoch
            .as_ref()
            .unwrap()
            .fence(deadline())
            .await
            .unwrap_err()
            .code,
        "epoch_key_unavailable"
    );
    let _watch = b.watch();
    assert_eq!(register(&rig).await, old);
    within(through(&b.node.handler, 1)).await;
    let active = register(&rig).await;
    assert_eq!(active["state"], "committed");
    assert_eq!(active["e"], 2);
    assert_eq!(active["minter"], B);
    assert_ne!(active["mint_id"], old_mint);
    assert_eq!(active["base_mint_id"], old["base_mint_id"]);
    assert_eq!(active["removed"], old["removed"]);
    let _watch = a.watch();
    within(async {
        while a.epoch_view() != json!({"current":2,"highest_held":2}) {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert_eq!(a.vault.state.lock().unwrap().imports.last(), Some(&2));
    assert_eq!(b.epoch_view(), json!({"current":2,"highest_held":2}));
    assert_eq!(
        b.node.handler.health.fleet_epoch.generation(),
        before_pending + 2
    );
    assert!(rig
        .kv
        .get(format!("wrap.2.{old_mint}.{D}"))
        .await
        .unwrap()
        .is_none());
    let winning_id = active["mint_id"].as_str().unwrap();
    assert!(rig
        .kv
        .get(format!("wrap.2.{winning_id}.{A}"))
        .await
        .unwrap()
        .is_some());
    assert!(rig
        .kv
        .get(format!("wrap.2.{winning_id}.{D}"))
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn hub_epoch_survivor_repairs_both_absorbed_removals_and_cold_rebuild_opens_history() {
    let Some(rig) = IdentityRig::new().await else {
        return;
    };
    let (fleet, a, b) = devices(&rig).await;
    let before = AgentOperation::create(20, "before epoch rotation".into());
    before.plan(&a.node).append().await.unwrap();
    within(through(&a.node.handler, 1)).await;
    roster(&fleet, &rig, &[A, B]);
    kill_minter_at(&a, "epoch.commit:", event(C, "300:permanent:9")).await;
    assert_eq!(register(&rig).await["state"], "committed");
    assert!(history(&rig).await.is_empty());
    let _watch = b.watch();
    within(through(&b.node.handler, 3)).await;
    let removals = history(&rig).await;
    assert_eq!(removals.len(), 2);
    assert_eq!(removals[0]["box_id"], C);
    assert_eq!(removals[1]["box_id"], D);
    assert!(removals[1]["removal_id"].is_null());
    b.controller
        .deliver(&event(D, "301:permanent:9"), deadline())
        .await
        .unwrap();
    b.controller
        .deliver(&event(D, "301:permanent:9"), deadline())
        .await
        .unwrap();
    assert_eq!(history(&rig).await.len(), 3);
    assert_eq!(register(&rig).await["e"], 2);
    let after = AgentOperation::create(21, "after rotation".into());
    after.plan(&b.node).append().await.unwrap();
    within(through(&b.node.handler, 5)).await;
    let cold = EpochNode::new(
        &rig,
        "cold-rebuild",
        rig.b.leaf.clone(),
        fleet.clone(),
        Some(b.vault.clone()),
    )
    .await;
    assert_eq!(cold.node.log.applied_sequence(deadline()).await.unwrap(), 5);
    assert_eq!(cold.fence().await, 2);
}

#[tokio::test]
async fn hub_epoch_repaired_box_is_excluded_until_admission() {
    let Some(rig) = IdentityRig::new().await else {
        return;
    };
    let (fleet, a, b) = devices(&rig).await;
    roster(&fleet, &rig, &[A, B, D]);
    kill_minter_at(&a, "wrap.2.", event(C, "400:permanent:10")).await;
    let pending = register(&rig).await;
    roster(&fleet, &rig, &[A, B, C, D]);
    fleet
        .current
        .lock()
        .unwrap()
        .iter_mut()
        .find(|peer| peer.box_id == C)
        .unwrap()
        .fingerprint = "c".repeat(32);
    let _watch = b.watch();
    // The vault requires every paired fingerprint. Recovery cannot include C
    // merely because it re-paired, so it records the pending removal and waits.
    within(through(&b.node.handler, 1)).await;
    assert_eq!(register(&rig).await, pending);
    assert!(b.vault.state.lock().unwrap().pending.is_none());
    assert_eq!(
        b.vault.state.lock().unwrap().mint_exclusions,
        [vec![format!("fed-computer:{}", "c".repeat(32))], vec![]]
    );
    assert_eq!(history(&rig).await.len(), 1);
    assert_eq!(b.fence().await, 1);
    admit(&b, C, "400:permanent:10").await;
    within(async {
        loop {
            if register(&rig).await["state"] == "committed" {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let active = register(&rig).await;
    assert_eq!(active["e"], 2);
    assert!(active["recipients"]
        .as_array()
        .unwrap()
        .iter()
        .any(|id| id == C));
    assert_eq!(active["removed"], pending["removed"]);
    assert_eq!(history(&rig).await.len(), 1);
}

#[tokio::test]
async fn hub_epoch_readers_refuse_rollback_without_rejecting_unchanged_old_kv() {
    let Some(rig) = IdentityRig::new().await else {
        return;
    };
    let (fleet, a, b) = devices(&rig).await;
    let old = AgentOperation::create(22, "old epoch".into());
    let old_plan = old.plan(&a.node);
    let signer = ClaustrumCredentialResolver::with_target(rig.keys.clone(), 0);
    let make = |key: &str, kind: &str, id: u64| crate::fleet_record::Header {
        key: key.into(),
        kind: kind.into(),
        writer: A.into(),
        writer_ep: 0,
        entry_id: format!("{id:032x}"),
    };
    let rolled_back = crate::fleet_record::prepare(
        &signer,
        a.vault.as_ref(),
        &make(LOG_SUBJECT, "agent.create", 15),
        &old_plan.body,
    )
    .await
    .unwrap()
    .into_bytes();
    let key = "residence.agent_0000000000000016";
    let body = json!({"v":1,"holder":A,"holder_ep":0,"writer":A,"gen":1,"forced":false});
    let old_kv = crate::fleet_record::prepare(
        &signer,
        a.vault.as_ref(),
        &make(key, "residence", 16),
        &body,
    )
    .await
    .unwrap()
    .into_bytes();
    roster(&fleet, &rig, &[A, B, D]);
    a.controller
        .deliver(&event(C, "500:permanent:11"), deadline())
        .await
        .unwrap();
    let new = AgentOperation::create(23, "new epoch".into());
    new.plan(&a.node).append().await.unwrap();
    within(through(&b.node.handler, 2)).await;
    let cold = EpochNode::new(
        &rig,
        "rollback-cold",
        rig.b.leaf.clone(),
        fleet.clone(),
        Some(b.vault.clone()),
    )
    .await;
    let accepted = cold
        .node
        .log
        .verify_kv(
            key,
            &old_kv,
            crate::hub_membership::Sight::FirstOrAfterGap,
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(accepted.record.epoch(), Some(1));
    let new_kv = crate::fleet_record::prepare(
        &signer,
        a.vault.as_ref(),
        &make(key, "residence", 17),
        &body,
    )
    .await
    .unwrap()
    .into_bytes();
    cold.node
        .log
        .sql
        .test_store()
        .record_hub_kv(key, 5, Some((&new_kv, Some(&body))), 1)
        .unwrap();
    assert_eq!(
        cold.node
            .log
            .verify_kv(
                key,
                &old_kv,
                crate::hub_membership::Sight::ReplacesCached,
                deadline()
            )
            .await
            .err()
            .unwrap()
            .code,
        "epoch_rollback"
    );
    a.node
        .leaf
        .session()
        .append(rolled_back, 2, &format!("{:032x}", 15), deadline())
        .await
        .unwrap();
    within(async {
        while cold.node.handler.health.log_state.load(Ordering::Relaxed) != 6 {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert_eq!(cold.node.log.applied_sequence(deadline()).await.unwrap(), 2);
}
