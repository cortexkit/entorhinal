//! In-process wire log for replication tests. Each send is independent: holding
//! an append never joins a resend, and cancelling a waiter never cancels a send.

use std::{
    collections::{BTreeSet, VecDeque},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::oneshot;

use super::log_client::{
    self as wire, AppendRequest, EntryKind, LogConnector, LogTransport, TransportError,
};

#[derive(Clone, Debug)]
pub(crate) enum Action {
    Refuse { code: String, detail: Option<Value> },
    DropReply,
    Hang,
    Hold,
}

struct Held {
    request: AppendRequest,
    author: [u8; 16],
    reply: oneshot::Sender<Result<Vec<u8>, TransportError>>,
}

struct StoredEntry {
    request: AppendRequest,
    author: [u8; 16],
}

struct State {
    entries: Vec<StoredEntry>,
    member: bool,
    append_actions: VecDeque<Action>,
    read_actions: VecDeque<Action>,
    held: VecDeque<Held>,
    skipped: BTreeSet<u64>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            member: true,
            append_actions: VecDeque::new(),
            read_actions: VecDeque::new(),
            held: VecDeque::new(),
            skipped: BTreeSet::new(),
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct FakeLog {
    state: Arc<Mutex<State>>,
    calls: Arc<AtomicUsize>,
    calls_changed: Arc<tokio::sync::Notify>,
    author: [u8; 16],
}

impl FakeLog {
    /// A store's view of the same log, with its explicit rotation-stable author.
    /// Cloning keeps the identity; a restored copy uses the same author, while
    /// another store uses a different one. The default view is the zero author.
    pub(crate) fn with_author(&self, author: [u8; 16]) -> Self {
        Self {
            author,
            ..self.clone()
        }
    }

    pub(crate) fn client(&self) -> wire::LogClient {
        wire::LogClient::new(Arc::new(self.clone()))
    }
    pub(crate) fn head(&self) -> u64 {
        self.state.lock().unwrap().entries.len() as u64
    }
    pub(crate) fn held_count(&self) -> usize {
        self.state.lock().unwrap().held.len()
    }
    pub(crate) fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
    pub(crate) fn set_member(&self, member: bool) {
        self.state.lock().unwrap().member = member;
    }
    pub(crate) fn skip_position(&self, position: u64) {
        self.state.lock().unwrap().skipped.insert(position);
    }
    pub(crate) fn on_append(&self, action: Action) {
        self.state.lock().unwrap().append_actions.push_back(action);
    }
    pub(crate) fn on_read(&self, action: Action) {
        self.state.lock().unwrap().read_actions.push_back(action);
    }

    /// A deterministic barrier: actions and held requests are installed before
    /// the call counter advances, so no test needs a sleep to park a writer.
    pub(crate) async fn wait_for_calls(&self, count: usize) {
        loop {
            let changed = self.calls_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.calls() >= count {
                return;
            }
            changed.await;
        }
    }

    pub(crate) fn release_next(&self) -> Result<u64, TransportError> {
        let mut state = self.state.lock().unwrap();
        let held = state.held.pop_front().expect("no held append to release");
        let result = accept(&mut state, held.request, held.author);
        let reply = result
            .clone()
            .map(|position| response(json!({"position":position})));
        // Applying the request is independent of whether its waiter still lives.
        let _ = held.reply.send(reply);
        result
    }
}

fn refuse(code: &str, detail: Option<Value>) -> TransportError {
    TransportError::Refusal {
        code: code.into(),
        detail,
    }
}

fn response(result: Value) -> Vec<u8> {
    serde_json::to_vec(&json!({"result":result})).unwrap()
}

fn parse_append(params: Value) -> Result<AppendRequest, TransportError> {
    let invalid = || refuse("invalid_request", None);
    let data = wire::decode_hex(params["entry"]["data"].as_str().ok_or_else(invalid)?)
        .map_err(|_| invalid())?;
    if data.len() > wire::ENTRY_DATA_CAP {
        return Err(refuse(
            "too_large",
            Some(json!({"field":"entry.data","cap":wire::ENTRY_DATA_CAP})),
        ));
    }
    Ok(AppendRequest {
        expected_head: params["expected_head"].as_u64().ok_or_else(invalid)?,
        entry_id: wire::decode_hex(params["entry_id"].as_str().ok_or_else(invalid)?)
            .map_err(|_| invalid())?
            .try_into()
            .map_err(|_| invalid())?,
        kind: match params["entry"]["kind"].as_str() {
            Some("change") => EntryKind::Change,
            Some("snapshot") => EntryKind::Snapshot,
            _ => return Err(invalid()),
        },
        data,
    })
}

/// One serialized cloud transaction, checked in engram's order: membership,
/// then an earlier append with the same entry id, then the expected head. A
/// repeated entry id counts as a resend, and returns the original position,
/// only when the author, expected_head, kind and data all match the first
/// append; anything else is refused `id_reused`, as engram does. The author
/// stands in for the signer's rotation-stable pseudonym.
fn accept(
    state: &mut State,
    request: AppendRequest,
    author: [u8; 16],
) -> Result<u64, TransportError> {
    if !state.member {
        return Err(refuse(wire::NOT_MEMBER, None));
    }
    if let Some((index, old)) = state
        .entries
        .iter()
        .enumerate()
        .find(|(_, old)| old.request.entry_id == request.entry_id)
    {
        if old.author != author
            || old.request.expected_head != request.expected_head
            || old.request.kind != request.kind
            || old.request.data != request.data
        {
            return Err(refuse(wire::ID_REUSED, None));
        }
        return Ok(index as u64 + 1);
    }
    let head = state.entries.len() as u64;
    if request.expected_head != head {
        return Err(refuse(wire::HEAD_MOVED, Some(json!({"head":head}))));
    }
    state.entries.push(StoredEntry { request, author });
    Ok(head + 1)
}

#[async_trait]
impl LogConnector for FakeLog {
    async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
        Ok(Arc::new(self.clone()))
    }
}

#[async_trait]
impl LogTransport for FakeLog {
    async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
        let (action, held_reply) = {
            let mut state = self.state.lock().unwrap();
            let action = match method {
                wire::APPEND => state.append_actions.pop_front(),
                wire::READ => state.read_actions.pop_front(),
                _ => return Err(refuse("invalid_request", None)),
            };
            let held_reply = if matches!(action, Some(Action::Hold)) && method == wire::APPEND {
                let request = parse_append(params.clone())?;
                let (tx, rx) = oneshot::channel();
                state.held.push_back(Held {
                    request,
                    author: self.author,
                    reply: tx,
                });
                Some(rx)
            } else {
                None
            };
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.calls_changed.notify_waiters();
            (action, held_reply)
        };
        match action {
            Some(Action::Refuse { code, detail }) => {
                return Err(TransportError::Refusal { code, detail })
            }
            Some(Action::Hang) => return std::future::pending().await,
            Some(Action::Hold) => {
                return match held_reply {
                    Some(rx) => rx.await.unwrap_or(Err(TransportError::NoReply)),
                    None => std::future::pending().await,
                }
            }
            _ => {}
        }
        let result = {
            let mut state = self.state.lock().unwrap();
            if method == wire::APPEND {
                let position = accept(&mut state, parse_append(params)?, self.author)?;
                response(json!({"position":position}))
            } else {
                let after = params["after"]
                    .as_u64()
                    .ok_or_else(|| refuse("invalid_request", None))?;
                let limit = params["limit"]
                    .as_u64()
                    .filter(|n| (1..=wire::PAGE_ENTRIES_CAP as u64).contains(n))
                    .ok_or_else(|| refuse("invalid_request", None))?;
                let mut size = 0;
                let mut entries = Vec::new();
                for (index, stored) in state.entries.iter().enumerate() {
                    let request = &stored.request;
                    let position = index as u64 + 1;
                    if position <= after || state.skipped.contains(&position) {
                        continue;
                    }
                    if entries.len() == limit as usize
                        || size + request.data.len() > wire::PAGE_DATA_CAP
                    {
                        break;
                    }
                    size += request.data.len();
                    entries.push(json!({
                        "position":position, "entry_id":wire::encode_hex(&request.entry_id),
                        "kind":request.kind.as_str(), "entry":wire::encode_hex(&request.data),
                        "signer":wire::encode_hex(&[0x51;32]), "key_id":{"family":"bmk","epoch":0},
                        "author":wire::encode_hex(&stored.author),
                        "signed_by_self":stored.author == self.author,
                        "envelope_version":1,
                    }));
                }
                response(json!({"head":state.entries.len(),"entries":entries}))
            }
        };
        if matches!(action, Some(Action::DropReply)) {
            Err(TransportError::NoReply)
        } else {
            Ok(result)
        }
    }
}

/// Later read/disabled/cache tests install this before startup. Any accidental
/// dependency on engram panics at the first connector call, not after a timeout.
#[derive(Default)]
pub(crate) struct FailConnector {
    calls: AtomicUsize,
}

impl FailConnector {
    pub(crate) fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl LogConnector for FailConnector {
    async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        panic!("unexpected engram connector call");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use wire::{AppendReply, LogError};

    fn request(head: u64, id: u8, size: usize) -> AppendRequest {
        AppendRequest {
            expected_head: head,
            entry_id: [id; 16],
            kind: EntryKind::Change,
            data: vec![id; size],
        }
    }

    #[tokio::test]
    async fn authorship_is_stable_per_entry_and_relative_to_reader() {
        let fake = FakeLog::default();
        let a = fake.with_author([0x11; 16]);
        let b = fake.with_author([0x22; 16]);
        a.client().append(&request(0, 1, 1)).await.unwrap();
        b.client().append(&request(1, 2, 1)).await.unwrap();
        let restored_a = fake.with_author([0x11; 16]);
        restored_a.client().append(&request(2, 3, 1)).await.unwrap();

        // Literal wire rows check the fake independently of the client's decoder.
        let reply: Value = serde_json::from_slice(
            &b.call("identity_log.read", json!({"after":0,"limit":128}))
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            reply,
            json!({"result":{"head":3,"entries":[
                {"position":1,"entry_id":"01010101010101010101010101010101",
                 "signer":"5151515151515151515151515151515151515151515151515151515151515151",
                 "author":"11111111111111111111111111111111","signed_by_self":false,
                 "key_id":{"family":"bmk","epoch":0},"envelope_version":1,"kind":"change","entry":"01"},
                {"position":2,"entry_id":"02020202020202020202020202020202",
                 "signer":"5151515151515151515151515151515151515151515151515151515151515151",
                 "author":"22222222222222222222222222222222","signed_by_self":true,
                 "key_id":{"family":"bmk","epoch":0},"envelope_version":1,"kind":"change","entry":"02"},
                {"position":3,"entry_id":"03030303030303030303030303030303",
                 "signer":"5151515151515151515151515151515151515151515151515151515151515151",
                 "author":"11111111111111111111111111111111","signed_by_self":false,
                 "key_id":{"family":"bmk","epoch":0},"envelope_version":1,"kind":"change","entry":"03"}
            ]}})
        );
        for view in [&a, &a.clone(), &restored_a] {
            let page = view.client().read(0, 128).await.unwrap();
            assert_eq!(
                page.entries
                    .iter()
                    .map(|e| e.signed_by_self)
                    .collect::<Vec<_>>(),
                vec![true, false, true]
            );
            assert_eq!(page.entries[0].author, "11111111111111111111111111111111");
            assert_eq!(page.entries[1].author, "22222222222222222222222222222222");
            assert_eq!(page.entries[2].author, page.entries[0].author);
        }
    }

    #[tokio::test]
    async fn held_appends_and_receipts_keep_the_original_author() {
        let fake = FakeLog::default();
        let a = fake.with_author([0x11; 16]);
        let b = fake.with_author([0x22; 16]);
        let original = request(0, 1, 1);
        a.on_append(Action::Hold);
        let held = tokio::spawn({
            let client = a.client();
            let original = original.clone();
            async move { client.append(&original).await }
        });
        fake.wait_for_calls(1).await;
        // Releasing through B's view cannot change who sent the held request.
        assert_eq!(b.release_next().unwrap(), 1);
        assert_eq!(held.await.unwrap().unwrap().position, 1);
        assert_eq!(a.client().append(&original).await.unwrap().position, 1);
        assert_eq!(
            b.client().append(&original).await.unwrap_err(),
            LogError::IdReused
        );
        assert_eq!(
            b.client().read(0, 128).await.unwrap().entries[0].author,
            "11111111111111111111111111111111"
        );

        b.on_append(Action::DropReply);
        let next = request(1, 2, 1);
        assert_eq!(
            b.client().append(&next).await.unwrap_err(),
            LogError::NoReply
        );
        assert_eq!(b.client().append(&next).await.unwrap().position, 2);
        assert_eq!(
            a.client().read(0, 128).await.unwrap().entries[1].author,
            "22222222222222222222222222222222"
        );
        assert_eq!(
            fake.with_author([0x11; 16])
                .client()
                .append(&original)
                .await
                .unwrap()
                .position,
            1
        );
        assert_eq!(fake.head(), 2);
    }

    #[tokio::test]
    async fn dense_cas_receipts_check_membership_and_all_digest_fields() {
        let fake = FakeLog::default();
        let client = fake.client();
        let original = request(0, 1, 3);
        assert_eq!(client.append(&original).await.unwrap().position, 1);
        assert_eq!(
            client.append(&request(0, 2, 3)).await.unwrap_err(),
            LogError::HeadMoved { head: Some(1) }
        );
        assert_eq!(client.append(&request(1, 2, 3)).await.unwrap().position, 2);
        // The original expected_head is stale now, but the receipt beats CAS.
        assert_eq!(client.append(&original).await.unwrap().position, 1);
        for changed in [
            AppendRequest {
                expected_head: 2,
                ..original.clone()
            },
            AppendRequest {
                data: vec![2; 3],
                ..original.clone()
            },
            AppendRequest {
                kind: EntryKind::Snapshot,
                ..original.clone()
            },
        ] {
            assert_eq!(
                client.append(&changed).await.unwrap_err(),
                LogError::IdReused
            );
        }
        fake.set_member(false);
        assert_eq!(
            client.append(&original).await.unwrap_err(),
            LogError::NotMember
        );
        fake.set_member(true);
        let page = client.read(0, 128).await.unwrap();
        assert_eq!(page.head, 2);
        assert_eq!(
            page.entries.iter().map(|e| e.position).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(page.entries[0].entry, original.data);
    }

    #[tokio::test]
    async fn data_and_page_caps_count_decoded_bytes_at_exact_boundaries() {
        let fake = FakeLog::default();
        let client = fake.client();
        let over = request(0, 1, 262145);
        assert_eq!(
            client.append(&over).await.unwrap_err(),
            LogError::TooLarge {
                field: "entry.data".into(),
                cap: 262144
            }
        );
        assert_eq!(
            fake.calls(),
            0,
            "client size check must precede even a connector call"
        );
        // Bypass the client's size guard so this independently checks the fake.
        let error = fake
            .call(
                "identity_log.append",
                json!({"expected_head":0,"entry_id":"01010101010101010101010101010101",
            "entry":{"kind":"change","data":"01".repeat(262145)}}),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(error,TransportError::Refusal { code,detail:Some(detail) }
            if code == "too_large" && detail == json!({"field":"entry.data","cap":262144}))
        );
        assert_eq!(fake.head(), 0);
        for id in 1..=5 {
            client
                .append(&request(id as u64 - 1, id, 262144))
                .await
                .unwrap();
        }
        let page = client.read(0, 128).await.unwrap();
        assert_eq!(page.head, 5);
        assert_eq!(page.entries.len(), 4);
        assert_eq!(
            page.entries.iter().map(|e| e.entry.len()).sum::<usize>(),
            1048576
        );
        assert_eq!(client.read(4, 128).await.unwrap().entries.len(), 1);
        let small = FakeLog::default();
        for id in 1..=129 {
            small
                .client()
                .append(&request(id as u64 - 1, id, 0))
                .await
                .unwrap();
        }
        let page = small.client().read(0, 128).await.unwrap();
        assert_eq!(page.head, 129);
        assert_eq!(page.entries.len(), 128);
        assert_eq!(
            small.client().read(128, 128).await.unwrap().entries[0].position,
            129
        );
        for limit in [0, 129] {
            assert!(
                matches!(small.call("identity_log.read",json!({"after":0,"limit":limit})).await,
                Err(TransportError::Refusal { code,.. }) if code == "invalid_request")
            );
        }
    }

    #[tokio::test]
    async fn dropped_reply_resend_preserves_original_head_and_commits_once() {
        let fake = FakeLog::default();
        let client = fake.client();
        let original = request(0, 1, 3);
        fake.on_append(Action::DropReply);
        assert_eq!(
            client.append(&original).await.unwrap_err(),
            LogError::NoReply
        );
        assert_eq!(fake.head(), 1);
        client.append(&request(1, 2, 3)).await.unwrap();
        let head = client.read(0, 128).await.unwrap().head;
        assert_eq!(head, 2);
        assert_eq!(
            client.append(&original).await.unwrap(),
            AppendReply { position: 1 }
        );
        assert_eq!(
            original.expected_head, 0,
            "the client must not rewrite the retry request"
        );
        assert_eq!(
            client
                .append(&AppendRequest {
                    expected_head: head,
                    ..original.clone()
                })
                .await
                .unwrap_err(),
            LogError::IdReused
        );
        assert_eq!(fake.head(), 2);
        let page = client.read(0, 128).await.unwrap();
        assert_eq!(
            page.entries
                .iter()
                .filter(|e| e.entry_id == original.entry_id)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn held_appends_outlive_cancelled_waiters_and_resends_never_join() {
        let fake = FakeLog::default();
        fake.on_append(Action::Hold);
        let client = fake.client();
        let original = request(0, 1, 3);
        let held = tokio::spawn({
            let client = client.clone();
            let original = original.clone();
            async move { client.append(&original).await }
        });
        fake.wait_for_calls(1).await;
        assert_eq!(fake.held_count(), 1);
        assert_eq!(fake.head(), 0);
        fake.on_append(Action::Refuse {
            code: "unavailable".into(),
            detail: None,
        });
        assert_eq!(
            client.append(&original).await.unwrap_err(),
            LogError::Unavailable
        );
        held.abort();
        assert!(held.await.unwrap_err().is_cancelled());
        assert_eq!(fake.release_next().unwrap(), 1);
        assert_eq!(
            client.read(0, 128).await.unwrap().entries[0].entry_id,
            original.entry_id
        );
        // The resend is processed independently while the original is held.
        fake.on_append(Action::Hold);
        let next = request(1, 2, 3);
        let held = tokio::spawn({
            let client = client.clone();
            let next = next.clone();
            async move { client.append(&next).await }
        });
        fake.wait_for_calls(4).await;
        assert_eq!(fake.held_count(), 1);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), client.append(&next))
                .await
                .unwrap()
                .unwrap()
                .position,
            2
        );
        assert_eq!(fake.release_next().unwrap(), 2);
        assert_eq!(held.await.unwrap().unwrap().position, 2);
        assert_eq!(fake.head(), 2);
        // Another id winning the slot proves the held request can no longer land.
        fake.on_append(Action::Hold);
        let held = tokio::spawn({
            let client = client.clone();
            async move { client.append(&request(2, 3, 3)).await }
        });
        fake.wait_for_calls(6).await;
        client.append(&request(2, 4, 3)).await.unwrap();
        assert!(
            matches!(fake.release_next(),Err(TransportError::Refusal { code,.. }) if code == "head_moved")
        );
        assert_eq!(
            held.await.unwrap().unwrap_err(),
            LogError::HeadMoved { head: Some(3) }
        );
        assert_eq!(fake.head(), 3);
    }

    #[tokio::test]
    async fn faults_can_hang_reads_and_appends_skip_positions_and_inject_refusals() {
        let fake = FakeLog::default();
        let client = fake.client();
        for id in 1..=3 {
            client.append(&request(id as u64 - 1, id, 3)).await.unwrap();
        }
        fake.skip_position(2);
        assert_eq!(
            client
                .read(0, 128)
                .await
                .unwrap()
                .entries
                .iter()
                .map(|e| e.position)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
        for (code, detail, expected) in [
            (
                "head_moved",
                Some(json!({"head":6})),
                LogError::HeadMoved { head: Some(6) },
            ),
            ("not_member", None, LogError::NotMember),
            ("unavailable", None, LogError::Unavailable),
            ("id_reused", None, LogError::IdReused),
            ("key_unavailable", None, LogError::KeyUnavailable),
            (
                "verify_failed",
                Some(json!({"position":2})),
                LogError::VerifyFailed { position: Some(2) },
            ),
        ] {
            fake.on_append(Action::Refuse {
                code: code.into(),
                detail: detail.clone(),
            });
            assert_eq!(
                client.append(&request(3, 4, 3)).await.unwrap_err(),
                expected
            );
            fake.on_read(Action::Refuse {
                code: code.into(),
                detail,
            });
            assert_eq!(client.read(0, 128).await.unwrap_err(), expected);
            assert_eq!(fake.head(), 3);
        }
        fake.on_read(Action::DropReply);
        assert_eq!(client.read(0, 128).await.unwrap_err(), LogError::NoReply);
        fake.on_append(Action::Hang);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), client.append(&request(3, 4, 3)))
                .await
                .is_err()
        );
        fake.on_read(Action::Hang);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), client.read(0, 128))
                .await
                .is_err()
        );
        assert_eq!(fake.head(), 3);
        assert_eq!(
            client.read(0, 128).await.unwrap().head,
            3,
            "hanging calls hold no log lock"
        );
    }
}
