//! In-tree Cingulate. A dedup key refreshes only a pending card raised by the
//! same owner; settlements receive positions in one store-wide answer log.
//! Requester references correlate records but are intentionally not unique.

use async_trait::async_trait;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
};
use subc_protocol::scope::ScopeStamp;
use tokio::time::Instant;

use crate::{
    consent::{CARD_LIFETIME_MS, RETENTION_MS},
    consent_client::{ConsentConnector, ConsentError, ConsentRoute, Owner},
};
#[path = "consent_decoder.rs"]
pub(crate) mod decoder;

#[derive(Clone)]
pub(crate) enum Effect {
    Refuse(&'static str),
    LoseReply,
    FailBefore,
    Hang,
    Answer(&'static str),
    Record(Value),
}
#[derive(Clone, Debug)]
pub(crate) struct Call {
    pub owner: Owner,
    pub method: String,
    pub params: Value,
    pub deadline: Instant,
}
#[derive(Clone)]
struct Card {
    owner: Owner,
    body: Value,
    created: i64,
    expires: i64,
    state: String,
    choice: Option<String>,
    settled: Option<i64>,
    cursor: Option<i64>,
    acked: bool,
    detached: bool,
}
#[derive(Default)]
struct State {
    now: i64,
    next_id: u64,
    cursor: i64,
    cards: BTreeMap<String, Card>,
    scripts: BTreeMap<String, VecDeque<Effect>>,
    calls: Vec<Call>,
    opened: Vec<ScopeStamp>,
    closed: usize,
    page_size: usize,
}
#[derive(Clone, Default)]
pub(crate) struct FakeCingulate {
    state: Arc<Mutex<State>>,
}

impl FakeCingulate {
    pub fn new() -> Self {
        let fake = Self::default();
        fake.state.lock().unwrap().page_size = 100;
        fake.set_now(1_000);
        fake
    }
    pub fn now(&self) -> i64 {
        self.state.lock().unwrap().now
    }
    pub fn set_now(&self, now: i64) {
        self.state.lock().unwrap().now = now;
    }
    pub fn script(&self, method: &str, effect: Effect) {
        self.state
            .lock()
            .unwrap()
            .scripts
            .entry(method.into())
            .or_default()
            .push_back(effect);
    }
    pub fn calls(&self) -> Vec<Call> {
        self.state.lock().unwrap().calls.clone()
    }
    pub fn routes(&self) -> (Vec<ScopeStamp>, usize) {
        let s = self.state.lock().unwrap();
        (s.opened.clone(), s.closed)
    }
    pub fn page_size(&self, size: usize) {
        self.state.lock().unwrap().page_size = size;
    }
    pub fn answer(&self, id: &str, choice: &str) {
        let mut s = self.state.lock().unwrap();
        assert!(s.cards[id].body["options"]
            .as_array()
            .unwrap()
            .iter()
            .any(|o| o["id"] == choice));
        settle(&mut s, id, "answered", Some(choice.into()));
    }
    pub fn record(&self, id: &str) -> Value {
        let s = self.state.lock().unwrap();
        project(id, &s.cards[id], false)
    }
    pub fn card_count(&self) -> usize {
        self.state.lock().unwrap().cards.len()
    }
}

#[async_trait]
impl ConsentConnector for FakeCingulate {
    async fn open(
        &self,
        stamp: &ScopeStamp,
        _deadline: Instant,
    ) -> Result<Box<dyn ConsentRoute>, ConsentError> {
        if !stamp.owner_authorized
            || stamp
                .attributes
                .agent_id
                .as_deref()
                .is_none_or(|s| s.trim().is_empty())
        {
            return Err(ConsentError::Refused(
                "elicitation_requester_refused".into(),
            ));
        }
        self.state.lock().unwrap().opened.push(stamp.clone());
        Ok(Box::new(FakeRoute {
            fake: self.clone(),
            owner: Owner::from_stamp(stamp)?,
            closed: std::sync::atomic::AtomicBool::new(false),
        }))
    }
}

struct FakeRoute {
    fake: FakeCingulate,
    owner: Owner,
    closed: std::sync::atomic::AtomicBool,
}
impl FakeRoute {
    fn close_once(&self) {
        if !self.closed.swap(true, std::sync::atomic::Ordering::Relaxed) {
            self.fake.state.lock().unwrap().closed += 1;
        }
    }
}
impl Drop for FakeRoute {
    fn drop(&mut self) {
        self.close_once();
    }
}

#[async_trait]
impl ConsentRoute for FakeRoute {
    async fn close(&self) {
        self.close_once();
    }
    async fn call(
        &self,
        method: &str,
        params: Value,
        deadline: Instant,
    ) -> Result<Value, ConsentError> {
        let effect = {
            let mut s = self.fake.state.lock().unwrap();
            s.calls.push(Call {
                owner: self.owner.clone(),
                method: method.into(),
                params: params.clone(),
                deadline,
            });
            s.scripts.get_mut(method).and_then(VecDeque::pop_front)
        };
        match &effect {
            Some(Effect::Refuse(code)) => return Err(ConsentError::Refused((*code).into())),
            Some(Effect::FailBefore) => return Err(ConsentError::Ambiguous),
            Some(Effect::Hang) => return std::future::pending().await,
            Some(Effect::Record(record)) => return Ok(record.clone()),
            _ => {}
        }
        let mut s = self.fake.state.lock().unwrap();
        sweep(&mut s);
        if let Some(Effect::Answer(choice)) = effect.as_ref() {
            let id = params["elicitation_id"].as_str().unwrap();
            settle(&mut s, id, "answered", Some((*choice).into()));
        }
        let result = execute(&mut s, &self.owner, method, params)?;
        if matches!(effect, Some(Effect::LoseReply)) {
            Err(ConsentError::Ambiguous)
        } else {
            Ok(result)
        }
    }
}

fn settle(s: &mut State, id: &str, state: &str, choice: Option<String>) {
    let card = s.cards.get_mut(id).unwrap();
    if card.state != "pending" {
        return;
    }
    s.cursor += 1;
    card.state = state.into();
    card.choice = choice;
    card.settled = Some(s.now);
    card.cursor = Some(s.cursor);
}
fn sweep(s: &mut State) {
    let due = s
        .cards
        .iter()
        .filter(|(_, c)| c.state == "pending" && c.expires <= s.now)
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    for id in due {
        settle(s, &id, "expired", None);
    }
}
fn project(id: &str, c: &Card, detach: bool) -> Value {
    let mut record = c.body.clone();
    let o = record.as_object_mut().unwrap();
    o.insert("elicitation_id".into(), json!(id));
    o.insert("created_at_ms".into(), json!(c.created));
    o.insert("expires_at_ms".into(), json!(c.expires));
    o.insert("state".into(), json!(c.state));
    if let Some(choice) = &c.choice {
        o.insert("answered_choice_id".into(), json!(choice));
    }
    if let Some(time) = c.settled {
        o.insert("answered_at_ms".into(), json!(time));
    }
    if detach && c.detached {
        o.insert("detached".into(), json!(true));
    }
    record
}
fn comparable(body: &Value) -> Value {
    let mut v = body.clone();
    for key in ["expires_in_ms", "dedup_key", "requester_ref"] {
        v.as_object_mut().unwrap().remove(key);
    }
    v
}
fn refused(code: &str) -> ConsentError {
    ConsentError::Refused(code.into())
}

fn execute(
    s: &mut State,
    owner: &Owner,
    method: &str,
    params: Value,
) -> Result<Value, ConsentError> {
    match method {
        "consent.request" => {
            let body = decoder::decode(params).map_err(refused)?;
            let key = body["dedup_key"].as_str();
            let matched = key.and_then(|key| {
                s.cards
                    .iter()
                    .find(|(_, c)| {
                        &c.owner == owner && c.state == "pending" && c.body["dedup_key"] == key
                    })
                    .map(|(id, _)| id.clone())
            });
            if let Some(id) = matched {
                if comparable(&s.cards[&id].body) == comparable(&body) {
                    let c = s.cards.get_mut(&id).unwrap();
                    c.expires = s.now + body["expires_in_ms"].as_i64().unwrap();
                    c.body = body;
                    c.detached = false;
                    return Ok(json!({"elicitation_id":id}));
                }
                settle(s, &id, "withdrawn", None);
            }
            s.next_id += 1;
            let id = format!("el_{}", s.next_id);
            let expires = s.now + body["expires_in_ms"].as_i64().unwrap_or(CARD_LIFETIME_MS);
            s.cards.insert(
                id.clone(),
                Card {
                    owner: owner.clone(),
                    body,
                    created: s.now,
                    expires,
                    state: "pending".into(),
                    choice: None,
                    settled: None,
                    cursor: None,
                    acked: false,
                    detached: false,
                },
            );
            Ok(json!({"elicitation_id":id}))
        }
        "consent.get" | "consent.await" | "consent.detach" => {
            let id = params["elicitation_id"]
                .as_str()
                .ok_or_else(|| refused("elicitation_invalid_request"))?;
            let c = s
                .cards
                .get_mut(id)
                .filter(|c| &c.owner == owner)
                .ok_or_else(|| refused("elicitation_not_owner"))?;
            if c.acked || c.settled.is_some_and(|time| s.now - time >= RETENTION_MS) {
                return Err(refused("elicitation_not_found"));
            }
            if method == "consent.await" && params["timeout_ms"].as_i64().is_none_or(|n| n < 0) {
                return Err(refused("elicitation_invalid_request"));
            }
            if method == "consent.detach" && c.state == "pending" {
                c.detached = true;
            }
            Ok(project(id, c, method == "consent.detach"))
        }
        "consent.answers" => {
            let since = params["since"]
                .as_i64()
                .filter(|n| *n >= 0)
                .ok_or_else(|| refused("elicitation_invalid_request"))?;
            let mut cards = s
                .cards
                .iter()
                .filter(|(_, c)| {
                    &c.owner == owner
                        && !c.acked
                        && c.cursor.is_some_and(|pos| pos > since)
                        && c.state != "withdrawn"
                })
                .collect::<Vec<_>>();
            cards.sort_by_key(|(_, c)| c.cursor);
            let partial = cards.len() > s.page_size;
            cards.truncate(s.page_size);
            let cursor = if partial {
                cards.last().unwrap().1.cursor.unwrap()
            } else {
                s.cursor
            };
            let records = cards
                .iter()
                .map(|(id, c)| {
                    if s.now - c.settled.unwrap() >= RETENTION_MS {
                        json!({"elicitation_id":id, "state":"expired", "settled_at":c.settled})
                    } else {
                        project(id, c, false)
                    }
                })
                .collect::<Vec<_>>();
            let mut page = json!({"records":records, "cursor":cursor});
            if partial {
                page["continuation"] = json!(cursor);
            }
            Ok(page)
        }
        "consent.ack" => {
            let cursor = params["cursor"]
                .as_i64()
                .filter(|n| *n >= 0)
                .ok_or_else(|| refused("elicitation_invalid_request"))?;
            for c in s.cards.values_mut() {
                if &c.owner == owner && c.cursor.is_some_and(|pos| pos <= cursor) {
                    c.acked = true;
                }
            }
            Ok(json!({"ok":true}))
        }
        _ => Err(refused("unknown_method")),
    }
}
