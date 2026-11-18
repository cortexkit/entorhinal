//! Local role lease watches. A notice gives the consumer an absolute deadline
//! in the computer's monotonic clock domain; reception never extends it.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use subc_protocol::Principal;
use tokio::{
    sync::{mpsc, oneshot, watch, Notify},
    time::Instant,
};

use crate::{encode_result, watches::WatchTransport, HandlerError, ProjectsHandler, RouteKey};

#[cfg(test)]
pub(crate) mod tests;

pub(super) const WATCH: &str = "role.lease";
pub(super) const ACK: &str = "role.lease.ack";
pub(super) const RELEASE: &str = "role.lease.release";
pub(super) const YIELD: &str = "role.lease.yield";
pub(super) const RENEW: &str = "role.renew";
const ACQUIRE: &str = "role.acquire";
const TRANSFER: &str = "role.handback";
const ACK_MS: u64 = 5_000;
pub(super) const LEASE_MS: u64 = 60_000;
const RENEW_MS: u64 = 20_000;
const LINK_MS: u64 = 25_000;
const ACT_MS: u64 = 45_000;
const RETRY_MS: u64 = 1_000;
pub(super) const MAX_ROLE_BYTES: usize = 128;

pub(super) fn valid_role(role: &str) -> bool {
    role.len() <= MAX_ROLE_BYTES
        && role.split('.').all(|part| {
            !part.is_empty()
                && part.bytes().all(|b| {
                    b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'-')
                })
        })
}

fn consumer(role: &str) -> Option<&'static str> {
    match role {
        "connector_home" => Some("plexus"),
        role if role.starts_with("dreamer.") => Some("magic-context"),
        _ => None,
    }
}

#[cfg(unix)]
pub(super) fn monotonic_ms() -> u64 {
    let now = rustix::time::clock_gettime(rustix::time::ClockId::Monotonic);
    (now.tv_sec as u64) * 1_000 + (now.tv_nsec as u64) / 1_000_000
}

#[cfg(windows)]
#[allow(unsafe_code)]
pub(super) fn monotonic_ms() -> u64 {
    // SAFETY: GetTickCount64 has no arguments or pointer accesses and reads
    // the system uptime counter shared by all processes.
    unsafe { windows_sys::Win32::System::SystemInformation::GetTickCount64() }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct View {
    pub gen: i64,
    pub until_ms: Option<u64>,
    pub handback: bool,
}

#[derive(Serialize)]
#[cfg_attr(test, derive(Deserialize))]
struct Notice {
    holding: bool,
    gen: i64,
    valid_for_ms: u64,
    issued_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    handback: Option<bool>,
    seq: u64,
}

struct Client {
    route: RouteKey,
    awaiting: Mutex<Option<(u64, u64)>>,
    acked: Notify,
    changed: Notify,
    view: Mutex<View>,
    control: mpsc::Sender<Control>,
}

pub(super) struct Leases {
    clients: Arc<Mutex<HashMap<String, Arc<Client>>>>,
    seq: AtomicU64,
    held: AtomicU64,
    clock: Arc<dyn Fn() -> u64 + Send + Sync>,
    backend: watch::Sender<Option<Arc<dyn Backend>>>,
    #[cfg(test)]
    sample_views: HashMap<String, watch::Receiver<View>>,
}

impl Default for Leases {
    fn default() -> Self {
        Self {
            clients: Arc::default(),
            seq: AtomicU64::default(),
            held: AtomicU64::default(),
            clock: Arc::new(monotonic_ms),
            backend: watch::channel(None).0,
            #[cfg(test)]
            sample_views: HashMap::new(),
        }
    }
}

struct Registration<'a> {
    leases: &'a Leases,
    role: String,
    holding: bool,
}

impl Registration<'_> {
    fn holding(&mut self, holding: bool) {
        if holding && !self.holding {
            self.leases.held.fetch_add(1, Ordering::Relaxed);
        } else if !holding && self.holding {
            self.leases.held.fetch_sub(1, Ordering::Relaxed);
        }
        self.holding = holding;
    }
}

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        self.holding(false);
        self.leases.clients.lock().unwrap().remove(&self.role);
        if let Some(backend) = self.leases.backend.borrow().clone() {
            let clients = self.leases.clients.clone();
            let role = self.role.clone();
            tokio::spawn(async move {
                let deadline = Instant::now() + Duration::from_secs(5);
                if let Ok(Some(revision)) = backend
                    .read_handback_revision(&role, backend.local_box(), deadline)
                    .await
                {
                    // A reopened watch owns its request. A revision-checked
                    // delete protects a new request that lands after this check.
                    if !clients.lock().unwrap().contains_key(&role) {
                        let _ = backend
                            .delete_handback(&role, backend.local_box(), revision, deadline)
                            .await;
                    }
                }
            });
        }
    }
}

impl Leases {
    pub(super) fn held(&self) -> u64 {
        self.held.load(Ordering::Relaxed)
    }

    fn now(&self) -> u64 {
        (self.clock)()
    }

    #[cfg(test)]
    pub(super) fn sample_clock(&mut self, clock: impl Fn() -> u64 + Send + Sync + 'static) {
        self.clock = Arc::new(clock);
    }

    #[cfg(test)]
    pub(super) fn sample_view(&mut self, role: &str, view: watch::Receiver<View>) {
        self.sample_views.insert(role.into(), view);
    }

    fn register(
        &self,
        role: String,
        route: RouteKey,
        control: mpsc::Sender<Control>,
    ) -> Result<(Arc<Client>, Registration<'_>), HandlerError> {
        use std::collections::hash_map::Entry;

        let client = Arc::new(Client {
            route,
            awaiting: Mutex::default(),
            acked: Notify::new(),
            changed: Notify::new(),
            view: Mutex::default(),
            control,
        });
        match self.clients.lock().unwrap().entry(role.clone()) {
            Entry::Occupied(_) => {
                return Err(HandlerError::new(
                    "lease_watch_exists",
                    "a watch is already open for this role",
                ));
            }
            Entry::Vacant(entry) => entry.insert(client.clone()),
        };
        Ok((
            client,
            Registration {
                leases: self,
                role,
                holding: false,
            },
        ))
    }

    fn client(&self, role: &str, route: RouteKey) -> Result<Arc<Client>, HandlerError> {
        self.clients
            .lock()
            .unwrap()
            .get(role)
            .filter(|client| client.route == route)
            .cloned()
            .ok_or_else(|| {
                HandlerError::new(
                    "caller_forbidden",
                    "use the route that opened the role lease watch",
                )
            })
    }

    async fn notices(
        &self,
        transport: &impl WatchTransport,
        client: &Client,
        mut registration: Registration<'_>,
        mut views: watch::Receiver<View>,
    ) -> Result<(), HandlerError> {
        let mut requested = false;
        loop {
            let view = *views.borrow_and_update();
            let issued_at_ms = self.now();
            let valid_for_ms = view
                .until_ms
                .unwrap_or_default()
                .saturating_sub(issued_at_ms);
            let holding = valid_for_ms != 0;
            let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
            *client.awaiting.lock().unwrap() = Some((seq, issued_at_ms + ACK_MS));
            *client.view.lock().unwrap() = view;
            let notice = Notice {
                holding,
                gen: view.gen,
                valid_for_ms,
                issued_at_ms,
                handback: (holding && (requested || view.handback)).then_some(view.handback),
                seq,
            };
            requested = holding && view.handback;
            let body = serde_json::to_vec(&notice).map_err(crate::invalid_params)?;
            registration.holding(holding);
            let expires = tokio::time::sleep(Duration::from_millis(valid_for_ms));
            tokio::pin!(expires);
            let ack = tokio::time::timeout(Duration::from_millis(ACK_MS), async {
                transport.emit(body).await?;
                client.acked.notified().await;
                Ok::<_, HandlerError>(())
            });
            tokio::pin!(ack);
            loop {
                tokio::select! {
                    result = &mut ack => {
                        match result {
                            Ok(result) => result?,
                            Err(_) => return Ok(()),
                        }
                        break;
                    }
                    _ = &mut expires, if registration.holding => registration.holding(false),
                }
            }
            loop {
                tokio::select! {
                    result = views.changed() => {
                        if result.is_err() {
                            return Ok(());
                        }
                        break;
                    }
                    _ = &mut expires, if registration.holding => registration.holding(false),
                }
            }
        }
    }

    pub(super) fn attach(&self, backend: Option<Arc<dyn Backend>>) {
        self.backend.send_replace(backend);
    }

    pub(super) fn changed(&self, role: &str) {
        if let Some(client) = self.clients.lock().unwrap().get(role) {
            client.changed.notify_one();
        }
    }

    pub(super) fn holding(&self, role: &str, gen: i64) -> bool {
        self.clients
            .lock()
            .unwrap()
            .get(role)
            .is_some_and(|client| {
                let view = client.view.lock().unwrap();
                view.gen == gen && view.until_ms.is_some_and(|until| self.now() < until)
            })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Record {
    pub v: u64,
    pub holder: Option<String>,
    pub holder_ep: u64,
    pub gen: i64,
    pub writer: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub forced: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub seeded: bool,
    pub lease_ms: u64,
}

impl Record {
    pub(super) fn new(writer: &str, episode: u64, gen: i64, released: bool) -> Self {
        Self {
            v: 1,
            holder: (!released).then(|| writer.to_owned()),
            holder_ep: if released { 0 } else { episode },
            gen,
            writer: writer.into(),
            forced: false,
            seeded: false,
            lease_ms: LEASE_MS,
        }
    }
}

pub(super) fn accepts(
    body: &Record,
    previous: Option<&Record>,
    header: &crate::fleet_record::Header,
    writer_current: bool,
    historical: bool,
    seed: Option<(Option<String>, i64)>,
    is_current: impl Fn(&str, u64) -> bool,
) -> bool {
    use crate::hub_membership::box_id;
    if body.v != 1
        || body.lease_ms != LEASE_MS
        || body.gen <= 0
        || body.writer != header.writer
        || body.holder.as_ref().is_some_and(|holder| {
            !box_id(holder) && !holder.strip_prefix("engram:").is_some_and(box_id)
        })
        || (body.holder.is_none() && body.holder_ep != 0)
        || (body.forced && body.seeded)
    {
        return false;
    }
    if body.seeded {
        return (writer_current || historical)
            && previous.is_none()
            && body.holder_ep == 0
            && seed == Some((body.holder.clone(), body.gen));
    }
    if let Some(prior) = previous {
        if body.gen < prior.gen {
            return false;
        }
        if body.gen == prior.gen {
            return writer_current
                && !body.forced
                && prior.holder.as_deref() == Some(header.writer.as_str())
                && prior.holder_ep == header.writer_ep
                && (body.holder.is_none()
                    || (body.holder == prior.holder && body.holder_ep == prior.holder_ep));
        }
    }
    let consecutive = previous.filter(|prior| prior.gen.checked_add(1) == Some(body.gen));
    let replaced_removed = consecutive.is_some_and(|prior| {
        prior
            .holder
            .as_deref()
            .is_some_and(|holder| !is_current(holder, prior.holder_ep))
    });
    if body.forced {
        // A reader without a consecutive predecessor can accept a historical
        // holder. A closed episode can't replace a known predecessor.
        return (writer_current || (historical && consecutive.is_none()))
            && body.gen > 1
            && body.holder.as_deref() == Some(header.writer.as_str())
            && body.holder_ep == header.writer_ep
            && (consecutive.is_none() || replaced_removed);
    }
    // A first read or revision gap may expose a prior write from a computer
    // that has since been removed. It cannot replace a known predecessor.
    (writer_current || (historical && consecutive.is_none()))
        && !replaced_removed
        && body.holder.as_deref().is_some_and(|holder| {
            box_id(holder)
                && if holder == header.writer {
                    body.holder_ep == header.writer_ep
                } else {
                    consecutive.is_none_or(|prior| {
                        prior.holder.as_deref() == Some(header.writer.as_str())
                            && prior.holder_ep == header.writer_ep
                    })
                }
        })
}

pub(super) struct Observation {
    pub ready: bool,
    pub watch: (u64, u64),
    pub revision: u64,
    pub record: Option<Record>,
    pub local_episode: Option<u64>,
    pub holder_current: bool,
}

impl Observation {
    fn own(&self, local: &str) -> bool {
        self.holder_current
            && self.record.as_ref().is_some_and(|record| {
                record.holder.as_deref() == Some(local)
                    && self.local_episode == Some(record.holder_ep)
            })
    }

    pub(super) fn gen(&self) -> i64 {
        self.record.as_ref().map_or(0, |record| record.gen)
    }
}

pub(super) struct Change {
    pub request_key: String,
    pub step: &'static str,
    pub expected: u64,
    pub writer_ep: u64,
    pub body: Record,
    pub vacancy_preference: Option<u64>,
}

pub(super) struct Confirmed {
    pub revision: u64,
    pub sent_ms: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Preference {
    pub v: u64,
    #[serde(rename = "box")]
    pub box_id: String,
    pub writer: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Handback {
    pub v: u64,
    #[serde(rename = "box")]
    pub box_id: String,
    pub gen: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Preferred {
    pub revision: u64,
    pub box_id: String,
    pub episode: u64,
}

#[derive(Clone)]
pub(super) struct CandidateRequest {
    pub revision: u64,
    pub gen: i64,
    pub episode: u64,
}

#[async_trait::async_trait]
pub(super) trait Backend: Send + Sync {
    fn local_box(&self) -> &str;
    fn role(&self, name: &str) -> Result<Observation, HandlerError>;
    fn preferred(&self, role: &str) -> Result<Option<Preferred>, HandlerError>;
    fn handback(&self, role: &str, box_id: &str) -> Result<Option<CandidateRequest>, HandlerError>;
    fn link_events(&self) -> (&Notify, &Notify);
    async fn reread_offer(
        &self,
        role: &str,
        target: &Preferred,
        gen: i64,
        deadline: Instant,
    ) -> Result<bool, HandlerError>;
    async fn read_handback_revision(
        &self,
        role: &str,
        box_id: &str,
        deadline: Instant,
    ) -> Result<Option<u64>, HandlerError>;
    async fn put_handback(
        &self,
        role: &str,
        gen: i64,
        deadline: Instant,
    ) -> Result<(), HandlerError>;
    async fn delete_handback(
        &self,
        role: &str,
        box_id: &str,
        revision: u64,
        deadline: Instant,
    ) -> Result<(), HandlerError>;
    async fn write(
        &self,
        role: &str,
        change: &Change,
        deadline: Instant,
    ) -> Result<Confirmed, HandlerError>;
}

struct Control {
    gen: i64,
    yielding: bool,
    reply: oneshot::Sender<Result<bool, HandlerError>>,
}

fn no_offer() -> HandlerError {
    HandlerError::new(
        "lease_handback_not_requested",
        "there is no handback on this watch",
    )
}

fn lease_gen_moved() -> HandlerError {
    HandlerError::new(
        "epoch_moved",
        "the local role lease generation no longer matches",
    )
}

fn change(step: &'static str, prior: &Observation, local: &str) -> Result<Change, HandlerError> {
    let writer_ep = prior
        .local_episode
        .ok_or_else(|| HandlerError::new("hub_not_ready", "local membership is not ready"))?;
    let gen = if matches!(step, ACQUIRE | TRANSFER) {
        prior
            .gen()
            .checked_add(1)
            .ok_or_else(|| HandlerError::new("record_invalid", "role generation is exhausted"))?
    } else {
        prior.gen()
    };
    Ok(Change {
        request_key: format!("role:{}", crate::hub_write::new_op_id()?),
        step,
        expected: prior.revision,
        writer_ep,
        body: Record::new(local, writer_ep, gen, step == RELEASE),
        vacancy_preference: None,
    })
}

impl Leases {
    async fn drive(
        &self,
        handler: &ProjectsHandler,
        role: &str,
        client: &Client,
        mut controls: mpsc::Receiver<Control>,
        notices: watch::Sender<View>,
    ) -> Result<(), HandlerError> {
        let mut installed = self.backend.subscribe();
        loop {
            let backend = installed.borrow_and_update().clone();
            if let Some(backend) = backend {
                tokio::select! {
                    result = self.run_candidate(role, client, backend.as_ref(), &mut controls, &notices) => return result,
                    _ = installed.changed() => { notices.send_replace(View::default()); }
                }
            } else {
                let changed = handler.role_cache_changed.notified();
                let now = self.now();
                let next = handler.local_role_view(role, now);
                let current = *notices.borrow();
                let due = current
                    .until_ms
                    .map_or(now, |until| until.saturating_sub(ACT_MS - RENEW_MS));
                if next.gen != current.gen
                    || next.until_ms.is_some() != current.until_ms.is_some()
                    || (next.until_ms.is_some()
                        && now >= due
                        && client.awaiting.lock().unwrap().is_none())
                {
                    notices.send_replace(next);
                }
                tokio::select! {
                    request = controls.recv() => {
                        if let Some(request) = request {
                            if request.yielding {
                                let _ = request.reply.send(Err(no_offer()));
                                continue;
                            }
                            let result = if next.gen == request.gen && next.until_ms.is_some() {
                                Ok(true)
                            } else {
                                Err(HandlerError::new("epoch_moved", "the local role lease generation no longer matches"))
                            };
                            let _ = request.reply.send(result);
                            return Ok(());
                        }
                    }
                    _ = installed.changed() => {}
                    _ = changed, if role == "connector_home" => {}
                    _ = tokio::time::sleep(Duration::from_millis(RENEW_MS)) => {}
                }
            }
        }
    }

    async fn run_candidate(
        &self,
        role: &str,
        client: &Client,
        backend: &dyn Backend,
        controls: &mut mpsc::Receiver<Control>,
        notices: &watch::Sender<View>,
    ) -> Result<(), HandlerError> {
        let (link, membership) = backend.link_events();
        let local = backend.local_box();
        let mut last_seen = None;
        let mut pending: Option<Change> = None;
        let mut wait_revision = 0;
        let mut renewal_due = 0;
        let mut retry_at = 0;
        let mut failed_since = None;
        let mut offered: Option<Preferred> = None;
        let mut expired_request: Option<(String, u64)> = None;
        let mut cleanup: Option<(String, u64)> = None;
        let mut signal_retry = 0;
        loop {
            // Register for readiness changes before reading the snapshot.
            let link_changed = link.notified();
            let members_changed = membership.notified();
            let now = self.now();
            let current = backend.role(role)?;
            let prior = *notices.borrow();
            let mut wake_at = None;
            let lapsed = prior.until_ms.is_some_and(|until| until <= now);
            if lapsed
                || (current.record.is_some() && !current.holder_current && prior.until_ms.is_some())
            {
                notices.send_replace(View {
                    gen: prior.gen,
                    ..View::default()
                });
            }
            if !current.ready {
                last_seen = None;
                if pending.as_ref().is_some_and(|op| op.step == ACQUIRE) {
                    pending = None;
                }
            } else {
                let preferred = backend.preferred(role)?;
                let slot_free = current
                    .record
                    .as_ref()
                    .is_none_or(|row| row.holder.is_none());
                let defer = preferred
                    .as_ref()
                    .filter(|p| slot_free && p.box_id != local);
                let identity = (current.watch, current.revision, defer.map(|p| p.revision));
                if last_seen.is_none_or(|(prior, _)| prior != identity) {
                    last_seen = Some((identity, now));
                    failed_since = None;
                }
                let mine = current.own(local);
                if !mine || current.gen() != prior.gen {
                    offered = None;
                }
                notices.send_if_modified(|view| {
                    if current.gen() == view.gen && (mine || view.until_ms.is_none()) {
                        return false;
                    }
                    *view = View {
                        gen: current.gen(),
                        ..View::default()
                    };
                    true
                });
                let count_done = last_seen.unwrap().1.saturating_add(LEASE_MS);
                let allowed =
                    failed_since.is_none_or(|first: u64| now.saturating_sub(first) < LINK_MS);
                if current.revision >= wait_revision
                    && allowed
                    && client.awaiting.lock().unwrap().is_none()
                    && *client.view.lock().unwrap() == *notices.borrow()
                {
                    let can_acquire = (slot_free && defer.is_none())
                        || (!mine && (slot_free || current.holder_current) && now >= count_done);
                    if pending.as_ref().is_some_and(|op| match op.step {
                        RENEW => !mine || current.gen() != op.body.gen,
                        ACQUIRE => !can_acquire || op.expected != current.revision,
                        _ => false,
                    }) {
                        pending = None;
                        expired_request = None;
                    }
                    let due = if pending.is_some() || can_acquire {
                        Some(retry_at)
                    } else if mine {
                        Some(renewal_due.max(retry_at))
                    } else if slot_free || current.holder_current {
                        Some(count_done.max(retry_at))
                    } else {
                        None
                    };
                    if due.is_some_and(|time| now >= time) {
                        if pending.is_none() {
                            let mut next =
                                change(if mine { RENEW } else { ACQUIRE }, &current, local)?;
                            if slot_free {
                                next.vacancy_preference = Some(defer.map_or(0, |p| p.revision));
                            }
                            pending = Some(next);
                            if let Some(previous) = preferred.as_ref().filter(|p| {
                                !mine
                                    && current.record.as_ref().is_some_and(|r| {
                                        r.holder.as_deref() == Some(p.box_id.as_str())
                                    })
                            }) {
                                let deadline = Instant::now() + Duration::from_secs(5);
                                expired_request = backend
                                    .read_handback_revision(role, &previous.box_id, deadline)
                                    .await
                                    .ok()
                                    .flatten()
                                    .map(|revision| (previous.box_id.clone(), revision));
                            }
                        }
                        let op = pending.as_ref().unwrap();
                        let (step, gen) = (op.step, op.body.gen);
                        match backend
                            .write(role, op, Instant::now() + Duration::from_secs(5))
                            .await
                        {
                            Ok(confirmed) => {
                                wait_revision = confirmed.revision;
                                failed_since = None;
                                retry_at = 0;
                                pending = None;
                                if step == RENEW {
                                    let read_back = backend.role(role)?;
                                    if read_back.ready
                                        && read_back.own(local)
                                        && read_back.gen() == gen
                                    {
                                        let sent =
                                            confirmed.sent_ms.filter(|sent| *sent <= self.now());
                                        renewal_due = sent.unwrap_or(now).saturating_add(RENEW_MS);
                                        if let Some(until) = sent
                                            .and_then(|sent| sent.checked_add(ACT_MS))
                                            .filter(|until| *until > self.now())
                                        {
                                            notices.send_replace(View {
                                                gen,
                                                until_ms: Some(until),
                                                handback: offered.is_some(),
                                            });
                                        }
                                    }
                                } else {
                                    // Acquisition alone never gives an acting window.
                                    renewal_due = 0;
                                    cleanup = expired_request.take();
                                }
                            }
                            Err(error) => {
                                retry_at = self.now().saturating_add(RETRY_MS);
                                if matches!(
                                    error.code.as_str(),
                                    "hub_unavailable" | "hub_write_unsettled"
                                ) {
                                    failed_since.get_or_insert(now);
                                } else {
                                    pending = None;
                                    expired_request = None;
                                }
                            }
                        }
                        wake_at = Some(retry_at);
                    } else {
                        wake_at = due;
                    }
                }
                // Write or clear this computer's hand-back request whether or not the
                // module has acknowledged its last notice: a waiting candidate may have
                // no acting window, so it has nothing to acknowledge yet.
                if current.revision >= wait_revision && allowed {
                    let own_request = backend.handback(role, local)?;
                    let wants_handback = !mine
                        && current.holder_current
                        && preferred.as_ref().is_some_and(|p| p.box_id == local);
                    let update_request = if wants_handback {
                        own_request
                            .as_ref()
                            .is_none_or(|request| request.gen != current.gen())
                    } else {
                        own_request.is_some()
                    };
                    if cleanup.is_some() || update_request {
                        if now >= signal_retry {
                            let deadline = Instant::now() + Duration::from_secs(5);
                            if let Some((box_id, revision)) = &cleanup {
                                match backend
                                    .delete_handback(role, box_id, *revision, deadline)
                                    .await
                                {
                                    Ok(()) => cleanup = None,
                                    Err(error) if error.code == "hub_write_superseded" => {
                                        cleanup = None
                                    }
                                    Err(_) => {}
                                }
                            } else if wants_handback {
                                let _ = backend.put_handback(role, current.gen(), deadline).await;
                            } else if let Some(request) = own_request {
                                let _ = backend
                                    .delete_handback(role, local, request.revision, deadline)
                                    .await;
                            }
                            signal_retry = self.now().saturating_add(RETRY_MS);
                        }
                        wake_at = Some(wake_at.map_or(signal_retry, |at| at.min(signal_retry)));
                    }
                }
                if mine
                    && offered.is_none()
                    && current.revision >= wait_revision
                    && client.awaiting.lock().unwrap().is_none()
                    && *client.view.lock().unwrap() == *notices.borrow()
                {
                    if let Some(target) = preferred.filter(|p| p.box_id != local) {
                        if backend
                            .handback(role, &target.box_id)?
                            .is_some_and(|request| {
                                request.gen == current.gen() && request.episode == target.episode
                            })
                        {
                            let announced = notices.send_if_modified(|view| {
                                if view.until_ms.is_some_and(|until| until > self.now()) {
                                    view.handback = true;
                                    true
                                } else {
                                    false
                                }
                            });
                            if announced {
                                offered = Some(target);
                            }
                        }
                    }
                }
            }
            if let Some(until) = prior.until_ms.filter(|_| !lapsed) {
                wake_at = Some(wake_at.map_or(until, |at| at.min(until)));
            }
            tokio::select! {
                biased;
                request = controls.recv() => {
                    if let Some(request) = request {
                        if request.yielding {
                            let result = match offered.as_ref() {
                                Some(target) => self.transfer(backend, role, request.gen, wait_revision, target).await,
                                None => Err(no_offer()),
                            };
                            let result = match result {
                                Ok(Some(revision)) => {
                                    offered = None;
                                    pending = None;
                                    wait_revision = revision;
                                    let stopped = View { gen: request.gen, ..View::default() };
                                    *client.view.lock().unwrap() = stopped;
                                    notices.send_replace(stopped);
                                    Ok(true)
                                }
                                Ok(None) => {
                                    offered = None;
                                    pending = None;
                                    renewal_due = 0;
                                    // Confirm a new renewal before extending the deadline.
                                    let mut view = *notices.borrow();
                                    view.handback = false;
                                    let observed = backend.role(role)?;
                                    if observed.ready && observed.own(local) && observed.gen() == request.gen {
                                        let renewal = change(RENEW, &observed, local)?;
                                        let deadline = Instant::now() + Duration::from_secs(5);
                                        if let Ok(confirmed) = backend.write(role, &renewal, deadline).await {
                                            wait_revision = confirmed.revision;
                                            if let Some(sent) = confirmed.sent_ms.filter(|sent| *sent <= self.now()) {
                                                renewal_due = sent.saturating_add(RENEW_MS);
                                                let until = sent.saturating_add(ACT_MS);
                                                view.until_ms = (until > self.now()).then_some(until);
                                            }
                                        }
                                    }
                                    notices.send_replace(view);
                                    Ok(false)
                                }
                                Err(error) => {
                                    if error.code == "hub_write_unsettled" {
                                        // A same-generation renewal must win the revision
                                        // before the module can safely resume acting.
                                        pending = None;
                                        renewal_due = 0;
                                        let stopped = View { gen: request.gen, ..View::default() };
                                        *client.view.lock().unwrap() = stopped;
                                        notices.send_replace(stopped);
                                    }
                                    Err(error)
                                }
                            };
                            let _ = request.reply.send(result);
                            continue;
                        }
                        let result = self.release(backend, role, request.gen, wait_revision, client).await.map(|()| true);
                        let _ = request.reply.send(result);
                        // A clean stop never turns back into a candidate, even
                        // if the hub could not confirm the release.
                        return Ok(());
                    }
                }
                _ = client.changed.notified() => {}
                _ = link_changed => {}
                _ = members_changed => {}
                _ = async {
                    if let Some(at) = wake_at {
                        tokio::time::sleep(Duration::from_millis(at.saturating_sub(self.now()))).await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {}
            }
        }
    }

    async fn transfer(
        &self,
        backend: &dyn Backend,
        role: &str,
        gen: i64,
        wait_revision: u64,
        target: &Preferred,
    ) -> Result<Option<u64>, HandlerError> {
        let current = backend.role(role)?;
        if !current.own(backend.local_box()) || current.gen() != gen {
            return Err(lease_gen_moved());
        }
        if !current.ready || current.revision < wait_revision {
            return Err(HandlerError::new(
                "hub_not_ready",
                "role watch is not current",
            ));
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        if !backend.reread_offer(role, target, gen, deadline).await? {
            return Ok(None);
        }
        let mut transfer = change(TRANSFER, &current, backend.local_box())?;
        transfer.body.holder = Some(target.box_id.clone());
        transfer.body.holder_ep = target.episode;
        backend
            .write(role, &transfer, deadline)
            .await
            .map(|landed| Some(landed.revision))
    }

    async fn release(
        &self,
        backend: &dyn Backend,
        role: &str,
        gen: i64,
        wait_revision: u64,
        client: &Client,
    ) -> Result<(), HandlerError> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let current = backend.role(role)?;
            if !current.ready {
                return Err(HandlerError::new(
                    "hub_not_ready",
                    "there is no live role watch on the hub",
                ));
            }
            if !current.own(backend.local_box()) || current.gen() != gen {
                return Err(HandlerError::new(
                    "epoch_moved",
                    "the local role lease generation no longer matches",
                ));
            }
            if current.revision >= wait_revision {
                let op = change(RELEASE, &current, backend.local_box())?;
                backend.write(role, &op, deadline).await?;
                return Ok(());
            }
            tokio::time::timeout_at(deadline, client.changed.notified())
                .await
                .map_err(|_| {
                    HandlerError::new("hub_not_ready", "role watch did not see the last renewal")
                })?;
        }
    }
}

fn decode<T: serde::de::DeserializeOwned>(params: Value) -> Result<T, HandlerError> {
    serde_json::from_value(params)
        .map_err(|error| HandlerError::new("invalid_request", error.to_string()))
}

impl ProjectsHandler {
    fn local_role_view(&self, role: &str, now: u64) -> View {
        if self.hub_backend_selected() || self.admit_store_write().is_err() {
            return View::default();
        }
        let local = |gen| View {
            gen,
            until_ms: Some(now.saturating_add(ACT_MS)),
            handback: false,
        };
        self.with_store(|store| Ok(store.clone()))
            .and_then(|store| {
                let state = store.identity_log_status()?.state;
                if state == "disabled"
                    || (role.starts_with("dreamer.")
                        && matches!(state.as_str(), "enabled" | "enabling" | "joining"))
                {
                    return Ok(local(1));
                }
                if role == "connector_home" && state == "enabled" {
                    let read = store.role_read(role)?;
                    return Ok(if read.held_here && !read.stale {
                        local(read.epoch)
                    } else {
                        View {
                            gen: read.epoch,
                            ..View::default()
                        }
                    });
                }
                Ok(View::default())
            })
            .unwrap_or_default()
    }

    fn admit_lease_role(&self, role: &str, key: RouteKey) -> Result<(), HandlerError> {
        if !valid_role(role) {
            return Err(HandlerError::new("invalid_request", "invalid role name"));
        }
        let admission = self.admission_for(key);
        if admission.scope.is_none()
            && admission.flow_id.is_none()
            && matches!(admission.principal, Some(Principal::Reserved { module_id }) if consumer(role) == Some(module_id.as_str()))
        {
            return Ok(());
        }
        Err(HandlerError::new(
            "caller_forbidden",
            "only the module assigned to this role may open or control its lease",
        ))
    }

    pub(super) fn admit_role_lease_watch(
        &self,
        params: Value,
        key: RouteKey,
    ) -> Result<String, HandlerError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Request {
            role: String,
        }
        let request: Request = decode(params)?;
        self.admit_lease_role(&request.role, key)?;
        Ok(request.role)
    }

    pub(super) fn ack_role_lease(
        &self,
        params: Value,
        key: RouteKey,
    ) -> Result<Vec<u8>, HandlerError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Request {
            role: String,
            seq: u64,
        }
        let request: Request = decode(params)?;
        self.admit_lease_role(&request.role, key)?;
        let client = self.role_leases.client(&request.role, key)?;
        let mut awaiting = client.awaiting.lock().unwrap();
        if awaiting
            .is_some_and(|(seq, deadline)| request.seq == seq && self.role_leases.now() < deadline)
        {
            *awaiting = None;
            client.acked.notify_one();
            client.changed.notify_one();
            return encode_result(json!({"acknowledged":true}));
        }
        Err(HandlerError::new(
            "lease_ack_stale",
            "no matching role lease notice is awaiting ack",
        ))
    }

    pub(super) async fn release_role_lease(
        &self,
        params: Value,
        key: RouteKey,
    ) -> Result<Vec<u8>, HandlerError> {
        self.control_role_lease(params, key, false).await
    }

    pub(super) async fn yield_role_lease(
        &self,
        params: Value,
        key: RouteKey,
    ) -> Result<Vec<u8>, HandlerError> {
        self.control_role_lease(params, key, true).await
    }

    async fn control_role_lease(
        &self,
        params: Value,
        key: RouteKey,
        yielding: bool,
    ) -> Result<Vec<u8>, HandlerError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Request {
            role: String,
            gen: i64,
        }
        let request: Request = decode(params)?;
        self.admit_lease_role(&request.role, key)?;
        let client = self.role_leases.client(&request.role, key)?;
        let view = *client.view.lock().unwrap();
        if view.gen != request.gen || view.until_ms.is_none() {
            return Err(HandlerError::new(
                "epoch_moved",
                "this watch has not held the specified role generation",
            ));
        }
        if yielding
            && (!view.handback
                || !view
                    .until_ms
                    .is_some_and(|until| self.role_leases.now() < until))
        {
            return Err(no_offer());
        }
        let (reply, result) = oneshot::channel();
        let closed = || {
            HandlerError::new(
                "stream_closed",
                "the role lease watch ended before the control request confirmed",
            )
        };
        client
            .control
            .try_send(Control {
                gen: request.gen,
                yielding,
                reply,
            })
            .map_err(|error| {
                if matches!(error, mpsc::error::TrySendError::Full(_)) {
                    HandlerError::new(
                        if yielding {
                            "lease_yield_pending"
                        } else {
                            "lease_release_pending"
                        },
                        "a lease control request is already pending on this watch",
                    )
                } else {
                    closed()
                }
            })?;
        let done = result.await.map_err(|_| closed())??;
        encode_result(if yielding {
            json!({"yielded":done})
        } else {
            json!({"released":done})
        })
    }

    pub(super) async fn serve_role_lease(
        &self,
        transport: &impl WatchTransport,
        role: String,
        key: RouteKey,
    ) -> Result<(), HandlerError> {
        let (release, releases) = mpsc::channel(1);
        let (client, registration) = self.role_leases.register(role.clone(), key, release)?;
        let initial = if self.role_leases.backend.borrow().is_some() {
            View::default()
        } else {
            self.local_role_view(&role, self.role_leases.now())
        };
        let (source, views) = watch::channel(initial);
        #[cfg(test)]
        if let Some(sample) = self.role_leases.sample_views.get(&role) {
            return self
                .role_leases
                .notices(transport, &client, registration, sample.clone())
                .await;
        }
        tokio::select! {
            biased;
            result = self.role_leases.notices(transport, &client, registration, views) => result,
            result = self.role_leases.drive(self, &role, &client, releases, source) => result,
        }
    }
}
