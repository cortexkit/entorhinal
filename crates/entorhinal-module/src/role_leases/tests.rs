use super::*;
use std::sync::atomic::AtomicBool;
use tokio::{sync::Barrier, task::JoinHandle};

const ROLE: &str = "connector_home";
const A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const C: &str = "cccccccccccccccccccccccccccccccc";
const ROUTE: RouteKey = (3, 1);

pub(crate) struct Hub {
    row: Mutex<(u64, Option<Record>)>,
    preferred: Mutex<Option<Preferred>>,
    requests: Mutex<HashMap<String, CandidateRequest>>,
    removed: Mutex<Vec<&'static str>>,
    signal_revision: AtomicU64,
    attempts: Mutex<Vec<(&'static str, u64)>>,
    landed: Mutex<Vec<(&'static str, Record)>>,
    updates: Notify,
    race: Option<Barrier>,
    clock: Instant,
    delay: AtomicU64,
    missing_send: AtomicBool,
    reachable: AtomicBool,
}

impl Hub {
    fn new(holder: Option<&str>, race: bool) -> Arc<Self> {
        Arc::new(Self {
            row: Mutex::new((
                u64::from(holder.is_some()),
                holder.map(|box_id| Record::new(box_id, 0, 1, false)),
            )),
            attempts: Mutex::default(),
            preferred: Mutex::default(),
            requests: Mutex::default(),
            removed: Mutex::default(),
            signal_revision: AtomicU64::new(1),
            landed: Mutex::default(),
            updates: Notify::new(),
            race: race.then(|| Barrier::new(2)),
            clock: Instant::now(),
            delay: AtomicU64::new(0),
            missing_send: AtomicBool::new(false),
            reachable: AtomicBool::new(true),
        })
    }

    fn now(&self) -> u64 {
        100_000 + self.clock.elapsed().as_millis() as u64
    }

    fn writes(&self, step: &str) -> Vec<Record> {
        self.landed
            .lock()
            .unwrap()
            .iter()
            .filter(|(kind, _)| *kind == step)
            .map(|(_, row)| row.clone())
            .collect()
    }

    fn prefer(&self, box_id: &str) {
        *self.preferred.lock().unwrap() = Some(Preferred {
            revision: self.signal_revision.fetch_add(1, Ordering::Relaxed),
            box_id: box_id.into(),
            episode: 0,
        });
        self.updates.notify_waiters();
    }

    fn current(&self, box_id: &str) -> bool {
        !box_id.starts_with("engram:") && !self.removed.lock().unwrap().contains(&box_id)
    }

    pub(crate) fn request_sample_handback(&self) {
        self.prefer(B);
        self.requests.lock().unwrap().insert(
            B.into(),
            CandidateRequest {
                revision: 7,
                gen: 1,
                episode: 0,
            },
        );
        self.updates.notify_waiters();
    }

    pub(crate) fn withdraw_sample_handback(&self) {
        self.requests.lock().unwrap().remove(B);
        self.updates.notify_waiters();
    }
}

pub(crate) fn sample_holder(leases: &mut Leases) -> Arc<Hub> {
    let hub = Hub::new(Some(A), false);
    let clock = hub.clone();
    leases.sample_clock(move || clock.now());
    leases.attach(Some(Arc::new(Peer::new(A, &hub, true))));
    hub
}

struct Peer {
    local: &'static str,
    hub: Arc<Hub>,
    watching: AtomicBool,
    link_incarnation: AtomicU64,
    link: Notify,
}

impl Peer {
    fn new(local: &'static str, hub: &Arc<Hub>, live: bool) -> Self {
        Self {
            local,
            hub: hub.clone(),
            watching: AtomicBool::new(live),
            link_incarnation: AtomicU64::default(),
            link: Notify::new(),
        }
    }

    fn link(&self, live: bool) {
        self.watching.store(live, Ordering::Relaxed);
        self.link_incarnation.fetch_add(1, Ordering::Relaxed);
        self.link.notify_waiters();
    }
}

#[async_trait::async_trait]
impl Backend for Peer {
    fn local_box(&self) -> &str {
        self.local
    }

    fn role(&self, _: &str) -> Result<Observation, HandlerError> {
        let (revision, record) = self.hub.row.lock().unwrap().clone();
        Ok(Observation {
            revision,
            holder_current: record
                .as_ref()
                .and_then(|r| r.holder.as_deref())
                .is_some_and(|holder| self.hub.current(holder)),
            record,
            ready: self.watching.load(Ordering::Relaxed),
            local_episode: Some(0),
            watch: (self.link_incarnation.load(Ordering::Relaxed), 0),
        })
    }

    fn link_events(&self) -> (&Notify, &Notify) {
        (&self.link, &self.hub.updates)
    }

    fn preferred(&self, _: &str) -> Result<Option<Preferred>, HandlerError> {
        Ok(self
            .hub
            .preferred
            .lock()
            .unwrap()
            .clone()
            .filter(|p| self.hub.current(&p.box_id)))
    }

    fn handback(&self, _: &str, box_id: &str) -> Result<Option<CandidateRequest>, HandlerError> {
        Ok(self.hub.requests.lock().unwrap().get(box_id).cloned())
    }

    async fn reread_offer(
        &self,
        role: &str,
        target: &Preferred,
        gen: i64,
        _: Instant,
    ) -> Result<bool, HandlerError> {
        Ok(self
            .preferred(role)?
            .is_some_and(|p| p.box_id == target.box_id && p.episode == target.episode)
            && self
                .handback(role, &target.box_id)?
                .is_some_and(|r| r.gen == gen))
    }

    async fn read_handback_revision(
        &self,
        role: &str,
        box_id: &str,
        _: Instant,
    ) -> Result<Option<u64>, HandlerError> {
        Ok(self.handback(role, box_id)?.map(|request| request.revision))
    }

    async fn put_handback(&self, _: &str, gen: i64, _: Instant) -> Result<(), HandlerError> {
        self.hub.requests.lock().unwrap().insert(
            self.local.into(),
            CandidateRequest {
                revision: self.hub.signal_revision.fetch_add(1, Ordering::Relaxed),
                gen,
                episode: 0,
            },
        );
        self.hub.updates.notify_waiters();
        Ok(())
    }

    async fn delete_handback(
        &self,
        _: &str,
        box_id: &str,
        revision: u64,
        _: Instant,
    ) -> Result<(), HandlerError> {
        let mut requests = self.hub.requests.lock().unwrap();
        if requests
            .get(box_id)
            .is_some_and(|request| request.revision == revision)
        {
            requests.remove(box_id);
            self.hub.updates.notify_waiters();
        }
        Ok(())
    }

    async fn write(&self, _: &str, change: &Change, _: Instant) -> Result<Confirmed, HandlerError> {
        let sent = self.hub.now();
        self.hub.attempts.lock().unwrap().push((change.step, sent));
        if !self.hub.reachable.load(Ordering::Relaxed) {
            return Err(HandlerError::new(
                "hub_unavailable",
                "the simulated writer fence is unreachable",
            ));
        }
        if let Some(race) = self.hub.race.as_ref().filter(|_| change.step == ACQUIRE) {
            race.wait().await;
        }
        let revision = {
            let mut row = self.hub.row.lock().unwrap();
            if row.0 != change.expected {
                return Err(HandlerError::new(
                    "hub_write_superseded",
                    "the revision moved",
                ));
            }
            row.0 += 1;
            row.1 = Some(change.body.clone());
            self.hub
                .landed
                .lock()
                .unwrap()
                .push((change.step, change.body.clone()));
            row.0
        };
        self.hub.updates.notify_waiters();
        let delay = self.hub.delay.swap(0, Ordering::Relaxed);
        if delay != 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        Ok(Confirmed {
            revision,
            sent_ms: (!self.hub.missing_send.load(Ordering::Relaxed)).then_some(sent),
        })
    }
}

struct Sink(mpsc::UnboundedSender<Notice>);

#[async_trait::async_trait]
impl WatchTransport for Sink {
    async fn emit(&self, body: Vec<u8>) -> Result<(), HandlerError> {
        self.0.send(serde_json::from_slice(&body).unwrap()).unwrap();
        Ok(())
    }

    async fn ended(&self) {
        std::future::pending().await
    }
}

struct ClientWatch {
    handler: Arc<ProjectsHandler>,
    peer: Arc<Peer>,
    notices: mpsc::UnboundedReceiver<Notice>,
    task: JoinHandle<()>,
}

async fn pump() {
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
}

async fn tick(ms: u64) {
    tokio::time::advance(Duration::from_millis(ms)).await;
    pump().await;
}

impl ClientWatch {
    async fn open(local: &'static str, hub: &Arc<Hub>, live: bool) -> Self {
        let peer = Arc::new(Peer::new(local, hub, live));
        let mut handler = ProjectsHandler::with_runtime("role-test".into(), || 0);
        handler.route_admissions().insert(
            ROUTE,
            crate::RouteAdmission::from_bind(Some(crate::tests::reserved("plexus")), None),
        );
        let leases = Arc::get_mut(&mut handler.role_leases).unwrap();
        let clock = hub.clone();
        leases.sample_clock(move || clock.now());
        leases.attach(Some(peer.clone()));
        let handler = Arc::new(handler);
        let server = handler.clone();
        let (tx, notices) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            server
                .serve_role_lease(&Sink(tx), ROLE.into(), ROUTE)
                .await
                .unwrap();
        });
        pump().await;
        Self {
            handler,
            peer,
            notices,
            task,
        }
    }

    fn next(&mut self) -> Notice {
        self.notices.try_recv().expect("role lease notice")
    }

    fn ack(&self, notice: &Notice) {
        self.handler
            .ack_role_lease(json!({"role":ROLE, "seq":notice.seq}), ROUTE)
            .unwrap();
    }

    async fn ack_waiting(&mut self) -> Vec<Notice> {
        let mut all = Vec::new();
        pump().await;
        while let Ok(notice) = self.notices.try_recv() {
            self.ack(&notice);
            all.push(notice);
            pump().await;
        }
        all
    }
}

impl Drop for ClientWatch {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test(start_paused = true)]
async fn lease_renews_same_generation_every_twenty_seconds() {
    let hub = Hub::new(None, false);
    let mut a = ClientWatch::open(A, &hub, true).await;
    assert_eq!(hub.row.lock().unwrap().0, 0);
    let notices = a.ack_waiting().await;
    assert!(!notices[0].holding);
    assert_eq!(hub.writes(ACQUIRE).len(), 1);
    assert_eq!(hub.writes(RENEW).len(), 1);
    assert!(notices.last().unwrap().holding);
    assert_eq!(a.handler.role_leases.held(), 1);
    tick(19_999).await;
    assert_eq!(hub.writes(RENEW).len(), 1);
    tick(2).await;
    let renewals = hub.writes(RENEW);
    assert_eq!(renewals.len(), 2);
    assert_eq!(renewals[0], renewals[1]);
    let notice = a.next();
    assert!(notice.holding);
    a.ack(&notice);
    for count in 3..=5 {
        tick(20_001).await;
        let notice = a.next();
        assert!(notice.holding);
        a.ack(&notice);
        assert_eq!(hub.writes(RENEW), vec![renewals[0].clone(); count]);
    }
    tick(9_996).await;
    assert_eq!(hub.writes(RENEW).len(), 5);
    assert_eq!(hub.writes(ACQUIRE).len(), 1);
}

#[tokio::test(start_paused = true)]
async fn lease_late_confirmation_cannot_extend_send_time_window() {
    let hub = Hub::new(Some(A), false);
    hub.delay.store(25_000, Ordering::Relaxed);
    let mut a = ClientWatch::open(A, &hub, true).await;
    assert!(a.ack_waiting().await.iter().all(|n| !n.holding));
    tick(25_001).await;
    let notice = a.next();
    assert!(notice.holding);
    assert_eq!(notice.issued_at_ms + notice.valid_for_ms, 145_000);
    a.peer.link(false);
    a.ack(&notice);
    tick(20_001).await;
    assert_eq!(a.handler.role_leases.held(), 0);
    assert!(!a.next().holding);
}

#[tokio::test(start_paused = true)]
async fn lease_landed_renewal_at_or_beyond_45_seconds_grants_nothing() {
    let hub = Hub::new(Some(A), false);
    hub.delay.store(45_000, Ordering::Relaxed);
    let mut a = ClientWatch::open(A, &hub, true).await;
    a.ack_waiting().await;
    tick(45_001).await;
    tick(2).await;
    // The late confirmation never becomes an acting notice; a newer renewal
    // may grant a fresh window, but only at its own send time.
    let notice = a.next();
    assert!(notice.holding);
    assert!(notice.issued_at_ms + notice.valid_for_ms >= 190_000);
    assert_eq!(hub.writes(RENEW).len(), 2);
}

#[test]
fn lease_uses_the_landed_op_id_not_its_reseal_or_an_earlier_process() {
    use entorhinal_core::hub_store::HubAttemptSend;
    let send = |op_id: &str, process: &str, sent_at_ms| HubAttemptSend {
        op_id: op_id.into(),
        process: process.into(),
        sent_at_ms,
    };
    let sends = [
        send("original", "current", 100_000),
        send("resealed", "current", 144_000),
    ];
    assert_eq!(
        crate::hub_write::matching_send_ms(&sends, "original", "current"),
        Some(100_000)
    );
    assert_eq!(
        crate::hub_write::matching_send_ms(&sends, "resealed", "current"),
        Some(144_000)
    );
    assert_eq!(
        crate::hub_write::matching_send_ms(&sends, "original", "older"),
        None
    );
}

#[test]
fn lease_readers_restrict_equal_generation_and_first_record_seed() {
    let record = Record::new(A, 1, 7, false);
    let mut header = crate::fleet_record::Header {
        kind: "role".into(),
        key: "role.connector_home".into(),
        writer: A.into(),
        writer_ep: 1,
        entry_id: "e".repeat(32),
    };
    let member = |_: &str, episode| episode == 1;
    assert!(accepts(
        &record,
        Some(&record),
        &header,
        true,
        false,
        None,
        member
    ));
    assert!(accepts(
        &Record::new(A, 1, 7, true),
        Some(&record),
        &header,
        true,
        false,
        None,
        member
    ));
    header.writer = B.into();
    assert!(!accepts(
        &Record::new(B, 1, 7, false),
        Some(&record),
        &header,
        true,
        false,
        None,
        member
    ));
    assert!(accepts(
        &Record::new(B, 1, 8, false),
        Some(&record),
        &header,
        true,
        false,
        None,
        member
    ));
    assert!(!accepts(
        &Record::new(B, 1, 8, false),
        Some(&record),
        &header,
        false,
        false,
        None,
        member
    ));
    let seeded = Record {
        seeded: true,
        ..Record::new(B, 0, 8, false)
    };
    let seed = || Some((Some(B.into()), 8));
    assert!(accepts(&seeded, None, &header, true, false, seed(), member));
    assert!(!accepts(
        &seeded,
        Some(&record),
        &header,
        true,
        false,
        seed(),
        member
    ));
    assert!(!accepts(&seeded, None, &header, true, false, None, member));
}

#[tokio::test(start_paused = true)]
async fn lease_takeover_requires_sixty_seconds_without_a_new_revision() {
    let hub = Hub::new(Some(A), false);
    let mut b = ClientWatch::open(B, &hub, true).await;
    b.ack_waiting().await;
    tick(59_999).await;
    assert!(hub.writes(ACQUIRE).is_empty());
    tick(2).await;
    assert_eq!(hub.writes(ACQUIRE)[0].gen, 2);
    assert!(b.ack_waiting().await.last().unwrap().holding);
}

#[tokio::test(start_paused = true)]
async fn lease_racing_takers_have_one_cas_winner() {
    let hub = Hub::new(Some(A), true);
    let mut b = ClientWatch::open(B, &hub, true).await;
    let mut c = ClientWatch::open(C, &hub, true).await;
    b.ack_waiting().await;
    c.ack_waiting().await;
    tick(60_001).await;
    assert_eq!(
        hub.attempts
            .lock()
            .unwrap()
            .iter()
            .filter(|(kind, _)| *kind == ACQUIRE)
            .count(),
        2
    );
    assert_eq!(hub.writes(ACQUIRE).len(), 1);
    assert_eq!(hub.writes(ACQUIRE)[0].gen, 2);
    b.ack_waiting().await;
    c.ack_waiting().await;
    assert_eq!(
        b.handler.role_leases.held() + c.handler.role_leases.held(),
        1
    );
}

#[tokio::test(start_paused = true)]
async fn lease_watcher_restart_and_reconnect_discard_elapsed_time() {
    let hub = Hub::new(Some(A), false);
    let mut b = ClientWatch::open(B, &hub, true).await;
    b.ack_waiting().await;
    tick(59_000).await;
    drop(b);
    let mut b = ClientWatch::open(B, &hub, true).await;
    b.ack_waiting().await;
    tick(2_000).await;
    assert!(hub.writes(ACQUIRE).is_empty());
    tick(57_000).await;
    b.peer.link(false);
    tick(2_000).await;
    assert!(hub.writes(ACQUIRE).is_empty());
    b.peer.link(true);
    pump().await;
    tick(59_999).await;
    assert!(hub.writes(ACQUIRE).is_empty());
    tick(2).await;
    assert_eq!(hub.writes(ACQUIRE).len(), 1);
}

#[tokio::test(start_paused = true)]
async fn lease_explicit_release_is_immediate_but_a_missing_ack_or_route_does_not_release() {
    for action in ["release", "missed_ack", "route_drop"] {
        let hub = Hub::new(Some(A), false);
        let mut a = ClientWatch::open(A, &hub, true).await;
        assert!(a.ack_waiting().await.last().unwrap().holding);
        let mut b = ClientWatch::open(B, &hub, true).await;
        b.ack_waiting().await;
        if action == "release" {
            a.handler
                .release_role_lease(json!({"role":ROLE,"gen":1}), ROUTE)
                .await
                .unwrap();
            let releases = hub.writes(RELEASE);
            assert_eq!(releases.len(), 1);
            assert_eq!(releases[0].gen, 1);
            assert_eq!(releases[0].holder, None);
            b.ack_waiting().await;
            assert_eq!(hub.writes(ACQUIRE)[0].gen, 2);
        } else {
            if action == "missed_ack" {
                tick(20_001).await;
                assert!(a.next().holding);
                tick(5_001).await;
                assert!(a.task.is_finished());
            } else {
                a.task.abort();
                pump().await;
            }
            tick(54_990).await;
            assert!(hub.writes(ACQUIRE).is_empty());
            assert!(hub.writes(RELEASE).is_empty());
            tick(5_012).await;
            assert_eq!(hub.writes(ACQUIRE)[0].gen, 2);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn lease_unreachable_hub_never_grants_and_stops_retrying_after_25_seconds() {
    let hub = Hub::new(Some(A), false);
    hub.reachable.store(false, Ordering::Relaxed);
    let mut a = ClientWatch::open(A, &hub, true).await;
    assert!(a.ack_waiting().await.iter().all(|n| !n.holding));
    for _ in 0..27 {
        tick(1_001).await;
    }
    let attempts = hub.attempts.lock().unwrap().clone();
    assert!(attempts.len() > 1);
    assert!(attempts.last().unwrap().1 - attempts[0].1 < 25_000);
    assert_eq!(a.handler.role_leases.held(), 0);
    assert!(hub.writes(RENEW).is_empty());
    a.peer.link(false);
    tick(1_001).await;
    hub.reachable.store(true, Ordering::Relaxed);
    a.peer.link(true);
    assert!(a.ack_waiting().await.last().unwrap().holding);
}

async fn yield_from(client: &ClientWatch, gen: i64) -> Value {
    let response = client
        .handler
        .handle_active_request(
            &serde_json::to_vec(&json!({"method":YIELD,"params":{"role":ROLE,"gen":gen}})).unwrap(),
            ROUTE,
        )
        .await;
    let crate::HandlerOutcome::Response(response) = response else {
        panic!("yield failed: {response:?}");
    };
    serde_json::from_slice::<Value>(&response).unwrap()["result"].clone()
}

#[tokio::test(start_paused = true)]
async fn lease_handback_waits_for_safe_point_and_transfers_directly() {
    let hub = Hub::new(Some(A), false);
    let mut a = ClientWatch::open(A, &hub, true).await;
    assert!(a.ack_waiting().await.iter().any(|notice| notice.holding));
    hub.prefer(B);
    let mut b = ClientWatch::open(B, &hub, true).await;
    let mut c = ClientWatch::open(C, &hub, true).await;
    b.ack_waiting().await;
    c.ack_waiting().await;
    assert!(a
        .ack_waiting()
        .await
        .iter()
        .any(|notice| notice.handback == Some(true)));

    // The owner is still in an irreversible act and hasn't called yield.
    for _ in 0..4 {
        tick(RENEW_MS).await;
        a.ack_waiting().await;
        assert!(hub.writes(TRANSFER).is_empty());
        assert_eq!(
            hub.row
                .lock()
                .unwrap()
                .1
                .as_ref()
                .unwrap()
                .holder
                .as_deref(),
            Some(A)
        );
    }
    assert!(hub.writes(RENEW).len() >= 4);
    assert_eq!(yield_from(&a, 1).await, json!({"yielded":true}));
    assert!(hub.writes(RELEASE).is_empty());
    assert!(hub.writes(ACQUIRE).is_empty());
    let transfers = hub.writes(TRANSFER);
    assert_eq!(transfers.len(), 1);
    assert_eq!(
        (transfers[0].gen, transfers[0].holder.as_deref()),
        (2, Some(B))
    );
    assert!(b
        .ack_waiting()
        .await
        .iter()
        .any(|notice| notice.holding && notice.gen == 2));
    assert!(hub.requests.lock().unwrap().get(B).is_none());
    assert!(c.ack_waiting().await.iter().all(|notice| !notice.holding));
}

#[tokio::test(start_paused = true)]
async fn lease_handback_withdrawal_keeps_and_renews_holder() {
    let hub = Hub::new(Some(A), false);
    let mut a = ClientWatch::open(A, &hub, true).await;
    a.ack_waiting().await;
    hub.prefer(B);
    let mut b = ClientWatch::open(B, &hub, true).await;
    b.ack_waiting().await;
    let offered = a
        .ack_waiting()
        .await
        .into_iter()
        .find(|notice| notice.handback == Some(true))
        .unwrap();
    tick(1_000).await;
    drop(b);
    pump().await;
    assert!(hub.requests.lock().unwrap().get(B).is_none());
    assert_eq!(yield_from(&a, 1).await, json!({"yielded":false}));
    let cancelled = a
        .ack_waiting()
        .await
        .into_iter()
        .find(|notice| notice.handback == Some(false))
        .unwrap();
    assert!(cancelled.holding);
    assert!(
        cancelled.issued_at_ms + cancelled.valid_for_ms
            > offered.issued_at_ms + offered.valid_for_ms
    );
    assert!(hub.writes(TRANSFER).is_empty());
    assert_eq!(
        hub.row
            .lock()
            .unwrap()
            .1
            .as_ref()
            .unwrap()
            .holder
            .as_deref(),
        Some(A)
    );

    let mut reopened = ClientWatch::open(B, &hub, true).await;
    reopened.ack_waiting().await;
    assert!(hub.requests.lock().unwrap().get(B).is_some());
    assert!(a
        .ack_waiting()
        .await
        .iter()
        .any(|notice| notice.handback == Some(true)));
}

#[tokio::test(start_paused = true)]
async fn lease_handback_timeout_taker_clears_preferred_holders_request() {
    let hub = Hub::new(Some(A), false);
    hub.prefer(A);
    // A vanished before removing its old request after becoming the holder.
    hub.requests.lock().unwrap().insert(
        A.into(),
        CandidateRequest {
            revision: 7,
            episode: 0,
            gen: 1,
        },
    );
    let mut b = ClientWatch::open(B, &hub, true).await;
    b.ack_waiting().await;
    tick(LEASE_MS - 1).await;
    assert!(hub.writes(ACQUIRE).is_empty());
    tick(1).await;
    b.ack_waiting().await;
    assert_eq!(hub.writes(ACQUIRE)[0].holder.as_deref(), Some(B));
    assert!(hub.requests.lock().unwrap().get(A).is_none());
}

#[tokio::test(start_paused = true)]
async fn lease_handback_vacancy_prefers_the_named_candidate() {
    let hub = Hub::new(None, false);
    hub.prefer(B);
    let mut a = ClientWatch::open(A, &hub, true).await;
    a.ack_waiting().await;
    assert!(hub.writes(ACQUIRE).is_empty());
    let mut b = ClientWatch::open(B, &hub, true).await;
    assert!(b
        .ack_waiting()
        .await
        .iter()
        .any(|notice| notice.gen == 1 && notice.holding));
    assert_eq!(hub.writes(ACQUIRE)[0].holder.as_deref(), Some(B));
    assert_eq!(hub.writes(ACQUIRE).len(), 1);
}

#[tokio::test(start_paused = true)]
async fn lease_handback_vacancy_waits_a_fresh_sixty_seconds_for_missing_preferred_candidate() {
    let hub = Hub::new(None, false);
    hub.prefer(B);
    let mut a = ClientWatch::open(A, &hub, true).await;
    a.ack_waiting().await;
    tick(LEASE_MS - 1).await;
    assert!(hub.writes(ACQUIRE).is_empty());
    a.peer.link(false);
    pump().await;
    a.peer.link(true);
    pump().await;
    tick(LEASE_MS - 1).await;
    assert!(hub.writes(ACQUIRE).is_empty());
    tick(1).await;
    assert_eq!(hub.writes(ACQUIRE)[0].holder.as_deref(), Some(A));
}

#[tokio::test(start_paused = true)]
async fn lease_handback_vacancy_ignores_removed_preferred_box() {
    let hub = Hub::new(None, false);
    hub.prefer(B);
    hub.removed.lock().unwrap().push(B);
    let mut a = ClientWatch::open(A, &hub, true).await;
    assert!(a.ack_waiting().await.iter().any(|notice| notice.holding));
    assert_eq!(hub.writes(ACQUIRE)[0].holder.as_deref(), Some(A));
}

#[test]
fn role_gen_high_water_carries_hub_observation_across_restart_and_local_takes() {
    let (dir, descriptor) = crate::tests::scratch_descriptor("role-gen-high-water");
    let handler = ProjectsHandler::with_log_connector(
        "role-gen".into(),
        || 1,
        Arc::new(crate::fake_log::FakeLog::default()),
    );
    let store = entorhinal_core::RegistryStore::open(&descriptor).unwrap();
    *handler.store.lock().unwrap() = Some(store.clone());
    let mut grant = None;
    let fresh = handler.local_role_view(ROLE, 1, &mut grant);
    assert_eq!(fresh.gen, 1);
    let renewed = handler.local_role_view(ROLE, 10_000, &mut grant);
    assert_eq!(renewed.gen, 1);
    assert!(renewed.until_ms > fresh.until_ms);
    for _ in 0..2 {
        handler.local_role_view(ROLE, 10_000, &mut None);
    }
    let observed = Observation {
        ready: true,
        watch: (0, 0),
        revision: 1,
        record: Some(Record::new(A, 0, 2, false)),
        local_episode: Some(0),
        holder_current: true,
    };
    let take = change(
        ACQUIRE,
        &observed,
        B,
        store.role_gen_high_water(ROLE).unwrap(),
    )
    .unwrap();
    assert_eq!(take.body.gen, 4);

    let hub_body = serde_json::json!({"gen": 12});
    store
        .record_hub_kv(
            "role.connector_home",
            1,
            Some((b"verified", Some(&hub_body))),
            99,
        )
        .unwrap();
    assert_eq!(store.role_gen_high_water(ROLE).unwrap(), 12);
    handler.store.lock().unwrap().take();
    drop(store);
    let restarted = entorhinal_core::RegistryStore::open(&descriptor).unwrap();
    *handler.store.lock().unwrap() = Some(restarted.clone());
    assert_eq!(handler.local_role_view(ROLE, 20_000, &mut None).gen, 13);
    assert_eq!(restarted.role_gen_high_water("dreamer.global").unwrap(), 0);
    handler.store.lock().unwrap().take();
    drop(restarted);
    let _ = std::fs::remove_dir_all(dir);
}
