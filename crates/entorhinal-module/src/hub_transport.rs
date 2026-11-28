//! The provisioned hub's byte transport. It does not apply or queue writes.
//! The backend owns verification, settlement, and the durable read cursors.

mod feed;

use std::{
    error::Error as _,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use async_nats::{
    connection::State,
    header::{
        NATS_EXPECTED_LAST_SEQUENCE, NATS_EXPECTED_LAST_SUBJECT_SEQUENCE, NATS_MARKER_REASON,
        NATS_MESSAGE_ID,
    },
    jetstream::{
        self,
        consumer::{push, DeliverPolicy},
        context::PublishErrorKind,
        kv,
        stream::{DirectGetErrorKind, LastRawMessageErrorKind, RawMessageErrorKind},
    },
    HeaderMap, HeaderValue,
};
use cortexkit_bus_client::{BucketItem, BusClient, BusClientOptions, Error as BusError, HubClient};
use cortexkit_bus_naming::federation::{BoxId, RESIDENCE_STREAM};
use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio::{
    sync::{watch, Mutex, Notify},
    time::{timeout_at, Instant},
};

use self::feed::Subscription;
pub(super) use self::feed::{Feed, FeedEvent};

#[cfg(test)]
pub(super) fn test_tail_feed<S>(
    health: Arc<HealthGauges>,
    applied: u64,
    mut open: impl FnMut(u64) -> S + Send + 'static,
) -> Feed<LogDelivery>
where
    S: futures_util::Stream<Item = Option<LogDelivery>> + Send + 'static,
{
    Feed::log(health, applied, move |cursor| {
        let stream = open(cursor.unwrap());
        async move {
            Ok(Subscription::new(
                None,
                stream.map(|delivery| {
                    delivery
                        .map(|delivery| (delivery.sequence, delivery))
                        .ok_or_else(|| Failure::code("log_delivery:MissingHeartbeat"))
                }),
            ))
        }
    })
    .with_durable_cursor()
}
use crate::{log_client::SubcConnector, HandlerError, HealthGauges};

const DOMAIN: &str = "hub";
const LOG_STREAM: &str = "HUB_IDLOG";
pub(super) const LOG_SUBJECT: &str = "ck.fleet.idlog";
const BUCKET: &str = "residence";
pub(super) const BUCKET_RESET: &str = "hub_bucket_reset";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_KV_VALUE: i32 = 16 * 1024;
const MAX_KV_BYTES: i64 = 64 * 1024 * 1024;

fn bucket_reset_error() -> HandlerError {
    HandlerError::new(
        BUCKET_RESET,
        "hub bucket replaced; operator re-seed required",
    )
}

pub(super) enum KvDelivery {
    CaughtUp,
    Entry(WatchedEntry),
}

pub(super) struct WatchedEntry {
    pub key: String,
    pub revision: u64,
    pub value: Vec<u8>,
    pub operation: kv::Operation,
}

pub(super) struct KvEntry {
    pub revision: u64,
    // None is a delete or purge marker, not a never-written key.
    pub value: Option<Vec<u8>>,
}

pub(super) fn check_kv_size(key: &str, len: usize) -> Result<(), HandlerError> {
    let fits = if key.starts_with("wrapjoin.") {
        len <= 12 * 1024
    } else {
        len < 4 * 1024
    };
    if !fits {
        return Err(HandlerError::new(
            "record_too_large",
            "fleet KV value exceeds its write limit",
        ));
    }
    Ok(())
}

#[derive(Default)]
pub(super) struct Gauges {
    active: AtomicBool,
    connected: AtomicBool,
    log_tail_live: AtomicBool,
    kv_watch_live: AtomicBool,
    failures: AtomicU64,
    bucket_reset: AtomicBool,
    reset_notified: Notify,
    pub(super) kv_records_refused: AtomicU64,
    pub(super) epoch_held: AtomicU64,
    writer_hold: std::sync::RwLock<Option<String>>,
}

impl Gauges {
    pub(super) fn report(&self, metrics: &mut Value) {
        if !self.active.load(Ordering::Relaxed) {
            return;
        }
        metrics["hubConnected"] = json!(self.connected.load(Ordering::Relaxed));
        metrics["hubLogTailLive"] = json!(self.log_tail_live.load(Ordering::Relaxed));
        let reset = self.bucket_was_reset();
        metrics["hubKvWatchLive"] = json!(!reset && self.kv_watch_live.load(Ordering::Relaxed));
        metrics["hubFailures"] = json!(self.failures.load(Ordering::Relaxed));
        metrics["hubBucketResets"] = json!(u64::from(reset));
        if reset {
            metrics["hubResetBucket"] = json!(BUCKET);
        }
        metrics["kvRecordsRefused"] = json!(self.kv_records_refused.load(Ordering::Relaxed));
        let epoch = self.epoch_held.load(Ordering::Relaxed);
        metrics["epochHeld"] = json!((epoch != 0).then_some(epoch));
        if let Ok(hold) = self.writer_hold.try_read() {
            if let Some(writer) = hold.as_ref() {
                metrics["hubLogHold"] = json!({"code": "writer_unknown", "writer": writer});
            }
        }
    }

    pub(super) fn set_writer_hold(&self, writer: Option<String>) {
        *self.writer_hold.write().unwrap() = writer;
    }

    /// A replacement restarts the KV revisions. They cannot be compared to the
    /// revisions of the last verified claims and epoch records in SQLite. A
    /// future operator re-seed must verify and install the new hub before
    /// clearing this latch and restarting either watch. The latch is
    /// process-local; a restart retains the verified records, not this flag.
    fn mark_bucket_reset(&self) {
        if !self.bucket_reset.swap(true, Ordering::AcqRel) {
            self.kv_watch_live.store(false, Ordering::Relaxed);
            tracing::warn!(target: "hub_transport", bucket = BUCKET, code = BUCKET_RESET,
                "hub bucket replaced; operator re-seed required");
            self.reset_notified.notify_waiters();
        }
    }

    pub(super) fn bucket_was_reset(&self) -> bool {
        self.bucket_reset.load(Ordering::Acquire)
    }

    pub(super) fn require_bucket(&self) -> Result<(), HandlerError> {
        if self.bucket_was_reset() {
            Err(bucket_reset_error())
        } else {
            Ok(())
        }
    }

    async fn wait_for_bucket_reset(&self) {
        let mut notified = std::pin::pin!(self.reset_notified.notified());
        notified.as_mut().enable();
        if !self.bucket_was_reset() {
            notified.await;
        }
    }

    fn reached(&self) {
        self.connected.store(true, Ordering::Relaxed);
    }

    fn failed(&self) {
        self.connected.store(false, Ordering::Relaxed);
        self.failures.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct Failure(String);

impl Failure {
    fn code(code: &str) -> Self {
        Self(code.into())
    }

    fn kind(operation: &str, kind: impl std::fmt::Debug) -> Self {
        Self(format!("{operation}:{kind:?}"))
    }
}

pub(super) struct Hub {
    connector: SubcConnector,
    bus: Mutex<Option<Arc<HubClient>>>,
    local_bus: Mutex<Option<BusClient>>,
    health: Arc<HealthGauges>,
}

impl Hub {
    pub(super) fn new(connector: SubcConnector, health: Arc<HealthGauges>) -> Arc<Self> {
        Arc::new(Self {
            connector,
            bus: Mutex::default(),
            local_bus: Mutex::default(),
            health,
        })
    }

    pub(super) fn activate(&self) {
        self.health.hub.active.store(true, Ordering::Relaxed);
    }

    async fn bus_consumer(&self) -> Result<Arc<subc_client_rs::consumer::SubcConsumer>, Failure> {
        self.connector
            .consumer()
            .await
            .map_err(|error| match error {
                crate::log_client::TransportError::Refusal { code, .. } => Failure(code),
                _ => Failure::code("daemon_unavailable"),
            })
    }

    pub(super) async fn fleet_vault(
        &self,
        deadline: Instant,
    ) -> Result<crate::fleet_record::vault::SubcEpochVault, HandlerError> {
        let consumer = timeout_at(deadline, self.bus_consumer())
            .await
            .map_err(|_| unavailable("daemon_connect_timeout"))?
            .map_err(|error| unavailable(&error.0))?;
        Ok(crate::fleet_record::vault::SubcEpochVault::new(consumer))
    }

    fn bus_options() -> BusClientOptions {
        let mut options =
            BusClientOptions::new(crate::MODULE_ID).expect("valid entorhinal module id");
        options.request_timeout = REQUEST_TIMEOUT;
        options
    }

    fn bus_failure(error: cortexkit_bus_client::Error) -> Failure {
        use cortexkit_bus_client::Error;
        match error {
            Error::HubUnavailable => Failure::code("hub_unavailable"),
            Error::HubNotGranted => Failure::code("hub_not_granted"),
            Error::Refused(code) => Failure(code),
            Error::Reply(_) => Failure::code("invalid_credential_reply"),
            Error::Transport(_) => Failure::code("credential_unavailable"),
            _ => Failure::code("bus_connect_failed"),
        }
    }

    async fn hub_client(&self) -> Result<Arc<HubClient>, Failure> {
        let mut bus = self.bus.lock().await;
        if bus
            .as_ref()
            .is_some_and(|client| client.connection_changes().has_changed().is_err())
        {
            *bus = None;
        }
        if bus.is_none() {
            *bus = Some(Arc::new(
                HubClient::connect(self.bus_consumer().await?, Self::bus_options())
                    .await
                    .map_err(Self::bus_failure)?,
            ));
        }
        Ok(bus.as_ref().expect("initialized bus client").clone())
    }

    pub(super) fn require_bucket(&self) -> Result<(), HandlerError> {
        self.health.hub.require_bucket()
    }

    async fn binding(&self) -> Result<Binding, Failure> {
        let bus = self.hub_client().await?;
        let mut changes = bus.connection_changes();
        changes.borrow_and_update();
        let client = bus.client().map_err(Self::bus_failure)?;
        let mut js = bus.jetstream().map_err(Self::bus_failure)?;
        js.set_timeout(REQUEST_TIMEOUT);
        Ok(Binding {
            session: Session {
                client,
                js,
                health: self.health.clone(),
            },
            changes: Some(changes),
        })
    }

    /// The hub credential is managed independently. Use the usual box account
    /// only to check the signed writer ID; hub requests use the hub session.
    pub(super) async fn box_id(&self, deadline: Instant) -> Result<BoxId, HandlerError> {
        let lookup = async {
            let mut bus = self.local_bus.lock().await;
            if bus
                .as_ref()
                .is_some_and(|client| client.connection_changes().has_changed().is_err())
            {
                *bus = None;
            }
            if bus.is_none() {
                let consumer = self.bus_consumer().await?;
                *bus = Some(
                    self.health
                        .local_bus
                        .connect(async {
                            timeout_at(deadline, BusClient::connect(consumer, Self::bus_options()))
                                .await
                                .map_err(|_| Failure::code("bus_connect_timeout"))?
                                .map_err(Self::bus_failure)
                        })
                        .await?,
                );
            }
            bus.as_ref()
                .and_then(BusClient::box_id)
                .ok_or_else(|| Failure::code("not_box_account"))
        };
        timeout_at(deadline, lookup)
            .await
            .map_err(|_| unavailable("bus_connect_timeout"))?
            .map_err(|error| unavailable(&error.0))
    }

    async fn open(&self, deadline: Instant) -> Result<Binding, Failure> {
        timeout_at(deadline, self.binding())
            .await
            .map_err(|_| Failure::code("bus_connect_timeout"))?
    }

    /// One connection snapshot for a bounded read or write, not a cached
    /// connection to hold across credential replacements.
    pub(super) async fn session(&self, deadline: Instant) -> Result<Session, HandlerError> {
        match self.open(deadline).await {
            Ok(binding) => Ok(binding.session),
            Err(error) => {
                self.health.hub.failed();
                Err(unavailable(&error.0))
            }
        }
    }

    pub(super) fn epoch_watch(self: &Arc<Self>, own_box: String) -> Feed<KvDelivery> {
        let hub = self.clone();
        Feed::epoch(self.health.clone(), move |_| {
            let hub = hub.clone();
            let own_box = own_box.clone();
            async move { hub.bucket_watch(Some(own_box)).await }
        })
        .with_durable_cursor()
    }

    pub(super) fn residence_watch(self: &Arc<Self>) -> Feed<KvDelivery> {
        let hub = self.clone();
        Feed::kv(self.health.clone(), move |_| {
            let hub = hub.clone();
            async move { hub.bucket_watch(None).await }
        })
        .with_durable_cursor()
    }

    async fn bucket_watch(
        &self,
        own_box: Option<String>,
    ) -> Result<Subscription<KvDelivery>, Failure> {
        let bus = timeout_at(Instant::now() + REQUEST_TIMEOUT, self.hub_client())
            .await
            .map_err(|_| Failure::code("bus_connect_timeout"))??;
        if own_box.is_none() {
            let js = bus.jetstream().map_err(Self::bus_failure)?;
            let kv = js
                .get_key_value(BUCKET)
                .await
                .map_err(|error| Failure::kind("kv_bucket", error.kind()))?;
            check_bucket_config(&kv).await?;
        }
        let watch = bus.watch_bucket(BUCKET).await.map_err(bucket_failure)?;
        let deliveries =
            futures_util::stream::unfold((watch, own_box), |(mut watch, own_box)| async move {
                let delivery = loop {
                    let item = match watch.next().await {
                        Ok(BucketItem::Put {
                            key,
                            revision,
                            value,
                        }) => KvDelivery::Entry(WatchedEntry {
                            key,
                            revision,
                            value: value.to_vec(),
                            operation: kv::Operation::Put,
                        }),
                        Ok(BucketItem::Delete { key, revision }) => {
                            KvDelivery::Entry(WatchedEntry {
                                key,
                                revision,
                                value: Vec::new(),
                                operation: kv::Operation::Delete,
                            })
                        }
                        Ok(BucketItem::CaughtUp) => break Ok((0, KvDelivery::CaughtUp)),
                        Err(error) => break Err(bucket_failure(error)),
                        Ok(_) => break Err(Failure::code("kv_unknown_bucket_item")),
                    };
                    let KvDelivery::Entry(entry) = &item else {
                        unreachable!()
                    };
                    if own_box
                        .as_deref()
                        .is_none_or(|own| epoch_key(&entry.key, own))
                    {
                        break Ok((entry.revision, item));
                    }
                };
                Some((delivery, (watch, own_box)))
            });
        // BucketWatch itself tracks replacements, which preserves its stream
        // identity and lets it distinguish reconnection from bucket recreation.
        Ok(Subscription::new(None, deliveries))
    }

    pub(super) async fn list_bucket_keys(
        &self,
        prefix: &str,
        deadline: Instant,
    ) -> Result<Vec<String>, HandlerError> {
        let listing = async {
            let client = self.hub_client().await?;
            let mut watch = client.watch_bucket(BUCKET).await.map_err(bucket_failure)?;
            let mut keys = std::collections::BTreeSet::new();
            loop {
                match watch.next().await.map_err(bucket_failure)? {
                    BucketItem::Put { key, .. } if key.starts_with(prefix) => {
                        keys.insert(key);
                    }
                    BucketItem::Delete { key, .. } if key.starts_with(prefix) => {
                        keys.remove(&key);
                    }
                    BucketItem::CaughtUp => return Ok(keys.into_iter().collect()),
                    BucketItem::Put { .. } | BucketItem::Delete { .. } => {}
                    _ => return Err(Failure::code("kv_unknown_bucket_item")),
                }
            }
        };
        tokio::select! {
            biased;
            () = self.health.hub.wait_for_bucket_reset() => Err(bucket_reset_error()),
            result = timeout_at(deadline, listing) => match result {
                Ok(Ok(keys)) => Ok(keys),
                Ok(Err(error)) if error.0 == BUCKET_RESET => {
                    self.health.hub.mark_bucket_reset();
                    Err(bucket_reset_error())
                }
                Ok(Err(error)) => Err(unavailable(&error.0)),
                Err(_) => Err(unavailable("kv_keys_timeout")),
            },
        }
    }

    /// `applied` is a Hub stream sequence, not the registry's rebased position.
    pub(super) fn tail(self: &Arc<Self>, applied: u64) -> Feed<LogDelivery> {
        let hub = self.clone();
        Feed::log(self.health.clone(), applied, move |cursor| {
            let hub = hub.clone();
            async move {
                hub.open(Instant::now() + REQUEST_TIMEOUT)
                    .await?
                    .tail(cursor.expect("log cursor"))
                    .await
            }
        })
    }
}

struct Binding {
    session: Session,
    changes: Option<watch::Receiver<u64>>,
}

pub(super) struct LogDelivery {
    pub sequence: u64,
    pub bytes: Vec<u8>,
}

impl Binding {
    #[cfg(test)]
    async fn epoch_watch(self, own_box: &str) -> Result<Subscription<KvDelivery>, Failure> {
        let kv = self.session.kv().await?;
        let watch = kv
            .watch_many_with_history([
                "epoch".to_owned(),
                "wrapreq.*".to_owned(),
                format!("wrapjoin.{own_box}.*"),
                format!("wrap.*.*.{own_box}"),
            ])
            .await
            .map_err(|error| Failure::kind("kv_watch", error.kind()))?;
        Ok(Subscription::new(self.changes, test_snapshot(watch, true)))
    }

    #[cfg(test)]
    async fn residence_watch(self) -> Result<Subscription<KvDelivery>, Failure> {
        let kv = self.session.kv().await?;
        let watch = kv
            .watch_with_history(">")
            .await
            .map_err(|error| Failure::kind("kv_watch", error.kind()))?;
        // Read the bucket state after installing the watch so a write between
        // binding and subscribing cannot be mistaken for an empty replay.
        let values = check_bucket_config(&kv).await?;
        Ok(Subscription::new(
            self.changes,
            test_snapshot(watch, values == 0),
        ))
    }

    #[cfg(test)]
    async fn watch(self, cursor: Option<u64>) -> Result<Subscription<kv::Entry>, Failure> {
        let kv = self.session.kv().await?;
        let watch = match cursor {
            Some(last) => kv.watch_all_from_revision(last.saturating_add(1)).await,
            None => kv.watch_with_history(">").await,
        }
        .map_err(|error| Failure::kind("kv_watch", error.kind()))?;
        Ok(Subscription::new(self.changes, watch.map(map_kv_delivery)))
    }

    async fn tail(self, applied: u64) -> Result<Subscription<LogDelivery>, Failure> {
        let stream = self.session.log().await?;
        let consumer = stream
            .create_consumer(push::OrderedConfig {
                deliver_subject: self.session.client.new_inbox(),
                filter_subject: LOG_SUBJECT.into(),
                deliver_policy: DeliverPolicy::ByStartSequence {
                    start_sequence: applied.saturating_add(1),
                },
                ..Default::default()
            })
            .await
            .map_err(|error| Failure::kind("log_consumer", error.kind()))?;
        let messages = consumer
            .messages()
            .await
            .map_err(|error| Failure::kind("log_tail", error.kind()))?;
        Ok(Subscription::new(
            self.changes,
            messages.map(|event| {
                let message = event.map_err(|error| Failure::kind("log_delivery", error.kind()))?;
                let sequence = message
                    .info()
                    .map_err(|_| Failure::code("invalid_log_delivery"))?
                    .stream_sequence;
                Ok((
                    sequence,
                    LogDelivery {
                        sequence,
                        bytes: message.payload.to_vec(),
                    },
                ))
            }),
        ))
    }
}

async fn check_bucket_config(kv: &kv::Store) -> Result<u64, Failure> {
    let status = kv
        .status()
        .await
        .map_err(|error| Failure::kind("kv_status", error.kind()))?;
    let config = &status.info.config;
    if status.history() != 1
        || !status.max_age().is_zero()
        || config.max_message_size != MAX_KV_VALUE
        || config.max_bytes != MAX_KV_BYTES
    {
        return Err(Failure::code("kv_bucket_config_invalid"));
    }
    Ok(status.values())
}

fn bucket_failure(error: BusError) -> Failure {
    match error {
        BusError::BucketReset { .. } => Failure::code(BUCKET_RESET),
        _ => Failure::code(&format!("kv_watch:{error}")),
    }
}

fn epoch_key(key: &str, own_box: &str) -> bool {
    if key == "epoch" {
        return true;
    }
    let mut fields = key.split('.');
    match (
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
    ) {
        (Some("wrapreq"), Some(_), None, None, None) => true,
        (Some("wrapjoin"), Some(owner), Some(_), None, None)
        | (Some("wrap"), Some(_), Some(_), Some(owner), None) => owner == own_box,
        _ => false,
    }
}

#[cfg(test)]
impl From<kv::Entry> for WatchedEntry {
    fn from(entry: kv::Entry) -> Self {
        Self {
            key: entry.key,
            revision: entry.revision,
            value: entry.value.to_vec(),
            operation: entry.operation,
        }
    }
}

#[cfg(test)]
fn test_snapshot(
    watch: kv::Watch,
    empty: bool,
) -> impl futures_util::Stream<Item = Result<(u64, KvDelivery), Failure>> + Send {
    let mut caught_up = empty;
    futures_util::stream::iter(empty.then_some(Ok((0, KvDelivery::CaughtUp)))).chain(
        watch.flat_map(move |event| {
            let mut boundary = None;
            let entry = map_kv_delivery(event).map(|(revision, entry)| {
                if !caught_up && entry.seen_current {
                    caught_up = true;
                    boundary = Some(Ok((0, KvDelivery::CaughtUp)));
                }
                (revision, KvDelivery::Entry(entry.into()))
            });
            futures_util::stream::iter([Some(entry), boundary].into_iter().flatten())
        }),
    )
}

#[cfg(test)]
fn map_kv_delivery(
    event: Result<kv::Entry, kv::WatcherError>,
) -> Result<(u64, kv::Entry), Failure> {
    event.map(|entry| (entry.revision, entry)).map_err(|error| {
        error
            .source()
            .and_then(|source| source.downcast_ref::<push::OrderedError>())
            .map_or_else(
                || Failure::kind("kv_delivery", error.kind()),
                |ordered| Failure::kind("kv_delivery", ordered.kind()),
            )
    })
}

pub(super) struct Session {
    client: async_nats::Client,
    js: jetstream::Context,
    health: Arc<HealthGauges>,
}

impl Session {
    #[cfg(test)]
    fn new(client: async_nats::Client, health: Arc<HealthGauges>) -> Self {
        let mut js = jetstream::with_domain(client.clone(), DOMAIN);
        js.set_timeout(REQUEST_TIMEOUT);
        Self { client, js, health }
    }

    async fn kv(&self) -> Result<kv::Store, Failure> {
        self.js.get_key_value(BUCKET).await.map_err(|error| {
            Failure::kind(
                "kv_bucket",
                error
                    .source()
                    .and_then(|source| source.downcast_ref::<jetstream::context::GetStreamError>())
                    .map(|error| error.kind()),
            )
        })
    }

    async fn log(&self) -> Result<jetstream::stream::Stream, Failure> {
        self.js
            .get_stream(LOG_STREAM)
            .await
            .map_err(|error| Failure::kind("log_stream", error.kind()))
    }

    pub(super) async fn log_entry(
        &self,
        sequence: u64,
        deadline: Instant,
    ) -> Result<Option<Vec<u8>>, HandlerError> {
        self.confirm_hub(deadline, async {
            // A direct get only needs the stream name, not a fresh STREAM.INFO.
            let stream = self
                .js
                .get_stream_no_info(LOG_STREAM)
                .await
                .map_err(|error| Failure::kind("log_stream", error.kind()))?;
            match stream.direct_get(sequence).await {
                Ok(message) => Ok(Some(message.payload.to_vec())),
                Err(error) if error.kind() == DirectGetErrorKind::NotFound => Ok(None),
                Err(error) => Err(Failure::kind("log_entry", error.kind())),
            }
        })
        .await
    }

    pub(super) async fn log_head(&self, deadline: Instant) -> Result<u64, HandlerError> {
        self.confirm_hub(deadline, async {
            Ok(self.log().await?.cached_info().state.last_sequence)
        })
        .await
    }

    pub(super) async fn kv_entry(
        &self,
        key: &str,
        deadline: Instant,
    ) -> Result<Option<kv::Entry>, HandlerError> {
        self.confirm_hub(deadline, async {
            self.kv()
                .await?
                .entry(key)
                .await
                .map_err(|error| Failure::kind("kv_entry", error.kind()))
        })
        .await
    }

    pub(super) async fn kv_fence_entry(
        &self,
        key: &str,
        deadline: Instant,
    ) -> Result<Option<KvEntry>, HandlerError> {
        self.confirm_hub(deadline, async {
            if !valid_kv_key(key) {
                return Err(Failure::code("kv_fence_entry:InvalidKey"));
            }
            let stream = self
                .js
                .get_stream_no_info(RESIDENCE_STREAM)
                .await
                .map_err(|error| Failure::kind("kv_stream", error.kind()))?;
            // Stream MSG.GET goes to the leader. A replica serving DIRECT.GET
            // might still see the predecessor of a write that already landed.
            let subject = format!("$KV.{BUCKET}.{key}");
            match stream.get_last_raw_message_by_subject(&subject).await {
                Ok(message) => {
                    let deleted = match message.headers.get("KV-Operation") {
                        Some(op) => matches!(op.as_str(), "DEL" | "PURGE"),
                        None => message
                            .headers
                            .get(NATS_MARKER_REASON)
                            .is_some_and(|reason| {
                                matches!(reason.as_str(), "MaxAge" | "Purge" | "Remove")
                            }),
                    };
                    Ok(Some(KvEntry {
                        revision: message.sequence,
                        value: (!deleted).then(|| message.payload.to_vec()),
                    }))
                }
                Err(error) if error.kind() == LastRawMessageErrorKind::NoMessageFound => Ok(None),
                Err(error) => Err(Failure::kind("kv_fence_entry", error.kind())),
            }
        })
        .await
    }

    pub(super) async fn kv_published_at(
        &self,
        key: &str,
        revision: u64,
        deadline: Instant,
    ) -> Result<Option<i64>, HandlerError> {
        self.confirm_hub(deadline, async {
            let stream = self
                .js
                .get_stream_no_info(RESIDENCE_STREAM)
                .await
                .map_err(|error| Failure::kind("kv_stream", error.kind()))?;
            // The watch does not expose publish time. Read the exact revision
            // so a later PUT cannot supply the reaper's clock for this record.
            match stream.get_raw_message(revision).await {
                Ok(message)
                    if message.sequence == revision
                        && message.subject.as_str() == format!("$KV.{BUCKET}.{key}") =>
                {
                    Ok(i64::try_from(message.time.unix_timestamp_nanos() / 1_000_000).ok())
                }
                Ok(_) => Err(Failure::code("kv_publish_time_wrong_entry")),
                Err(error) if error.kind() == RawMessageErrorKind::NoMessageFound => Ok(None),
                Err(error) => Err(Failure::kind("kv_publish_time", error.kind())),
            }
        })
        .await
    }

    #[cfg(test)]
    pub(super) async fn join_keys(
        &self,
        prefix: &str,
        deadline: Instant,
    ) -> Result<Vec<String>, HandlerError> {
        self.confirm_hub(deadline, async {
            let mut keys = self
                .kv()
                .await?
                .keys()
                .await
                .map_err(|error| Failure::kind("kv_keys", error.kind()))?;
            let mut matching = Vec::new();
            while let Some(key) = keys.next().await {
                let key = key.map_err(|error| Failure::kind("kv_keys", error.kind()))?;
                if key.starts_with(prefix) {
                    matching.push(key);
                }
            }
            Ok(matching)
        })
        .await
    }

    pub(super) async fn update(
        &self,
        key: &str,
        bytes: Vec<u8>,
        expected: u64,
        op_id: &str,
        deadline: Instant,
    ) -> Result<u64, HandlerError> {
        self.update_record(key, bytes, expected, op_id, deadline, false)
            .await
    }

    pub(super) async fn update_record(
        &self,
        key: &str,
        bytes: Vec<u8>,
        expected: u64,
        op_id: &str,
        deadline: Instant,
        fenced_renewal: bool,
    ) -> Result<u64, HandlerError> {
        check_kv_size(key, bytes.len())?;
        if !valid_kv_key(key) {
            return Err(HandlerError::new("invalid_params", "invalid hub KV key"));
        }
        let mut headers = HeaderMap::new();
        headers.insert(
            NATS_EXPECTED_LAST_SUBJECT_SEQUENCE,
            HeaderValue::from(expected),
        );
        // Store::update combines submission and PubAck. Keep those stages
        // separate without changing the domain-qualified subject or CAS header.
        let subject = format!("$JS.{DOMAIN}.API.$KV.{BUCKET}.{key}");
        // A renewal reads the current `epoch` record from this bucket before
        // sealing, so a second reachability probe of the bucket adds no check.
        if !fenced_renewal {
            self.confirm_hub(deadline, self.kv()).await?;
        }
        self.publish(subject, headers, bytes, op_id, "revision_moved", deadline)
            .await
    }

    pub(super) async fn delete(
        &self,
        key: &str,
        expected: u64,
        op_id: &str,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        if !["move.", "wrapreq.", "rolehandback.", "wrapjoin.", "wrap."]
            .iter()
            .any(|prefix| key.starts_with(prefix))
        {
            return Err(HandlerError::new(
                "kv_delete_forbidden",
                "fleet key cannot be deleted",
            ));
        }
        let kv = self.confirm_hub(deadline, self.kv()).await?;
        if self.client.connection_state() != State::Connected || Instant::now() >= deadline {
            self.health.hub.failed();
            return Err(unavailable(
                "bus disconnected before delete or no time left",
            ));
        }
        // The Store API combines submission and acknowledgment. After the
        // preflight we can't tell whether a timeout preceded submission.
        match timeout_at(deadline, kv.delete_expect_revision(key, Some(expected))).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) if error.kind() == kv::DeleteErrorKind::InvalidKey => {
                Err(HandlerError::new("invalid_params", "invalid hub KV key"))
            }
            Ok(Err(error)) if error.kind() == kv::DeleteErrorKind::WrongLastRevision => Err(
                HandlerError::new("revision_moved", "hub compare-and-set refused"),
            ),
            _ => {
                self.health.hub.failed();
                Err(unsettled(op_id))
            }
        }
    }

    pub(super) async fn append(
        &self,
        bytes: Vec<u8>,
        expected: u64,
        op_id: &str,
        deadline: Instant,
    ) -> Result<u64, HandlerError> {
        let mut headers = HeaderMap::new();
        headers.insert(NATS_EXPECTED_LAST_SEQUENCE, HeaderValue::from(expected));
        headers.insert(NATS_MESSAGE_ID, op_id);
        self.confirm_hub(deadline, self.log()).await?;
        self.publish(
            LOG_SUBJECT.into(),
            headers,
            bytes,
            op_id,
            "head_moved",
            deadline,
        )
        .await
    }

    async fn confirm_hub<T>(
        &self,
        deadline: Instant,
        request: impl std::future::Future<Output = Result<T, Failure>>,
    ) -> Result<T, HandlerError> {
        // A connected leaf can be isolated from the hub. Probe the target, not
        // the local socket, before submitting a write to async-nats.
        let result =
            if self.client.connection_state() != State::Connected || Instant::now() >= deadline {
                Err(unavailable("no connected bus or no time left"))
            } else {
                match timeout_at(deadline, request).await {
                    Ok(Ok(value)) => Ok(value),
                    Ok(Err(error)) => Err(unavailable(&error.0)),
                    Err(_) => Err(unavailable("hub request deadline elapsed")),
                }
            };
        if result.is_ok() {
            self.health.hub.reached();
        } else {
            self.health.hub.failed();
        }
        result
    }

    async fn publish(
        &self,
        subject: String,
        headers: HeaderMap,
        bytes: Vec<u8>,
        op_id: &str,
        cas_refusal: &str,
        deadline: Instant,
    ) -> Result<u64, HandlerError> {
        if self.client.connection_state() != State::Connected || Instant::now() >= deadline {
            self.health.hub.failed();
            return Err(unavailable(
                "bus disconnected before publish or no time left",
            ));
        }
        let sent = timeout_at(
            deadline,
            self.js.publish_with_headers(subject, headers, bytes.into()),
        )
        .await;
        let ack = match sent {
            Ok(Ok(ack)) => ack,
            Ok(Err(error)) if error.kind() == PublishErrorKind::MaxPayloadExceeded => {
                return Err(HandlerError::new(
                    "payload_too_large",
                    "hub publish exceeds the bus payload limit",
                ));
            }
            _ => {
                self.health.hub.failed();
                return Err(unavailable("hub publish not submitted"));
            }
        };
        // Once async-nats accepted the send, an absent or unreadable reply
        // cannot establish that nothing landed. Do not resend on this path.
        match timeout_at(deadline, ack).await {
            Ok(Ok(ack)) => Ok(ack.sequence),
            Ok(Err(error)) if error.kind() == PublishErrorKind::WrongLastSequence => Err(
                HandlerError::new(cas_refusal, "hub compare-and-set refused"),
            ),
            _ => {
                self.health.hub.failed();
                Err(unsettled(op_id))
            }
        }
    }
}

fn valid_kv_key(key: &str) -> bool {
    key.split('.').all(|token| {
        !token.is_empty()
            && token
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-/_=".contains(&byte))
    })
}

pub(super) fn unsettled(op_id: &str) -> HandlerError {
    let mut error = HandlerError::new(
        "hub_write_unsettled",
        "hub publish acknowledgment unavailable",
    );
    error.detail = Some(json!({"op_id": op_id}));
    error
}

fn unavailable(reason: &str) -> HandlerError {
    HandlerError::new("hub_unavailable", format!("hub unavailable: {reason}"))
}

#[cfg(test)]
mod tests;
