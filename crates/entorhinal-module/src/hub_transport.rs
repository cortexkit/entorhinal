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
    header::{NATS_EXPECTED_LAST_SEQUENCE, NATS_EXPECTED_LAST_SUBJECT_SEQUENCE, NATS_MESSAGE_ID},
    jetstream::{
        self,
        consumer::{push, DeliverPolicy},
        context::PublishErrorKind,
        kv,
        stream::DirectGetErrorKind,
    },
    HeaderMap, HeaderValue,
};
use cortexkit_bus_client::{BusClient, BusClientOptions};
use cortexkit_bus_naming::federation::BoxId;
use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio::{
    sync::{watch, Mutex},
    time::{timeout_at, Instant},
};

use self::feed::Subscription;
pub(super) use self::feed::{Feed, FeedEvent};
use crate::{log_client::SubcConnector, HandlerError, HealthGauges};

const DOMAIN: &str = "hub";
const LOG_STREAM: &str = "HUB_IDLOG";
pub(super) const LOG_SUBJECT: &str = "ck.fleet.idlog";
const BUCKET: &str = "residence";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_KV_VALUE: i32 = 16 * 1024;
const MAX_KV_BYTES: i64 = 64 * 1024 * 1024;

pub(super) enum KvDelivery {
    Empty,
    Entry(kv::Entry),
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
    pub(super) kv_records_refused: AtomicU64,
    pub(super) epoch_held: AtomicU64,
}

impl Gauges {
    pub(super) fn report(&self, metrics: &mut Value) {
        if !self.active.load(Ordering::Relaxed) {
            return;
        }
        metrics["hubConnected"] = json!(self.connected.load(Ordering::Relaxed));
        metrics["hubLogTailLive"] = json!(self.log_tail_live.load(Ordering::Relaxed));
        metrics["hubKvWatchLive"] = json!(self.kv_watch_live.load(Ordering::Relaxed));
        metrics["hubFailures"] = json!(self.failures.load(Ordering::Relaxed));
        metrics["kvRecordsRefused"] = json!(self.kv_records_refused.load(Ordering::Relaxed));
        let epoch = self.epoch_held.load(Ordering::Relaxed);
        metrics["epochHeld"] = json!((epoch != 0).then_some(epoch));
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
    bus: Mutex<Option<BusClient>>,
    health: Arc<HealthGauges>,
}

impl Hub {
    /// Construct only after the registry has selected the hub backend.
    pub(super) fn new(connector: SubcConnector, health: Arc<HealthGauges>) -> Arc<Self> {
        health.hub.active.store(true, Ordering::Relaxed);
        Arc::new(Self {
            connector,
            bus: Mutex::default(),
            health,
        })
    }

    async fn binding(&self) -> Result<Binding, Failure> {
        let mut bus = self.bus.lock().await;
        if bus
            .as_ref()
            .is_some_and(|client| client.connection_changes().has_changed().is_err())
        {
            *bus = None;
        }
        if bus.is_none() {
            let subc = self
                .connector
                .consumer()
                .await
                .map_err(|error| match error {
                    crate::log_client::TransportError::Refusal { code, .. } => Failure(code),
                    _ => Failure::code("daemon_unavailable"),
                })?;
            let mut options =
                BusClientOptions::new(crate::MODULE_ID).expect("valid entorhinal module id");
            options.request_timeout = REQUEST_TIMEOUT;
            *bus = Some(BusClient::connect(subc, options).await.map_err(|error| {
                use cortexkit_bus_client::Error;
                match error {
                    Error::Refused(code) => Failure(code),
                    Error::Reply(_) => Failure::code("invalid_credential_reply"),
                    Error::Transport(_) => Failure::code("credential_unavailable"),
                    _ => Failure::code("bus_connect_failed"),
                }
            })?);
        }
        let bus = bus.as_ref().expect("initialized bus client");
        let box_id = bus
            .box_id()
            .ok_or_else(|| Failure::code("not_box_account"))?;
        let mut changes = bus.connection_changes();
        changes.borrow_and_update();
        let connection = bus.connection();
        // Reject the legacy inbox fallback. The bus client validates the issued
        // ckdel prefix and configures new_inbox() with it; never construct it here.
        if !connection.inbox_prefix().starts_with("ckdel.") {
            return Err(Failure::code("missing_deliver_prefix"));
        }
        Ok(Binding {
            box_id,
            session: Session::new(connection.client(), self.health.clone()),
            changes: Some(changes),
        })
    }

    async fn open(&self, deadline: Instant) -> Result<Binding, Failure> {
        timeout_at(deadline, self.binding())
            .await
            .map_err(|_| Failure::code("bus_connect_timeout"))?
    }

    /// One connection snapshot for a bounded read or write, not a cached
    /// connection to hold across credential replacements.
    pub(super) async fn session(
        &self,
        deadline: Instant,
    ) -> Result<(BoxId, Session), HandlerError> {
        match self.open(deadline).await {
            Ok(binding) => Ok((binding.box_id, binding.session)),
            Err(error) => {
                self.health.hub.failed();
                Err(unavailable(&error.0))
            }
        }
    }

    pub(super) fn watch(self: &Arc<Self>) -> Feed<kv::Entry> {
        let hub = self.clone();
        Feed::kv(self.health.clone(), move |cursor| {
            let hub = hub.clone();
            async move {
                hub.open(Instant::now() + REQUEST_TIMEOUT)
                    .await?
                    .watch(cursor)
                    .await
            }
        })
    }

    pub(super) fn epoch_watch(self: &Arc<Self>) -> Feed<kv::Entry> {
        let hub = self.clone();
        Feed::epoch(self.health.clone(), move |_| {
            let hub = hub.clone();
            async move {
                hub.open(Instant::now() + REQUEST_TIMEOUT)
                    .await?
                    .watch(None)
                    .await
            }
        })
    }

    pub(super) fn residence_watch(self: &Arc<Self>) -> Feed<KvDelivery> {
        let hub = self.clone();
        Feed::kv(self.health.clone(), move |_| {
            let hub = hub.clone();
            async move {
                hub.open(Instant::now() + REQUEST_TIMEOUT)
                    .await?
                    .residence_watch()
                    .await
            }
        })
        .with_durable_cursor()
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
    box_id: BoxId,
    session: Session,
    changes: Option<watch::Receiver<u64>>,
}

pub(super) struct LogDelivery {
    pub sequence: u64,
    pub bytes: Vec<u8>,
}

impl Binding {
    async fn residence_watch(self) -> Result<Subscription<KvDelivery>, Failure> {
        let kv = self.session.kv().await?;
        let watch = kv
            .watch_with_history(">")
            .await
            .map_err(|error| Failure::kind("kv_watch", error.kind()))?;
        // Read the bucket state after installing the watch so a write between
        // binding and subscribing cannot be mistaken for an empty replay.
        let status = self
            .session
            .kv()
            .await?
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
        let empty = futures_util::stream::iter(
            (status.values() == 0).then_some(Ok((0, KvDelivery::Empty))),
        );
        Ok(Subscription::new(
            self.changes,
            empty.chain(watch.map(|event| {
                map_kv_delivery(event).map(|(revision, entry)| (revision, KvDelivery::Entry(entry)))
            })),
        ))
    }

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
            match self.log().await?.direct_get(sequence).await {
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
        if !key.split('.').all(|token| {
            !token.is_empty()
                && token
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-/_=".contains(&byte))
        }) {
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
