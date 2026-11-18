use std::{future::Future, sync::Arc, time::Duration};

use futures_util::{
    future::BoxFuture,
    stream::{BoxStream, Stream, StreamExt},
};
use tokio::{
    sync::watch,
    time::{sleep_until, Instant},
};

use super::{Failure, HealthGauges, Ordering};

const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(60);

pub(super) struct Subscription<T> {
    changes: Option<watch::Receiver<u64>>,
    stream: BoxStream<'static, Result<(u64, T), Failure>>,
}

enum Delivery<T> {
    Item(Result<(u64, T), Failure>),
    Ended(Failure),
    Replaced,
}

impl<T> Subscription<T> {
    pub(super) fn new(
        changes: Option<watch::Receiver<u64>>,
        stream: impl Stream<Item = Result<(u64, T), Failure>> + Send + 'static,
    ) -> Self {
        Self {
            changes,
            stream: stream.boxed(),
        }
    }

    async fn next(&mut self) -> Delivery<T> {
        tokio::select! {
            biased;
            change = async {
                match &mut self.changes {
                    Some(changes) => changes.changed().await,
                    None => std::future::pending().await,
                }
            } => match change {
                Ok(()) => Delivery::Replaced,
                Err(_) => Delivery::Ended(Failure::code("credential_maintenance_stopped")),
            },
            event = self.stream.next() => match event {
                Some(item) => Delivery::Item(item),
                None => Delivery::Ended(Failure::code("subscription_ended")),
            },
        }
    }
}

#[derive(Default)]
struct Outage {
    previous: Option<String>,
    failures: u64,
}

impl Outage {
    fn failed(&mut self, feed: &str, error: Failure) -> bool {
        self.failures += 1;
        if self.previous.as_ref() == Some(&error.0) {
            return false;
        }
        tracing::warn!(target: "hub_transport", feed, code = error.0, "hub subscription unavailable");
        self.previous = Some(error.0);
        true
    }

    fn recovered(&mut self, feed: &str) {
        if self.previous.take().is_some() {
            tracing::info!(target: "hub_transport", feed, failures = self.failures, "hub subscription recovered");
            self.failures = 0;
        }
    }
}

type Open<T> =
    dyn FnMut(Option<u64>) -> BoxFuture<'static, Result<Subscription<T>, Failure>> + Send;

pub(crate) struct Feed<T> {
    health: Arc<HealthGauges>,
    kind: Kind,
    open: Box<Open<T>>,
    subscription: Option<Subscription<T>>,
    cursor: Option<u64>,
    durable_cursor: bool,
    backoff: Duration,
    retry_at: Option<Instant>,
    outage: Outage,
}

enum Kind {
    Kv,
    Log,
}

impl<T> Feed<T> {
    pub(super) fn kv<F, Fut>(health: Arc<HealthGauges>, open: F) -> Self
    where
        F: FnMut(Option<u64>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Subscription<T>, Failure>> + Send + 'static,
    {
        Self::new(health, Kind::Kv, None, open)
    }

    pub(super) fn log<F, Fut>(health: Arc<HealthGauges>, applied: u64, open: F) -> Self
    where
        F: FnMut(Option<u64>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Subscription<T>, Failure>> + Send + 'static,
    {
        Self::new(health, Kind::Log, Some(applied), open)
    }

    fn new<F, Fut>(health: Arc<HealthGauges>, kind: Kind, cursor: Option<u64>, mut open: F) -> Self
    where
        F: FnMut(Option<u64>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Subscription<T>, Failure>> + Send + 'static,
    {
        Self {
            health,
            kind,
            open: Box::new(move |cursor| Box::pin(open(cursor))),
            subscription: None,
            cursor,
            durable_cursor: false,
            backoff: INITIAL_BACKOFF,
            retry_at: None,
            outage: Outage::default(),
        }
    }

    /// Let the identity log check redeliveries against its journal and resume
    /// from committed progress, including when a snapshot is still buffered.
    pub(crate) fn with_durable_cursor(mut self) -> Self {
        self.durable_cursor = true;
        self
    }

    pub(crate) fn applied(&mut self, sequence: u64) {
        self.cursor = Some(sequence);
    }

    fn name(&self) -> &'static str {
        match self.kind {
            Kind::Kv => "residence_kv",
            Kind::Log => "identity_log",
        }
    }

    fn set_live(&self, live: bool) {
        let gauge = match self.kind {
            Kind::Kv => &self.health.hub.kv_watch_live,
            Kind::Log => &self.health.hub.log_tail_live,
        };
        gauge.store(live, Ordering::Relaxed);
    }

    fn failed(&mut self, error: Failure) -> bool {
        self.set_live(false);
        self.health.hub.failed();
        self.outage.failed(self.name(), error)
    }

    fn reached(&mut self) {
        self.set_live(true);
        self.health.hub.reached();
        self.outage.recovered(self.name());
        self.backoff = INITIAL_BACKOFF;
    }

    fn retry_open(&mut self, error: Failure) {
        self.subscription = None;
        self.failed(error);
        self.retry_at = Some(Instant::now() + self.backoff);
        self.backoff = (self.backoff * 2).min(MAX_BACKOFF);
    }

    /// Apply an item before asking for the next. On an apply refusal, drop the
    /// feed; a new feed starts from the caller's durable applied sequence.
    pub(crate) async fn next(&mut self) -> T {
        loop {
            if let Some(at) = self.retry_at {
                sleep_until(at).await;
                self.retry_at = None;
            }
            if self.subscription.is_none() {
                match (self.open)(self.cursor).await {
                    Ok(subscription) => {
                        self.subscription = Some(subscription);
                        self.reached();
                    }
                    Err(error) => {
                        self.retry_open(error);
                        continue;
                    }
                }
            }
            match self
                .subscription
                .as_mut()
                .expect("opened subscription")
                .next()
                .await
            {
                Delivery::Item(Ok((sequence, item))) => {
                    self.reached();
                    if self.durable_cursor {
                        return item;
                    }
                    if self.cursor.is_none_or(|last| sequence > last) {
                        self.cursor = Some(sequence);
                        return item;
                    }
                }
                Delivery::Item(Err(error)) => {
                    // Keep polling so async-nats can repair the ordered stream
                    // from its last delivery without reopening the watch here.
                    if !self.failed(error) {
                        // A closed client can emit the same error immediately
                        // until credential maintenance replaces the connection.
                        self.retry_at = Some(Instant::now() + INITIAL_BACKOFF);
                    }
                }
                Delivery::Ended(error) => self.retry_open(error),
                Delivery::Replaced => {
                    self.set_live(false);
                    self.subscription = None;
                }
            }
        }
    }
}

impl<T> Drop for Feed<T> {
    fn drop(&mut self) {
        self.set_live(false);
    }
}
