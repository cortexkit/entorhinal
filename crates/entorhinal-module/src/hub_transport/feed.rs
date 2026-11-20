use std::{future::Future, sync::Arc, time::Duration};

use futures_util::{
    future::BoxFuture,
    stream::{BoxStream, Stream, StreamExt},
};
use tokio::{
    sync::watch,
    time::{sleep_until, Instant},
};

use super::{Failure, HealthGauges, Ordering, BUCKET_RESET};

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
    // The lines this outage chose to log. Tests read these rather than a
    // captured subscriber: tracing caches per-callsite interest process-wide,
    // so a capture can miss lines while other tests run.
    #[cfg(test)]
    logged: Vec<String>,
}

impl Outage {
    fn failed(&mut self, feed: &str, error: Failure) {
        self.failures += 1;
        if self.previous.as_ref() == Some(&error.0) {
            return;
        }
        tracing::warn!(target: "hub_transport", feed, code = error.0, "hub subscription unavailable");
        #[cfg(test)]
        self.logged.push(format!("unavailable {}", error.0));
        self.previous = Some(error.0);
    }

    fn recovered(&mut self, feed: &str) {
        if self.previous.take().is_some() {
            tracing::info!(target: "hub_transport", feed, failures = self.failures, "hub subscription recovered");
            #[cfg(test)]
            self.logged
                .push(format!("recovered after {}", self.failures));
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

pub(crate) enum FeedEvent<T> {
    Connected,
    Disconnected,
    BucketReset,
    Item(T),
}

enum Kind {
    Kv,
    Log,
    Epoch,
}

impl<T> Feed<T> {
    #[cfg(test)]
    pub(super) fn logged(&self) -> &[String] {
        &self.outage.logged
    }

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

    pub(super) fn epoch<F, Fut>(health: Arc<HealthGauges>, open: F) -> Self
    where
        F: FnMut(Option<u64>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Subscription<T>, Failure>> + Send + 'static,
    {
        Self::new(health, Kind::Epoch, None, open)
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

    pub(crate) fn replay(&mut self) {
        self.set_live(false);
        self.subscription = None;
    }

    fn name(&self) -> &'static str {
        match self.kind {
            Kind::Kv => "residence_kv",
            Kind::Log => "identity_log",
            Kind::Epoch => "fleet_epoch",
        }
    }

    fn set_live(&self, live: bool) {
        let gauge = match self.kind {
            Kind::Kv => &self.health.hub.kv_watch_live,
            Kind::Log => &self.health.hub.log_tail_live,
            Kind::Epoch => return,
        };
        gauge.store(live, Ordering::Relaxed);
    }

    fn failed(&mut self, error: Failure) {
        self.set_live(false);
        self.health.hub.failed();
        self.outage.failed(self.name(), error);
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
            if let FeedEvent::Item(item) = self.next_event().await {
                return item;
            }
        }
    }

    pub(crate) async fn next_event(&mut self) -> FeedEvent<T> {
        if matches!(self.kind, Kind::Kv | Kind::Epoch) {
            let health = self.health.clone();
            tokio::select! {
                biased;
                () = health.hub.wait_for_bucket_reset() => {
                    self.subscription = None;
                    self.set_live(false);
                    FeedEvent::BucketReset
                }
                event = self.next_event_inner() => event,
            }
        } else {
            self.next_event_inner().await
        }
    }

    async fn next_event_inner(&mut self) -> FeedEvent<T> {
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
                        return FeedEvent::Connected;
                    }
                    Err(error) if error.0 == BUCKET_RESET => {
                        self.health.hub.mark_bucket_reset();
                        return FeedEvent::BucketReset;
                    }
                    Err(error) => {
                        self.retry_open(error);
                        return FeedEvent::Disconnected;
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
                    if self.durable_cursor || self.cursor.is_none_or(|last| sequence > last) {
                        if !self.durable_cursor {
                            self.cursor = Some(sequence);
                        }
                        return FeedEvent::Item(item);
                    }
                }
                Delivery::Item(Err(error)) if error.0 == BUCKET_RESET => {
                    self.subscription = None;
                    self.health.hub.mark_bucket_reset();
                    return FeedEvent::BucketReset;
                }
                Delivery::Item(Err(error)) | Delivery::Ended(error) => {
                    // An ordered consumer can stay open after an error without
                    // replaying. Reopen from the cursor to restore the view.
                    self.retry_open(error);
                    return FeedEvent::Disconnected;
                }
                Delivery::Replaced => {
                    self.replay();
                    return FeedEvent::Disconnected;
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
