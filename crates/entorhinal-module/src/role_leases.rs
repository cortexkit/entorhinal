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
use tokio::sync::{watch, Notify};

use crate::{encode_result, watches::WatchTransport, HandlerError, ProjectsHandler, RouteKey};

pub(super) const WATCH: &str = "role.lease";
pub(super) const ACK: &str = "role.lease.ack";
const ACK_MS: u64 = 5_000;
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
fn monotonic_ms() -> u64 {
    let now = rustix::time::clock_gettime(rustix::time::ClockId::Monotonic);
    (now.tv_sec as u64) * 1_000 + (now.tv_nsec as u64) / 1_000_000
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn monotonic_ms() -> u64 {
    // SAFETY: GetTickCount64 has no arguments or pointer accesses and reads
    // the system uptime counter shared by all processes.
    unsafe { windows_sys::Win32::System::SystemInformation::GetTickCount64() }
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct View {
    pub gen: i64,
    pub until_ms: Option<u64>,
    pub handback: bool,
}

#[derive(Serialize)]
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
}

pub(super) struct Leases {
    clients: Mutex<HashMap<String, Arc<Client>>>,
    seq: AtomicU64,
    held: AtomicU64,
    clock: Arc<dyn Fn() -> u64 + Send + Sync>,
    #[cfg(test)]
    sample_views: HashMap<String, watch::Receiver<View>>,
}

impl Default for Leases {
    fn default() -> Self {
        Self {
            clients: Mutex::default(),
            seq: AtomicU64::default(),
            held: AtomicU64::default(),
            clock: Arc::new(monotonic_ms),
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
    ) -> Result<(Arc<Client>, Registration<'_>), HandlerError> {
        use std::collections::hash_map::Entry;

        let client = Arc::new(Client {
            route,
            awaiting: Mutex::default(),
            acked: Notify::new(),
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
            let notice = Notice {
                holding,
                gen: view.gen,
                valid_for_ms,
                issued_at_ms,
                handback: (holding && view.handback).then_some(true),
                seq,
            };
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
}

fn decode<T: serde::de::DeserializeOwned>(params: Value) -> Result<T, HandlerError> {
    serde_json::from_value(params)
        .map_err(|error| HandlerError::new("invalid_request", error.to_string()))
}

impl ProjectsHandler {
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
            return encode_result(json!({"acknowledged":true}));
        }
        Err(HandlerError::new(
            "lease_ack_stale",
            "no matching role lease notice is awaiting ack",
        ))
    }

    pub(super) async fn serve_role_lease(
        &self,
        transport: &impl WatchTransport,
        role: String,
        key: RouteKey,
    ) -> Result<(), HandlerError> {
        let (client, registration) = self.role_leases.register(role, key)?;
        let (_source, views) = watch::channel(View::default());
        #[cfg(test)]
        let views = self
            .role_leases
            .sample_views
            .get(&registration.role)
            .cloned()
            .unwrap_or(views);
        self.role_leases
            .notices(transport, &client, registration, views)
            .await
    }
}
