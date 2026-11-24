//! Discovers the fleet backend and fences the legacy writer before migration.

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex, Weak},
};

use entorhinal_core::{
    remote_apply::{EngramFreeze, MigrationMarker},
    RegistryStore,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::time::{timeout_at, Instant};

use crate::{
    fleet_record::UnverifiedRecord,
    hub_identity_log::{IdentityLog, Link},
    hub_membership::{
        box_id,
        peers::{Callosum, LocalIdentity},
    },
    hub_transport::Hub,
    hub_write::Trust,
    log_client::{AppendRequest, EntryKind, LogClient, LogError, SubcConnector},
    participant_discovery::ParticipantDiscovery,
    record_client::{CasRequest, RecordClient, RecordReply},
    HandlerError, HealthGauges, ProjectsHandler, RouteKey,
};

pub(super) const EXCLUDE: &str = "identity_log.hub_switch";
mod drain;
pub(super) mod snapshot;
pub(super) use drain::MigrationImage;
const PROPOSAL: &str = "hubswitch";
const HUB_PROTOCOL: u64 = 1;
const CUTS: &str = "engram-hub-cuts/v1";
const PENDING: &str = "hub_switch_pending";

fn pending() -> HandlerError {
    HandlerError::new(PENDING, "the identity log is switching to the hub")
}

#[derive(Clone, Default)]
pub(crate) struct LegacyFence(Option<Weak<Mutex<Option<RegistryStore>>>>);

impl LegacyFence {
    pub(crate) fn new(slot: &Arc<Mutex<Option<RegistryStore>>>) -> Self {
        Self(Some(Arc::downgrade(slot)))
    }

    fn store(&self) -> Option<RegistryStore> {
        self.0.as_ref()?.upgrade()?.lock().unwrap().clone()
    }

    pub(crate) fn log(&self, append: Option<(&AppendRequest, bool)>) -> Result<(), &'static str> {
        let Some(store) = self.store() else {
            return Ok(());
        };
        if store.hub_log_base().map_err(|_| "storage_error")?.is_some() {
            return Err(PENDING);
        }
        if let Some((request, marker)) = append {
            match store.engram_freeze().map_err(|_| "storage_error")? {
                Some(EngramFreeze::Pending {
                    expected_head,
                    entry_id,
                    body,
                }) if marker => {
                    if request.expected_head != expected_head as u64
                        || request.entry_id != entry_id
                        || request.kind != EntryKind::Change
                        || serde_json::to_vec(&body).map_err(|_| "storage_error")? != request.data
                    {
                        return Err(PENDING);
                    }
                }
                Some(_) => return Err(PENDING),
                None if marker => return Err(PENDING),
                None => {}
            }
        }
        Ok(())
    }

    pub(crate) fn record(
        &self,
        cas_subject: Option<&str>,
        drain: bool,
    ) -> Result<(), &'static str> {
        let Some(store) = self.store() else {
            return Ok(());
        };
        if store.hub_log_base().map_err(|_| "storage_error")?.is_some() {
            return Err(PENDING);
        }
        if let Some(subject) = cas_subject {
            if drain {
                return if (subject.starts_with("claim:") || subject.starts_with("role:"))
                    && matches!(
                        store.engram_freeze().map_err(|_| "storage_error")?,
                        Some(EngramFreeze::Applied { .. })
                    ) {
                    Ok(())
                } else {
                    Err(PENDING)
                };
            }
            if !subject.starts_with("hubquiet:")
                && !subject.starts_with("hubdrained:")
                && store
                    .engram_freeze()
                    .map_err(|_| "storage_error")?
                    .is_some()
            {
                return Err(PENDING);
            }
        }
        Ok(())
    }
}

/// Snapshot verification supplies this evidence. Without it, a nonempty log
/// cannot be distinguished from a different switch or an incomplete enable.
#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(super) enum HubHistory<'a> {
    Empty,
    Unverified,
    Complete,
    Incomplete {
        engram_through: u64,
        enable_id: Option<&'a str>,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Startup {
    Hub,
    Follow,
    ResumeMigration,
    ResumeEnable,
    Wait,
    Migrate,
    NoHistory,
}

/// Snapshot seeding and activation supply the verified history and ownership
/// of agents and roles through `SnapshotClaims`, then follow or resume the log.
pub(super) fn select(
    store: &RegistryStore,
    history: HubHistory<'_>,
) -> Result<Startup, HandlerError> {
    if store.hub_log_base()?.is_some() {
        return Ok(Startup::Hub);
    }
    Ok(match history {
        HubHistory::Complete => Startup::Follow,
        HubHistory::Incomplete {
            engram_through: 1..,
            ..
        } => Startup::ResumeMigration,
        HubHistory::Incomplete {
            enable_id: Some(id),
            ..
        } if store.hub_enable_id()?.as_deref() == Some(id) => Startup::ResumeEnable,
        HubHistory::Incomplete { .. } => Startup::Wait,
        HubHistory::Empty if store.identity_log_status()?.state != "disabled" => Startup::Migrate,
        HubHistory::Empty => Startup::NoHistory,
        HubHistory::Unverified => {
            return Err(HandlerError::new(
                "hub_switch_unverified_log",
                "the hub log is nonempty and its snapshot has not been verified",
            ))
        }
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Proposal {
    v: u64,
    migration_id: String,
    excluded: BTreeSet<String>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Readiness {
    v: u64,
    #[serde(rename = "box")]
    box_id: String,
    entorhinal_hub: u64,
    engram_hub_cuts: bool,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Quiet {
    v: u64,
    migration_id: String,
    roles: BTreeSet<String>,
}

impl Proposal {
    fn from_reply(reply: &RecordReply) -> Result<Option<Self>, HandlerError> {
        if reply.absent {
            return Ok(None);
        }
        decoded::<Self>(reply)
            .filter(|p| {
                p.v == 1 && box_id(&p.migration_id) && p.excluded.iter().all(|id| box_id(id))
            })
            .map(Some)
            .ok_or_else(|| {
                HandlerError::new("hub_switch_invalid_proposal", "invalid hub switch proposal")
            })
    }
}

fn decoded<T: serde::de::DeserializeOwned>(reply: &RecordReply) -> Option<T> {
    if reply.stale {
        return None;
    }
    serde_json::from_slice(reply.body.as_deref()?).ok()
}

fn member_ready(reply: &RecordReply, author: &str, box_id: &str) -> bool {
    reply.author.as_deref() == Some(author)
        && decoded::<Readiness>(reply).is_some_and(|node| {
            node.v == 1
                && node.box_id == box_id
                && node.entorhinal_hub >= HUB_PROTOCOL
                && node.engram_hub_cuts
        })
}

pub(super) async fn local_hub_cuts(
    discovery: &dyn ParticipantDiscovery,
    deadline: Instant,
) -> Result<bool, HandlerError> {
    let catalog = timeout_at(deadline, discovery.catalog_list())
        .await
        .map_err(|_| {
            HandlerError::new(
                "hub_switch_catalog_unavailable",
                "module catalog deadline expired",
            )
        })?
        .map_err(|_| {
            HandlerError::new(
                "hub_switch_catalog_unavailable",
                "module catalog unavailable",
            )
        })?;
    Ok(catalog.modules.iter().any(|module| {
        module.module_id.as_str() == "engram"
            && module
                .capabilities
                .as_ref()
                .is_some_and(|caps| caps.provides.iter().any(|cap| cap == CUTS))
    }))
}

fn record_error(error: crate::record_client::RecordError) -> HandlerError {
    HandlerError::new(error.code(), format!("switch record: {error:?}"))
}

async fn read(records: &RecordClient, subject: &str) -> Result<RecordReply, HandlerError> {
    let reply = records.read(subject).await.map_err(record_error)?;
    if reply.stale {
        return Err(HandlerError::new(
            "engram_unavailable",
            "switch record read was stale",
        ));
    }
    Ok(reply)
}

async fn put<T: Serialize>(
    records: &RecordClient,
    subject: String,
    reply: &RecordReply,
    body: &T,
) -> Result<(), HandlerError> {
    let mut write_id = [0; 16];
    getrandom::getrandom(&mut write_id)
        .map_err(|e| HandlerError::new("storage_error", e.to_string()))?;
    records
        .cas(&CasRequest {
            subject,
            expected_version: reply.version,
            write_id,
            body: serde_json::to_vec(body).map_err(crate::invalid_params)?,
        })
        .await
        .map_err(record_error)?;
    Ok(())
}

#[derive(Clone)]
pub(super) struct Switch {
    probe: Arc<dyn Probe>,
    moves: Arc<Mutex<()>>,
    drain: Arc<tokio::sync::Mutex<drain::Progress>>,
}

#[async_trait::async_trait]
pub(super) trait Probe: Send + Sync {
    fn activate(&self);
    async fn head(&self, deadline: Instant) -> Result<u64, HandlerError>;
    async fn local_box(&self, deadline: Instant) -> Result<String, HandlerError>;
    async fn hub_cuts(&self, deadline: Instant) -> Result<bool, HandlerError>;
    fn transport(&self) -> Option<(&Arc<Hub>, &Arc<Callosum>)> {
        None
    }
    #[cfg(test)]
    async fn transfer_snapshot(
        &self,
        _image: Option<&MigrationImage>,
        _deadline: Instant,
    ) -> Option<Result<bool, HandlerError>> {
        None
    }
}

struct DaemonProbe {
    hub: Arc<Hub>,
    discovery: Arc<dyn ParticipantDiscovery>,
    peers: Arc<Callosum>,
}

#[async_trait::async_trait]
impl Probe for DaemonProbe {
    fn activate(&self) {
        self.hub.activate();
    }

    async fn head(&self, deadline: Instant) -> Result<u64, HandlerError> {
        self.hub.session(deadline).await?.log_head(deadline).await
    }

    async fn local_box(&self, deadline: Instant) -> Result<String, HandlerError> {
        Ok(self.hub.box_id(deadline).await?.to_string())
    }

    async fn hub_cuts(&self, deadline: Instant) -> Result<bool, HandlerError> {
        local_hub_cuts(self.discovery.as_ref(), deadline).await
    }

    fn transport(&self) -> Option<(&Arc<Hub>, &Arc<Callosum>)> {
        Some((&self.hub, &self.peers))
    }
}

struct BootstrapTrust;

impl Trust for BootstrapTrust {
    async fn writer_epoch(&self, _deadline: Instant) -> Result<u64, HandlerError> {
        Err(HandlerError::new(
            "epoch_key_unavailable",
            "fleet epoch control is unavailable",
        ))
    }

    async fn verification_key(&self, _record: &UnverifiedRecord) -> Result<String, HandlerError> {
        Err(crate::hub_membership::unknown_key())
    }
}

type DaemonLog = IdentityLog<Hub, BootstrapTrust>;

#[derive(Clone)]
pub(super) struct Runtime {
    pub(super) sql: crate::hub_write::HubSql,
    health: Arc<HealthGauges>,
    writer: Arc<tokio::sync::Mutex<()>>,
    commits: Arc<tokio::sync::Notify>,
    tail: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    shared: Arc<Mutex<Option<Arc<dyn crate::hub_identity_log::SharedLog>>>>,
    residence: crate::hub_residence::Runtime,
}

impl Runtime {
    pub(super) fn new(handler: &ProjectsHandler) -> Self {
        Self {
            sql: handler.hub_sql(),
            health: handler.health.clone(),
            writer: handler.writer.clone(),
            commits: handler.commits.clone(),
            tail: handler.hub_tail_task.clone(),
            shared: handler.hub_shared_log.clone(),
            residence: handler.hub_residence_runtime(),
        }
    }
}

impl Switch {
    #[cfg(test)]
    pub(super) fn with_probe(probe: Arc<dyn Probe>) -> Self {
        Self {
            probe,
            moves: Arc::default(),
            drain: Default::default(),
        }
    }

    pub(super) fn new(
        hub: Arc<Hub>,
        moves: Arc<Mutex<()>>,
        discovery: Arc<dyn ParticipantDiscovery>,
    ) -> Self {
        Self {
            probe: Arc::new(DaemonProbe {
                hub,
                discovery,
                peers: Arc::new(Callosum {
                    connector: SubcConnector::from_args(std::env::args_os().skip(1)),
                    local: Arc::new(LocalIdentity::default()),
                }),
            }),
            moves,
            drain: Default::default(),
        }
    }

    async fn identity_log(
        &self,
        runtime: &Runtime,
        base: i64,
        deadline: Instant,
    ) -> Result<Arc<DaemonLog>, HandlerError> {
        let (hub, peers) = self
            .probe
            .transport()
            .ok_or_else(|| HandlerError::new("hub_unavailable", "hub switch has no transport"))?;
        let connection = crate::log_client::subc_connection_file(std::env::args_os().skip(1))
            .ok_or_else(|| {
                HandlerError::new(
                    "credential_unavailable",
                    "supervised vault connection unavailable",
                )
            })?;
        let box_id = hub.box_id(deadline).await?;
        let vault = hub.fleet_vault(deadline).await?;
        let signer = cortexkit_claustrum_client::ClaustrumCredentialResolver::new(
            connection,
            crate::MODULE_ID,
        );
        Ok(Arc::new(IdentityLog::new(
            runtime.sql.clone(),
            runtime.health.clone(),
            runtime.writer.clone(),
            runtime.commits.clone(),
            box_id,
            base,
            Link {
                transport: hub.clone(),
                trust: Arc::new(BootstrapTrust),
                signer: Arc::new(signer),
                vault: Arc::new(vault),
                peers: peers.clone(),
            },
        )))
    }

    pub(super) async fn start_installed(
        &self,
        runtime: &Runtime,
        base: i64,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        self.probe.activate();
        {
            let tail = runtime.tail.lock().unwrap();
            if tail.as_ref().is_some_and(|tail| !tail.is_finished()) {
                return Ok(());
            }
        }
        let existing = runtime.shared.lock().unwrap().clone();
        if let Some(log) = existing {
            // Residence and epoch watches already use this membership view.
            let tail = log.start_installed_tail(deadline).await?;
            *runtime.tail.lock().unwrap() = Some(tail);
            return Ok(());
        }
        let snapshot = runtime
            .sql
            .run(deadline, "installed_hub_snapshot", |store| {
                Ok(store.installed_hub_snapshot()?)
            })
            .await?;
        let log = self.identity_log(runtime, base, deadline).await?;
        let seeds = Arc::new(snapshot::Seeds::new(snapshot.image));
        let (hub, _) = self.probe.transport().expect("daemon transport");
        let tail = log.start_installed_tail(deadline).await?;
        if let Err(error) =
            runtime
                .residence
                .activate(log.clone(), hub.residence_watch(), seeds.clone())
        {
            tail.abort();
            return Err(error);
        }
        *runtime.tail.lock().unwrap() = Some(tail);
        *runtime.shared.lock().unwrap() = Some(log.clone());
        runtime.residence.start_seeds(log, seeds);
        Ok(())
    }

    pub(super) async fn head(&self, deadline: Instant) -> Result<u64, HandlerError> {
        self.probe.head(deadline).await
    }

    pub(super) async fn finish(
        &self,
        runtime: &Runtime,
        image: Option<&MigrationImage>,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let local = runtime
            .sql
            .run(deadline, "hub_enable_plan", |store| {
                Ok(store.hub_enable_plan()?)
            })
            .await?
            .map(|plan| {
                let snapshot: MigrationImage = serde_json::from_str(&plan.preimage)
                    .map_err(|_| snapshot::mismatch("invalid saved hub enable image"))?;
                if snapshot.migration_id != plan.enable_id
                    || entorhinal_catalog::sha256_hex(plan.preimage.as_bytes())
                        != plan.snapshot_digest
                {
                    return Err(snapshot::mismatch(
                        "saved hub enable image differs from its plan",
                    ));
                }
                Ok(snapshot)
            })
            .transpose()?;
        let image = image.or(local.as_ref());
        if image.is_none() {
            runtime
                .sql
                .run(deadline, "check_enable_preconditions", |store| {
                    Ok(store.check_enable_preconditions(1)?)
                })
                .await?;
        }
        // Keep multipart verification and epoch recovery off the caller's stack.
        if !Box::pin(self.transfer_snapshot(runtime, image, deadline)).await? {
            return Err(waiting());
        }
        // Activation is retried independently after the base has committed.
        runtime
            .health
            .hub_membership
            .store(1, std::sync::atomic::Ordering::Release);
        let health = runtime.health.clone();
        runtime
            .sql
            .run(deadline, "identity_log_status", move |store| {
                crate::enable::refresh(&health, store);
                Ok(())
            })
            .await?;
        runtime.commits.notify_waiters();
        Ok(())
    }

    async fn transfer_snapshot(
        &self,
        runtime: &Runtime,
        image: Option<&MigrationImage>,
        deadline: Instant,
    ) -> Result<bool, HandlerError> {
        #[cfg(test)]
        if let Some(result) = self.probe.transfer_snapshot(image, deadline).await {
            return result;
        }
        let base = image.map_or(0, |image| image.engram_through);
        let log = self.identity_log(runtime, base, deadline).await?;
        snapshot::transfer(&log, image, deadline).await
    }

    pub(super) async fn prepare_enable(
        &self,
        sql: &crate::hub_write::HubSql,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let box_id = self.probe.local_box(deadline).await?;
        sql.run(deadline, "begin_hub_enable", move |store| {
            Ok(store.begin_hub_enable(&crate::hub_write::new_op_id()?, &box_id)?)
        })
        .await
    }

    /// The snapshot verifier can inspect exact stream sequences through this
    /// same hub transport, without consulting engram for backend discovery.
    pub(super) async fn discover(
        &self,
        sql: &crate::hub_write::HubSql,
        deadline: Instant,
    ) -> Result<Startup, HandlerError> {
        if sql
            .run(deadline, "hub_log_base", |store| {
                Ok(store.hub_log_base()?.is_some())
            })
            .await?
        {
            self.probe.activate();
            return Ok(Startup::Hub);
        }
        let head = self.probe.head(deadline).await?;
        sql.run(deadline, "select_hub_backend", move |store| {
            select(
                store,
                if head == 0 {
                    HubHistory::Empty
                } else {
                    HubHistory::Unverified
                },
            )
        })
        .await
    }

    // The legacy hub switch coordinator and its tests are removed together.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) async fn advance(
        &self,
        sql: &crate::hub_write::HubSql,
        log: &LogClient,
        records: &RecordClient,
        deadline: Instant,
    ) -> Result<Option<MigrationImage>, HandlerError> {
        timeout_at(deadline, async {
            if let Some(EngramFreeze::Applied {
                position,
                author,
                body,
            }) = sql
                .run(
                    deadline,
                    "engram_freeze",
                    |store| Ok(store.engram_freeze()?),
                )
                .await?
            {
                return drain::prepare(
                    &self.drain,
                    sql,
                    records,
                    position,
                    &author,
                    body,
                    deadline,
                )
                .await;
            }
            self.gate(sql, log, records, deadline).await?;
            Ok(None)
        })
        .await
        .map_err(|_| HandlerError::new("hub_switch_waiting", "switch deadline expired"))?
    }

    async fn gate(
        &self,
        sql: &crate::hub_write::HubSql,
        log: &LogClient,
        records: &RecordClient,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let Some((status, freeze, author, own_box)) = sql
            .run(deadline, "hub_switch_gate", |store| {
                let status = store.identity_log_status()?;
                if status.state != "enabled"
                    || status.last_applied_position != status.last_seen_head
                {
                    return Ok(None);
                }
                let freeze = store.engram_freeze()?;
                if matches!(&freeze, Some(EngramFreeze::Applied { .. })) {
                    return Ok(None);
                }
                let Some(author) = store.persisted_self_author()? else {
                    return Ok(None);
                };
                let Some(own_box) = store.own_member_box()? else {
                    return Ok(None);
                };
                Ok(Some((status, freeze, author, own_box)))
            })
            .await?
        else {
            return Ok(());
        };
        if self.probe.local_box(deadline).await? != own_box {
            return Err(HandlerError::new(
                "hub_switch_member_mismatch",
                "the local member box does not match the bus credential",
            ));
        }
        if freeze.is_none() {
            let node = Readiness {
                v: 1,
                box_id: own_box.clone(),
                entorhinal_hub: HUB_PROTOCOL,
                engram_hub_cuts: self.probe.hub_cuts(deadline).await?,
            };
            let subject = format!("node:{own_box}");
            let reply = read(records, &subject).await?;
            if reply.self_author != author {
                return Err(HandlerError::new(
                    "hub_switch_member_mismatch",
                    "engram returned a different local author",
                ));
            }
            if reply.author.as_deref() != Some(&author)
                || decoded::<Readiness>(&reply).as_ref() != Some(&node)
            {
                put(records, subject, &reply, &node).await?;
            }
        }

        let reply = read(records, PROPOSAL).await?;
        let proposal = Proposal::from_reply(&reply)?;
        let included: Vec<_> = sql
            .run(deadline, "switch_members", |store| {
                Ok(store.switch_members()?)
            })
            .await?
            .into_iter()
            .filter(|(_, id)| proposal.as_ref().is_none_or(|p| !p.excluded.contains(id)))
            .collect();
        let mut not_ready = BTreeSet::new();
        for (author, id) in &included {
            match records.read(&format!("node:{id}")).await {
                Ok(reply) if member_ready(&reply, author, id) => {}
                _ => {
                    not_ready.insert(id.clone());
                }
            }
        }
        if !not_ready.is_empty() {
            return Err(HandlerError {
                code: "hub_switch_members_not_ready".into(),
                message: format!(
                    "switch readiness missing for {}",
                    not_ready.iter().cloned().collect::<Vec<_>>().join(", ")
                ),
                detail: Some(json!({"boxes":not_ready})),
            });
        }
        if !included
            .iter()
            .any(|(member, id)| member == &author && id == &own_box)
        {
            return Err(HandlerError::new(
                "hub_switch_excluded",
                "the local member is excluded from the switch",
            ));
        }
        let Some(proposal) = proposal else {
            return put(
                records,
                PROPOSAL.into(),
                &reply,
                &Proposal {
                    v: 1,
                    migration_id: crate::hub_write::new_op_id()?,
                    excluded: BTreeSet::new(),
                },
            )
            .await;
        };

        let moves = self.moves.clone();
        let open = sql
            .run(deadline, "pause_legacy_moves", move |store| {
                let _lock = moves.lock().unwrap();
                store.pause_legacy_moves()?;
                Ok(!store.list_open_agent_moves()?.is_empty())
            })
            .await?;
        if open {
            return Ok(());
        }
        let inbox = read(records, &format!("inbox:{author}")).await?;
        if !inbox.absent
            && !entorhinal_core::move_record::decode_inbox_record(
                inbox.body.as_deref().unwrap_or_default(),
            )
            .map_err(|e| HandlerError::new(e.code(), "invalid switch inbox"))?
            .pending
            .is_empty()
        {
            return Ok(());
        }
        let own = Quiet {
            v: 1,
            migration_id: proposal.migration_id.clone(),
            roles: sql
                .run(deadline, "legacy_switch_quiet_roles", |store| {
                    Ok(store.legacy_switch_quiet_roles()?)
                })
                .await?,
        };
        let subject = format!("hubquiet:{author}");
        let quiet = read(records, &subject).await?;
        if quiet.author.as_deref() != Some(&author)
            || decoded::<Quiet>(&quiet).as_ref() != Some(&own)
        {
            put(records, subject, &quiet, &own).await?;
        }
        let mut roles = own.roles;
        for (member, _) in included {
            if member == author {
                continue;
            }
            let reply = read(records, &format!("hubquiet:{member}")).await?;
            let Some(quiet) = quiet_for(&reply, &member, &proposal.migration_id) else {
                return Ok(());
            };
            roles.extend(quiet.roles);
        }
        if read(records, PROPOSAL).await?.version != reply.version
            || self.discover(sql, deadline).await? != Startup::Migrate
        {
            return Ok(());
        }
        let (expected_head, entry_id, body) = match freeze {
            Some(EngramFreeze::Pending {
                expected_head,
                entry_id,
                body,
            }) => (expected_head, entry_id, body),
            _ => {
                let mut entry_id = [0; 16];
                getrandom::getrandom(&mut entry_id)
                    .map_err(|e| HandlerError::new("storage_error", e.to_string()))?;
                let marker = MigrationMarker {
                    op: "log.migrated".into(),
                    hub: true,
                    migration_id: proposal.migration_id.clone(),
                    excluded: proposal.excluded.iter().cloned().collect(),
                    roles: roles.iter().cloned().collect(),
                    tables_digest: String::new(),
                };
                let EngramFreeze::Pending {
                    expected_head,
                    entry_id,
                    body,
                } = sql
                    .run(deadline, "prepare_migration_marker", move |store| {
                        Ok(store.prepare_migration_marker(
                            status.last_applied_position,
                            entry_id,
                            marker,
                        )?)
                    })
                    .await?
                else {
                    return Ok(());
                };
                (expected_head, entry_id, body)
            }
        };
        // The durable freeze survives an unknown result. Catch-up settles it
        // by reading the verified next slot, not by trusting an append reply.
        if expected_head != status.last_applied_position
            || body.migration_id != proposal.migration_id
        {
            return Ok(());
        }
        let request = AppendRequest {
            expected_head: expected_head as u64,
            entry_id,
            kind: EntryKind::Change,
            data: serde_json::to_vec(&body).map_err(crate::invalid_params)?,
        };
        match log.append_migration_marker(&request).await {
            Ok(_) | Err(LogError::NoReply | LogError::HeadMoved { .. }) => Ok(()),
            Err(error) => Err(HandlerError::new(
                "hub_switch_marker_failed",
                format!("marker append: {error:?}"),
            )),
        }
    }
}

pub(super) fn waiting() -> HandlerError {
    HandlerError::new(
        "hub_switch_waiting",
        "waiting for the fleet switch or operator enable",
    )
}

fn quiet_for(reply: &RecordReply, author: &str, migration_id: &str) -> Option<Quiet> {
    (reply.author.as_deref() == Some(author))
        .then(|| decoded::<Quiet>(reply))
        .flatten()
        .filter(|quiet| {
            quiet.v == 1
                && quiet.migration_id == migration_id
                && quiet.roles.len() <= 256
                && quiet
                    .roles
                    .iter()
                    .all(|role| entorhinal_core::role_cache::valid_role(role))
        })
}

impl ProjectsHandler {
    pub(super) async fn exclude_switch_member(
        &self,
        params: Value,
        route: RouteKey,
        received: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Input {
            without: String,
        }
        let Input { without } = serde_json::from_value(params).map_err(crate::invalid_params)?;
        if !box_id(&without) {
            return Err(HandlerError::new(
                "invalid_request",
                "--without requires a 32-digit box ID",
            ));
        }
        self.admit_store_write()?;
        let mut confirmation =
            crate::agent_ops::ConfirmationGuard::take(&self.confirmation_slot, route)?;
        if !self.route_admissions().contains_key(&route) {
            return Err(crate::agent_ops::OperatorConfirmError::Declined.into());
        }
        let deadline = received + crate::shared_write::OPERATOR_BOUND;
        let summary = format!("Switch the registry log to the hub without {without}? Agents held there stay held by engram:<author>. Do not bring this computer back on its old build; upgrade it, re-pair, and join with an empty registry.");
        let peers = Callosum {
            connector: SubcConnector::from_args(std::env::args_os().skip(1)),
            local: Arc::new(LocalIdentity::default()),
        };
        let operation = async {
            self.operator_confirmer
                .confirm_operator(&summary, route)
                .await?;
            let _writer = self.writer.lock().await;
            let own = self
                .served_hub_sql()
                .run(deadline, "validate_switch_exclusion", |store| {
                    if store.identity_log_status()?.state != "enabled" {
                        return Err(HandlerError::new(
                            "identity_log_disabled",
                            "switch exclusions require an enabled legacy log",
                        ));
                    }
                    if store.hub_log_base()?.is_some() || store.engram_freeze()?.is_some() {
                        return Err(pending());
                    }
                    Ok(store.own_member_box()?)
                })
                .await?;
            if own.as_ref() == Some(&without) || peers.lists_box(&without).await? {
                return Err(HandlerError::new(
                    "member_still_paired",
                    format!("remove {without} in Callosum before excluding it"),
                ));
            }
            let reply = read(&self.record_client, PROPOSAL).await?;
            let mut excluded =
                Proposal::from_reply(&reply)?.map_or_else(BTreeSet::new, |p| p.excluded);
            excluded.insert(without);
            let proposal = Proposal {
                v: 1,
                migration_id: crate::hub_write::new_op_id()?,
                excluded,
            };
            put(&self.record_client, PROPOSAL.into(), &reply, &proposal).await?;
            self.catch_up_wake.notify_one();
            crate::encode_result(json!(proposal))
        };
        tokio::select! {
            biased;
            _ = confirmation.cancelled.wait_for(|closed| *closed) => Err(crate::agent_ops::OperatorConfirmError::Declined.into()),
            result = timeout_at(deadline, operation) => result.map_err(|_| HandlerError::from(crate::agent_ops::OperatorConfirmError::Declined))?,
        }
    }
}

#[cfg(test)]
mod tests;
