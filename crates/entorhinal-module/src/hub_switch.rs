//! Publishes the local registry as a snapshot on an empty hub or joins from an
//! existing snapshot. After installation, starts reading new hub log entries.

use std::sync::{Arc, Mutex};

use entorhinal_core::enable_state::HubSnapshot;
use tokio::time::{timeout_at, Instant};

use crate::{
    fleet_record::UnverifiedRecord,
    hub_identity_log::{IdentityLog, Link},
    hub_membership::peers::{Callosum, LocalIdentity},
    hub_transport::Hub,
    hub_write::Trust,
    log_client::SubcConnector,
    participant_discovery::ParticipantDiscovery,
    HandlerError, HealthGauges, ProjectsHandler,
};

pub(super) mod snapshot;
const CUTS: &str = "engram-hub-cuts/v1";
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

#[derive(Clone)]
pub(super) struct Switch {
    probe: Arc<dyn Probe>,
}

#[async_trait::async_trait]
pub(super) trait Probe: Send + Sync {
    fn activate(&self);
    async fn head(&self, deadline: Instant) -> Result<u64, HandlerError>;
    async fn local_box(&self, deadline: Instant) -> Result<String, HandlerError>;
    fn transport(&self) -> Option<(&Arc<Hub>, &Arc<Callosum>)> {
        None
    }
    #[cfg(test)]
    async fn transfer_snapshot(
        &self,
        _image: Option<&HubSnapshot>,
        _deadline: Instant,
    ) -> Option<Result<bool, HandlerError>> {
        None
    }
}

struct DaemonProbe {
    hub: Arc<Hub>,
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
        Self { probe }
    }

    pub(super) fn new(hub: Arc<Hub>) -> Self {
        Self {
            probe: Arc::new(DaemonProbe {
                hub,
                peers: Arc::new(Callosum {
                    connector: SubcConnector::from_args(std::env::args_os().skip(1)),
                    local: Arc::new(LocalIdentity::default()),
                }),
            }),
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
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let local = runtime
            .sql
            .run(deadline, "hub_enable_plan", |store| {
                Ok(store.hub_enable_plan()?)
            })
            .await?
            .map(|plan| {
                let snapshot: HubSnapshot = serde_json::from_str(&plan.preimage)
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
        let image = local.as_ref();
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
        image: Option<&HubSnapshot>,
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
}

pub(super) fn waiting() -> HandlerError {
    HandlerError::new(
        "hub_switch_waiting",
        "waiting for the fleet switch or operator enable",
    )
}

#[cfg(test)]
mod tests;
