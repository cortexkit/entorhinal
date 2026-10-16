//! Entorhinal's package-closure participant: the agent-move participant that
//! carries an agent's catalog package files (manifests and archives) to the
//! receiving Mac. It is machine-local. It never imports pins or publisher
//! permissions. The receiver already has the agent's shared pins (the exact
//! package versions and digests recorded for the agent and replicated to every
//! Mac through the identity log), and it reads only content that matches them.
mod request;
mod stream;
mod trust;

use crate::{
    catalog_blob::BlobStore,
    catalog_consumer, catch_up,
    door_client::{CutItem, CutRequest as DoorCut, DoorClient},
    HandlerError, ProjectsHandler,
};
use cortexkit_role_agent_data::{
    describe::{DescribeReply, Store},
    refs::Ref,
    request::CutRequest,
};
use entorhinal_catalog::{hex, record, sha256_hex};
use entorhinal_core::{
    catalog_agent::{CatalogPin, CatalogSpec, CatalogTarget, PreparedPackage, PreparedPlan},
    RegistryStore,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Weak},
};
use tokio::time::{Duration, Instant};

const KIND: &str = "package-closure";
const FORMAT: &str = "package-closure/v1";
const STREAM: &str = "reserved:entorhinal";
#[cfg(test)]
pub(crate) fn stream_bytes(text: &str) -> Result<Vec<u8>, HandlerError> {
    stream::unhex(text)
}
pub(super) fn invalid(message: impl Into<String>) -> HandlerError {
    HandlerError {
        code: "digest_mismatch".into(),
        message: message.into(),
        detail: None,
    }
}
fn unavailable(error: impl std::fmt::Debug) -> HandlerError {
    HandlerError::new("unavailable", format!("{error:?}"))
}
fn session_ref(agent: &str, cut: &str) -> String {
    sha256_hex(format!("{FORMAT}:{agent}:{cut}").as_bytes())[..32].into()
}
#[derive(Serialize, Deserialize)]
struct Checkpoint {
    agent: String,
    pin_digest: String,
    step: String,
    item: CutItem,
}
pub(crate) struct CatalogMove {
    store: Weak<Mutex<Option<RegistryStore>>>,
    catch_up: catch_up::Worker,
    door: DoorClient,
    consumers: Arc<dyn catalog_consumer::ConsumerDiscovery>,
    lock: Arc<tokio::sync::Mutex<()>>,
    clock: fn() -> i64,
    sources: Arc<crate::catalog_agent::TrustFactory>,
    root: Option<String>,
    request_timeout: Duration,
}
impl ProjectsHandler {
    pub(crate) fn catalog_move_context(&self) -> CatalogMove {
        CatalogMove {
            store: Arc::downgrade(&self.store),
            catch_up: self.catch_up_worker(),
            door: self.door_client.clone(),
            consumers: self.catalog_agent.consumers.clone(),
            lock: self.catalog_move_lock.clone(),
            clock: self.clock,
            sources: self.catalog_agent.sources.clone(),
            root: crate::catalog_trust::CORTEXKIT_ROOT.map(str::to_owned),
            request_timeout: Duration::from_secs(30),
        }
    }
}
impl CatalogMove {
    #[cfg(test)]
    pub(crate) fn with_test_root(mut self, root: String) -> Self {
        self.root = Some(root);
        self
    }
    #[cfg(test)]
    pub(crate) fn with_test_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }
    fn store(&self) -> Result<RegistryStore, HandlerError> {
        self.store
            .upgrade()
            .and_then(|s| s.lock().unwrap().clone())
            .ok_or_else(|| unavailable("catalog store is gone"))
    }
    fn directory(&self, cut: &str) -> Result<PathBuf, HandlerError> {
        self.catalog_path("moves", cut)
    }
    fn catalog_path(&self, section: &str, cut: &str) -> Result<PathBuf, HandlerError> {
        if !crate::door_client::valid_cut_id(cut) {
            return Err(HandlerError::new("invalid_request", "invalid cut id"));
        }
        Ok(self
            .store()?
            .store_folder()
            .join("catalog")
            .join(section)
            .join(cut))
    }
    fn load(&self, cut: &str) -> Result<Option<Checkpoint>, HandlerError> {
        match fs::read(
            self.catalog_path("move-checkpoints", cut)?
                .with_extension("json"),
        ) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(unavailable),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(unavailable(e)),
        }
    }
    fn save(&self, cut: &str, state: &Checkpoint) -> Result<(), HandlerError> {
        let _write = request::CheckpointWrite::take(self.directory(cut)?)?;
        if state.step != "aborted" {
            self.not_aborted(cut)?;
        }
        let path = self
            .catalog_path("move-checkpoints", cut)?
            .with_extension("json");
        let dir = path.parent().expect("catalog paths always have a parent");
        private_dir(dir)?;
        let temporary = path.with_extension("tmp");
        let mut file = File::create(&temporary).map_err(unavailable)?;
        file.write_all(&serde_json::to_vec(state).map_err(unavailable)?)
            .map_err(unavailable)?;
        file.sync_all().map_err(unavailable)?;
        fs::rename(temporary, &path).map_err(unavailable)?;
        #[cfg(unix)]
        File::open(dir)
            .and_then(|f| f.sync_all())
            .map_err(unavailable)?;
        Ok(())
    }
    /// Compare the source's pin digest (`expected`) with this Mac's pin digest
    /// for the agent. Only on a mismatch, first catch up: read new entries from
    /// the shared identity log and apply them, so newer pin changes arrive
    /// locally. Then check again. A log outage therefore cannot block a move
    /// whose pins already match.
    pub(crate) async fn preflight(
        &self,
        agent: &str,
        expected: &str,
    ) -> Result<Vec<CatalogPin>, HandlerError> {
        tokio::time::timeout(self.request_timeout, self.preflight_inner(agent, expected))
            .await
            .map_err(|_| unavailable("catalog move preflight deadline exceeded"))?
    }
    async fn preflight_inner(
        &self,
        agent: &str,
        expected: &str,
    ) -> Result<Vec<CatalogPin>, HandlerError> {
        let store = self.store()?;
        let local_agent = agent.to_owned();
        let local = crate::catalog_sql::run(move || {
            store
                .catalog_pin_digest(&local_agent)
                .map_err(HandlerError::from)
        })
        .await?;
        if local != expected {
            self.catch_up.for_move().await?;
        }
        self.local_preflight(agent, expected).await.map(|p| p.pins)
    }
    async fn local_preflight(
        &self,
        agent: &str,
        expected: &str,
    ) -> Result<trust::Preflight, HandlerError> {
        let store = self.store()?;
        let local_agent = agent.to_owned();
        let pins = crate::catalog_sql::run(move || {
            store.catalog_pins(&local_agent).map_err(HandlerError::from)
        })
        .await?;
        let actual = entorhinal_core::catalog_agent::pin_digest(&pins).map_err(unavailable)?;
        if actual != expected {
            return Err(HandlerError {
                code: "pin_digest_mismatch".into(),
                message: "target pins do not match the admitted move".into(),
                detail: Some(json!({"source_pin_digest":expected,"receiver_pin_digest":actual})),
            });
        }
        let checked = self.current_trust(pins).await?;
        self.coverage(agent, &checked, None).await?;
        Ok(checked)
    }
    async fn expected(&self, cut: &str, agent: &str) -> Result<String, HandlerError> {
        let store = self.store()?;
        let cut = cut.to_owned();
        let agent = agent.to_owned();
        crate::catalog_sql::run(move || {
            let row = store.agent_move(&cut)?.ok_or_else(|| {
                HandlerError::new("invalid_request", "catalog move is not admitted")
            })?;
            if row.agent_id != agent {
                return Err(HandlerError::new(
                    "id_reused",
                    "cut belongs to another agent",
                ));
            }
            row.plan["pin_digest"]
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| HandlerError::new("invalid_request", "move has no pinned closure"))
        })
        .await
    }
    pub(crate) async fn execute(&self, method: &str, params: Value) -> Result<Value, HandlerError> {
        let op = cortexkit_role_agent_data::Op::ALL
            .into_iter()
            .find(|op| op.method() == method)
            .ok_or_else(|| HandlerError::new("invalid_request", "unknown agent-data operation"))?;
        tokio::time::timeout(self.request_timeout, self.execute_inner(method, params))
            .await
            .unwrap_or_else(|_| Err(unavailable("catalog move request deadline exceeded")))
            .map_err(|mut error| {
                if matches!(error.code.as_str(), "trust_missing" | "pin_digest_mismatch") {
                    error.detail = Some(json!({"reason":error.code,"detail":error.detail}));
                    error.code = "invalid_request".into();
                }
                if !cortexkit_role_agent_data::errors::allowed_refusals(op).contains(
                    &cortexkit_role_agent_data::errors::RefusalCode::from_code(&error.code),
                ) {
                    error.code = "unavailable".into();
                }
                error
            })
    }
    async fn execute_inner(&self, method: &str, params: Value) -> Result<Value, HandlerError> {
        if method == "agent_data.describe" {
            let _: cortexkit_role_agent_data::describe::DescribeRequest =
                serde_json::from_value(params)
                    .map_err(|e| HandlerError::new("invalid_request", e.to_string()))?;
            return Ok(serde_json::to_value(DescribeReply::new(
                vec![KIND.into()],
                vec![Store::new(FORMAT, "agent")],
                false,
            ))
            .unwrap());
        }
        if method == "agent_data.busy" {
            let request: cortexkit_role_agent_data::busy::BusyRequest =
                serde_json::from_value(params)
                    .map_err(|e| HandlerError::new("invalid_request", e.to_string()))?;
            for r in request.refs.as_slice() {
                check_ref(r)?;
            }
            return Ok(
                json!({"refs":request.refs.as_slice().iter().map(|_| json!({"state":"idle"})).collect::<Vec<_>>()}),
            );
        }
        let cut: CutRequest = if method == "agent_data.release" {
            let release: cortexkit_role_agent_data::release::ReleaseRequest =
                serde_json::from_value(params)
                    .map_err(|e| HandlerError::new("invalid_request", e.to_string()))?;
            CutRequest::new(release.refs, release.cut_id)
        } else {
            serde_json::from_value(params)
                .map_err(|e| HandlerError::new("invalid_request", e.to_string()))?
        };
        let refs = cut.refs.as_slice();
        if refs.len() != 1 {
            return Err(HandlerError::new(
                "invalid_request",
                "one package closure required",
            ));
        }
        let agent = check_ref(&refs[0])?;
        let id = cut.cut_id.as_str();
        let mut saved = self.load(id)?;
        if method == "agent_data.status" {
            if saved.as_ref().is_some_and(|s| s.agent != agent) {
                return Err(HandlerError::new(
                    "invalid_request",
                    "cut belongs to another ref",
                ));
            }
            return Ok(json!({"step":saved.map_or("none".into(), |s| s.step)}));
        }
        let _admission = if method == "agent_data.abort" {
            None
        } else {
            Some(request::Admission::take(self.directory(id)?)?)
        };
        let expected = self.expected(id, agent).await?;
        if method != "agent_data.abort" {
            self.not_aborted(id)?;
        }
        if saved
            .as_ref()
            .is_some_and(|s| s.agent != agent || s.pin_digest != expected)
        {
            return Err(HandlerError::new("id_reused", "catalog cut changed"));
        }
        self.advance(method, id, agent, &expected, &mut saved).await
    }
}
impl CatalogMove {
    async fn advance(
        &self,
        method: &str,
        id: &str,
        agent: &str,
        expected: &str,
        saved: &mut Option<Checkpoint>,
    ) -> Result<Value, HandlerError> {
        match method {
            "agent_data.prepare" => {
                if saved.is_none() {
                    let store = self.store()?;
                    let local_agent = agent.to_owned();
                    let digest = expected.to_owned();
                    let pins = crate::catalog_sql::run(move || {
                        let pins = store.catalog_pins(&local_agent)?;
                        if store.catalog_pin_digest(&local_agent)? != digest {
                            return Err(HandlerError::new(
                                "invalid_state",
                                "source pins changed after admission",
                            ));
                        }
                        Ok(pins)
                    })
                    .await?;
                    let store = self.store()?;
                    let dir = self.directory(id)?;
                    private_dir(&dir)?;
                    let (end_offset, digest) = stream::encode(
                        &dir.join("payload"),
                        &BlobStore::new(store.store_folder())?,
                        &pins,
                    )
                    .map_err(|e| HandlerError::new("not_exportable", e.message))?;
                    let state = Checkpoint {
                        agent: agent.into(),
                        pin_digest: expected.into(),
                        step: "prepared".into(),
                        item: CutItem {
                            session_ref: session_ref(agent, id),
                            stream: STREAM.into(),
                            end_offset,
                            digest,
                        },
                    };
                    self.save(id, &state)?;
                    *saved = Some(state);
                }
                let state = saved.as_ref().unwrap();
                if !matches!(state.step.as_str(), "prepared" | "exported") {
                    return Err(HandlerError::new(
                        "invalid_state",
                        "cut is not a source cut",
                    ));
                }
                if state.step == "prepared" {
                    self.upload(id, &state.item).await?;
                }
                Ok(
                    json!({"items":[{"ref_kind":KIND,"ref":agent,"sessionRef":state.item.session_ref,"stream":STREAM,"endOffset":state.item.end_offset,"digest":state.item.digest,"format":FORMAT,"frozen":true}],"external":[]}),
                )
            }
            "agent_data.export" => {
                let state = saved.as_mut().ok_or_else(|| {
                    HandlerError::new("cut_unknown", "prepare the catalog cut first")
                })?;
                if !matches!(state.step.as_str(), "prepared" | "exported") {
                    return Err(HandlerError::new("invalid_state", "cut is not prepared"));
                }
                if state.step == "prepared" {
                    self.cover(&state.item).await?;
                    state.step = "exported".into();
                    self.save(id, state)?;
                }
                Ok(
                    json!({"items":[{"sessionRef":state.item.session_ref,"endOffset":state.item.end_offset}]}),
                )
            }
            "agent_data.diverged" => {
                self.local_preflight(agent, expected).await?;
                Ok(json!({"refs":[{"state":"at_cut"}]}))
            }
            "agent_data.import" => {
                let checked = self.local_preflight(agent, expected).await?;
                if saved
                    .as_ref()
                    .is_some_and(|s| matches!(s.step.as_str(), "imported" | "activated"))
                {
                    let item = &saved.as_ref().unwrap().item;
                    return Ok(
                        json!({"items":[{"sessionRef":item.session_ref,"endOffset":item.end_offset,"digest":item.digest}]}),
                    );
                }
                if saved.is_some() {
                    return Err(HandlerError::new("invalid_state", "cut cannot import"));
                }
                let read = self
                    .door
                    .read(&DoorCut { cut_id: id.into() })
                    .await
                    .map_err(|_| {
                        HandlerError::new("cut_not_ready", "catalog cut is not readable")
                    })?;
                let item = read
                    .items
                    .into_iter()
                    .find(|i| i.session_ref == session_ref(agent, id) && i.stream == STREAM)
                    .ok_or_else(|| invalid("cut has no package closure"))?;
                let dir = self.directory(id)?;
                private_dir(&dir)?;
                let stage = dir.join("staging");
                private_dir(&stage)?;
                self.download(id, &item, &dir.join("payload")).await?;
                stream::decode(&dir.join("payload"), &stage, &checked.pins)?;
                let checked = self.local_preflight(agent, expected).await?;
                let packages = staged_packages(&stage, &checked)?;
                self.coverage(agent, &checked, Some(&packages)).await?;
                self.install(id, agent, &stage, packages).await?;
                let state = Checkpoint {
                    agent: agent.into(),
                    pin_digest: expected.into(),
                    step: "imported".into(),
                    item,
                };
                self.save(id, &state)?;
                let reply = json!({"items":[{"sessionRef":state.item.session_ref,"endOffset":state.item.end_offset,"digest":state.item.digest}]});
                *saved = Some(state);
                Ok(reply)
            }
            "agent_data.activate" => {
                self.local_preflight(agent, expected).await?;
                let state = saved.as_mut().ok_or_else(|| {
                    HandlerError::new("cut_unknown", "catalog cut is not imported")
                })?;
                if !matches!(state.step.as_str(), "imported" | "activated") {
                    return Err(HandlerError::new(
                        "invalid_state",
                        "catalog cut is not imported",
                    ));
                }
                state.step = "activated".into();
                self.save(id, state)?;
                Ok(json!({}))
            }
            "agent_data.abort" => {
                let guard = self.admit().await?;
                *saved = self.load(id)?;
                if saved
                    .as_ref()
                    .is_some_and(|s| matches!(s.step.as_str(), "activated" | "released"))
                {
                    return Err(HandlerError::new(
                        "invalid_state",
                        "catalog cut cannot abort",
                    ));
                }
                let state = saved.get_or_insert_with(|| Checkpoint {
                    agent: agent.into(),
                    pin_digest: expected.into(),
                    step: "none".into(),
                    item: CutItem {
                        session_ref: session_ref(agent, id),
                        stream: STREAM.into(),
                        end_offset: 0,
                        digest: sha256_hex([]),
                    },
                });
                state.step = "aborted".into();
                self.save(id, state)?;
                let store = self.store()?;
                let cut = id.to_owned();
                let local_agent = agent.to_owned();
                crate::catalog_sql::run(move || {
                    let _guard = guard;
                    store
                        .catalog_discard_move_generation(&cut, &local_agent)
                        .map_err(unavailable)
                })
                .await?;
                Ok(json!({}))
            }
            "agent_data.release" => {
                let state = saved
                    .as_mut()
                    .ok_or_else(|| HandlerError::new("cut_unknown", "catalog cut is unknown"))?;
                if !matches!(state.step.as_str(), "exported" | "released") {
                    return Err(HandlerError::new(
                        "invalid_state",
                        "catalog cut cannot release",
                    ));
                }
                state.step = "released".into();
                self.save(id, state)?;
                Ok(json!({"enforced":true}))
            }
            _ => Err(HandlerError::new(
                "invalid_request",
                "unknown agent-data operation",
            )),
        }
    }
    async fn cover(&self, item: &CutItem) -> Result<(), HandlerError> {
        let cover = self
            .door
            .session("session.cover", json!({"sessionRef":item.session_ref}))
            .await
            .map_err(unavailable)?;
        if cover["durableOffset"].as_u64() != Some(item.end_offset)
            || cover["cloudOffset"].as_u64().unwrap_or(0) < item.end_offset
        {
            return Err(unavailable(
                "catalog stream is not covered in the cloud yet",
            ));
        }
        Ok(())
    }
    async fn upload(&self, id: &str, item: &CutItem) -> Result<(), HandlerError> {
        let mut file = File::open(self.directory(id)?.join("payload")).map_err(unavailable)?;
        let mut offset = 0;
        let mut buffer = vec![0; stream::ADMIT_CAP];
        loop {
            let n = file.read(&mut buffer).map_err(unavailable)?;
            if n == 0 {
                break;
            }
            let bytes = &buffer[..n];
            match self
                .door
                .session(
                    "session.admit",
                    json!({"sessionRef":item.session_ref,"fromOffset":offset,"bytes":hex(bytes)}),
                )
                .await
            {
                Ok(reply) if reply["durableOffset"].as_u64() == Some(offset + n as u64) => {}
                Err(crate::door_client::DoorError::Refused { code, detail })
                    if code == "offset_mismatch"
                        && detail
                            .as_ref()
                            .and_then(|d| d["expectedOffset"].as_u64())
                            .is_some_and(|end| {
                                end >= offset + n as u64 && end <= item.end_offset
                            }) =>
                {
                    let reply = self
                        .door
                        .session(
                            "session.read",
                            json!({"sessionRef":item.session_ref,"fromOffset":offset,"limit":n}),
                        )
                        .await
                        .map_err(unavailable)?;
                    if reply["bytes"].as_str() != Some(hex(bytes).as_str()) {
                        return Err(HandlerError::new(
                            "invalid_state",
                            "admitted catalog prefix differs",
                        ));
                    }
                }
                other => return Err(unavailable(format!("catalog admission: {other:?}"))),
            }
            offset += n as u64;
        }
        self.cover(item).await
    }
    async fn download(&self, id: &str, item: &CutItem, path: &Path) -> Result<(), HandlerError> {
        if item.end_offset > 26 + 512 * (40 + stream::BLOB_CAP) {
            return Err(invalid("catalog stream exceeds closure cap"));
        }
        let mut file = File::create(path).map_err(unavailable)?;
        let mut hash = Sha256::new();
        let mut offset = 0;
        while offset < item.end_offset {
            let limit = (item.end_offset - offset).min(stream::ADMIT_CAP as u64);
            let reply = self.door.session("session.read", json!({"sessionRef":item.session_ref,"fromOffset":offset,"limit":limit,"cutId":id})).await.map_err(unavailable)?;
            if reply["fromOffset"].as_u64() != Some(offset)
                || reply["endOffset"].as_u64() != Some(offset + limit)
                || reply["streamEnd"].as_u64() != Some(item.end_offset)
                || reply["digest"].as_str() != Some(item.digest.as_str())
            {
                return Err(invalid("cut read metadata differs"));
            }
            let bytes = stream::unhex(
                reply["bytes"]
                    .as_str()
                    .ok_or_else(|| invalid("cut read has no bytes"))?,
            )?;
            if bytes.len() as u64 != limit {
                return Err(invalid("short catalog read"));
            }
            hash.update(&bytes);
            file.write_all(&bytes).map_err(unavailable)?;
            offset += limit;
        }
        if hex(&hash.finalize()) != item.digest {
            let mut error = invalid("catalog cut hash differs");
            error.detail = Some(json!({"reason":"staging_digest_mismatch"}));
            return Err(error);
        }
        file.sync_all().map_err(unavailable)?;
        Ok(())
    }
    async fn admit(&self) -> Result<tokio::sync::OwnedMutexGuard<()>, HandlerError> {
        tokio::time::timeout(Duration::from_secs(1), self.lock.clone().lock_owned())
            .await
            .map_err(|_| unavailable("catalog move admission deadline exceeded"))
    }
    fn not_aborted(&self, cut: &str) -> Result<(), HandlerError> {
        if self.load(cut)?.is_some_and(|s| s.step == "aborted") {
            return Err(HandlerError::new(
                "invalid_state",
                "catalog cut was aborted",
            ));
        }
        Ok(())
    }
    async fn install(
        &self,
        cut: &str,
        agent: &str,
        stage: &Path,
        packages: Vec<PreparedPackage>,
    ) -> Result<(), HandlerError> {
        let plan_id = format!("move:{cut}");
        let generation_id = format!("generation:{plan_id}");
        let store = self.store()?;
        let stage = stage.to_path_buf();
        let worker_packages = packages.clone();
        crate::catalog_sql::run(move || {
            let blobs = BlobStore::new(store.store_folder())?;
            let pins: Vec<_> = worker_packages.iter().map(|p| p.pin.clone()).collect();
            for digest in stream::digests(&pins) {
                let bytes = fs::read(stage.join(&digest)).map_err(unavailable)?;
                if sha256_hex(&bytes) != digest {
                    return Err(invalid("staged blob hash differs from pin"));
                }
                blobs.publish(&bytes)?;
            }
            Ok::<_, HandlerError>(())
        })
        .await?;
        let consumers = catalog_consumer::discover(self.consumers.as_ref(), &packages)
            .await
            .map_err(unavailable)?;
        let checks = catalog_consumer::check(&consumers, &plan_id, &packages)
            .await
            .map_err(unavailable)?;
        let worker_plan = plan_id.clone();
        let worker_generation = generation_id.clone();
        let agent_id = agent.to_owned();
        let clock = self.clock;
        let worker_packages = packages.clone();
        let worker_checks = checks.clone();
        let digest = entorhinal_core::catalog_agent::pin_digest(
            &packages.iter().map(|p| p.pin.clone()).collect::<Vec<_>>(),
        )
        .map_err(unavailable)?;
        self.local_preflight(agent, &digest).await?;
        let guard = self.admit().await?;
        self.not_aborted(cut)?;
        let store = self.store()?;
        crate::catalog_sql::run(move || {
            // The blocking write may outlive a cancelled request. Keep admission
            // until the write finishes so abort cannot race a late approval.
            let _guard = guard;
            let plan_id = worker_plan;
            let generation_id = worker_generation;
            let agent = agent_id.as_str();
            let packages = worker_packages;
            let checks = worker_checks;
            let pins: Vec<_> = packages.iter().map(|p| p.pin.clone()).collect();
            let first = pins
                .first()
                .ok_or_else(|| invalid("empty catalog closure"))?;
            store
                .catalog_create_plan(
                    &plan_id,
                    agent,
                    CatalogTarget::Agent(agent.into()),
                    "install",
                    CatalogSpec {
                        package: first.name.clone(),
                        range: format!("={}", first.version),
                    },
                    clock(),
                )
                .map_err(unavailable)?;
            if store
                .catalog_plan(&plan_id, agent)
                .map_err(unavailable)?
                .state
                == "pending"
            {
                store
                    .catalog_save_prepared(
                        &plan_id,
                        agent,
                        &PreparedPlan {
                            packages: packages.clone(),
                            moves: json!([]),
                            checks: checks.clone(),
                        },
                    )
                    .map_err(unavailable)?;
                store
                    .catalog_plan_ready(&plan_id, agent, pins, clock())
                    .map_err(unavailable)?;
            }
            store
                .catalog_approve_plan(&plan_id, agent, &generation_id, clock())
                .map_err(unavailable)?;
            Ok::<_, HandlerError>(())
        })
        .await?;
        catalog_consumer::prefetch(
            self.consumers.as_ref(),
            &generation_id,
            &packages,
            &checks,
            Instant::now() + Duration::from_secs(25),
        )
        .await
        .map_err(unavailable)?;
        self.local_preflight(agent, &digest).await?;
        let guard = self.admit().await?;
        self.not_aborted(cut)?;
        let store = self.store()?;
        let agent = agent.to_owned();
        crate::catalog_sql::run(move || {
            let _guard = guard;
            store
                .catalog_generation_ready(&generation_id, &agent)
                .map_err(unavailable)
        })
        .await?;
        Ok(())
    }
}
fn staged_packages(
    stage: &Path,
    checked: &trust::Preflight,
) -> Result<Vec<PreparedPackage>, HandlerError> {
    checked
        .pins
        .iter()
        .map(|pin| {
            let manifest_bytes = fs::read(stage.join(&pin.manifest_sha256)).map_err(unavailable)?;
            let manifest =
                record::parse_manifest(&manifest_bytes).map_err(|e| invalid(e.to_string()))?;
            let archive = fs::read(stage.join(&pin.archive_sha256)).map_err(unavailable)?;
            let members =
                entorhinal_catalog::archive::read(&archive).map_err(|e| invalid(e.to_string()))?;
            if manifest.name != pin.name
                || manifest.version != pin.version
                || manifest.kind != pin.kind
                || members.get("ck.json").map(sha256_hex) != Some(pin.manifest_sha256.clone())
            {
                return Err(invalid("manifest identity differs from pins"));
            }
            let scripts = &manifest.body["scripts"];
            Ok(PreparedPackage {
                pin: pin.clone(),
                trust: "trusted".into(),
                capabilities: manifest.requires.capabilities,
                has_scripts: scripts.as_object().is_some_and(|s| !s.is_empty())
                    || scripts.as_array().is_some_and(|s| !s.is_empty()),
                root_certified: checked.certified.contains(&pin.name),
            })
        })
        .collect()
}
fn check_ref(r: &Ref) -> Result<&str, HandlerError> {
    if r.ref_kind != KIND || r.r#ref.is_empty() {
        return Err(HandlerError::new(
            "invalid_request",
            "invalid package closure ref",
        ));
    }
    Ok(&r.r#ref)
}
fn private_dir(path: &Path) -> Result<(), HandlerError> {
    fs::create_dir_all(path).map_err(unavailable)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(unavailable)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{HandlerOutcome, RouteAdmission};
    use subc_protocol::Principal;
    #[tokio::test]
    async fn catalog_move_participant_is_declared_and_rejects_unattested_callers() {
        let manifest = serde_json::to_value(crate::manifest()).unwrap();
        assert!(manifest["capabilities"]["provides"]
            .as_array()
            .unwrap()
            .contains(&json!(cortexkit_role_agent_data::PROVIDES)));
        assert!(manifest["provides"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["service_id"] == cortexkit_role_agent_data::SERVICE_ID));
        let handler = ProjectsHandler::with_runtime("catalog-route".into(), || 0);
        let request =
            serde_json::to_vec(&json!({"method":"agent_data.describe","params":{}})).unwrap();
        for (principal, flow, allowed) in [
            (Principal::Direct, false, false),
            (
                Principal::Reserved {
                    module_id: "other".into(),
                },
                false,
                false,
            ),
            (
                Principal::Reserved {
                    module_id: "entorhinal".into(),
                },
                true,
                false,
            ),
            (
                Principal::Reserved {
                    module_id: "entorhinal".into(),
                },
                false,
                true,
            ),
        ] {
            let mut admission = RouteAdmission::from_bind(Some(principal), None);
            if flow {
                admission.flow_id = Some("flow".into());
            }
            handler.route_admissions().insert((7, 1), admission);
            let reply = handler.handle_served_request(&request, (7, 1)).await;
            if allowed {
                let HandlerOutcome::Response(body) = reply else {
                    panic!("attested caller refused")
                };
                let value: Value = serde_json::from_slice(&body).unwrap();
                let description: DescribeReply =
                    serde_json::from_value(value["result"].clone()).unwrap();
                assert_eq!(description.ref_kinds, vec![KIND]);
            } else {
                assert!(
                    !matches!(reply, HandlerOutcome::Response(_)),
                    "unattested participant caller admitted"
                );
            }
        }
    }
}
