use base64::{engine::general_purpose::STANDARD, Engine};
use entorhinal_catalog::{is_hex, jcs, sha256_hex};
use entorhinal_core::{enable_state::HubSnapshot, remote_apply::RemoteEntry};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use tokio::time::Instant;

use crate::{
    fleet_record::LOG_KEY,
    hub_epoch::Controller,
    hub_identity_log::{IdentityLog, LogTransport},
    hub_residence::SnapshotClaims,
    hub_write::{Payload, Step, Trust, PROJECTION_SNAPSHOT_KIND},
    HandlerError,
};

const CHUNK: usize = 180 * 1024;

pub(super) fn mismatch(reason: impl Into<String>) -> HandlerError {
    HandlerError::new("hub_migration_mismatch", reason.into())
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Part {
    v: u8,
    id: String,
    part: usize,
    parts: usize,
    engram_through: i64,
    excluded: Vec<String>,
    snapshot_digest: String,
    chunk: String,
}

fn entry_id(id: &str, part: usize) -> String {
    let digest = sha256_hex(format!("hub-snapshot-v1:{id}:{part}").as_bytes());
    digest[..32].to_owned()
}

struct Framed {
    digest: String,
    bodies: Vec<String>,
}

impl Framed {
    fn new(snapshot: &HubSnapshot) -> Result<Self, HandlerError> {
        let preimage = snapshot.preimage()?;
        let digest = sha256_hex(preimage.as_bytes());
        let count = preimage.len().div_ceil(CHUNK);
        let bodies = preimage
            .as_bytes()
            .chunks(CHUNK)
            .enumerate()
            .map(|(i, chunk)| {
                let body = serde_json::to_value(Part {
                    v: 1,
                    id: snapshot.migration_id.clone(),
                    part: i,
                    parts: count,
                    engram_through: snapshot.engram_through,
                    excluded: snapshot.excluded.clone(),
                    snapshot_digest: digest.clone(),
                    chunk: STANDARD.encode(chunk),
                })
                .map_err(|error| mismatch(error.to_string()))?;
                jcs::to_string(&body).map_err(|error| mismatch(error.to_string()))
            })
            .collect::<Result<_, _>>()?;
        Ok(Self { digest, bodies })
    }
}

#[test]
fn hub_cutover_snapshot_sample() {
    use entorhinal_core::enable_state::{HubImage, HubRole};
    let snapshot = HubSnapshot {
        engram_through: 17,
        excluded: vec!["12".repeat(16)],
        image: HubImage {
            claims: vec![],
            roles: vec![
                HubRole {
                    generation: 4,
                    holder: Some("ab".repeat(16)),
                    role: "connector_home".into(),
                },
                HubRole {
                    generation: 7,
                    holder: Some(format!("engram:{}", "cd".repeat(16))),
                    role: "dreamer.global".into(),
                },
            ],
        },
        migration_id: "56".repeat(16),
        tables: BTreeMap::new(),
    };
    assert!(valid_image(&snapshot));
    let framed = Framed::new(&snapshot).unwrap();
    assert_eq!(framed.bodies.len(), 1);
    crate::served_vectors::published_file(
        "hub-snapshot-v1",
        "part.json",
        vec![format!(
            "  {{\"entry_id\":{},\"body\":{}}}",
            json!(entry_id(&snapshot.migration_id, 0)),
            framed.bodies[0]
        )],
    );
}

fn valid_image(snapshot: &HubSnapshot) -> bool {
    let ids = snapshot
        .tables
        .get("agent")
        .into_iter()
        .flatten()
        .map(|row| row.get("agent_id").and_then(Value::as_str));
    let claims = &snapshot.image.claims;
    let roles = &snapshot.image.roles;
    let valid_holder = |holder: Option<&str>| {
        holder.is_none_or(|id| {
            is_hex(id, 32)
                || id
                    .strip_prefix("engram:")
                    .is_some_and(|author| is_hex(author, 32))
        })
    };
    snapshot.engram_through >= 0
        && is_hex(&snapshot.migration_id, 32)
        && snapshot.excluded.iter().all(|id| is_hex(id, 32))
        && snapshot.excluded.windows(2).all(|pair| pair[0] < pair[1])
        && claims
            .windows(2)
            .all(|pair| pair[0].agent_id < pair[1].agent_id)
        && ids.eq(claims.iter().map(|row| Some(row.agent_id.as_str())))
        && claims.iter().all(|row| {
            entorhinal_core::agent::valid_agent_id(&row.agent_id)
                && row.generation >= 0
                && (row.generation > 0) == row.holder.is_some()
                && valid_holder(row.holder.as_deref())
        })
        && roles.windows(2).all(|pair| pair[0].role < pair[1].role)
        && roles.iter().all(|row| {
            crate::role_leases::valid_role(&row.role)
                && row.generation >= 0
                && (row.generation > 0 || row.holder.is_none())
                && valid_holder(row.holder.as_deref())
        })
}

struct Prefix {
    parts: Vec<(RemoteEntry, u64)>,
    complete: Option<HubSnapshot>,
}

async fn open_prefix<T: LogTransport, U: Trust + Send + Sync + 'static>(
    log: &IdentityLog<T, U>,
    expected: Option<&Framed>,
    head: u64,
    deadline: Instant,
) -> Result<Prefix, HandlerError> {
    let mut parts = Vec::new();
    let mut preimage = Vec::new();
    let mut prior_epoch = 0;
    let mut meta: Option<(String, usize, i64, Vec<String>, String)> = None;
    for sequence in 1..=head {
        let (signer, record, plain) = log.open_bootstrap_part(sequence, deadline).await?;
        let header = record.header();
        let part: Part =
            serde_json::from_slice(&plain).map_err(|_| mismatch("invalid snapshot part"))?;
        let epoch = record
            .epoch()
            .ok_or_else(|| mismatch("snapshot part is not sealed"))?;
        if epoch < prior_epoch
            || header.kind != PROJECTION_SNAPSHOT_KIND
            || part.v != 1
            || part.part != parts.len()
            || part.parts <= part.part
            || !is_hex(&part.id, 32)
            || !is_hex(&part.snapshot_digest, 64)
            || header.entry_id != entry_id(&part.id, part.part)
            || expected.is_some_and(|frame| {
                frame.bodies.get(part.part).map(String::as_bytes) != Some(&plain)
            })
        {
            return Err(mismatch("snapshot part differs from the expected prefix"));
        }
        prior_epoch = epoch;
        let identity = (
            part.id.clone(),
            part.parts,
            part.engram_through,
            part.excluded,
            part.snapshot_digest.clone(),
        );
        if meta.get_or_insert_with(|| identity.clone()) != &identity {
            return Err(mismatch("snapshot part metadata changed"));
        }
        let chunk = STANDARD
            .decode(&part.chunk)
            .map_err(|_| mismatch("invalid snapshot chunk"))?;
        if STANDARD.encode(&chunk) != part.chunk
            || chunk.is_empty()
            || chunk.len() > CHUNK
            || (part.part + 1 < part.parts && chunk.len() != CHUNK)
        {
            return Err(mismatch("snapshot chunk has the wrong length or encoding"));
        }
        preimage.extend_from_slice(&chunk);
        let position = i64::try_from(sequence)
            .ok()
            .and_then(|n| part.engram_through.checked_add(n))
            .ok_or_else(|| mismatch("snapshot position is out of range"))?;
        parts.push((
            RemoteEntry {
                position,
                entry_id: header.entry_id.clone(),
                author: header.writer.clone(),
                signer,
                key_id: String::new(),
                envelope_version: 1,
                kind: "snapshot".into(),
                entry: plain,
            },
            header.writer_ep,
        ));
        if part.part + 1 == part.parts {
            let snapshot: HubSnapshot = serde_json::from_slice(&preimage)
                .map_err(|_| mismatch("invalid snapshot preimage"))?;
            if !valid_image(&snapshot)
                || snapshot.preimage()?.as_bytes() != preimage
                || part.engram_through != snapshot.engram_through
                || part.id != snapshot.migration_id
                || identity.3 != snapshot.excluded
                || sha256_hex(&preimage) != part.snapshot_digest
            {
                return Err(mismatch("snapshot preimage does not match its framing"));
            }
            return Ok(Prefix {
                parts,
                complete: Some(snapshot),
            });
        }
    }
    Ok(Prefix {
        parts,
        complete: None,
    })
}

pub(crate) async fn transfer<T: LogTransport, U: Trust + Send + Sync + 'static>(
    log: &std::sync::Arc<IdentityLog<T, U>>,
    expected: Option<&HubSnapshot>,
    deadline: Instant,
) -> Result<bool, HandlerError> {
    let frame = expected.map(Framed::new).transpose()?;
    if expected.is_some_and(|snapshot| !valid_image(snapshot)) {
        return Err(mismatch("local snapshot image is invalid"));
    }
    loop {
        let head = log.log_head(deadline).await?;
        log.discover_bootstrap_membership(head, deadline).await?;
        if head > 0 {
            log.ensure_bootstrap_key(deadline).await?;
        }
        let prefix = open_prefix(log, frame.as_ref(), head, deadline).await?;
        if let Some(snapshot) = prefix.complete {
            if expected.is_some_and(|image| *image != snapshot) {
                return Err(mismatch("snapshot differs from the agreed local image"));
            }
            if log.store.hub_enable_plan()?.is_some() {
                Seeds::new(snapshot.image.clone())
                    .write(log, deadline)
                    .await?;
            }
            log.store
                .install_hub_snapshot(&snapshot, &prefix.parts, &log.box_id.to_string())?;
            return Ok(true);
        }
        let (Some(expected), Some(frame)) = (expected, frame.as_ref()) else {
            return Ok(false);
        };
        if head == 0 {
            Controller::new(log.clone())
                .ok_or_else(|| mismatch("epoch controller unavailable"))?
                .bootstrap(deadline)
                .await?;
        }
        for (i, plain) in frame.bodies.iter().enumerate().skip(prefix.parts.len()) {
            let id = entry_id(&expected.migration_id, i);
            let body =
                serde_json::from_str(plain).map_err(|_| mismatch("invalid local snapshot part"))?;
            match log
                .write_epoch_step(
                    &format!("hub.snapshot:{id}"),
                    &frame.digest,
                    &Step {
                        name: "bootstrap".into(),
                        key: LOG_KEY.into(),
                        expected: i as u64,
                        payload: Payload::Record {
                            kind: PROJECTION_SNAPSHOT_KIND.into(),
                            body,
                            snapshot_id: Some(id),
                        },
                    },
                    deadline,
                )
                .await
            {
                Ok(_) => (),
                Err(error) if error.code == "head_moved" => break,
                Err(error) => return Err(error),
            }
        }
        // A concurrent publisher can fill the same slot. Read and verify the
        // response at its position before accepting it or writing another part.
        if Instant::now() >= deadline {
            return Err(HandlerError::new(
                "hub_unavailable",
                "snapshot transfer deadline expired",
            ));
        }
        tokio::task::yield_now().await;
    }
}

type Holders = BTreeMap<String, (Option<String>, i64)>;

pub(crate) struct Seeds {
    claims: Holders,
    roles: Holders,
    keys: Vec<String>,
}

impl Seeds {
    pub(crate) fn new(image: entorhinal_core::enable_state::HubImage) -> Self {
        let keys = image
            .claims
            .iter()
            .filter(|row| row.generation > 0)
            .map(|row| format!("residence.{}", row.agent_id))
            .chain(
                image
                    .roles
                    .iter()
                    .filter(|row| row.generation > 0)
                    .map(|row| format!("role.{}", row.role)),
            )
            .collect();
        Self {
            keys,
            claims: image
                .claims
                .into_iter()
                .map(|row| (row.agent_id, (row.holder, row.generation)))
                .collect(),
            roles: image
                .roles
                .into_iter()
                .map(|row| (row.role, (row.holder, row.generation)))
                .collect(),
        }
    }

    pub(crate) async fn write<T: LogTransport, U: Trust + Send + Sync + 'static>(
        &self,
        log: &IdentityLog<T, U>,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        for key in &self.keys {
            let existing = log.kv_entry(key, deadline).await?;
            if existing.is_some_and(|entry| entry.value.is_some()) {
                continue;
            }
            let (kind, name) = key.split_once('.').expect("seed key has a kind");
            let (holder, generation) = if kind == "residence" {
                &self.claims[name]
            } else {
                &self.roles[name]
            };
            let mut body = json!({"v":1,"holder":holder,"holder_ep":0,"gen":generation,
                "writer":log.box_id.to_string(),"seeded":true});
            if kind == "residence" {
                body["forced"] = false.into();
            } else {
                body["lease_ms"] = crate::role_leases::LEASE_MS.into();
            }
            let digest = crate::hub_write::params_digest(key, &body)?;
            match log
                .write_epoch_step(
                    &format!("hub.seed:{key}"),
                    &digest,
                    &Step {
                        name: "seed".into(),
                        key: key.clone(),
                        expected: 0,
                        payload: Payload::Record {
                            kind: kind.into(),
                            body,
                            snapshot_id: None,
                        },
                    },
                    deadline,
                )
                .await
            {
                Ok(_) => (),
                Err(error)
                    if error.code == "hub_write_superseded"
                        && log
                            .kv_entry(key, deadline)
                            .await?
                            .is_some_and(|entry| entry.value.is_some()) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}

impl SnapshotClaims for Seeds {
    fn claim(&self, agent: &str) -> Option<(Option<String>, i64)> {
        self.claims.get(agent).cloned()
    }

    fn role(&self, name: &str) -> Option<(Option<String>, i64)> {
        self.roles.get(name).cloned()
    }

    fn hub_ready(&self) -> bool {
        true
    }

    fn required_keys(&self) -> &[String] {
        &self.keys
    }
}
