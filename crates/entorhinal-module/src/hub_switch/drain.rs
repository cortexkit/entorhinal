use super::{decoded, put, read, record_error, HandlerError};
use crate::{
    claim_codec::{AgentClaim, RoleClaim},
    record_client::{CasRequest, RecordClient, RecordError, RecordReply, MANY_CAP},
};
use entorhinal_core::{
    remote_apply::MigrationMarker,
    shared_entry::{MigrationSource, SharedRow},
    RegistryStore,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tokio::sync::Mutex;

/// Snapshot preimage in JSON Canonicalization Scheme (JCS) key order:
/// engram_through, excluded, image,
/// migration_id, tables. The image contains claims (ordered by agent_id) and
/// roles (ordered by role). Claim keys are agent_id, generation, holder;
/// role keys are generation, holder, role.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub(crate) struct MigrationImage {
    pub engram_through: i64,
    pub excluded: Vec<String>,
    pub image: ClaimsAndRoles,
    pub migration_id: String,
    pub tables: BTreeMap<String, Vec<SharedRow>>,
}

#[derive(Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct ClaimsAndRoles {
    pub claims: Vec<Claim>,
    pub roles: Vec<Role>,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
pub(crate) struct Claim {
    pub agent_id: String,
    pub generation: i64,
    pub holder: Option<String>,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
pub(crate) struct Role {
    pub generation: i64,
    pub holder: Option<String>,
    pub role: String,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Drained {
    migration_id: String,
}

impl Drained {
    fn matches(&self, reply: &RecordReply, author: &str) -> bool {
        reply.author.as_deref() == Some(author) && decoded::<Self>(reply).as_ref() == Some(self)
    }
}

#[derive(Default)]
pub(super) struct Progress {
    marker: Option<(i64, String, String)>,
    next: usize,
    pending: Option<CasRequest>,
}

enum Key<'a> {
    Claim(&'a str),
    Role(&'a str),
}

impl Key<'_> {
    fn subject(&self) -> String {
        match self {
            Self::Claim(id) => format!("claim:{id}"),
            Self::Role(role) => format!("role:{role}"),
        }
    }

    fn unclaimed(&self) -> Result<Vec<u8>, HandlerError> {
        match self {
            Self::Claim(id) => serde_json::to_vec(&AgentClaim {
                v: 1,
                agent_id: (*id).into(),
                holder: None,
                epoch: 0,
                cut_id: None,
                reason: None,
            }),
            Self::Role(role) => serde_json::to_vec(&RoleClaim {
                v: 1,
                role: (*role).into(),
                holder: None,
                epoch: 0,
                reason: "hub switch fence".into(),
            }),
        }
        .map_err(crate::invalid_params)
    }

    fn collect(
        &self,
        reply: RecordReply,
        source: &MigrationSource,
        image: &mut ClaimsAndRoles,
    ) -> Result<(), HandlerError> {
        if reply.stale {
            return Err(HandlerError::new(
                "engram_unavailable",
                "switch image read was stale",
            ));
        }
        let invalid = |code| {
            HandlerError::new(
                code,
                format!("invalid switch image record {}", self.subject()),
            )
        };
        let map =
            |holder: Option<String>| holder.as_deref().map(|author| source.map_author(author));
        match self {
            Self::Claim(id) => {
                let (holder, generation) = match reply.body {
                    None => (None, 0),
                    Some(body) => {
                        let claim = AgentClaim::decode(id, &body).map_err(invalid)?;
                        (map(claim.holder), claim.epoch)
                    }
                };
                image.claims.push(Claim {
                    agent_id: (*id).into(),
                    holder,
                    generation,
                });
            }
            Self::Role(role) => {
                let (holder, generation) = match reply.body {
                    None => (None, 0),
                    Some(body) => {
                        let claim = RoleClaim::decode(role, &body).map_err(invalid)?;
                        (map(claim.holder), claim.epoch)
                    }
                };
                image.roles.push(Role {
                    role: (*role).into(),
                    holder,
                    generation,
                });
            }
        }
        Ok(())
    }
}

async fn fence(
    records: &RecordClient,
    keys: &[Key<'_>],
    progress: &mut Progress,
) -> Result<(), HandlerError> {
    while let Some(key) = keys.get(progress.next) {
        if progress.pending.is_none() {
            let subject = key.subject();
            let reply = read(records, &subject).await?;
            let mut write_id = [0; 16];
            getrandom::getrandom(&mut write_id)
                .map_err(|e| HandlerError::new("storage_error", e.to_string()))?;
            progress.pending = Some(CasRequest {
                subject,
                expected_version: reply.version,
                body: match reply.body {
                    Some(body) => body,
                    None => key.unclaimed()?,
                },
                write_id,
            });
        }
        match records.cas_drain(progress.pending.as_ref().unwrap()).await {
            Ok(_) => {
                progress.next += 1;
                progress.pending = None;
            }
            Err(error) if error.code() == "version_moved" => progress.pending = None,
            Err(error) => {
                // A later refresh can resend the same unknown attempt, including
                // when the refresh deadline cut short the first send.
                if !matches!(error, RecordError::NoReply) {
                    progress.pending = None;
                }
                return Err(record_error(error));
            }
        }
    }
    Ok(())
}

async fn read_image(
    records: &RecordClient,
    source: &MigrationSource,
    keys: &[Key<'_>],
) -> Result<ClaimsAndRoles, HandlerError> {
    let mut image = ClaimsAndRoles::default();
    for batch in keys.chunks(MANY_CAP) {
        let subjects: Vec<_> = batch.iter().map(Key::subject).collect();
        let mut offset = 0;
        loop {
            // read_many validates that the entries describe exactly the
            // answered prefix and next is the first unanswered index.
            let (entries, next) = records
                .read_many(&subjects[offset..])
                .await
                .map_err(record_error)?;
            for (key, entry) in batch[offset..].iter().zip(entries) {
                key.collect(entry.map_err(record_error)?, source, &mut image)?;
            }
            match next {
                Some(next) => offset += next,
                None => break,
            }
        }
    }
    Ok(image)
}

pub(super) async fn prepare(
    progress: &Mutex<Progress>,
    store: &RegistryStore,
    records: &RecordClient,
    position: i64,
    marker_author: &str,
    marker: MigrationMarker,
) -> Result<Option<MigrationImage>, HandlerError> {
    let Some(author) = store.persisted_self_author()? else {
        return Err(HandlerError::new(
            "hub_switch_waiting",
            "the local engram author is not known",
        ));
    };
    let source = store.capture_migration_source(position, marker.excluded.clone())?;
    if !source.included_authors().any(|member| member == author) {
        return Err(HandlerError::new(
            "hub_switch_excluded",
            "the local member is excluded from the switch",
        ));
    }
    let keys: Vec<_> = source
        .agents()
        .map(Key::Claim)
        .chain(marker.roles.iter().map(|role| Key::Role(role)))
        .collect();
    let drained = Drained {
        migration_id: marker.migration_id,
    };
    let own_subject = format!("hubdrained:{author}");
    let mut progress = progress.lock().await;
    let reply = read(records, &own_subject).await?;
    if reply.self_author != author {
        return Err(HandlerError::new(
            "hub_switch_member_mismatch",
            "engram returned a different local author",
        ));
    }
    if !drained.matches(&reply, &author) {
        let marker = (position, drained.migration_id.clone(), author.clone());
        if progress.marker.as_ref() != Some(&marker) {
            *progress = Progress {
                marker: Some(marker),
                ..Progress::default()
            };
        }
        fence(records, &keys, &mut progress).await?;
        put(records, own_subject, &reply, &drained).await?;
    }

    // The marker writer froze before sending it. This drain fenced its earlier
    // sends too, so a surviving computer does not need that writer's receipt.
    for member in source
        .included_authors()
        .filter(|member| *member != author && *member != marker_author)
    {
        let reply = read(records, &format!("hubdrained:{member}")).await?;
        if !drained.matches(&reply, member) {
            return Ok(None);
        }
    }
    let image = read_image(records, &source, &keys).await?;
    Ok(Some(MigrationImage {
        engram_through: position,
        excluded: marker.excluded,
        image,
        migration_id: drained.migration_id,
        tables: source.into_mapped_tables()?,
    }))
}
