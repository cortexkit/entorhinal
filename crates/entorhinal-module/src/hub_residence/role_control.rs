//! Role preference and candidate requests share the role record's watched KV.

use super::*;
use crate::{
    hub_membership::box_id,
    role_leases::{Backend as _, CandidateRequest, Handback, Preference, Preferred},
};

const PREFER: &str = "role.take.prefer";
const FORCE: &str = "role.take.force";

pub(super) fn role_key_name(key: &str) -> Option<&str> {
    key.strip_prefix("role.")
        .or_else(|| key.strip_prefix("roleprefer."))
        .or_else(|| {
            key.strip_prefix("rolehandback.")?
                .rsplit_once('.')
                .map(|(name, _)| name)
        })
}

fn preference_key(role: &str) -> String {
    format!("roleprefer.{role}")
}

fn handback_key(role: &str, box_id: &str) -> String {
    format!("rolehandback.{role}.{box_id}")
}

fn preference(record: &AcceptedKv) -> Result<Preference, HandlerError> {
    let body: Preference =
        serde_json::from_value(record.body.clone()).map_err(crate::invalid_params)?;
    if body.v != 1 || !box_id(&body.box_id) || body.writer != record.record.header().writer {
        return Err(bad_record("invalid role preference"));
    }
    Ok(body)
}

fn handback(record: &AcceptedKv, candidate: &str) -> Result<Handback, HandlerError> {
    let body: Handback =
        serde_json::from_value(record.body.clone()).map_err(crate::invalid_params)?;
    if body.v != 1
        || body.gen <= 0
        || !box_id(candidate)
        || body.box_id != candidate
        || body.box_id != record.record.header().writer
    {
        return Err(bad_record("invalid role handback request"));
    }
    Ok(body)
}

fn record_step(name: &str, key: String, expected: u64, body: Value) -> Step {
    Step {
        name: name.into(),
        payload: Payload::Record {
            kind: key.split('.').next().expect("role key kind").into(),
            body,
            snapshot_id: None,
        },
        key,
        expected,
    }
}

impl Residence {
    pub(super) async fn observe_role_signal(
        &self,
        key: &str,
        role: &str,
        bytes: &[u8],
        revision: u64,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let result = async {
            if !role_leases::valid_role(role) {
                return Err(bad_record("invalid role name"));
            }
            check_kv_size(key, bytes.len())?;
            let accepted = self.backend.verify(key, bytes, deadline).await?;
            if key.starts_with("roleprefer.") {
                preference(&accepted)?;
            } else {
                let candidate = key.rsplit_once('.').expect("role handback key").1;
                handback(&accepted, candidate)?;
            }
            Ok::<_, HandlerError>(accepted)
        }
        .await;
        match result {
            Ok(accepted) => {
                self.accept(key, revision, bytes, Some(&accepted.body), deadline)
                    .await?;
                if let Err(error) = self
                    .backend
                    .record_applied(&accepted.record, revision, deadline)
                    .await
                {
                    tracing::debug!(target: "hub_residence", key, revision, code = error.code, "could not settle applied role control");
                }
                Ok(())
            }
            Err(error) if deferred(&error) => Err(error),
            Err(error) => self.refuse(key, revision, &error.code, deadline).await,
        }
    }

    pub(super) async fn raise_role_gen(
        &self,
        role: &str,
        generation: i64,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let role = role.to_owned();
        self.sql
            .run(deadline, "observe_role_gen", move |store| {
                Ok(store.observe_role_gen(&role, generation)?)
            })
            .await
    }

    async fn cached_signal(
        &self,
        key: &str,
        deadline: Instant,
    ) -> Result<Option<HubKvCached>, HandlerError> {
        let row = self.sql.cached(key, deadline).await?;
        let watched = self.watched.read().unwrap();
        if !watched.initial || watched.held.contains(key) {
            return Ok(None);
        }
        Ok(row.filter(|row| {
            !row.integrity_failed && watched.revisions.get(key) == Some(&row.revision)
        }))
    }

    pub(super) async fn cached_preference(
        &self,
        role: &str,
        deadline: Instant,
    ) -> Result<Option<Preferred>, HandlerError> {
        let Some(row) = self.cached_signal(&preference_key(role), deadline).await? else {
            return Ok(None);
        };
        let Some(body) = row.body else {
            return Ok(None);
        };
        let body: Preference = serde_json::from_value(body).map_err(crate::invalid_params)?;
        Ok(self
            .backend
            .current_member(&body.box_id, self.backend.applied(deadline).await?)
            .map(|episode| Preferred {
                revision: row.revision,
                box_id: body.box_id,
                episode,
            }))
    }

    pub(super) async fn live_preference(
        &self,
        role: &str,
        deadline: Instant,
    ) -> Result<Option<Preferred>, HandlerError> {
        let key = preference_key(role);
        let Some(row) = self.backend.kv_entry(&key, deadline).await? else {
            return Ok(None);
        };
        let Some(value) = row.value else {
            return Ok(None);
        };
        let body = match self
            .backend
            .verify(&key, &value, deadline)
            .await
            .and_then(|record| preference(&record))
        {
            Ok(body) => body,
            Err(error) if deferred(&error) => return Err(error),
            Err(_) => return Ok(None),
        };
        Ok(self
            .backend
            .current_member(&body.box_id, self.backend.applied(deadline).await?)
            .map(|episode| Preferred {
                revision: row.revision,
                box_id: body.box_id,
                episode,
            }))
    }

    pub(super) async fn cached_handback(
        &self,
        role: &str,
        candidate: &str,
        deadline: Instant,
    ) -> Result<Option<CandidateRequest>, HandlerError> {
        let key = handback_key(role, candidate);
        let Some(row) = self.cached_signal(&key, deadline).await? else {
            return Ok(None);
        };
        let Some(body) = row.body else {
            return Ok(None);
        };
        let body: Handback = serde_json::from_value(body).map_err(crate::invalid_params)?;
        let record = crate::fleet_record::UnverifiedRecord::parse(
            &key,
            row.value.as_deref().unwrap_or_default(),
        )?;
        let episode = record.header().writer_ep;
        Ok((self
            .backend
            .current_member(candidate, self.backend.applied(deadline).await?)
            == Some(episode))
        .then_some(CandidateRequest {
            revision: row.revision,
            gen: body.gen,
            episode,
        }))
    }

    pub(super) async fn reread_role_offer(
        &self,
        role: &str,
        target: &Preferred,
        gen: i64,
        deadline: Instant,
    ) -> Result<bool, HandlerError> {
        let prefer_key = preference_key(role);
        let request_key = handback_key(role, &target.box_id);
        let (prefer, request) = tokio::try_join!(
            self.backend.kv_entry(&prefer_key, deadline),
            self.backend.kv_entry(&request_key, deadline),
        )?;
        let (Some(prefer), Some(request)) = (
            prefer.and_then(|entry| entry.value),
            request.and_then(|entry| entry.value),
        ) else {
            return Ok(false);
        };
        let prefer = self.backend.verify(&prefer_key, &prefer, deadline).await?;
        let request = self
            .backend
            .verify(&request_key, &request, deadline)
            .await?;
        Ok(preference(&prefer)?.box_id == target.box_id
            && handback(&request, &target.box_id)?.gen == gen
            && request.record.header().writer_ep == target.episode
            && self
                .backend
                .current_member(&target.box_id, self.backend.applied(deadline).await?)
                == Some(target.episode))
    }

    pub(super) async fn live_handback_revision(
        &self,
        role: &str,
        candidate: &str,
        deadline: Instant,
    ) -> Result<Option<u64>, HandlerError> {
        Ok(self
            .backend
            .kv_entry(&handback_key(role, candidate), deadline)
            .await?
            .and_then(|entry| entry.value.map(|_| entry.revision)))
    }

    pub(super) async fn request_role_handback(
        &self,
        role: &str,
        gen: i64,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let key = handback_key(role, &self.box_id);
        let revision = self
            .backend
            .kv_entry(&key, deadline)
            .await?
            .map_or(0, |row| row.revision);
        let episode = self.backend.writer_episode(deadline).await?;
        let observed = self.role(role, deadline).await?;
        if !observed.ready
            || !observed.holder_current
            || observed.gen() != gen
            || observed.record.as_ref().and_then(|r| r.holder.as_deref())
                == Some(self.box_id.as_str())
            || self
                .cached_preference(role, deadline)
                .await?
                .is_none_or(|p| p.box_id != self.box_id)
        {
            return Ok(());
        }
        let body = Handback {
            v: 1,
            box_id: self.box_id.clone(),
            gen,
        };
        let step = record_step("role.handback.request", key, revision, json!(body));
        self.write_role_signal(&step, episode, deadline).await
    }

    pub(super) async fn delete_role_handback(
        &self,
        role: &str,
        candidate: &str,
        revision: u64,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let step = Step {
            name: "role.handback.delete".into(),
            key: handback_key(role, candidate),
            expected: revision,
            payload: Payload::Delete,
        };
        self.write_role_signal(
            &step,
            self.backend.writer_episode(deadline).await?,
            deadline,
        )
        .await
    }

    async fn write_role_signal(
        &self,
        step: &Step,
        writer_ep: u64,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let request_key = format!("role:{}", hub_write::new_op_id()?);
        let digest = hub_write::params_digest(&step.name, &json!([step.key, step.expected]))?;
        self.backend
            .write(
                &hub_write::Request {
                    request_key: &request_key,
                    params_digest: &digest,
                    writer: &self.box_id,
                    writer_ep,
                },
                step,
                None,
                None,
                deadline,
            )
            .await?;
        Ok(())
    }
}

impl Residence {
    pub(in crate::hub_residence) async fn take_role(
        &self,
        input: &entorhinal_core::role_cache::RoleTakeInput,
        digest: &str,
        deadline: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        let saved = self.sql.attempt(&input.request_id, deadline).await?;
        let (step, writer_ep, forced_gen) = if let Some(saved) = &saved {
            if saved.write.params_digest != digest {
                return Err(HandlerError::new(
                    "request_key_conflict",
                    "request_id has different arguments",
                ));
            }
            let forced_gen = match saved.write.step.as_str() {
                FORCE => {
                    let verified = self
                        .backend
                        .verify(&saved.write.key, &saved.write.value, deadline)
                        .await?;
                    Some(
                        serde_json::from_value::<role_leases::Record>(verified.body)
                            .map_err(crate::invalid_params)?
                            .gen,
                    )
                }
                PREFER => None,
                _ => return Err(bad_record("invalid saved role take step")),
            };
            (
                resume_only(),
                self.request_episode(Some(saved), deadline).await?,
                forced_gen,
            )
        } else {
            let current = self.role(&input.role, deadline).await?;
            if !current.ready {
                return Err(not_ready());
            }
            if current.gen() != input.expected_epoch {
                return Err(HandlerError {
                    code: "epoch_moved".into(),
                    message: "the role epoch moved".into(),
                    detail: Some(
                        json!({"current_epoch": current.gen(), "holder": current.record.and_then(|r| r.holder)}),
                    ),
                });
            }
            let writer_ep = self.backend.writer_episode(deadline).await?;
            let forced = !current.holder_current
                && current.record.as_ref().is_some_and(|r| r.holder.is_some());
            let (step, forced_gen) = if forced {
                let gen = role_leases::next_gen(
                    current.gen(),
                    self.role_high_water(&input.role, deadline).await?,
                )?;
                let mut record = role_leases::Record::new(&self.box_id, writer_ep, gen, false);
                record.forced = true;
                (
                    record_step(
                        FORCE,
                        format!("role.{}", input.role),
                        current.revision,
                        json!(record),
                    ),
                    Some(gen),
                )
            } else {
                let key = preference_key(&input.role);
                let revision = self
                    .backend
                    .kv_entry(&key, deadline)
                    .await?
                    .map_or(0, |entry| entry.revision);
                // This write uses a different key from the role. Check the
                // role's live revision rather than relying on the preference write's revision check.
                let live = self
                    .backend
                    .kv_entry(&format!("role.{}", input.role), deadline)
                    .await?
                    .map_or(0, |entry| entry.revision);
                if live != current.revision {
                    return Err(not_ready());
                }
                let record = Preference {
                    v: 1,
                    box_id: self.box_id.clone(),
                    writer: self.box_id.clone(),
                };
                (record_step(PREFER, key, revision, json!(record)), None)
            };
            (step, writer_ep, forced_gen)
        };
        self.backend
            .write(
                &hub_write::Request {
                    request_key: &input.request_id,
                    params_digest: digest,
                    writer: &self.box_id,
                    writer_ep,
                },
                &step,
                None,
                None,
                deadline,
            )
            .await?;
        // A completed attempt retains its original step, so a preference
        // replay doesn't report that the role subsequently arrived here.
        encode_result(if let Some(gen) = forced_gen {
            self.raise_role_gen(&input.role, gen, deadline).await?;
            json!({"epoch": gen, "held_here": true})
        } else {
            json!({"epoch": input.expected_epoch, "held_here": false, "handback_requested": true})
        })
    }
}

impl ProjectsHandler {
    pub(crate) async fn take_hub_role(
        &self,
        input: entorhinal_core::role_cache::RoleTakeInput,
        route: RouteKey,
        received: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        let service = self.residence().ok_or_else(not_ready)?;
        let digest = hub_write::params_digest(
            crate::roles::TAKE,
            &json!([input.role, input.expected_epoch, input.reason]),
        )?;
        if service
            .sql
            .attempt(&input.request_id, received + BUDGET)
            .await?
            .is_some()
        {
            return service.take_role(&input, &digest, received + BUDGET).await;
        }
        let mut guard = ConfirmationGuard::take(&self.confirmation_slot, route)?;
        if !self.route_admissions().contains_key(&route) {
            return Err(OperatorConfirmError::Declined.into());
        }
        let summary = format!(
            "Take role '{}' at epoch {}: {}",
            input.role, input.expected_epoch, input.reason
        );
        let bound = received + crate::shared_write::OPERATOR_BOUND;
        tokio::select! {
            biased;
            _ = guard.cancelled.wait_for(|closed| *closed) => return Err(OperatorConfirmError::Declined.into()),
            result = timeout_at(bound, self.operator_confirmer.confirm_operator(&summary, route)) => {
                result.map_err(|_| HandlerError::from(OperatorConfirmError::Declined))??;
            }
        }
        if Instant::now() >= bound {
            return Err(OperatorConfirmError::Declined.into());
        }
        tokio::select! {
            biased;
            _ = guard.cancelled.wait_for(|closed| *closed) => Err(OperatorConfirmError::Declined.into()),
            result = service.take_role(&input, &digest, bound.min(Instant::now() + BUDGET)) => result,
        }
    }
}
