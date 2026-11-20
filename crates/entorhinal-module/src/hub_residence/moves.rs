//! Move coordination on the same watched, authenticated KV as residence.

use super::*;
use crate::hub_moves::{self as moves, Move, Node, Participant, SeenMove, State};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Position {
    Source,
    Receiver,
    Returned,
    Forced,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Transfer {
    expected: u64,
    body: Record,
}

impl Residence {
    pub(crate) fn move_box(&self) -> &str {
        &self.box_id
    }

    pub(crate) async fn move_member(&self, box_id: &str) -> Result<Option<u64>, HandlerError> {
        if !self.ready() {
            return Err(not_ready());
        }
        Ok(self
            .backend
            .current_member(box_id, self.backend.applied(Instant::now() + BUDGET).await?))
    }

    async fn move_cached(&self, key: &str) -> Result<Option<HubKvCached>, HandlerError> {
        if !self.ready() {
            return Err(not_ready());
        }
        let row = self.sql.cached(key, Instant::now() + BUDGET).await?;
        let watched = self.watched.read().unwrap();
        if !watched.initial || watched.held.contains(key) {
            return Err(not_ready());
        }
        if let Some(row) = &row {
            if row.integrity_failed {
                return Err(bad_record("fleet KV key has an integrity mark"));
            }
            if watched.revisions.get(key) != Some(&row.revision) {
                return Err(not_ready());
            }
        }
        Ok(row)
    }

    pub(crate) async fn watched_move(&self, cut: &str) -> Result<Option<SeenMove>, HandlerError> {
        let key = moves::move_key(cut);
        self.move_cached(&key)
            .await?
            .map(|row| {
                let body = row.body.ok_or_else(moves::conflict)?;
                let record =
                    UnverifiedRecord::parse(&key, row.value.as_deref().unwrap_or_default())?;
                Ok(SeenMove {
                    body: serde_json::from_value(body).map_err(crate::invalid_params)?,
                    revision: row.revision,
                    writer: record.header().writer.clone(),
                    writer_ep: record.header().writer_ep,
                })
            })
            .transpose()
    }

    pub(crate) async fn watched_moves(&self) -> Vec<SeenMove> {
        let cuts: Vec<_> = self
            .watched
            .read()
            .unwrap()
            .revisions
            .keys()
            .filter_map(|key| key.strip_prefix("move.").map(str::to_owned))
            .collect();
        let mut seen = Vec::new();
        for cut in cuts {
            if let Ok(Some(entry)) = self.watched_move(&cut).await {
                seen.push(entry);
            }
        }
        seen
    }

    pub(crate) async fn watched_node(&self, box_id: &str) -> Result<Option<Node>, HandlerError> {
        let key = moves::node_key(box_id);
        let Some(row) = self.move_cached(&key).await? else {
            return Ok(None);
        };
        let Some(body) = row.body else {
            return Ok(None);
        };
        let writer = UnverifiedRecord::parse(&key, row.value.as_deref().unwrap_or_default())?;
        if self.move_member(box_id).await? != Some(writer.header().writer_ep) {
            return Ok(None);
        }
        Ok(Some(
            serde_json::from_value(body).map_err(crate::invalid_params)?,
        ))
    }

    pub(super) async fn judge_move_signal(
        &self,
        key: &str,
        bytes: &[u8],
        old: Option<&HubKvCached>,
        deadline: Instant,
    ) -> Result<AcceptedKv, HandlerError> {
        check_kv_size(key, bytes.len())?;
        let accepted = self.backend.verify(key, bytes, deadline).await?;
        let header = accepted.record.header();
        if let Some(cut) = key.strip_prefix("move.") {
            // A deleted cut id cannot be reused, even with a different plan.
            if old.is_some_and(|row| row.body.is_none() && row.accepted_revision.is_some()) {
                return Err(moves::conflict());
            }
            let body: Move =
                serde_json::from_value(accepted.body.clone()).map_err(crate::invalid_params)?;
            let previous = old
                .and_then(|row| row.body.clone())
                .map(serde_json::from_value)
                .transpose()
                .map_err(crate::invalid_params)?;
            let source_can_abort = if body.state == State::Aborted && header.writer == body.source {
                let residence = key_for_move(&body);
                let has_transfer = |r: &Value| {
                    r["gen"].as_i64() == body.gen_n.checked_add(1)
                        && r["cut_id"].as_str() == Some(body.cut_id.as_str())
                };
                let watched_transfer = self
                    .sql
                    .cached(&residence, deadline)
                    .await?
                    .and_then(|row| row.body)
                    .is_some_and(|r| has_transfer(&r));
                if watched_transfer {
                    false
                } else if let Some(bytes) = self
                    .backend
                    .kv_fence_entry(&residence, deadline)
                    .await?
                    .and_then(|row| row.value)
                {
                    // A watch can skip the handing revision and lag behind the residence write.
                    !has_transfer(
                        &self
                            .backend
                            .verify(&residence, &bytes, deadline)
                            .await?
                            .body,
                    )
                } else {
                    true
                }
            } else {
                false
            };
            body.judge(cut, &header.writer, previous.as_ref(), source_can_abort)?;
        } else if let Some(box_id) = key.strip_prefix("node.") {
            let body: Node =
                serde_json::from_value(accepted.body.clone()).map_err(crate::invalid_params)?;
            if body.v != 1
                || body.box_id != box_id
                || header.writer != box_id
                || !crate::hub_membership::box_id(box_id)
                || body
                    .participants
                    .windows(2)
                    .any(|pair| pair[0].module >= pair[1].module)
                || body.participants.iter().any(|p| {
                    p.module.is_empty() || p.ref_kinds.windows(2).any(|pair| pair[0] >= pair[1])
                })
            {
                return Err(bad_record("invalid participant advertisement"));
            }
        }
        Ok(accepted)
    }

    pub(crate) async fn move_origin(&self, agent: &str) -> Result<(i64, String), HandlerError> {
        let Some((record, _)) = self.move_residence(agent).await? else {
            return Err(HandlerError::new(
                "not_holder",
                "claim the agent before moving it",
            ));
        };
        if record.holder != self.box_id
            || self.move_member(&self.box_id).await? != Some(record.holder_ep)
        {
            return Err(HandlerError::new(
                "not_holder",
                "residence is held elsewhere",
            ));
        }
        Ok((record.gen, self.box_id.clone()))
    }

    async fn move_residence(&self, agent: &str) -> Result<Option<(Record, u64)>, HandlerError> {
        let Some(row) = self.move_cached(&key(agent)).await? else {
            return Ok(None);
        };
        let body = row.body.ok_or_else(moves::conflict)?;
        Ok(Some((
            serde_json::from_value(body).map_err(crate::invalid_params)?,
            row.revision,
        )))
    }

    pub(crate) async fn move_position(&self, body: &Move) -> Result<Position, HandlerError> {
        let (r, _) = self
            .move_residence(&body.agent_id)
            .await?
            .ok_or_else(moves::conflict)?;
        position(body, &r)
    }

    pub(crate) async fn move_receiver_holds(&self, body: &Move) -> Result<bool, HandlerError> {
        let (r, _) = self
            .move_residence(&body.agent_id)
            .await?
            .ok_or_else(moves::conflict)?;
        Ok(self.box_id == body.receiver
            && position(body, &r)? == Position::Receiver
            && self.move_member(&r.holder).await? == Some(r.holder_ep))
    }

    // The move and residence records can arrive in either order. A current
    // hub read distinguishes a lagging watch from records that disagree.
    pub(crate) async fn move_live_position(&self, body: &Move) -> Result<Position, HandlerError> {
        let key = key_for_move(body);
        let deadline = Instant::now() + BUDGET;
        let bytes = self
            .backend
            .kv_fence_entry(&key, deadline)
            .await?
            .and_then(|row| row.value)
            .ok_or_else(moves::conflict)?;
        let r = self.backend.verify(&key, &bytes, deadline).await?;
        position(
            body,
            &serde_json::from_value(r.body).map_err(crate::invalid_params)?,
        )
    }

    pub(crate) async fn plan_transfer(
        &self,
        body: &Move,
        return_to_episode: Option<u64>,
    ) -> Result<Transfer, HandlerError> {
        let (previous, expected) = self
            .move_residence(&body.agent_id)
            .await?
            .ok_or_else(moves::conflict)?;
        let (sender, target, current) = if return_to_episode.is_some() {
            (&body.receiver, &body.source, Position::Receiver)
        } else {
            (&body.source, &body.receiver, Position::Source)
        };
        if sender != &self.box_id
            || previous.holder != self.box_id
            || self.move_member(sender).await? != Some(previous.holder_ep)
            || position(body, &previous)? != current
        {
            return Err(moves::conflict());
        }
        let target_episode = match return_to_episode {
            Some(episode) => episode,
            None => self.move_member(target).await?.ok_or_else(|| {
                HandlerError::new(
                    "participant_unavailable_on_target",
                    "move peer is not a current member",
                )
            })?,
        };
        let mut next = Record::new(
            target,
            target_episode,
            previous.gen.checked_add(1).ok_or_else(moves::conflict)?,
            false,
        );
        next.writer.clone_from(sender);
        if return_to_episode.is_none() {
            next.cut_id = Some(body.cut_id.clone());
        }
        Ok(Transfer {
            expected,
            body: next,
        })
    }

    pub(crate) async fn transfer_move(
        &self,
        body: &Move,
        intent: &Transfer,
        returning: bool,
    ) -> Result<(), HandlerError> {
        let name = if returning {
            "move.return"
        } else {
            "move.transfer"
        };
        self.write_move_step(
            &body.cut_id,
            intent
                .body
                .clone()
                .step(name, key_for_move(body), intent.expected),
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn write_move(
        &self,
        body: &Move,
        old: Option<&SeenMove>,
    ) -> Result<u64, HandlerError> {
        body.check_send(old.map(|old| old.body.state), self.box_id == body.source)?;
        if self.box_id != body.source && self.box_id != body.receiver {
            return Err(moves::conflict());
        }
        if let Some(old) = old {
            old.body.pin(body)?;
        }
        self.write_move_step(
            &body.cut_id,
            Step {
                name: format!("move.{}", body.state.name()),
                key: moves::move_key(&body.cut_id),
                expected: old.map_or(0, |old| old.revision),
                payload: Payload::Record {
                    kind: "move".into(),
                    body: json!(body),
                    snapshot_id: None,
                },
            },
        )
        .await
    }

    async fn write_move_step(&self, id: &str, step: Step) -> Result<u64, HandlerError> {
        let body = match &step.payload {
            Payload::Record { body, .. } => body.clone(),
            Payload::Delete => Value::Null,
        };
        let digest = hub_write::params_digest(&step.name, &json!([step.key, step.expected, body]))?;
        let request_key = format!("{}:{id}:{}", step.name, step.expected);
        let request = hub_write::Request {
            request_key: &request_key,
            params_digest: &digest,
            writer: &self.box_id,
            writer_ep: self.backend.writer_episode(Instant::now() + BUDGET).await?,
        };
        let result = self
            .backend
            .write(&request, &step, None, None, Instant::now() + BUDGET)
            .await?;
        // A settled delete may have no useful new position.
        Ok(result.position.unwrap_or_default())
    }

    pub(crate) async fn advertise_node(
        &self,
        participants: Vec<Participant>,
        engram_hub_cuts: bool,
    ) -> Result<(), HandlerError> {
        let key = moves::node_key(&self.box_id);
        let previous = self.move_cached(&key).await?;
        if self.watched_node(&self.box_id).await?.is_some_and(|node| {
            node.participants == participants && node.engram_hub_cuts == engram_hub_cuts
        }) {
            return Ok(());
        }
        let expected = previous.as_ref().map_or(0, |row| row.revision);
        // Reuse the recorded timestamp and payload on retry, including a restart.
        let request_key = format!("node.advertise:{}:{expected}", self.box_id);
        let deadline = Instant::now() + BUDGET;
        let body = if let Some(attempt) = self.sql.attempt(&request_key, deadline).await? {
            self.backend
                .verify(&key, &attempt.write.value, deadline)
                .await?
                .body
        } else {
            json!(Node {
                v: 1,
                box_id: self.box_id.clone(),
                participants,
                engram_hub_cuts,
                written_at: (self.clock)()
            })
        };
        self.write_move_step(
            &self.box_id,
            Step {
                name: "node.advertise".into(),
                key,
                expected,
                payload: Payload::Record {
                    kind: "node".into(),
                    body,
                    snapshot_id: None,
                },
            },
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn reap_move(
        &self,
        seen: &SeenMove,
        source_done: bool,
        now_ms: i64,
    ) -> Result<(), HandlerError> {
        if !seen.body.state.terminal() {
            return Ok(());
        }
        let key = moves::move_key(&seen.body.cut_id);
        let Some(created) = self.watched.read().unwrap().move_times.get(&key).copied() else {
            return Ok(());
        };
        let age = now_ms.saturating_sub(created);
        if age < moves::WEEK
            && !(source_done && seen.body.source == self.box_id && age >= moves::HOUR)
        {
            return Ok(());
        }
        self.write_move_step(
            &seen.body.cut_id,
            Step {
                name: "move.delete".into(),
                key,
                expected: seen.revision,
                payload: Payload::Delete,
            },
        )
        .await?;
        Ok(())
    }
}

fn key_for_move(body: &Move) -> String {
    key(&body.agent_id)
}

fn position(body: &Move, r: &Record) -> Result<Position, HandlerError> {
    if r.gen == body.gen_n && r.holder == body.source {
        Ok(Position::Source)
    } else if r.gen == body.gen_n + 1
        && r.holder == body.receiver
        && r.cut_id.as_deref() == Some(&body.cut_id)
        && !r.forced
        && r.writer == body.source
    {
        Ok(Position::Receiver)
    } else if r.gen == body.gen_n + 2
        && r.holder == body.source
        && r.cut_id.is_none()
        && !r.forced
        && r.writer == body.receiver
    {
        Ok(Position::Returned)
    } else if r.gen == body.gen_n + 1 && r.forced {
        Ok(Position::Forced)
    } else {
        Err(moves::conflict())
    }
}
