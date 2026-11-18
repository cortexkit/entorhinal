//! Receiver-side decisions for a move coordinated by the fleet KV.

use super::*;
use crate::door_client::{DoorError, TakeoverState};

fn conflict() -> Failure {
    let mut failure = Failure::hold("move_state_conflict", json!({"op":"hub.move"}));
    failure.violation = true;
    failure
}

fn cut_failure(op: &str, error: DoorError, claimed: bool) -> Failure {
    let mut failure = Failure::hold(error.code(), json!({"op":op,"code":error.code()}));
    if let DoorError::Refused { message, .. } = &error {
        failure.refusal = Some(Box::new(PeerRefusal {
            message: message.clone(),
        }));
    }
    if !error.retryable() {
        failure.action = Some(DriverAction::TerminalForStep);
        if !claimed {
            failure.reason = Some(MoveReason::DoorRefused);
        }
    }
    failure
}

impl Receiver {
    pub(super) fn read_hub_move(
        &self,
        checkpoint: &SeenMove,
    ) -> Result<(Arc<Residence>, SeenMove), Failure> {
        let hub = self
            .worker
            .hub()
            .ok_or_else(|| Failure::hold("hub_not_ready", json!({"op":"hub.move"})))?;
        let seen = hub.watched_move(&self.cut)?.ok_or_else(conflict)?;
        checkpoint.body.pin(&seen.body).map_err(|_| conflict())?;
        Ok((hub, seen))
    }

    async fn fetch_cut(
        &self,
        row: &mut AgentMove,
        progress: &mut Progress,
        published: &SeenMove,
    ) -> Result<bool, Failure> {
        if progress.hub_items.is_some() {
            return Ok(true);
        }
        let cut = DoorCut {
            cut_id: self.cut.clone(),
        };
        let previous = if progress.takeover_sent {
            match self.worker.door.status(&cut).await {
                Ok(status) => status.takeover,
                Err(DoorError::Refused { code, .. }) if code == "not_found" => None,
                Err(error) => return Err(cut_failure("cut.status", error, false)),
            }
        } else {
            None
        };
        let (state, code) = match previous {
            Some(status) if status.state != TakeoverState::NotStarted => {
                (status.state, status.code)
            }
            _ => {
                progress.takeover_sent = true;
                self.save(row, progress).await?;
                if row.abort_requested.is_some() {
                    return Ok(false);
                }
                let reply = self
                    .worker
                    .door
                    .takeover_hub(&self.cut, &published.writer, &published.body.cut)
                    .await
                    .map_err(|error| cut_failure("cut.takeover", error, false))?;
                (reply.takeover, reply.code)
            }
        };
        match state {
            TakeoverState::Taken => {
                let read = self
                    .worker
                    .door
                    .read(&cut)
                    .await
                    .map_err(|error| cut_failure("cut.read", error, false))?;
                if read.author != published.writer {
                    return Err(Failure::abort(
                        MoveReason::CutAuthorMismatch,
                        json!({"op":"cut.read"}),
                    ));
                }
                progress.hub_items = Some(read.items);
                self.save(row, progress).await?;
                Ok(true)
            }
            TakeoverState::Failed => {
                let Some(code) = code else {
                    return Err(Failure::abort(
                        MoveReason::DoorRefused,
                        json!({"op":"cut.takeover","code":"failed"}),
                    ));
                };
                let failure = cut_failure(
                    "cut.takeover",
                    DoorError::Refused {
                        code,
                        detail: None,
                        message: None,
                    },
                    false,
                );
                if failure.reason.is_none() {
                    // A failed retryable job is restarted with the same cut locator.
                    let _ = self
                        .worker
                        .door
                        .takeover_hub(&self.cut, &published.writer, &published.body.cut)
                        .await
                        .map_err(|error| cut_failure("cut.takeover", error, false))?;
                }
                Err(failure)
            }
            _ => Ok(false),
        }
    }

    pub(super) async fn advance_hub(
        &mut self,
        hub: &Residence,
        seen: &SeenMove,
        published: &SeenMove,
        row: &mut AgentMove,
        plan: &MoveRecord,
        progress: &mut Progress,
    ) -> Result<Duration, Failure> {
        let state = seen.body.state;
        if !matches!(state, HubState::Published | HubState::Aborted)
            && progress.optional_skips(plan) != seen.body.skipped
        {
            return Err(conflict());
        }
        if row.step == Step::Activated {
            if !matches!(state, HubState::Activated | HubState::Released) {
                return Err(conflict());
            }
            Self::step(row, progress, Step::Done);
            self.save(row, progress).await?;
            return Ok(Duration::ZERO);
        }
        let source_current = hub.move_member(&seen.body.source)? == Some(published.writer_ep);
        let removed_early =
            !source_current && matches!(state, HubState::Published | HubState::Staged);
        let position = hub.move_position(&seen.body);
        let abort_without_residence =
            matches!(state, HubState::Published | HubState::Staged) && removed_early;
        let position = if abort_without_residence || state == HubState::Aborted {
            None
        } else {
            match position {
                Ok(position) => Some(position),
                Err(error) if error.code == "move_state_conflict" => {
                    hub.move_live_position(&seen.body).await.map_err(|e| {
                        if e.code == "move_state_conflict" {
                            conflict()
                        } else {
                            e.into()
                        }
                    })?;
                    return Ok(POLL);
                }
                Err(error) => return Err(error.into()),
            }
        };
        // A pending operator abort wins before we enter Activating. Once a
        // decision is checkpointed, neither a restart nor a stale watch can reverse it.
        if progress.hub_decision.is_none()
            && (row.abort_requested.is_some()
                || removed_early
                || state == HubState::Aborted
                || (state == HubState::Handed && position == Some(MovePosition::Returned))
                || (state == HubState::Handing
                    && !source_current
                    && position == Some(MovePosition::Forced)))
        {
            progress.hub_decision = Some(Decision::Abort);
            if row.abort_requested.is_none() {
                let reason = if state == HubState::Aborted {
                    seen.body.reason.clone().unwrap_or(MoveReason::Operator)
                } else if removed_early || position == Some(MovePosition::Forced) {
                    MoveReason::ClaimMoved
                } else {
                    MoveReason::Operator
                };
                row.abort_requested = Some(json!({"reason":reason,"detail":seen.body.detail}));
            }
            self.save(row, progress).await?;
        }
        if progress.hub_decision == Some(Decision::Abort) {
            let abort = AbortContext {
                position,
                source_current,
                source_episode: published.writer_ep,
            };
            return self.abort_hub(hub, seen, row, plan, progress, abort).await;
        }
        match (row.step, state) {
            (Step::PickedUp, HubState::Published) => {
                if position != Some(MovePosition::Source) {
                    return Err(conflict());
                }
                if !self.fetch_cut(row, progress, published).await? || row.abort_requested.is_some()
                {
                    return Ok(POLL);
                }
            }
            (
                Step::Imported,
                HubState::Published | HubState::Staged | HubState::Handing | HubState::Handed,
            ) => {
                if matches!(state, HubState::Published | HubState::Staged)
                    && position != Some(MovePosition::Source)
                {
                    // The residence watch can run ahead of the move watch.
                    if position == Some(MovePosition::Receiver) && state == HubState::Staged {
                        return Ok(POLL);
                    }
                    return Err(conflict());
                }
            }
            (Step::Staged, HubState::Staged) => {
                if !matches!(
                    position,
                    Some(MovePosition::Source | MovePosition::Receiver)
                ) {
                    return Err(conflict());
                }
                return Ok(POLL);
            }
            (
                Step::Staged | Step::Claimed | Step::Activating,
                HubState::Handing | HubState::Handed | HubState::Activated | HubState::Released,
            ) => {
                if position != Some(MovePosition::Receiver)
                    || !hub.move_receiver_holds(&seen.body)?
                {
                    if position == Some(MovePosition::Source)
                        && (state == HubState::Handing
                            || hub.move_live_position(&seen.body).await? == MovePosition::Receiver)
                    {
                        return Ok(POLL);
                    }
                    return Err(conflict());
                }
                if matches!(state, HubState::Activated | HubState::Released)
                    && row.step != Step::Activating
                {
                    return Err(conflict());
                }
                if row.step == Step::Staged {
                    Self::step(row, progress, Step::Claimed);
                    self.save(row, progress).await?;
                    return Ok(Duration::ZERO);
                }
                if row.step == Step::Claimed {
                    self.catalog_preflight(plan).await?;
                    self.local_writers(plan).await?;
                    progress.hub_decision = Some(Decision::Activate);
                    Self::step(row, progress, Step::Activating);
                    self.save(row, progress).await?;
                    if row.step != Step::Activating {
                        return Ok(Duration::ZERO);
                    }
                }
                if progress.hub_decision != Some(Decision::Activate) {
                    return Err(conflict());
                }
                if progress.hub_accepted.is_none() {
                    let reply = self
                        .worker
                        .door
                        .accept(&DoorCut {
                            cut_id: self.cut.clone(),
                        })
                        .await
                        .map_err(|error| cut_failure("cut.accept", error, true))?;
                    progress.hub_accepted = Some(reply.lifted);
                    self.save(row, progress).await?;
                }
            }
            _ => return Err(conflict()),
        }
        self.advance(
            row,
            plan,
            progress,
            seen.body.participant_plan(),
            Some((hub, seen)),
        )
        .await?;
        Ok(Duration::ZERO)
    }

    async fn abort_hub(
        &mut self,
        hub: &Residence,
        seen: &SeenMove,
        row: &mut AgentMove,
        plan: &MoveRecord,
        progress: &mut Progress,
        abort: AbortContext,
    ) -> Result<Duration, Failure> {
        let AbortContext {
            position,
            source_current,
            source_episode,
        } = abort;
        match seen.body.state {
            HubState::Aborted => {
                match hub.move_live_position(&seen.body).await {
                    Ok(MovePosition::Receiver) => return Err(conflict()),
                    Err(error) if error.code != "move_state_conflict" || source_current => {
                        return Err(error.into())
                    }
                    _ => {}
                };
                self.cleanup(row, plan, progress, true).await?;
                return Ok(Duration::ZERO);
            }
            HubState::Published | HubState::Staged => {
                if source_current && position != Some(MovePosition::Source) {
                    return Err(conflict());
                }
            }
            HubState::Handing => match position {
                Some(MovePosition::Source | MovePosition::Receiver) => return Ok(POLL),
                Some(MovePosition::Forced) if !source_current => {}
                _ => return Err(conflict()),
            },
            HubState::Handed => {
                match position {
                    Some(MovePosition::Source)
                        if matches!(
                            hub.move_live_position(&seen.body).await?,
                            MovePosition::Receiver | MovePosition::Returned
                        ) =>
                    {
                        return Ok(POLL)
                    }
                    Some(MovePosition::Receiver) => {
                        if progress.hub_return.is_none() {
                            progress.hub_return =
                                Some(hub.plan_transfer(&seen.body, Some(source_episode))?);
                            self.save(row, progress).await?;
                        }
                    }
                    Some(MovePosition::Returned) => {}
                    _ => return Err(conflict()),
                }
                if let Some(intent) = &progress.hub_return {
                    hub.transfer_move(&seen.body, intent, true).await?;
                }
            }
            _ => return Err(conflict()),
        }
        // Write the abort revision before undoing staged data so an in-flight
        // write to handing loses. If it won first, wait for the return of residence.
        let mut body = seen.body.clone();
        body.state = HubState::Aborted;
        if let Some(request) = &row.abort_requested {
            body.reason = request["reason"]
                .as_str()
                .map(|reason| MoveReason::from(reason.to_owned()));
            body.detail = request
                .get("detail")
                .filter(|detail| !detail.is_null())
                .map(wire::bounded_detail);
        }
        match hub.write_move(&body, Some(seen)).await {
            Ok(_) => {}
            Err(error) if error.code == "hub_write_superseded" => {}
            Err(error) => return Err(error.into()),
        }
        Ok(POLL)
    }
}

struct AbortContext {
    position: Option<MovePosition>,
    source_current: bool,
    source_episode: u64,
}
