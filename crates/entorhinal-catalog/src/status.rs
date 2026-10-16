//! Status verification and monotonic client application; no clock is read.
use crate::{
    is_hex, jcs, names, positive, shape, time, verify_signature, Kind, Refusal, RefusalCode,
    RollbackReason,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum State {
    Available,
    Withdrawn,
    Tombstoned,
    Revoked,
}
impl State {
    pub fn permits(self, next: Self) -> bool {
        match self {
            Self::Available | Self::Withdrawn => true,
            Self::Revoked => matches!(next, Self::Revoked | Self::Tombstoned),
            Self::Tombstoned => next == Self::Tombstoned,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatusRecord {
    pub schema: String,
    pub registry_key_id: String,
    pub name: String,
    pub version: String,
    pub archive_sha256: String,
    pub publisher_key: String,
    pub sequence: u64,
    pub state: State,
    pub reason: Option<String>,
    pub compromise_from: Option<String>,
    pub published_at: String,
    pub changed_at: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Pin {
    pub name: String,
    pub version: String,
    pub archive_sha256: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatusKey {
    pub key_id: String,
    pub public_key: String,
    pub valid_from: Option<String>,
}
/// A record previously accepted by the caller, including its signer epoch.
/// Fields are private so callers cannot accidentally fabricate a verified hold.
#[derive(Clone, Debug)]
pub struct HeldStatus {
    record: StatusRecord,
    canonical: String,
    signer_index: usize,
    bundle_attested: bool,
}
impl HeldStatus {
    pub(crate) fn from_bundle(text: &str) -> Result<Self, Refusal> {
        let (record, canonical) = parse(text)?;
        // The bundle signature authenticates this initial status without a
        // registry key. Its placeholder index must not constrain later key epochs.
        Ok(Self {
            record,
            canonical,
            signer_index: 0,
            bundle_attested: true,
        })
    }
    /// Authenticate a saved status with its original registry signer. A later
    /// successor may retire that signer for new records, but cannot undo a status
    /// already accepted before retirement. Callers compare stored index columns
    /// (sequence, timestamps and identity) with the authenticated record.
    pub fn restore(
        text: &str,
        signature: &str,
        chain: &[StatusKey],
        signer_index: usize,
    ) -> Result<Self, Refusal> {
        let (record, canonical) = parse(text)?;
        let key = chain
            .get(signer_index)
            .filter(|key| key.key_id == record.registry_key_id)
            .ok_or_else(|| Refusal::new(RefusalCode::KeyUnknown))?;
        if !verify_signature(&key.public_key, "ck-status/v1", &canonical, signature) {
            return Err(Refusal::new(RefusalCode::SignatureInvalid));
        }
        Ok(Self {
            record,
            canonical,
            signer_index,
            bundle_attested: false,
        })
    }
    pub fn record(&self) -> &StatusRecord {
        &self.record
    }
    pub fn canonical(&self) -> &str {
        &self.canonical
    }
    pub fn signer_index(&self) -> usize {
        self.signer_index
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Apply {
    Applied,
    Duplicate,
}
#[derive(Clone, Debug)]
pub struct AppliedStatus {
    pub result: Apply,
    pub held: HeldStatus,
}
pub(crate) fn parse(text: &str) -> Result<(StatusRecord, String), Refusal> {
    let invalid = |f| Refusal::field(RefusalCode::StatusInvalid, f);
    let s = shape(
        text,
        "ck-status/v1",
        &[
            "schema",
            "registry_key_id",
            "name",
            "version",
            "archive_sha256",
            "publisher_key",
            "sequence",
            "state",
            "reason",
            "compromise_from",
            "published_at",
            "changed_at",
        ],
        &[],
        RefusalCode::StatusInvalid,
    )?;
    let state: State = serde_json::from_value(s["state"].clone()).map_err(|_| invalid("state"))?;
    let seq = positive(&s["sequence"]).ok_or_else(|| invalid("sequence"))?;
    if if state == State::Available {
        !(s["reason"].is_null() || s["reason"].is_string())
    } else {
        !s["reason"].as_str().is_some_and(|s| !s.is_empty())
    } {
        return Err(invalid("reason"));
    }
    if if state == State::Revoked {
        !s["compromise_from"].as_str().is_some_and(time)
    } else {
        !s["compromise_from"].is_null()
    } {
        return Err(invalid("compromise_from"));
    }
    for f in ["published_at", "changed_at"] {
        if !s[f].as_str().is_some_and(time) {
            return Err(invalid(f));
        }
    }
    if !["archive_sha256", "publisher_key"]
        .iter()
        .all(|f| s[*f].as_str().is_some_and(|s| is_hex(s, 64)))
    {
        return Err(invalid("digest"));
    }
    if !s["registry_key_id"].as_str().is_some_and(|s| !s.is_empty()) {
        return Err(invalid("registry_key_id"));
    }
    if !s["name"].as_str().is_some_and(names::canonical) {
        return Err(invalid("name"));
    }
    if !s["version"]
        .as_str()
        .is_some_and(|s| s.parse::<crate::semver::Version>().is_ok())
    {
        return Err(invalid("version"));
    }
    let canonical = jcs::to_string(&s).map_err(|_| invalid("json"))?;
    let mut s = s;
    s["sequence"] = seq.into();
    let record = serde_json::from_value(s).map_err(|_| invalid("json"))?;
    Ok((record, canonical))
}
pub fn apply_status(
    held: Option<&HeldStatus>,
    text: &str,
    sig: &str,
    chain: &[StatusKey],
    pin: &Pin,
) -> Result<AppliedStatus, Refusal> {
    let (s, canonical) = parse(text)?;
    let index = chain
        .iter()
        .position(|k| k.key_id == s.registry_key_id)
        .ok_or_else(|| Refusal::new(RefusalCode::KeyUnknown))?;
    if !verify_signature(&chain[index].public_key, "ck-status/v1", &canonical, sig) {
        return Err(Refusal::new(RefusalCode::SignatureInvalid));
    }
    let retirement = chain[index + 1..]
        .iter()
        .filter_map(|k| k.valid_from.as_deref())
        .min();
    if retirement.is_some_and(|at| s.changed_at.as_str() >= at) {
        return Err(Refusal::new(RefusalCode::KeyNotValid));
    }
    if s.name != pin.name || s.version != pin.version || s.archive_sha256 != pin.archive_sha256 {
        return Err(Refusal::new(RefusalCode::RecordMismatch));
    }
    let next = HeldStatus {
        record: s,
        canonical,
        signer_index: index,
        bundle_attested: false,
    };
    let result = compare(held, &next)?;
    Ok(AppliedStatus {
        result,
        held: if result == Apply::Duplicate && held.is_some_and(|h| !h.bundle_attested) {
            held.cloned().unwrap_or(next)
        } else {
            next
        },
    })
}

/// Compare authenticated status records independently of how their trust was
/// established. Bundle attestations and online registry signatures share this
/// monotonic boundary; callers still enforce immutable version bindings.
pub fn compare(held: Option<&HeldStatus>, next: &HeldStatus) -> Result<Apply, Refusal> {
    if let Some(prior) = held {
        let s = &next.record;
        if s.sequence < prior.record.sequence {
            return Err(Refusal::rollback(RollbackReason::LowerSequence));
        }
        if s.sequence == prior.record.sequence {
            return if next.canonical == prior.canonical {
                Ok(Apply::Duplicate)
            } else {
                Err(Refusal::rollback(RollbackReason::SameSequenceOtherBytes))
            };
        }
        if s.changed_at < prior.record.changed_at {
            return Err(Refusal::rollback(RollbackReason::ChangedAtRegressed));
        }
        if !next.bundle_attested && !prior.bundle_attested {
            let index = next.signer_index;
            if index < prior.signer_index {
                return Err(Refusal::rollback(RollbackReason::KeyEpochRegressed));
            }
        }
        if !prior.record.state.permits(s.state) {
            return Err(Refusal::new(RefusalCode::StatusTerminal));
        }
    }
    Ok(Apply::Applied)
}
/// A compromised key blocks unknown publication times as well as times at the
/// boundary; only an earlier registry-attested publication remains usable.
pub fn revoked_at(published_at: Option<&str>, compromise_from: &str) -> bool {
    published_at.is_none_or(|at| at >= compromise_from)
}
#[derive(Clone, Debug, Serialize)]
pub struct Effect {
    pub kind: Kind,
    pub state: State,
    pub user_notice: &'static str,
    pub resolution: &'static str,
    pub exact_fetch: &'static str,
    pub activation: &'static str,
    pub running: &'static str,
    pub core_flow_revocation: bool,
    pub scripts_may_start: Option<bool>,
}
pub fn effect(kind: Kind, state: State) -> Effect {
    let blocked = matches!(state, State::Tombstoned | State::Revoked);
    Effect {
        kind,
        state,
        user_notice: if state == State::Available {
            "none"
        } else if blocked {
            "blocking"
        } else {
            "notice"
        },
        resolution: if state == State::Available {
            "eligible"
        } else {
            "skipped"
        },
        exact_fetch: if state == State::Tombstoned {
            "gone"
        } else {
            "allowed"
        },
        activation: if blocked { "blocked" } else { "allowed" },
        running: if !blocked {
            "continues"
        } else if kind == Kind::Flow {
            "runs_stop_at_next_activation"
        } else {
            "sessions_get_blocking_notice"
        },
        core_flow_revocation: blocked && kind == Kind::Flow,
        scripts_may_start: (kind == Kind::Skill).then_some(!blocked),
    }
}
#[derive(Clone, Debug, Default)]
pub struct FeedState {
    pub feed_seq: Option<u64>,
    pub held: BTreeMap<String, HeldStatus>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeedMode {
    Full,
    After,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NextStep {
    Done,
    FetchFullFeed,
}
#[derive(Clone, Debug, Deserialize)]
pub struct FeedRecord {
    pub id: String,
    pub record_text: String,
    pub signature: String,
}
#[derive(Clone, Debug)]
pub enum FeedReply {
    Unavailable,
    Records {
        feed_seq: u64,
        records: Vec<FeedRecord>,
    },
}
#[derive(Clone, Debug)]
pub struct FeedOutcome {
    pub refusal: Option<Refusal>,
    pub records: Vec<(String, Result<Apply, Refusal>)>,
    pub next_state: FeedState,
    pub next_step: NextStep,
}
pub fn apply_feed(
    state: &FeedState,
    mode: FeedMode,
    reply: &FeedReply,
    chain: &[StatusKey],
    pins: &BTreeMap<String, Pin>,
) -> FeedOutcome {
    let fallback = || {
        if mode == FeedMode::After {
            NextStep::FetchFullFeed
        } else {
            NextStep::Done
        }
    };
    let (seq, records) = match reply {
        FeedReply::Unavailable => {
            return FeedOutcome {
                refusal: Some(Refusal::new(RefusalCode::FeedAfterUnavailable)),
                records: Vec::new(),
                next_state: state.clone(),
                next_step: fallback(),
            }
        }
        FeedReply::Records { feed_seq, records } => (*feed_seq, records),
    };
    if state.feed_seq.is_some_and(|n| seq < n) {
        return FeedOutcome {
            refusal: Some(Refusal::field(RefusalCode::FeedRollback, "feed_seq")),
            records: Vec::new(),
            next_state: state.clone(),
            next_step: fallback(),
        };
    }
    let mut next = state.clone();
    let mut results = Vec::new();
    for r in records {
        let applied: Result<Apply, Refusal> = (|| {
            let (s, _) = parse(&r.record_text)?;
            let key = format!("{}@{}", s.name, s.version);
            let pin = pins
                .get(&key)
                .ok_or_else(|| Refusal::new(RefusalCode::RecordMismatch))?;
            let a = apply_status(
                next.held.get(&key),
                &r.record_text,
                &r.signature,
                chain,
                pin,
            )?;
            next.held.insert(key, a.held);
            Ok(a.result)
        })();
        results.push((r.id.clone(), applied));
    }
    let rollback = results.iter().any(|(_, r)| {
        r.as_ref()
            .is_err_and(|e| e.code == RefusalCode::FeedRollback)
    });
    if results.iter().all(|(_, r)| r.is_ok()) {
        next.feed_seq = Some(seq);
    }
    FeedOutcome {
        refusal: rollback.then(|| Refusal::field(RefusalCode::FeedRollback, "record")),
        records: results,
        next_state: next,
        next_step: if rollback { fallback() } else { NextStep::Done },
    }
}

#[derive(Clone, Debug)]
pub struct FeedWrite {
    pub feed_seq: u64,
    pub id: String,
    pub record: StatusRecord,
}
#[derive(Clone, Debug, Serialize)]
pub struct FeedItem {
    pub feed_seq: u64,
    pub id: String,
}
#[derive(Clone, Debug, Serialize)]
pub struct FeedView {
    pub feed_seq: u64,
    pub records: Vec<FeedItem>,
}
/// Select latest per-version writes before filtering the scope-wide cursor.
/// Intermediate per-version sequences may legitimately be skipped.
pub fn select_feed(
    log: &[FeedWrite],
    oldest_servable_after: u64,
    after: Option<u64>,
) -> Result<FeedView, Refusal> {
    let current = log.iter().map(|w| w.feed_seq).max().unwrap_or(0);
    if after.is_some_and(|n| n > current || n < oldest_servable_after) {
        return Err(Refusal::new(RefusalCode::FeedAfterUnavailable));
    }
    let mut latest: BTreeMap<(&str, &str), &FeedWrite> = BTreeMap::new();
    for w in log {
        let key = (w.record.name.as_str(), w.record.version.as_str());
        if latest
            .get(&key)
            .is_none_or(|prior| prior.feed_seq < w.feed_seq)
        {
            latest.insert(key, w);
        }
    }
    let mut records: Vec<_> = latest
        .values()
        .filter(|w| w.feed_seq > after.unwrap_or(0))
        .map(|w| FeedItem {
            feed_seq: w.feed_seq,
            id: w.id.clone(),
        })
        .collect();
    records.sort_by_key(|w| w.feed_seq);
    Ok(FeedView {
        feed_seq: current,
        records,
    })
}

#[cfg(test)]
mod restore_tests {
    use super::*;
    #[test]
    fn restore_authenticates_bytes_and_signer_without_new_cutoffs() {
        let v: serde_json::Value =
            serde_json::from_str(include_str!("../tests/vectors/ck-catalog-v1/status.json"))
                .unwrap();
        let doc = &v["records"][0];
        let key = StatusKey {
            key_id: "test-registry-1".into(),
            public_key: v["registry_keys"]["test-registry-1"]["public_key_hex"]
                .as_str()
                .unwrap()
                .into(),
            valid_from: None,
        };
        let chain = [
            key,
            StatusKey {
                key_id: "new".into(),
                public_key: "ab".repeat(32),
                valid_from: Some("2020-01-01T00:00:00Z".into()),
            },
        ];
        let canonical = doc["canonical"].as_str().unwrap();
        let signature = doc["signature"].as_str().unwrap();
        let held = HeldStatus::restore(canonical, signature, &chain, 0).unwrap();
        assert_eq!(held.record().sequence, 1);
        assert_eq!(held.signer_index(), 0);
        assert_eq!(
            HeldStatus::restore(canonical, signature, &chain, 1)
                .unwrap_err()
                .code,
            RefusalCode::KeyUnknown
        );
        assert_eq!(
            HeldStatus::restore(canonical, "00", &chain, 0)
                .unwrap_err()
                .code,
            RefusalCode::SignatureInvalid
        );
        let tampered = canonical.replace("\"sequence\":1", "\"sequence\":2");
        assert_eq!(
            HeldStatus::restore(&tampered, signature, &chain, 0)
                .unwrap_err()
                .code,
            RefusalCode::SignatureInvalid
        );
        let duplicate = canonical.replacen("{", "{\"sequence\":1,", 1);
        assert_eq!(
            HeldStatus::restore(&duplicate, signature, &chain, 0)
                .unwrap_err()
                .code,
            RefusalCode::StatusInvalid
        );
    }
}
