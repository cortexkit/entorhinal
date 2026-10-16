//! Shared package identities and machine-local preparation/activation state.
use crate::catalog::{store_error, CatalogWriteError};
use crate::{JournalWriter, RegistryError, RegistryStore};
use entorhinal_catalog::{is_hex, jcs, names, semver, sha256_hex, Kind};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeSet;

/// Journal op that replaces one agent's whole pin set. Pins are shared agent
/// state, but this op is left out of both journal read feeds. `agent.changes`
/// is the agent-identity change feed that core's identity replica reads, and
/// that replica refuses ops it does not know. `journal_tail` is the project
/// history feed, and an agent's pins are not project history.
pub(crate) const PIN_OP: &str = "catalog.pin";

/// Package identities and privileges authenticated before consumers inspect the
/// bytes and render consent text. Capabilities bound any new publisher permission.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedPackage {
    #[serde(flatten)]
    pub pin: CatalogPin,
    pub trust: String,
    pub capabilities: Vec<String>,
    pub has_scripts: bool,
    pub root_certified: bool,
}
impl PreparedPackage {
    pub fn coverage(&self) -> crate::catalog_grants::CoveragePackage {
        crate::catalog_grants::CoveragePackage {
            name: self.pin.name.clone(),
            publisher_fingerprint: self.pin.publisher_fingerprint.clone(),
            capabilities: self.capabilities.clone(),
            has_scripts: self.has_scripts,
            root_certified: self.root_certified,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerCheck {
    pub module_id: String,
    pub kinds: Vec<Kind>,
    pub card_section: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedPlan {
    pub packages: Vec<PreparedPackage>,
    pub moves: Value,
    pub checks: Vec<ConsumerCheck>,
}

fn prepared(conn: &Connection, id: &str) -> rusqlite::Result<PreparedPlan> {
    decode(conn.query_row(
        "SELECT detail FROM catalog_prepared WHERE plan_id=?1",
        [id],
        |r| r.get(0),
    )?)
}

pub(crate) const V12_CATALOG_AGENT: &str = r#"
CREATE TABLE catalog_pin_set(agent_id TEXT PRIMARY KEY REFERENCES agent(agent_id), pin_digest TEXT NOT NULL);
CREATE TABLE catalog_pin(agent_id TEXT NOT NULL REFERENCES catalog_pin_set(agent_id), name TEXT NOT NULL,
 version TEXT NOT NULL, kind TEXT NOT NULL, manifest_sha256 TEXT NOT NULL, archive_sha256 TEXT NOT NULL,
 publisher_fingerprint TEXT NOT NULL, root INTEGER NOT NULL CHECK(root IN (0,1)), range TEXT,
 PRIMARY KEY(agent_id,name), CHECK((root=1 AND range IS NOT NULL) OR (root=0 AND range IS NULL)));
CREATE TABLE catalog_plan(plan_id TEXT PRIMARY KEY, agent_id TEXT NOT NULL,
 operation TEXT NOT NULL CHECK(operation IN ('install','upgrade')), spec TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('pending','ready','failed')), closure TEXT, refusal TEXT,
  base_pin_digest TEXT NOT NULL, status_snapshot TEXT, created_at_ms INTEGER NOT NULL, ready_at_ms INTEGER);
CREATE TABLE catalog_prepared(plan_id TEXT PRIMARY KEY REFERENCES catalog_plan(plan_id), detail TEXT NOT NULL);
CREATE TABLE catalog_generation(generation_id TEXT PRIMARY KEY, agent_id TEXT NOT NULL,
 plan_id TEXT NOT NULL UNIQUE REFERENCES catalog_plan(plan_id),
 state TEXT NOT NULL CHECK(state IN ('installed','approved','ready','active','degraded','superseded')),
 closure TEXT NOT NULL, pin_digest TEXT NOT NULL, created_at_ms INTEGER NOT NULL, activated_at_ms INTEGER, error TEXT,
 publisher_facts TEXT NOT NULL DEFAULT '[]');
CREATE UNIQUE INDEX catalog_one_active ON catalog_generation(agent_id) WHERE state='active';
CREATE TABLE catalog_transition(entity TEXT NOT NULL, id TEXT NOT NULL, transition TEXT NOT NULL,
 args_digest TEXT NOT NULL, result TEXT NOT NULL, PRIMARY KEY(entity,id,transition));
"#;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogPin {
    pub name: String,
    pub version: String,
    pub kind: Kind,
    pub manifest_sha256: String,
    pub archive_sha256: String,
    pub publisher_fingerprint: String,
    pub root: bool,
    pub range: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogPinSet {
    pub op: String,
    pub agent_id: String,
    pub pins: Vec<CatalogPin>,
    pub pin_digest: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinWriteRequest {
    pub agent_id: String,
    pub pins: Vec<CatalogPin>,
    pub request_key: Option<String>,
}

fn invalid(message: &str) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message.to_owned(),
    )))
}
/// Validate shared pins with one set of rules for local writes, remote apply,
/// replay and verification. Pins can describe any catalog package kind. Plan
/// preparation (resolving and checking an install or upgrade request) is
/// stricter: it refuses any package that is not a `Kind::Skill`, because skills
/// are the only kind with a consumer today.
pub fn validate_pins(pins: &[CatalogPin]) -> rusqlite::Result<()> {
    let mut seen = BTreeSet::new();
    for pin in pins {
        if !seen.insert(&pin.name)
            || !names::map_name(&pin.name).is_ok_and(|n| n.canonical == pin.name)
            || pin.version.parse::<semver::Version>().is_err()
            || !is_hex(&pin.manifest_sha256, 64)
            || !is_hex(&pin.archive_sha256, 64)
            || !is_hex(&pin.publisher_fingerprint, 64)
            || pin.root != pin.range.is_some()
            || pin
                .range
                .as_ref()
                .is_some_and(|r| r.parse::<semver::Range>().is_err())
        {
            return Err(invalid("invalid catalog pin set"));
        }
    }
    Ok(())
}
/// Lowercase SHA-256 of a JCS array sorted by name. Each object has exactly
/// name, version, manifest_sha256, archive_sha256, publisher_fingerprint, root
/// (boolean), and range (string for roots, null otherwise). Agent id, kind and
/// machine-local generation ids are excluded so source and destination Macs can
/// compare the same package/root identities when an agent moves between them.
pub fn pin_digest(pins: &[CatalogPin]) -> rusqlite::Result<String> {
    validate_pins(pins)?;
    let mut sorted: Vec<_> = pins.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    let rows: Vec<_> = sorted
        .into_iter()
        .map(|p| {
            json!({
                "name":p.name,"version":p.version,"manifest_sha256":p.manifest_sha256,
                "archive_sha256":p.archive_sha256,"publisher_fingerprint":p.publisher_fingerprint,
                "root":p.root,"range":p.range
            })
        })
        .collect();
    Ok(sha256_hex(
        &jcs::to_string(&json!(rows)).map_err(|_| invalid("invalid pin JCS"))?,
    ))
}
impl CatalogPinSet {
    pub fn validate(&self, conn: &Connection, require_live: bool) -> rusqlite::Result<()> {
        if self.op != PIN_OP || self.pin_digest != pin_digest(&self.pins)? {
            return Err(invalid("invalid catalog pin digest or op"));
        }
        let status: Option<String> =
            crate::agent::load_row(conn, &self.agent_id)?.map(|row| row.status);
        if status.is_none() || (require_live && status.as_deref() != Some("live")) {
            return Err(invalid("catalog pin agent is missing or not live"));
        }
        Ok(())
    }
    pub(crate) fn restore(&self, tx: &Transaction<'_>) -> rusqlite::Result<()> {
        self.validate(tx, true)?;
        tx.execute("INSERT INTO catalog_pin_set VALUES(?1,?2) ON CONFLICT(agent_id) DO UPDATE SET pin_digest=excluded.pin_digest", params![self.agent_id,self.pin_digest])?;
        tx.execute(
            "DELETE FROM catalog_pin WHERE agent_id=?1",
            [&self.agent_id],
        )?;
        for p in &self.pins {
            tx.execute(
                "INSERT INTO catalog_pin VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                params![
                    self.agent_id,
                    p.name,
                    p.version,
                    serde_json::to_value(p.kind).unwrap().as_str().unwrap(),
                    p.manifest_sha256,
                    p.archive_sha256,
                    p.publisher_fingerprint,
                    p.root,
                    p.range
                ],
            )?;
        }
        Ok(())
    }
}
pub(crate) fn read_pins(conn: &Connection, agent: &str) -> rusqlite::Result<Vec<CatalogPin>> {
    conn.prepare("SELECT name,version,kind,manifest_sha256,archive_sha256,publisher_fingerprint,root,range FROM catalog_pin WHERE agent_id=?1 ORDER BY name")?
        .query_map([agent], |r| {
            let kind: String = r.get(2)?;
            Ok(CatalogPin {name:r.get(0)?,version:r.get(1)?,kind:serde_json::from_value(json!(kind)).map_err(crate::shared_entry::decode_error)?,manifest_sha256:r.get(3)?,archive_sha256:r.get(4)?,publisher_fingerprint:r.get(5)?,root:r.get(6)?,range:r.get(7)?})
        })?.collect()
}
pub(crate) fn current_digest(conn: &Connection, agent: &str) -> rusqlite::Result<String> {
    pin_digest(&read_pins(conn, agent)?)
}
pub(crate) fn validate_pin_projection(conn: &Connection) -> rusqlite::Result<()> {
    let sets = conn
        .prepare("SELECT agent_id,pin_digest FROM catalog_pin_set ORDER BY agent_id")?
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (agent_id, pin_digest) in sets {
        CatalogPinSet {
            op: PIN_OP.into(),
            pins: read_pins(conn, &agent_id)?,
            agent_id,
            pin_digest,
        }
        .validate(conn, false)?;
    }
    Ok(())
}
pub(crate) fn replay_pin(tx: &Transaction<'_>, value: Value) -> rusqlite::Result<()> {
    let mut value = value.get("entry").cloned().unwrap_or(value);
    let object = value
        .as_object_mut()
        .ok_or_else(|| invalid("invalid catalog pin entry"))?;
    let caller = object.remove("requesting_agent_id");
    let authorization = object.remove("authorization");
    let caller = caller
        .map(serde_json::from_value::<String>)
        .transpose()
        .map_err(crate::shared_entry::decode_error)?;
    let authorization = authorization
        .map(serde_json::from_value::<crate::agent::consent::Authorization>)
        .transpose()
        .map_err(crate::shared_entry::decode_error)?;
    let entry: CatalogPinSet =
        serde_json::from_value(value).map_err(crate::shared_entry::decode_error)?;
    if caller.is_some() != authorization.is_some()
        || caller.is_some_and(|caller| caller != entry.agent_id)
    {
        return Err(invalid("invalid catalog pin authorization evidence"));
    }
    entry.restore(tx)
}
impl RegistryStore {
    /// Tombstones forbid serving either object unless another version bound to
    /// the same digest is still usable. Include prepared closures so consumers
    /// can read verified bytes before the install commits its pins.
    pub fn catalog_blob_tombstone(&self, digest: &str) -> Result<Option<Value>, RegistryError> {
        self.read(|conn| {
            let statuses = conn.prepare(
                "WITH bindings(name,version) AS (
                    SELECT name,version FROM catalog_pin WHERE manifest_sha256=?1 OR archive_sha256=?1
                    UNION SELECT json_extract(p.value,'$.name'),json_extract(p.value,'$.version')
                    FROM catalog_plan, json_each(catalog_plan.closure) p
                    WHERE json_extract(p.value,'$.manifest_sha256')=?1 OR json_extract(p.value,'$.archive_sha256')=?1
                    UNION SELECT json_extract(p.value,'$.name'),json_extract(p.value,'$.version')
                    FROM catalog_generation, json_each(catalog_generation.closure) p
                    WHERE json_extract(p.value,'$.manifest_sha256')=?1 OR json_extract(p.value,'$.archive_sha256')=?1
                    UNION SELECT name,version FROM catalog_status WHERE json_extract(canonical,'$.archive_sha256')=?1
                ) SELECT s.canonical FROM bindings b LEFT JOIN catalog_status s ON s.name=b.name AND s.version=b.version"
            )?.query_map([digest], |r| r.get::<_,Option<String>>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
            let mut tombstone = None;
            for text in statuses {
                let Some(text) = text else { return Ok(None) };
                let status: Value = decode(text)?;
                if status["state"] != "tombstoned" { return Ok(None) }
                tombstone = Some(status);
            }
            Ok(tombstone)
        })
    }
    pub fn catalog_pins(&self, agent: &str) -> Result<Vec<CatalogPin>, RegistryError> {
        self.read(|conn| read_pins(conn, agent))
    }
    pub fn catalog_pin_digest(&self, agent: &str) -> Result<String, RegistryError> {
        self.read(|conn| current_digest(conn, agent))
    }
}
impl JournalWriter<'_> {
    pub fn catalog_set_pins(
        &self,
        req: PinWriteRequest,
        now: i64,
    ) -> Result<Vec<u8>, RegistryError> {
        let mut req = req;
        req.pins.sort_by(|a, b| a.name.cmp(&b.name));
        self.mutation(PIN_OP, req.request_key.as_deref(), |tx| {
            write_pins(tx, &req, now, self.principal)
        })
    }
}

fn write_pins(
    tx: &Transaction<'_>,
    req: &PinWriteRequest,
    now: i64,
    principal: &str,
) -> Result<crate::mutations::Action, RegistryError> {
    let entry = CatalogPinSet {
        op: PIN_OP.into(),
        agent_id: req.agent_id.clone(),
        pin_digest: pin_digest(&req.pins)?,
        pins: req.pins.clone(),
    };
    entry.validate(tx, true)?;
    let old = read_pins(tx, &req.agent_id)?;
    if old == entry.pins && current_digest(tx, &req.agent_id)? == entry.pin_digest {
        return Ok(crate::mutations::Action {
            seq: None,
            changed: false,
            value: json!({"pin_digest":entry.pin_digest}),
            payload: json!({}),
        });
    }
    let payload = json!({"entry":entry});
    let seq = crate::mutations::append(
        tx,
        "catalog.pin",
        &payload,
        "catalog",
        req.request_key.as_deref(),
        now,
        principal,
    )?;
    entry.restore(tx)?;
    Ok(crate::mutations::Action {
        seq: Some(seq),
        changed: true,
        value: json!({"pin_digest":entry.pin_digest}),
        payload,
    })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogSpec {
    pub package: String,
    pub range: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CatalogTarget {
    Agent(String),
    Tier(String),
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CatalogPlan {
    pub plan_id: String,
    pub agent_id: String,
    pub operation: String,
    pub spec: CatalogSpec,
    pub state: String,
    pub closure: Option<Vec<CatalogPin>>,
    pub refusal: Option<Value>,
    pub base_pin_digest: String,
    pub created_at_ms: i64,
    pub ready_at_ms: Option<i64>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CatalogGeneration {
    pub generation_id: String,
    pub agent_id: String,
    pub plan_id: String,
    /// `superseded` retains the old execution closure after a replacement becomes
    /// active, until consumers finish switching. It is never a catalog wire state.
    pub state: String,
    pub closure: Vec<CatalogPin>,
    pub pin_digest: String,
    pub created_at_ms: i64,
    pub activated_at_ms: Option<i64>,
    pub error: Option<Value>,
    /// Non-available pin statuses read in the same transaction as the generation.
    /// The module serializes only their public notice fields and omits an empty list.
    #[serde(skip)]
    pub pinned_status: Vec<Value>,
    /// Publisher provenance recorded when this local generation was installed.
    /// It is intentionally local state and never part of shared pins.
    #[serde(default)]
    pub publisher_facts: Vec<CatalogPublisherFact>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogPublisherFact {
    pub name: String,
    pub fingerprint: String,
    pub scope: String,
    pub first_party: bool,
}
fn decode<T: serde::de::DeserializeOwned>(s: String) -> rusqlite::Result<T> {
    serde_json::from_str(&s).map_err(crate::shared_entry::decode_error)
}
fn read_plan(conn: &Connection, id: &str) -> rusqlite::Result<CatalogPlan> {
    conn.query_row("SELECT plan_id,agent_id,operation,spec,state,closure,refusal,base_pin_digest,created_at_ms,ready_at_ms FROM catalog_plan WHERE plan_id=?1",[id],|r| Ok(CatalogPlan {
        plan_id:r.get(0)?,agent_id:r.get(1)?,operation:r.get(2)?,spec:decode(r.get(3)?)?,state:r.get(4)?,closure:r.get::<_,Option<String>>(5)?.map(decode).transpose()?,refusal:r.get::<_,Option<String>>(6)?.map(decode).transpose()?,base_pin_digest:r.get(7)?,created_at_ms:r.get(8)?,ready_at_ms:r.get(9)?
    }))
}
fn read_generation(conn: &Connection, id: &str) -> rusqlite::Result<CatalogGeneration> {
    let mut generation = conn.query_row("SELECT generation_id,agent_id,plan_id,state,closure,pin_digest,created_at_ms,activated_at_ms,error,publisher_facts FROM catalog_generation WHERE generation_id=?1",[id],|r| Ok(CatalogGeneration {
        generation_id:r.get(0)?,agent_id:r.get(1)?,plan_id:r.get(2)?,state:r.get(3)?,closure:decode(r.get(4)?)?,pin_digest:r.get(5)?,created_at_ms:r.get(6)?,activated_at_ms:r.get(7)?,error:r.get::<_,Option<String>>(8)?.map(decode).transpose()?,pinned_status:Vec::new(),publisher_facts:decode(r.get(9)?)?
    }))?;
    generation.pinned_status = pin_statuses(conn, &generation.closure)?;
    Ok(generation)
}
fn publisher_facts(
    pins: &[CatalogPin],
    prepared: Option<&PreparedPlan>,
) -> Vec<CatalogPublisherFact> {
    pins.iter()
        .map(|pin| {
            let scope = pin
                .name
                .split_once('/')
                .map_or("", |(scope, _)| scope)
                .to_owned();
            let first_party = scope == "@cortexkit"
                && prepared.is_some_and(|detail| {
                    detail.packages.iter().any(|package| {
                        package.pin.name == pin.name
                            && package.pin.publisher_fingerprint == pin.publisher_fingerprint
                            && package.root_certified
                    })
                });
            CatalogPublisherFact {
                name: pin.name.clone(),
                fingerprint: pin.publisher_fingerprint.clone(),
                scope,
                first_party,
            }
        })
        .collect()
}
fn live(conn: &Connection, agent: &str) -> Result<(), CatalogWriteError> {
    let status: Option<String> = crate::agent::load_row(conn, agent)
        .map_err(store_error)?
        .map(|row| row.status);
    if status.as_deref() != Some("live") {
        return Err(CatalogWriteError::Refused("gone"));
    }
    Ok(())
}
fn owner(agent: &str, caller: &str) -> Result<(), CatalogWriteError> {
    if agent != caller {
        return Err(CatalogWriteError::Refused("caller_forbidden"));
    }
    Ok(())
}
fn transition<T: Serialize + serde::de::DeserializeOwned>(
    tx: &Transaction<'_>,
    entity: &str,
    id: &str,
    step: &str,
    args: Value,
    action: impl FnOnce() -> Result<T, CatalogWriteError>,
) -> Result<T, CatalogWriteError> {
    let digest = sha256_hex(
        &jcs::to_string(&args).map_err(|_| CatalogWriteError::Refused("invalid_request"))?,
    );
    let saved: Option<(String,String)> = tx.query_row("SELECT args_digest,result FROM catalog_transition WHERE entity=?1 AND id=?2 AND transition=?3",params![entity,id,step],|r| Ok((r.get(0)?,r.get(1)?))).optional().map_err(store_error)?;
    if let Some((old, result)) = saved {
        if old != digest {
            return Err(CatalogWriteError::Refused("request_key_conflict"));
        }
        return decode(result).map_err(store_error);
    }
    let result = action()?;
    tx.execute(
        "INSERT INTO catalog_transition VALUES(?1,?2,?3,?4,?5)",
        params![
            entity,
            id,
            step,
            digest,
            serde_json::to_string(&result).unwrap()
        ],
    )
    .map_err(store_error)?;
    Ok(result)
}
fn status_snapshot(conn: &Connection, pins: &[CatalogPin]) -> rusqlite::Result<Value> {
    let mut out = serde_json::Map::new();
    for pin in pins {
        let row: Option<(i64, String)> = conn
            .query_row(
                "SELECT sequence,canonical FROM catalog_status WHERE name=?1 AND version=?2",
                params![pin.name, pin.version],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        out.insert(pin.name.clone(), json!(row));
    }
    Ok(Value::Object(out))
}
/// A publication time from the registry's signed status exempts a version only
/// when it is strictly earlier than the compromise cutoff. Unknown times block.
pub(crate) fn status_blocks(value: &Value) -> bool {
    match value["state"].as_str() {
        Some("tombstoned") => true,
        Some("revoked") => entorhinal_catalog::status::revoked_at(
            value["published_at"].as_str(),
            value["compromise_from"].as_str().unwrap_or(""),
        ),
        _ => false,
    }
}
pub(crate) fn pin_statuses(conn: &Connection, pins: &[CatalogPin]) -> rusqlite::Result<Vec<Value>> {
    let mut statuses = Vec::new();
    for pin in pins {
        let text: Option<String> = conn
            .query_row(
                "SELECT canonical FROM catalog_status WHERE name=?1 AND version=?2",
                params![pin.name, pin.version],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(text) = text {
            let value: Value = decode(text)?;
            if value["state"] != "available" {
                statuses.push(value);
            }
        }
    }
    Ok(statuses)
}
pub(crate) fn blocked_status(
    conn: &Connection,
    pins: &[CatalogPin],
) -> rusqlite::Result<Option<Value>> {
    Ok(pin_statuses(conn, pins)?.into_iter().find(status_blocks))
}
/// The trust transaction calls this after writing status rows, so degradation
/// of active skills and its blocking reason commit together with the status.
pub(crate) fn apply_status_effects(tx: &Transaction<'_>) -> rusqlite::Result<()> {
    let ids = tx
        .prepare(
            "SELECT generation_id FROM catalog_generation WHERE state IN ('active','degraded')",
        )?
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for id in ids {
        let generation = read_generation(tx, &id)?;
        let skills = generation
            .closure
            .into_iter()
            .filter(|p| p.kind == Kind::Skill)
            .collect::<Vec<_>>();
        if let Some(status) = blocked_status(tx, &skills)? {
            let code = if status["state"] == "tombstoned" {
                "pinned_tombstoned"
            } else {
                "pinned_revoked"
            };
            let error = json!({"code":code,"name":status["name"],"version":status["version"],"reason":status["reason"]});
            tx.execute(
                "UPDATE catalog_generation SET state='degraded',error=?1 WHERE generation_id=?2",
                params![error.to_string(), id],
            )?;
        }
    }
    Ok(())
}

impl RegistryStore {
    pub fn catalog_pin_statuses(&self, pins: &[CatalogPin]) -> Result<Vec<Value>, RegistryError> {
        self.read(|conn| pin_statuses(conn, pins))
    }
    pub fn catalog_blocked_pin(&self, pins: &[CatalogPin]) -> Result<Option<Value>, RegistryError> {
        self.read(|conn| blocked_status(conn, pins))
    }
    pub fn catalog_pinned_scopes(&self) -> Result<Vec<String>, RegistryError> {
        self.read(|conn| {
            let names = conn
                .prepare("SELECT DISTINCT name FROM catalog_pin ORDER BY name")?
                .query_map([], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(names
                .into_iter()
                .filter_map(|n| names::map_name(&n).ok().map(|n| format!("@{}", n.scope)))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect())
        })
    }
    /// Return this Mac's hourly registry poll phase, a millisecond offset within
    /// the hour. The first call saves `candidate` modulo one hour; later calls
    /// return the saved value. The phase is a machine-local value: unlike agent
    /// pins, it is never recorded in the identity journal or replicated to other
    /// Macs.
    pub fn catalog_poll_offset(&self, candidate: u64) -> Result<u64, RegistryError> {
        self.catalog_transaction(|tx| {
            tx.execute_batch("CREATE TABLE IF NOT EXISTS catalog_poll_clock(id INTEGER PRIMARY KEY CHECK(id=1), offset_ms INTEGER NOT NULL CHECK(offset_ms>=0 AND offset_ms<3600000))").map_err(store_error)?;
            tx.execute("INSERT OR IGNORE INTO catalog_poll_clock VALUES(1,?1)", [candidate % 3_600_000]).map_err(store_error)?;
            tx.query_row("SELECT offset_ms FROM catalog_poll_clock WHERE id=1", [], |r| r.get(0)).map_err(store_error)
        }).map_err(registry_error)
    }
    pub fn catalog_save_prepared(
        &self,
        id: &str,
        caller: &str,
        detail: &PreparedPlan,
    ) -> Result<(), CatalogWriteError> {
        self.catalog_transaction(|tx| {
            let plan = read_plan(tx, id).map_err(store_error)?;
            owner(&plan.agent_id, caller)?;
            if plan.state.as_str() != "pending" { return Err(CatalogWriteError::Refused("invalid_transition")); }
            tx.execute("INSERT INTO catalog_prepared VALUES(?1,?2) ON CONFLICT(plan_id) DO UPDATE SET detail=excluded.detail", params![id,serde_json::to_string(detail).unwrap()]).map_err(store_error)?;
            Ok(())
        })
    }
    pub fn catalog_prepared(
        &self,
        id: &str,
        caller: &str,
    ) -> Result<PreparedPlan, CatalogWriteError> {
        self.catalog_plan(id, caller)?;
        self.read(|conn| prepared(conn, id))
            .map_err(CatalogWriteError::Store)
    }
    pub fn catalog_install_target(
        &self,
        id: &str,
        caller: &str,
    ) -> Result<Value, CatalogWriteError> {
        self.catalog_transaction(|tx| {
            let plan = installable(tx, id, caller)?;
            Ok(json!({"plan_id":id,"base_pin_digest":plan.base_pin_digest,"prepared":prepared(tx,id).map_err(store_error)?}))
        })
    }
    /// Omit superseded generations from protocol replies. Their closures remain
    /// stored locally until consumers finish switching to the replacement.
    pub fn catalog_agent_generations(
        &self,
        agent: &str,
    ) -> Result<Vec<CatalogGeneration>, RegistryError> {
        self.read(|conn| {
            let ids = conn.prepare("SELECT generation_id FROM catalog_generation WHERE agent_id=?1 AND state!='superseded' ORDER BY created_at_ms,generation_id")?.query_map([agent], |r| r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
            ids.iter().map(|id| read_generation(conn,id)).collect()
        })
    }
    pub fn catalog_prefetch_error(
        &self,
        id: &str,
        caller: &str,
        error: Value,
    ) -> Result<(), CatalogWriteError> {
        self.catalog_transaction(|tx| {
            let generation = read_generation(tx, id).map_err(store_error)?;
            owner(&generation.agent_id, caller)?;
            tx.execute("UPDATE catalog_generation SET error=?2 WHERE generation_id=?1 AND state IN ('approved','installed')", params![id,error.to_string()]).map_err(store_error)?;
            Ok(())
        })
    }
    pub fn catalog_plan(&self, id: &str, caller: &str) -> Result<CatalogPlan, CatalogWriteError> {
        let plan = self
            .read(|conn| read_plan(conn, id))
            .map_err(CatalogWriteError::Store)?;
        owner(&plan.agent_id, caller)?;
        Ok(plan)
    }
    pub fn catalog_generation(
        &self,
        id: &str,
        caller: &str,
    ) -> Result<CatalogGeneration, CatalogWriteError> {
        let generation = self
            .read(|conn| read_generation(conn, id))
            .map_err(CatalogWriteError::Store)?;
        owner(&generation.agent_id, caller)?;
        Ok(generation)
    }
    pub fn catalog_create_plan(
        &self,
        id: &str,
        caller: &str,
        target: CatalogTarget,
        operation: &str,
        spec: CatalogSpec,
        now: i64,
    ) -> Result<CatalogPlan, CatalogWriteError> {
        if !matches!(operation, "install" | "upgrade") {
            return Err(CatalogWriteError::Refused("operation_not_yet_supported"));
        }
        let CatalogTarget::Agent(agent) = target else {
            return Err(CatalogWriteError::Refused("operation_not_yet_supported"));
        };
        owner(&agent, caller)?;
        if id.is_empty()
            || !names::map_name(&spec.package).is_ok_and(|n| n.canonical == spec.package)
            || spec.range.parse::<semver::Range>().is_err()
        {
            return Err(CatalogWriteError::Refused("invalid_request"));
        }
        self.catalog_transaction(|tx| transition(tx,"plan",id,"create",json!([agent,operation,spec]),|| {
            live(tx,&agent)?;
            let base = current_digest(tx,&agent).map_err(store_error)?;
            tx.execute("INSERT INTO catalog_plan VALUES(?1,?2,?3,?4,'pending',NULL,NULL,?5,NULL,?6,NULL)",params![id,agent,operation,serde_json::to_string(&spec).unwrap(),base,now]).map_err(store_error)?;
            read_plan(tx,id).map_err(store_error)
        }))
    }
    pub fn catalog_plan_ready(
        &self,
        id: &str,
        caller: &str,
        pins: Vec<CatalogPin>,
        now: i64,
    ) -> Result<CatalogPlan, CatalogWriteError> {
        let mut pins = pins;
        pins.sort_by(|a, b| a.name.cmp(&b.name));
        self.catalog_transaction(|tx| {
            let plan = read_plan(tx,id).map_err(store_error)?;
            owner(&plan.agent_id,caller)?;
            transition(tx,"plan",id,"ready",json!(pins),|| {
                live(tx,&plan.agent_id)?;
                if plan.state != "pending" { return Err(CatalogWriteError::Refused("invalid_transition")); }
                validate_pins(&pins).map_err(store_error)?;
                if let Some(pin) = pins.iter().find(|p| p.kind != Kind::Skill) {
                    tx.execute("UPDATE catalog_plan SET state='failed',refusal=?1 WHERE plan_id=?2",params![json!({"code":"kind_not_yet_supported","package":pin.name}).to_string(),id]).map_err(store_error)?;
                } else {
                    if current_digest(tx,&plan.agent_id).map_err(store_error)? != plan.base_pin_digest { return Err(CatalogWriteError::Refused("plan_stale")); }
                    tx.execute("UPDATE catalog_plan SET state='ready',closure=?1,ready_at_ms=?2,status_snapshot=?3 WHERE plan_id=?4",params![serde_json::to_string(&pins).unwrap(),now,status_snapshot(tx,&pins).map_err(store_error)?.to_string(),id]).map_err(store_error)?;
                }
                read_plan(tx,id).map_err(store_error)
            })
        })
    }
    pub fn catalog_plan_failed(
        &self,
        id: &str,
        caller: &str,
        refusal: Value,
    ) -> Result<CatalogPlan, CatalogWriteError> {
        self.catalog_transaction(|tx| {
            let plan = read_plan(tx, id).map_err(store_error)?;
            owner(&plan.agent_id, caller)?;
            transition(tx, "plan", id, "failed", refusal.clone(), || {
                if plan.state != "pending" {
                    return Err(CatalogWriteError::Refused("invalid_transition"));
                }
                tx.execute(
                    "UPDATE catalog_plan SET state='failed',refusal=?1 WHERE plan_id=?2",
                    params![refusal.to_string(), id],
                )
                .map_err(store_error)?;
                read_plan(tx, id).map_err(store_error)
            })
        })
    }
    /// Approval creates execution state only on this Mac. Shared pins must use
    /// JournalWriter::catalog_set_pins and the module's log-append coordinator,
    /// so other Macs receive the same pin set; this local transaction cannot do that.
    pub fn catalog_approve_plan(
        &self,
        plan_id: &str,
        caller: &str,
        generation_id: &str,
        now: i64,
    ) -> Result<CatalogGeneration, CatalogWriteError> {
        self.catalog_transaction(|tx| {
            let plan = read_plan(tx,plan_id).map_err(store_error)?;
            owner(&plan.agent_id,caller)?;
            transition(tx,"generation",generation_id,"approve",json!([plan_id,caller]),|| {
                live(tx,&plan.agent_id)?;
                if plan.state != "ready" { return Err(CatalogWriteError::Refused("invalid_transition")); }
                let pins = plan.closure.as_ref().ok_or(CatalogWriteError::Refused("invalid_transition"))?;
                let saved: String = tx.query_row("SELECT status_snapshot FROM catalog_plan WHERE plan_id=?1",[plan_id],|r|r.get(0)).map_err(store_error)?;
                if current_digest(tx,&plan.agent_id).map_err(store_error)? != plan.base_pin_digest || decode::<Value>(saved).map_err(store_error)? != status_snapshot(tx,pins).map_err(store_error)? {
                    return Err(CatalogWriteError::Refused("plan_stale"));
                }
                let existing: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM catalog_generation WHERE plan_id=?1)",[plan_id],|r|r.get(0)).map_err(store_error)?;
                if existing || generation_id.is_empty() { return Err(CatalogWriteError::Refused("request_key_conflict")); }
                let detail = tx
                    .query_row(
                        "SELECT detail FROM catalog_prepared WHERE plan_id=?1",
                        [plan_id],
                        |r| r.get::<_, String>(0),
                    )
                    .optional()
                    .map_err(store_error)?
                    .map(decode::<PreparedPlan>)
                    .transpose()
                    .map_err(store_error)?;
                let facts = publisher_facts(pins, detail.as_ref());
                tx.execute("INSERT INTO catalog_generation(generation_id,agent_id,plan_id,state,closure,pin_digest,created_at_ms,activated_at_ms,error,publisher_facts) VALUES(?1,?2,?3,'approved',?4,?5,?6,NULL,NULL,?7)",params![generation_id,plan.agent_id,plan_id,serde_json::to_string(pins).unwrap(),pin_digest(pins).map_err(store_error)?,now,serde_json::to_string(&facts).unwrap()]).map_err(store_error)?;
                read_generation(tx,generation_id).map_err(store_error)
            })
        })
    }
    pub fn catalog_generation_ready(
        &self,
        id: &str,
        caller: &str,
    ) -> Result<CatalogGeneration, CatalogWriteError> {
        self.catalog_generation_step(id, caller, "ready", None, 0)
    }
    /// When a cross-Mac agent move is aborted, delete the generation this
    /// receiving Mac prepared for the moved agent, so it can never be activated.
    /// Only a generation still approved, installed or ready is deleted. Shared
    /// pins and locally granted publisher permissions are untouched.
    pub fn catalog_discard_move_generation(
        &self,
        cut: &str,
        agent: &str,
    ) -> Result<(), CatalogWriteError> {
        let generation = format!("generation:move:{cut}");
        self.catalog_transaction(|tx| {
            tx.execute("DELETE FROM catalog_generation WHERE generation_id=?1 AND agent_id=?2 AND state IN ('approved','installed','ready')", params![generation,agent]).map_err(store_error)?;
            Ok(())
        })
    }
    pub fn catalog_activate_generation(
        &self,
        id: &str,
        caller: &str,
        now: i64,
    ) -> Result<CatalogGeneration, CatalogWriteError> {
        self.catalog_generation_step(id, caller, "active", None, now)
    }
    pub fn catalog_degrade_generation(
        &self,
        id: &str,
        caller: &str,
        error: Value,
    ) -> Result<CatalogGeneration, CatalogWriteError> {
        self.catalog_generation_step(id, caller, "degraded", Some(error), 0)
    }
    fn catalog_generation_step(
        &self,
        id: &str,
        caller: &str,
        step: &str,
        error: Option<Value>,
        now: i64,
    ) -> Result<CatalogGeneration, CatalogWriteError> {
        self.catalog_transaction(|tx| {
            let generation = read_generation(tx,id).map_err(store_error)?;
            owner(&generation.agent_id,caller)?;
            if step == "active" && blocked_status(tx,&generation.closure).map_err(store_error)?.is_some() {
                return Err(CatalogWriteError::Refused("activation_blocked"));
            }
            transition(tx,"generation",id,step,json!(error),|| {
                live(tx,&generation.agent_id)?;
                let allowed = match step {
                    "ready" => matches!(generation.state.as_str(),"approved" | "installed"),
                    "active" => generation.state == "ready",
                    "degraded" => generation.state == "active",
                    _ => false,
                };
                if !allowed { return Err(CatalogWriteError::Refused("invalid_transition")); }
                if step == "active" {
                    // An agent may have only one active generation (a unique index
                    // enforces it), so first mark the current one superseded,
                    // whether it is active or degraded. Its row and closure are
                    // kept so consumers can finish switching away from its packages.
                    tx.execute("UPDATE catalog_generation SET state='superseded' WHERE agent_id=?1 AND state IN ('active','degraded')",[&generation.agent_id]).map_err(store_error)?;
                }
                tx.execute("UPDATE catalog_generation SET state=?1,error=?2,activated_at_ms=CASE WHEN ?1='active' THEN ?3 ELSE activated_at_ms END WHERE generation_id=?4",params![step,error.map(|e|e.to_string()),now,id]).map_err(store_error)?;
                read_generation(tx,id).map_err(store_error)
            })
        })
    }
}

fn installable(
    conn: &Connection,
    id: &str,
    caller: &str,
) -> Result<CatalogPlan, CatalogWriteError> {
    let plan = read_plan(conn, id).map_err(store_error)?;
    owner(&plan.agent_id, caller)?;
    live(conn, caller)?;
    match plan.state.as_str() {
        "pending" => return Err(CatalogWriteError::Refused("plan_not_ready")),
        "failed" => return Err(CatalogWriteError::Refused("plan_failed")),
        "ready" => (),
        _ => return Err(CatalogWriteError::Refused("invalid_transition")),
    }
    let saved: String = conn
        .query_row(
            "SELECT status_snapshot FROM catalog_plan WHERE plan_id=?1",
            [id],
            |r| r.get(0),
        )
        .map_err(store_error)?;
    if current_digest(conn, caller).map_err(store_error)? != plan.base_pin_digest
        || decode::<Value>(saved).map_err(store_error)?
            != status_snapshot(conn, plan.closure.as_deref().unwrap_or_default())
                .map_err(store_error)?
    {
        return Err(CatalogWriteError::Refused("plan_stale"));
    }
    Ok(plan)
}
fn registry_error(error: CatalogWriteError) -> RegistryError {
    match error {
        CatalogWriteError::Store(e) => e,
        CatalogWriteError::Refused(code) => crate::mutations::domain(code, code),
    }
}
impl JournalWriter<'_> {
    /// Pin replacement, generation, publisher permissions and consent receipt
    /// commit together. If identity sharing is enabled, the module confirms the
    /// shared log append before allowing this transaction to commit.
    pub fn catalog_install(&self, plan_id: &str, now: i64) -> Result<Vec<u8>, RegistryError> {
        let commit = self.consent.as_ref().ok_or_else(|| {
            crate::mutations::domain("caller_forbidden", "catalog install requires authorization")
        })?;
        let caller = &commit.request.identity.agent_id;
        self.mutation(PIN_OP, None, |tx| {
            let plan = installable(tx, plan_id, caller).map_err(registry_error)?;
            let detail = prepared(tx, plan_id)?;
            let expected =
                json!({"plan_id":plan_id,"base_pin_digest":plan.base_pin_digest,"prepared":detail});
            if commit.action != "catalog_install" || commit.target["target"] != expected {
                return Err(crate::mutations::domain(
                    "plan_stale",
                    "approved closure changed",
                ));
            }
            let packages: Vec<_> = detail
                .packages
                .iter()
                .map(PreparedPackage::coverage)
                .collect();
            let operation = crate::catalog_grants::authorization_operation(
                &plan.operation,
                &read_pins(tx, caller)?,
                plan.closure.as_deref().unwrap_or_default(),
            );
            let always = match &commit.authorization {
                crate::agent::consent::Authorization::Card { id, answer } => {
                    let saved = crate::agent::consent::load_request(tx, &commit.request.identity)?;
                    if !saved.and_then(|r| r.settlement).is_some_and(|s| {
                        s.elicitation_id == *id
                            && s.state == "answered"
                            && s.choice.as_ref() == Some(answer)
                    }) {
                        return Err(crate::mutations::domain(
                            "consent_lost",
                            "approval was not recorded",
                        ));
                    }
                    match answer.as_str() {
                        "approve_once" => false,
                        "approve_always_publisher"
                            if operation == crate::catalog_grants::Operation::Install
                                && detail.packages.iter().all(|p| !p.root_certified) =>
                        {
                            true
                        }
                        _ => {
                            return Err(crate::mutations::domain(
                                "invalid_reply",
                                "approval choice not offered",
                            ))
                        }
                    }
                }
                crate::agent::consent::Authorization::PublisherGrants { .. } => {
                    if crate::catalog_grants::coverage(
                        &packages,
                        &crate::catalog_grants::read_grants(tx)?,
                        operation,
                        caller,
                        true,
                    ) != crate::catalog_grants::Coverage::Covered
                    {
                        return Err(crate::mutations::domain(
                            "plan_stale",
                            "publisher coverage changed",
                        ));
                    }
                    false
                }
                _ => {
                    return Err(crate::mutations::domain(
                        "caller_forbidden",
                        "invalid catalog authorization",
                    ))
                }
            };
            let pins = plan
                .closure
                .ok_or_else(|| crate::mutations::domain("plan_not_ready", "no closure"))?;
            if detail
                .packages
                .iter()
                .map(|p| p.pin.clone())
                .collect::<Vec<_>>()
                != pins
            {
                return Err(crate::mutations::domain(
                    "plan_stale",
                    "checked closure changed",
                ));
            }
            let generation_id = format!("generation:{plan_id}");
            let facts = publisher_facts(&pins, Some(&detail));
            tx.execute(
                "INSERT INTO catalog_generation(generation_id,agent_id,plan_id,state,closure,pin_digest,created_at_ms,activated_at_ms,error,publisher_facts) VALUES(?1,?2,?3,'approved',?4,?5,?6,NULL,NULL,?7)",
                params![
                    generation_id,
                    caller,
                    plan_id,
                    serde_json::to_string(&pins).unwrap(),
                    pin_digest(&pins)?,
                    now,
                    serde_json::to_string(&facts).unwrap()
                ],
            )?;
            if always {
                let mut ceilings = std::collections::BTreeMap::<String, BTreeSet<String>>::new();
                for p in &detail.packages {
                    ceilings
                        .entry(p.pin.publisher_fingerprint.clone())
                        .or_default()
                        .extend(p.capabilities.clone());
                }
                for (fingerprint, ceiling) in ceilings {
                    let grant = crate::catalog_grants::PublisherGrant {
                        id: format!("publisher:{plan_id}:{fingerprint}"),
                        publisher_fingerprint: fingerprint,
                        operation: crate::catalog_grants::Operation::Install,
                        placement_agent_id: caller.clone(),
                        ceiling: ceiling.into_iter().collect(),
                        card_ref: commit.request.elicitation_id.clone().or_else(|| {
                            match &commit.authorization {
                                crate::agent::consent::Authorization::Card { id, .. } => {
                                    Some(id.clone())
                                }
                                _ => None,
                            }
                        }),
                        granted_at_ms: now,
                        revoked_at_ms: None,
                    };
                    crate::catalog_grants::put_grant(
                        tx,
                        &grant,
                        &json!({"requesting_agent_id":caller,"authorization":commit.authorization}),
                    )?;
                }
            }
            let mut action = write_pins(
                tx,
                &PinWriteRequest {
                    agent_id: caller.clone(),
                    pins,
                    request_key: None,
                },
                now,
                self.principal,
            )?;
            action.value = json!({"generation_id":generation_id});
            Ok(action)
        })
    }
}

#[cfg(test)]
pub(crate) mod tests;
