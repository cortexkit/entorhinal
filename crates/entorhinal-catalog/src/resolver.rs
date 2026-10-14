//! Deterministic, non-backtracking closure resolution with accumulated ranges.
use crate::{
    semver::{Range, Version},
    status::State,
    Kind,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
const CAPABILITY_NAME_MAX: usize = 64;

fn capability_part(s: &str) -> bool {
    let mut bytes = s.bytes();
    matches!(bytes.next(), Some(b'a'..=b'z'))
        && bytes.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}

fn capability_major(s: &str) -> bool {
    !s.is_empty() && !s.starts_with('0') && s.bytes().all(|b| b.is_ascii_digit())
}

pub fn capability_token(s: &str) -> bool {
    let Some(body) = s.strip_prefix("cap/") else {
        return false;
    };
    let Some((parts, major)) = body.split_once('@') else {
        return false;
    };
    let mut parts = parts.split('.');
    let Some(area) = parts.next() else {
        return false;
    };
    let verb = parts.next();
    capability_part(area)
        && verb.is_none_or(capability_part)
        && parts.next().is_none()
        && capability_major(major)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum CapabilityError {
    CapabilityNotAToken,
    TokenTooLong,
}

impl std::fmt::Display for CapabilityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::CapabilityNotAToken => "capability_not_a_token",
            Self::TokenTooLong => "token_too_long",
        })
    }
}

impl std::error::Error for CapabilityError {}

pub fn capability_to_manifest(token: &str) -> Result<String, CapabilityError> {
    if !capability_token(token) {
        return Err(CapabilityError::CapabilityNotAToken);
    }
    let body = token
        .strip_prefix("cap/")
        .ok_or(CapabilityError::CapabilityNotAToken)?;
    let (identifier, version) = body
        .split_once('@')
        .ok_or(CapabilityError::CapabilityNotAToken)?;
    let (area, verb) = identifier
        .split_once('.')
        .map_or((identifier, None), |(area, verb)| (area, Some(verb)));
    let name = match verb {
        Some(verb) => format!("cap-{area}-{verb}"),
        None => format!("cap-{area}"),
    };
    if name.len() > CAPABILITY_NAME_MAX {
        return Err(CapabilityError::TokenTooLong);
    }
    Ok(format!("{name}/v{version}"))
}

pub fn manifest_to_capability(manifest: &str) -> Result<String, CapabilityError> {
    let (name, version) = manifest
        .split_once("/v")
        .ok_or(CapabilityError::CapabilityNotAToken)?;
    let body = name
        .strip_prefix("cap-")
        .ok_or(CapabilityError::CapabilityNotAToken)?;
    let mut parts = body.split('-');
    let area = parts.next().ok_or(CapabilityError::CapabilityNotAToken)?;
    let verb = parts.next();
    if parts.next().is_some()
        || !capability_part(area)
        || verb.is_some_and(|part| !capability_part(part))
        || !capability_major(version)
    {
        return Err(CapabilityError::CapabilityNotAToken);
    }
    if name.len() > CAPABILITY_NAME_MAX {
        return Err(CapabilityError::TokenTooLong);
    }
    let token = match verb {
        Some(verb) => format!("cap/{area}.{verb}@{version}"),
        None => format!("cap/{area}@{version}"),
    };
    if capability_to_manifest(&token).as_deref() != Ok(manifest) {
        return Err(CapabilityError::CapabilityNotAToken);
    }
    Ok(token)
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefField {
    Extends,
    Skills,
    Flows,
    MemoryProfile,
    DelegatesTo,
}
impl RefField {
    fn kind(self) -> Kind {
        match self {
            Self::Extends | Self::DelegatesTo => Kind::Persona,
            Self::Skills => Kind::Skill,
            Self::Flows => Kind::Flow,
            Self::MemoryProfile => Kind::MemoryProfile,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BodyRef {
    pub field: RefField,
    pub package: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegistryVersion {
    pub version: String,
    pub kind: Kind,
    pub status: State,
    pub requires: crate::record::Requires,
    #[serde(default)]
    pub refs: Vec<BodyRef>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Root {
    pub package: String,
    pub range: String,
}
#[derive(Clone, Debug, Serialize)]
pub struct AskedRange {
    pub requested_by: String,
    pub range: String,
}
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum ResolveError {
    PinConflict {
        package: String,
        pinned_version: String,
        range: String,
        requested_by: String,
    },
    PackageNotFound {
        package: String,
    },
    RangeInvalid {
        package: String,
        range: String,
        requested_by: String,
    },
    VersionInvalid {
        package: String,
        version: String,
    },
    NoMatchingVersion {
        package: String,
        ranges: Vec<AskedRange>,
    },
    RangeConflict {
        package: String,
        ranges: Vec<AskedRange>,
    },
    DependencyCycle {
        cycle: Vec<String>,
    },
    BodyRefMissing {
        package: String,
        field: RefField,
        #[serde(rename = "ref")]
        reference: String,
    },
    KindMismatch {
        package: String,
        field: RefField,
        #[serde(rename = "ref")]
        reference: String,
        expected: Kind,
        found: Kind,
    },
    DelegateMissing {
        contract: String,
    },
    DelegateAmbiguous {
        contract: String,
        providers: Vec<String>,
    },
}
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Added,
    Removed,
    Down,
    Same,
    Up,
}
#[derive(Clone, Debug, Serialize)]
pub struct Move {
    pub from: Option<String>,
    pub to: Option<String>,
    pub direction: Direction,
}
#[derive(Clone, Debug, Serialize)]
pub struct Resolution {
    pub picks: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub moves: Option<BTreeMap<String, Move>>,
}
pub fn resolve(
    registry: &BTreeMap<String, Vec<RegistryVersion>>,
    root: &Root,
    caps: &[String],
    pins: Option<&BTreeMap<String, String>>,
) -> Result<Resolution, ResolveError> {
    let mut ranges: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::from([(
        root.package.clone(),
        BTreeMap::from([("(root)".into(), root.range.clone())]),
    )]);
    let mut picks: BTreeMap<String, &RegistryVersion>;
    loop {
        picks = BTreeMap::new();
        let mut added = false;
        let mut queue = VecDeque::from([root.package.clone()]);
        while let Some(pkg) = queue.pop_front() {
            if picks.contains_key(&pkg) {
                continue;
            }
            let versions = registry
                .get(&pkg)
                .ok_or_else(|| ResolveError::PackageNotFound {
                    package: pkg.clone(),
                })?;
            let asked = ranges.get(&pkg).cloned().unwrap_or_default();
            let mut parsed = Vec::new();
            for (by, text) in &asked {
                parsed.push(
                    text.parse::<Range>()
                        .map_err(|_| ResolveError::RangeInvalid {
                            package: pkg.clone(),
                            range: text.clone(),
                            requested_by: by.clone(),
                        })?,
                );
            }
            let mut usable = Vec::new();
            for rv in versions {
                if rv.status == State::Available
                    && rv.requires.capabilities.iter().all(|t| caps.contains(t))
                {
                    let ver = rv.version.parse::<Version>().map_err(|_| {
                        ResolveError::VersionInvalid {
                            package: pkg.clone(),
                            version: rv.version.clone(),
                        }
                    })?;
                    usable.push((rv, ver));
                }
            }
            let mut candidates: Vec<_> = usable
                .iter()
                .filter(|(_, v)| parsed.iter().all(|r| r.satisfies(v)))
                .collect();
            if candidates.is_empty() {
                let each = parsed
                    .iter()
                    .all(|r| usable.iter().any(|(_, v)| r.satisfies(v)));
                let detail = asked
                    .into_iter()
                    .map(|(requested_by, range)| AskedRange {
                        requested_by,
                        range,
                    })
                    .collect();
                return Err(if parsed.len() > 1 && each {
                    ResolveError::RangeConflict {
                        package: pkg,
                        ranges: detail,
                    }
                } else {
                    ResolveError::NoMatchingVersion {
                        package: pkg,
                        ranges: detail,
                    }
                });
            }
            candidates.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.version.cmp(&b.0.version)));
            let pick = candidates[0].0;
            picks.insert(pkg.clone(), pick);
            for (dep, text) in &pick.requires.packages {
                let by = format!("{pkg}@{}", pick.version);
                let recorded = ranges.entry(dep.clone()).or_default();
                if let std::collections::btree_map::Entry::Vacant(e) = recorded.entry(by) {
                    e.insert(text.clone());
                    added = true;
                }
            }
            queue.extend(pick.requires.packages.keys().cloned());
        }
        if !added {
            break;
        }
    }
    // Iterative DFS avoids a call-stack overflow for large untrusted closures.
    let mut stack = vec![(root.package.clone(), false)];
    let mut path = Vec::new();
    let mut done = BTreeSet::new();
    while let Some((pkg, exit)) = stack.pop() {
        if exit {
            path.pop();
            done.insert(pkg);
            continue;
        }
        if let Some(i) = path.iter().position(|p| p == &pkg) {
            let mut cycle = path[i..].to_vec();
            cycle.push(pkg);
            return Err(ResolveError::DependencyCycle { cycle });
        }
        if done.contains(&pkg) {
            continue;
        }
        path.push(pkg.clone());
        stack.push((pkg.clone(), true));
        if let Some(pick) = picks.get(&pkg) {
            for dep in pick.requires.packages.keys().rev() {
                stack.push((dep.clone(), false));
            }
        }
    }
    for (pkg, pick) in &picks {
        for r in &pick.refs {
            if !pick.requires.packages.contains_key(&r.package) {
                return Err(ResolveError::BodyRefMissing {
                    package: pkg.clone(),
                    field: r.field,
                    reference: r.package.clone(),
                });
            }
        }
    }
    for (pkg, pick) in &picks {
        for r in &pick.refs {
            let found = picks.get(&r.package).map(|p| p.kind).ok_or_else(|| {
                ResolveError::PackageNotFound {
                    package: r.package.clone(),
                }
            })?;
            let expected = r.field.kind();
            if found != expected {
                return Err(ResolveError::KindMismatch {
                    package: pkg.clone(),
                    field: r.field,
                    reference: r.package.clone(),
                    expected,
                    found,
                });
            }
        }
    }
    let out: BTreeMap<_, _> = picks
        .iter()
        .map(|(p, v)| (p.clone(), v.version.clone()))
        .collect();
    let moves = if let Some(pins) = pins {
        let mut moves = BTreeMap::new();
        let names: BTreeSet<_> = pins.keys().chain(out.keys()).collect();
        for pkg in names {
            let from = pins.get(pkg).cloned();
            let to = out.get(pkg).cloned();
            let direction = match (&from, &to) {
                (None, _) => Direction::Added,
                (_, None) => Direction::Removed,
                (Some(a), Some(b)) => {
                    let parse = |s: &str| {
                        s.parse::<Version>()
                            .map_err(|_| ResolveError::VersionInvalid {
                                package: pkg.clone(),
                                version: s.into(),
                            })
                    };
                    match parse(b)?.cmp(&parse(a)?) {
                        std::cmp::Ordering::Less => Direction::Down,
                        std::cmp::Ordering::Equal => Direction::Same,
                        std::cmp::Ordering::Greater => Direction::Up,
                    }
                }
            };
            moves.insert(
                pkg.clone(),
                Move {
                    from,
                    to,
                    direction,
                },
            );
        }
        Some(moves)
    } else {
        None
    };
    Ok(Resolution { picks: out, moves })
}
/// Select an unnamed worker provider from the resolved closure, never from the
/// host registry. Explicitly named providers are checked by the consumer.
pub fn worker_provider(
    contract: &str,
    implements: &BTreeMap<String, Vec<String>>,
) -> Result<String, ResolveError> {
    let providers: Vec<_> = implements
        .iter()
        .filter(|(_, ids)| ids.iter().any(|id| id == contract))
        .map(|(p, _)| p.clone())
        .collect();
    match providers.len() {
        0 => Err(ResolveError::DelegateMissing {
            contract: contract.into(),
        }),
        1 => Ok(providers[0].clone()),
        _ => Err(ResolveError::DelegateAmbiguous {
            contract: contract.into(),
            providers,
        }),
    }
}

/// Build multi-root requirements using dependency edges from each pin's own
/// version, never from the highest candidate. Non-subject roots and everything
/// they reach stay exact pins; subject roots retain their requested ranges and
/// dependencies not reached by another root are free to resolve or disappear.
pub fn synthetic_root_ranges(
    registry: &BTreeMap<String, Vec<RegistryVersion>>,
    roots: &BTreeMap<String, String>,
    subject_roots: &[String],
    current_pins: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, ResolveError> {
    let subjects: BTreeSet<_> = subject_roots.iter().cloned().collect();
    for subject in &subjects {
        if !roots.contains_key(subject) {
            return Err(ResolveError::PackageNotFound {
                package: subject.clone(),
            });
        }
    }
    let mut non_subject_reachable = BTreeSet::new();
    let mut queue: VecDeque<_> = roots
        .keys()
        .filter(|r| !subjects.contains(*r))
        .cloned()
        .collect();
    while let Some(package) = queue.pop_front() {
        if !non_subject_reachable.insert(package.clone()) {
            continue;
        }
        if let Some(pinned) = current_pins.get(&package).and_then(|version| {
            registry
                .get(&package)?
                .iter()
                .find(|r| &r.version == version)
        }) {
            queue.extend(
                pinned
                    .requires
                    .packages
                    .keys()
                    .filter(|p| current_pins.contains_key(*p))
                    .cloned(),
            );
        }
    }
    let mut subject_set = subjects.clone();
    for package in current_pins.keys() {
        if !roots.contains_key(package) && !non_subject_reachable.contains(package) {
            subject_set.insert(package.clone());
        }
    }
    let mut ranges = BTreeMap::new();
    for (root, range) in roots {
        let range = if subjects.contains(root) {
            range.clone()
        } else {
            format!(
                "={}",
                current_pins
                    .get(root)
                    .ok_or_else(|| ResolveError::PackageNotFound {
                        package: root.clone()
                    })?
            )
        };
        ranges.insert(root.clone(), range);
    }
    for (package, version) in current_pins {
        if !subject_set.contains(package) {
            ranges.insert(package.clone(), format!("={version}"));
        }
    }
    Ok(ranges)
}

/// Resolve several roots with the single-root algorithm. The artificial package
/// never appears in returned picks/moves; its constraints are attributed to
/// `(root)`. An incompatible exact pin becomes the contract's pin_conflict.
pub fn resolve_roots(
    registry: &BTreeMap<String, Vec<RegistryVersion>>,
    roots: &BTreeMap<String, String>,
    subject_roots: &[String],
    current_pins: &BTreeMap<String, String>,
    caps: &[String],
) -> Result<Resolution, ResolveError> {
    const SYNTHETIC: &str = "\0catalog-roots";
    let ranges = synthetic_root_ranges(registry, roots, subject_roots, current_pins)?;
    let mut input = registry.clone();
    input.insert(
        SYNTHETIC.into(),
        vec![RegistryVersion {
            version: "0.0.0".into(),
            kind: Kind::Skill,
            status: State::Available,
            requires: crate::record::Requires {
                capabilities: vec![],
                packages: ranges.clone(),
            },
            refs: vec![],
        }],
    );
    match resolve(
        &input,
        &Root {
            package: SYNTHETIC.into(),
            range: "*".into(),
        },
        caps,
        Some(current_pins),
    ) {
        Ok(mut result) => {
            result.picks.remove(SYNTHETIC);
            result
                .moves
                .as_mut()
                .expect("pins produce moves")
                .remove(SYNTHETIC);
            Ok(result)
        }
        Err(mut error) => {
            let asked = match &mut error {
                ResolveError::RangeConflict { ranges, .. }
                | ResolveError::NoMatchingVersion { ranges, .. } => Some(ranges),
                ResolveError::RangeInvalid { requested_by, .. } => {
                    if requested_by == &format!("{SYNTHETIC}@0.0.0") {
                        *requested_by = "(root)".into();
                    }
                    None
                }
                _ => None,
            };
            if let Some(asked) = asked {
                for constraint in asked {
                    if constraint.requested_by == format!("{SYNTHETIC}@0.0.0") {
                        constraint.requested_by = "(root)".into();
                    }
                }
            }
            if let ResolveError::RangeConflict {
                package,
                ranges: asked,
            }
            | ResolveError::NoMatchingVersion {
                package,
                ranges: asked,
            } = &error
            {
                if let Some(version) = current_pins
                    .get(package)
                    .filter(|v| ranges.get(package) == Some(&format!("={v}")))
                {
                    if let Ok(pinned) = version.parse::<Version>() {
                        if let Some(conflict) = asked.iter().find(|a| {
                            !(a.requested_by == "(root)" && a.range == format!("={version}"))
                                && a.range
                                    .parse::<Range>()
                                    .is_ok_and(|r| !r.satisfies(&pinned))
                        }) {
                            return Err(ResolveError::PinConflict {
                                package: package.clone(),
                                pinned_version: version.clone(),
                                range: conflict.range.clone(),
                                requested_by: conflict.requested_by.clone(),
                            });
                        }
                    }
                }
            }
            Err(error)
        }
    }
}
