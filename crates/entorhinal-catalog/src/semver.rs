//! The catalog's deliberately small SemVer dialect, without build metadata.
use crate::MAX_SAFE_INTEGER;
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, str::FromStr};
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Version {
    pub major: u64,
    pub minor: u64,
    pub patch: u64,
    pub pre: Vec<String>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VersionError {
    VersionInvalid,
}
fn numeric(s: &str) -> Option<u64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) || (s.len() > 1 && s.starts_with('0'))
    {
        return None;
    }
    s.parse::<u64>().ok().filter(|n| *n <= MAX_SAFE_INTEGER)
}
fn partial(s: &str) -> Option<(Version, usize)> {
    let (base, pre) = s.split_once('-').map_or((s, None), |(a, b)| (a, Some(b)));
    let nums: Vec<_> = base.split('.').map(numeric).collect::<Option<_>>()?;
    if nums.is_empty() || nums.len() > 3 || (pre.is_some() && nums.len() != 3) {
        return None;
    }
    let mut ids = Vec::new();
    if let Some(pre) = pre {
        for id in pre.split('.') {
            if id.is_empty()
                || !id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
                || (id.bytes().all(|b| b.is_ascii_digit()) && id.len() > 1 && id.starts_with('0'))
            {
                return None;
            }
            ids.push(id.into());
        }
    }
    Some((
        Version {
            major: nums[0],
            minor: *nums.get(1).unwrap_or(&0),
            patch: *nums.get(2).unwrap_or(&0),
            pre: ids,
        },
        nums.len(),
    ))
}
impl FromStr for Version {
    type Err = VersionError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        partial(s)
            .filter(|(_, k)| *k == 3)
            .map(|(v, _)| v)
            .ok_or(VersionError::VersionInvalid)
    }
}
impl Ord for Version {
    fn cmp(&self, b: &Self) -> Ordering {
        let core = (self.major, self.minor, self.patch).cmp(&(b.major, b.minor, b.patch));
        if core != Ordering::Equal {
            return core;
        }
        match (self.pre.is_empty(), b.pre.is_empty()) {
            (true, false) => return Ordering::Greater,
            (false, true) => return Ordering::Less,
            _ => {}
        }
        for (x, y) in self.pre.iter().zip(&b.pre) {
            let xn = x.bytes().all(|b| b.is_ascii_digit());
            let yn = y.bytes().all(|b| b.is_ascii_digit());
            let cmp = match (xn, yn) {
                (true, true) => x.len().cmp(&y.len()).then_with(|| x.cmp(y)),
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
                _ => x.cmp(y),
            };
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        self.pre.len().cmp(&b.pre.len())
    }
}
impl PartialOrd for Version {
    fn partial_cmp(&self, b: &Self) -> Option<Ordering> {
        Some(self.cmp(b))
    }
}
impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)?;
        if !self.pre.is_empty() {
            write!(f, "-{}", self.pre.join("."))?;
        }
        Ok(())
    }
}
#[derive(Clone, Debug)]
struct Bound {
    lo: Option<Version>,
    hi: Option<Version>,
    lo_inclusive: bool,
    hi_inclusive: bool,
    exact: Option<Version>,
}
#[derive(Clone, Debug)]
pub struct Range(Vec<Vec<Bound>>);
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeError {
    RangeInvalid,
}
fn v(a: u64, b: u64, c: u64) -> Version {
    Version {
        major: a,
        minor: b,
        patch: c,
        pre: Vec::new(),
    }
}
fn interval(lo: Option<Version>, li: bool, hi: Option<Version>, hi_i: bool) -> Bound {
    Bound {
        lo,
        hi,
        lo_inclusive: li,
        hi_inclusive: hi_i,
        exact: None,
    }
}
fn comparator(text: &str) -> Option<Bound> {
    if text == "*" {
        return Some(interval(None, true, None, false));
    }
    let op = [">=", "<=", "^", "~", ">", "<", "="]
        .into_iter()
        .find(|p| text.starts_with(p))
        .unwrap_or("");
    let (lo, k) = partial(&text[op.len()..])?;
    if !lo.pre.is_empty() && !op.is_empty() && op != "=" {
        return None;
    }
    let next = if k == 1 {
        v(lo.major + 1, 0, 0)
    } else {
        v(lo.major, lo.minor + 1, 0)
    };
    Some(match op {
        "" | "=" if k == 3 => Bound {
            exact: Some(lo),
            ..interval(None, true, None, false)
        },
        "" | "=" => interval(Some(lo), true, Some(next), false),
        "^" => {
            let hi = if lo.major > 0 || k == 1 {
                v(lo.major + 1, 0, 0)
            } else if lo.minor > 0 || k == 2 {
                v(0, lo.minor + 1, 0)
            } else {
                v(0, 0, lo.patch + 1)
            };
            interval(Some(lo), true, Some(hi), false)
        }
        "~" => interval(Some(lo), true, Some(next), false),
        ">=" => interval(Some(lo), true, None, false),
        ">" => interval(Some(if k == 3 { lo } else { next }), k != 3, None, false),
        "<" => interval(None, true, Some(lo), false),
        "<=" => interval(None, true, Some(if k == 3 { lo } else { next }), k == 3),
        _ => return None,
    })
}
impl FromStr for Range {
    type Err = RangeError;
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let mut sets = Vec::new();
        for set in text.split("||") {
            let parts: Vec<_> = set.split_whitespace().collect();
            if parts.is_empty() || (parts.len() > 1 && parts.contains(&"*")) {
                return Err(RangeError::RangeInvalid);
            }
            sets.push(
                parts
                    .into_iter()
                    .map(comparator)
                    .collect::<Option<Vec<_>>>()
                    .ok_or(RangeError::RangeInvalid)?,
            );
            if sets.last().is_some_and(|set| {
                set.len() > 1
                    && set
                        .iter()
                        .any(|b| b.exact.as_ref().is_some_and(|v| !v.pre.is_empty()))
            }) {
                return Err(RangeError::RangeInvalid);
            }
        }
        Ok(Self(sets))
    }
}
impl Range {
    pub fn satisfies(&self, ver: &Version) -> bool {
        self.0.iter().any(|set| {
            if !ver.pre.is_empty() {
                return set.len() == 1 && set[0].exact.as_ref() == Some(ver);
            }
            set.iter().all(|b| {
                if let Some(exact) = &b.exact {
                    return ver == exact;
                }
                b.lo.as_ref()
                    .is_none_or(|lo| ver > lo || (b.lo_inclusive && ver == lo))
                    && b.hi
                        .as_ref()
                        .is_none_or(|hi| ver < hi || (b.hi_inclusive && ver == hi))
            })
        })
    }
}
