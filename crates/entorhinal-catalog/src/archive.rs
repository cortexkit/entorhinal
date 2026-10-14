//! Strict ustar reader and deterministic regular-file writer.
use crate::RefusalCode;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
pub const ARCHIVE_MAX: usize = 10_485_760;
pub const MANIFEST_MAX: usize = 65_536;
pub const MAX_MEMBERS: usize = 1000;
pub const PATH_MAX: usize = 255;
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArchiveRule {
    HeaderChecksum,
    Magic,
    MemberType,
    SizeEncoding,
    PathInvalid,
    PathTooLong,
    PathNotUstar,
    TooManyMembers,
    PathDuplicate,
    PathCollision,
    Truncated,
    TerminatorMissing,
    TrailingData,
    CkJsonMissing,
    CkJsonNotFirst,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ArchiveError {
    pub code: RefusalCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rule: Option<ArchiveRule>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub member: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub typeflag: Option<String>,
}
impl ArchiveError {
    fn cap(code: RefusalCode) -> Self {
        Self {
            code,
            rule: None,
            offset: None,
            member: None,
            typeflag: None,
        }
    }
    fn new(rule: ArchiveRule, offset: Option<usize>) -> Self {
        Self {
            code: RefusalCode::ArchiveInvalid,
            rule: Some(rule),
            offset,
            member: None,
            typeflag: None,
        }
    }
    fn member(rule: ArchiveRule, offset: Option<usize>, path: &str) -> Self {
        Self {
            member: Some(path.into()),
            ..Self::new(rule, offset)
        }
    }
}
impl std::fmt::Display for ArchiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "archive_invalid: {:?}", self.rule)
    }
}
impl std::error::Error for ArchiveError {}
pub fn ustar_split(path: &str) -> Option<(&str, &str)> {
    if path.len() <= 100 {
        return Some(("", path));
    }
    let start = path.len().saturating_sub(101).max(1);
    let end = 155.min(path.len().saturating_sub(2));
    (start..=end)
        .find(|&i| path.as_bytes()[i] == b'/')
        .map(|i| (&path[..i], &path[i + 1..]))
}
pub fn check_path(path: &str) -> Result<(), ArchiveRule> {
    if path.split('/').any(|p| {
        p.is_empty()
            || p == "."
            || p == ".."
            || !p
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    }) {
        return Err(ArchiveRule::PathInvalid);
    }
    if path.len() > PATH_MAX {
        return Err(ArchiveRule::PathTooLong);
    }
    if ustar_split(path).is_none() {
        return Err(ArchiveRule::PathNotUstar);
    }
    Ok(())
}
fn octal(field: &[u8]) -> Option<usize> {
    let n = field
        .iter()
        .take_while(|b| (b'0'..=b'7').contains(b))
        .count();
    if n == 0 || n == field.len() || field[n..].iter().any(|b| *b != 0 && *b != b' ') {
        return None;
    }
    field[..n].iter().try_fold(0usize, |v, b| {
        v.checked_mul(8)?.checked_add((b - b'0') as usize)
    })
}
fn text(field: &[u8]) -> String {
    field
        .iter()
        .take_while(|b| **b != 0)
        .map(|b| char::from(*b))
        .collect()
}
pub fn read(archive: &[u8]) -> Result<BTreeMap<String, &[u8]>, ArchiveError> {
    if archive.len() > ARCHIVE_MAX {
        return Err(ArchiveError::cap(RefusalCode::ArchiveTooLarge));
    }
    let mut members = BTreeMap::new();
    let mut lower = BTreeMap::new();
    let mut at = 0;
    loop {
        let fail = |rule| ArchiveError::new(rule, Some(at));
        if archive.len().saturating_sub(at) < 512 {
            return Err(fail(ArchiveRule::TerminatorMissing));
        }
        let h = &archive[at..at + 512];
        if h.iter().all(|b| *b == 0) {
            if archive.len() - at < 1024 || archive[at + 512..at + 1024].iter().any(|b| *b != 0) {
                return Err(fail(ArchiveRule::TerminatorMissing));
            }
            if let Some(i) = archive[at + 1024..].iter().position(|b| *b != 0) {
                return Err(ArchiveError::new(
                    ArchiveRule::TrailingData,
                    Some(at + 1024 + i),
                ));
            }
            break;
        }
        let sum: usize = h
            .iter()
            .enumerate()
            .map(|(i, b)| {
                if (148..156).contains(&i) {
                    32
                } else {
                    *b as usize
                }
            })
            .sum();
        if octal(&h[148..156]) != Some(sum) {
            return Err(fail(ArchiveRule::HeaderChecksum));
        }
        if &h[257..263] != b"ustar\0" || &h[263..265] != b"00" {
            return Err(fail(ArchiveRule::Magic));
        }
        if h[156] != b'0' && h[156] != 0 {
            return Err(ArchiveError {
                typeflag: Some(char::from(h[156]).to_string()),
                ..fail(ArchiveRule::MemberType)
            });
        }
        let size = octal(&h[124..136]).ok_or_else(|| fail(ArchiveRule::SizeEncoding))?;
        let name = text(&h[..100]);
        let prefix = text(&h[345..500]);
        let path = if prefix.is_empty() {
            name
        } else {
            format!("{prefix}/{name}")
        };
        let member_fail = |r| ArchiveError::member(r, Some(at), &path);
        check_path(&path).map_err(member_fail)?;
        if members.len() + 1 > MAX_MEMBERS {
            return Err(member_fail(ArchiveRule::TooManyMembers));
        }
        if let Some(seen) = lower.get(&path.to_ascii_lowercase()) {
            return Err(member_fail(if seen == &path {
                ArchiveRule::PathDuplicate
            } else {
                ArchiveRule::PathCollision
            }));
        }
        lower.insert(path.to_ascii_lowercase(), path.clone());
        let end = (at + 512)
            .checked_add(size)
            .filter(|end| *end <= archive.len())
            .ok_or_else(|| member_fail(ArchiveRule::Truncated))?;
        if path == "ck.json" && size > MANIFEST_MAX {
            return Err(ArchiveError::cap(RefusalCode::ManifestTooLarge));
        }
        members.insert(path, &archive[at + 512..end]);
        at = size
            .checked_add(511)
            .and_then(|n| n.checked_div(512))
            .and_then(|n| n.checked_mul(512))
            .and_then(|n| n.checked_add(at + 512))
            .ok_or_else(|| fail(ArchiveRule::Truncated))?;
    }
    if !members.contains_key("ck.json") {
        return Err(ArchiveError::new(ArchiveRule::CkJsonMissing, None));
    }
    Ok(members)
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MemberKind {
    Regular,
    Directory,
    Link,
    Device,
    Fifo,
    Extension,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Member<'a> {
    pub path: &'a str,
    pub bytes: &'a [u8],
    pub kind: MemberKind,
}
impl<'a> Member<'a> {
    pub fn regular(path: &'a str, bytes: &'a [u8]) -> Self {
        Self {
            path,
            bytes,
            kind: MemberKind::Regular,
        }
    }
}
/// Write ordered regular-file members with fixed metadata. The caller supplies
/// ck.json first; remaining members retain their supplied order.
pub fn write(members: &[Member<'_>]) -> Result<Vec<u8>, ArchiveError> {
    if members.len() > MAX_MEMBERS {
        return Err(ArchiveError::new(ArchiveRule::TooManyMembers, None));
    }
    let mut lower = BTreeSet::new();
    let mut exact = BTreeSet::new();
    let mut total = 1024usize;
    for m in members {
        check_path(m.path).map_err(|r| ArchiveError {
            code: match r {
                ArchiveRule::PathInvalid => RefusalCode::PathInvalid,
                ArchiveRule::PathTooLong => RefusalCode::PathTooLong,
                _ => RefusalCode::PathNotUstar,
            },
            rule: None,
            offset: None,
            member: Some(m.path.into()),
            typeflag: None,
        })?;
        if m.kind != MemberKind::Regular {
            return Err(ArchiveError::member(ArchiveRule::MemberType, None, m.path));
        }
        if !exact.insert(m.path) {
            return Err(ArchiveError::member(
                ArchiveRule::PathDuplicate,
                None,
                m.path,
            ));
        }
        if !lower.insert(m.path.to_ascii_lowercase()) {
            return Err(ArchiveError::member(
                ArchiveRule::PathCollision,
                None,
                m.path,
            ));
        }
        if m.path == "ck.json" && m.bytes.len() > MANIFEST_MAX {
            return Err(ArchiveError::cap(RefusalCode::ManifestTooLarge));
        }
        total = m
            .bytes
            .len()
            .checked_add(511)
            .and_then(|s| s.checked_div(512))
            .and_then(|s| s.checked_mul(512))
            .and_then(|s| s.checked_add(512))
            .and_then(|s| total.checked_add(s))
            .filter(|s| *s <= ARCHIVE_MAX)
            .ok_or_else(|| ArchiveError::cap(RefusalCode::ArchiveTooLarge))?;
    }
    if !exact.contains("ck.json") {
        return Err(ArchiveError::new(ArchiveRule::CkJsonMissing, None));
    }
    if members.first().is_none_or(|m| m.path != "ck.json") {
        return Err(ArchiveError::new(ArchiveRule::CkJsonNotFirst, None));
    }
    let mut out = Vec::with_capacity(total);
    for m in members {
        let (prefix, name) = ustar_split(m.path)
            .ok_or_else(|| ArchiveError::member(ArchiveRule::PathNotUstar, None, m.path))?;
        let mut h = [0u8; 512];
        h[..name.len()].copy_from_slice(name.as_bytes());
        h[345..345 + prefix.len()].copy_from_slice(prefix.as_bytes());
        h[100..108].copy_from_slice(b"0000644\0");
        h[108..116].copy_from_slice(b"0000000\0");
        h[116..124].copy_from_slice(b"0000000\0");
        h[124..136].copy_from_slice(format!("{:011o}\0", m.bytes.len()).as_bytes());
        h[136..148].copy_from_slice(b"00000000000\0");
        h[148..156].fill(b' ');
        h[156] = b'0';
        h[257..263].copy_from_slice(b"ustar\0");
        h[263..265].copy_from_slice(b"00");
        let sum: usize = h.iter().map(|b| *b as usize).sum();
        h[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
        out.extend_from_slice(&h);
        out.extend_from_slice(m.bytes);
        out.resize(out.len().div_ceil(512) * 512, 0);
    }
    out.resize(out.len() + 1024, 0);
    Ok(out)
}
