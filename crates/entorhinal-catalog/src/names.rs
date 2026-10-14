//! ASCII package identity and authoritative binding checks.
use crate::{sha256_hex, Refusal, RefusalCode};
use serde::Serialize;
use std::collections::BTreeMap;
pub const SCOPE_MAX: usize = 39;
pub const PACKAGE_NAME_MAX: usize = 64;
pub const BASAL_ID_MAX: usize = 46;
pub const RESERVED_SCOPES: &[&str] = &["cortexkit", "ck", "catalog", "registry"];
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum NamePart {
    Whole,
    Scope,
    Name,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct NameError {
    pub code: RefusalCode,
    pub part: NamePart,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum IdForm {
    Short,
    Hashed,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct MappedName {
    pub canonical: String,
    pub scope: String,
    pub name: String,
    pub short: String,
    #[serde(rename = "shortBytes")]
    pub short_bytes: usize,
    pub sha256: String,
    pub form: IdForm,
    pub id: String,
}
pub(crate) fn part(s: &str) -> bool {
    !s.is_empty()
        && s.split('-').all(|p| {
            !p.is_empty()
                && p.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        })
}
pub fn map_name(input: &str) -> Result<MappedName, NameError> {
    let lowered = input.to_ascii_lowercase();
    let err = |part| NameError {
        code: RefusalCode::NameInvalid,
        part,
    };
    let (scope, name) = lowered
        .strip_prefix('@')
        .and_then(|s| s.split_once('/'))
        .ok_or_else(|| err(NamePart::Whole))?;
    if name.contains('/') {
        return Err(err(NamePart::Whole));
    }
    if !part(scope) || scope.len() > SCOPE_MAX {
        return Err(err(NamePart::Scope));
    }
    if !part(name) || name.len() > PACKAGE_NAME_MAX {
        return Err(err(NamePart::Name));
    }
    let short = format!("{scope}--{name}");
    let digest = sha256_hex(&lowered);
    let hashed = short.len() > BASAL_ID_MAX;
    let id = if hashed {
        format!("{}_{}", &short[..29], &digest[..16])
    } else {
        short.clone()
    };
    Ok(MappedName {
        scope: scope.into(),
        name: name.into(),
        canonical: lowered,
        short_bytes: short.len(),
        short,
        sha256: digest,
        form: if hashed {
            IdForm::Hashed
        } else {
            IdForm::Short
        },
        id,
    })
}
pub(crate) fn canonical(s: &str) -> bool {
    map_name(s).is_ok_and(|n| n.canonical == s)
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Binding {
    pub id: String,
    pub new_binding: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct BindError {
    pub code: RefusalCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bound_to: Option<String>,
}
pub fn bind_name(table: &BTreeMap<String, String>, input: &str) -> Result<Binding, BindError> {
    let err = |code| BindError {
        code,
        id: None,
        bound_to: None,
    };
    let n = map_name(input).map_err(|_| err(RefusalCode::NameInvalid))?;
    if n.canonical != input {
        return Err(err(RefusalCode::NameNotCanonical));
    }
    if let Some(bound) = table.get(&n.id) {
        if *bound != n.canonical {
            return Err(BindError {
                code: RefusalCode::IdBound,
                id: Some(n.id),
                bound_to: Some(bound.clone()),
            });
        }
    }
    Ok(Binding {
        new_binding: !table.contains_key(&n.id),
        id: n.id,
    })
}
pub fn claim_scope(input: &str) -> Result<(), Refusal> {
    let lowered = input.to_ascii_lowercase();
    if !part(&lowered) || lowered.len() > SCOPE_MAX {
        return Err(Refusal::new(RefusalCode::NameInvalid));
    }
    if RESERVED_SCOPES.contains(&lowered.as_str()) {
        return Err(Refusal::new(RefusalCode::ScopeReserved));
    }
    if lowered != input {
        return Err(Refusal::new(RefusalCode::NameNotCanonical));
    }
    Ok(())
}
