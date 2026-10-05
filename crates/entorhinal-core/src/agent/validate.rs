use std::collections::BTreeSet;

use icu_casemap::CaseMapper;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{names::trim_unicode_15_1, AgentRegistryError, InvalidAgentLabelReason};

// Identity fields have separate bounds: tag/scope ids use bytes, labels use
// Unicode scalars, and a label list has its own count limit.
// Source: prefrontal 873870be8, crates/prefrontal-core-store/src/agent_registry.rs:30-33.
const MAX_TAG_BYTES: usize = 256;
const MAX_AGENT_LABELS: usize = 16;
const MAX_AGENT_LABEL_SCALARS: usize = 32;
const MAX_SCOPE_BYTES: usize = 512;

/// Represent a GitHub App or user-token identity using credential references,
/// never the credentials themselves; reject fields outside the tagged variant.
/// Source: prefrontal 873870be8,
/// crates/prefrontal-core-store/src/agent_registry.rs:600-628.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum GithubIdentity {
    App {
        app_id: i64,
        app_slug: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        installation_id: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        client_id: Option<String>,
        credential_ref: String,
        coauthor_line: String,
    },
    UserToken {
        login: String,
        credential_ref: String,
        coauthor_line: Option<String>,
    },
}

/// Carry an avatar genome with optional layout type and rendering version.
/// Absent type/version fields are omitted from the set-avatar reply, not null.
/// Source: prefrontal 873870be8,
/// crates/prefrontal-core-store/src/agent_registry.rs:638-647.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentAvatar {
    pub genome: String,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub avatar_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<i64>,
}

/// Trim a tag using Unicode 15.1 whitespace, then require a non-empty result of
/// at most 256 bytes; the limit counts UTF-8 bytes, not Unicode scalars.
/// Source: prefrontal 873870be8,
/// crates/prefrontal-core-store/src/agent_registry.rs:1324-1330.
pub fn validate_agent_tag(raw: &str) -> Result<String, AgentRegistryError> {
    let tag = trim_unicode_15_1(raw);
    if tag.is_empty() || tag.len() > MAX_TAG_BYTES {
        return Err(AgentRegistryError::InvalidTag);
    }
    Ok(tag.to_owned())
}

/// Accept at most 16 trimmed, non-empty labels of at most 32 Unicode scalars,
/// preserving request order and spelling while refusing case-folded duplicates.
/// Source: prefrontal 873870be8,
/// crates/prefrontal-core-store/src/agent_registry.rs:1332-1361.
pub fn validate_agent_labels(raw: &[String]) -> Result<Vec<String>, AgentRegistryError> {
    if raw.len() > MAX_AGENT_LABELS {
        return Err(AgentRegistryError::InvalidLabels {
            reason: InvalidAgentLabelReason::TooMany,
        });
    }

    let mut normalized = BTreeSet::new();
    raw.iter()
        .map(|label| {
            let label = trim_unicode_15_1(label);
            if label.is_empty() {
                return Err(AgentRegistryError::InvalidLabels {
                    reason: InvalidAgentLabelReason::Empty,
                });
            }
            if label.chars().count() > MAX_AGENT_LABEL_SCALARS {
                return Err(AgentRegistryError::InvalidLabels {
                    reason: InvalidAgentLabelReason::TooLong,
                });
            }
            if !normalized.insert(CaseMapper::new().fold_string(label)) {
                return Err(AgentRegistryError::InvalidLabels {
                    reason: InvalidAgentLabelReason::Duplicate,
                });
            }
            Ok(label.to_owned())
        })
        .collect()
}

/// Validate a present string after Unicode 15.1 trimming against a byte bound,
/// preserving an absent optional value and using the caller's refusal reason.
/// Source: prefrontal 873870be8,
/// crates/prefrontal-core-store/src/agent_registry.rs:1363-1377.
fn validate_optional_scalar(
    raw: Option<&str>,
    maximum_bytes: usize,
    error: AgentRegistryError,
) -> Result<Option<String>, AgentRegistryError> {
    raw.map(|value| {
        let value = trim_unicode_15_1(value);
        if value.is_empty() || value.len() > maximum_bytes {
            Err(error.clone())
        } else {
            Ok(value.to_owned())
        }
    })
    .transpose()
}

/// Require a project id that is non-empty after trimming and at most 512 bytes.
/// Identity validation imposes no prefix or character-set restriction.
/// Source: prefrontal 873870be8,
/// crates/prefrontal-core-store/src/agent_registry.rs:1379-1386.
pub fn validate_project_id(raw: &str) -> Result<String, AgentRegistryError> {
    validate_optional_scalar(
        Some(raw),
        MAX_SCOPE_BYTES,
        AgentRegistryError::InvalidProjectId,
    )?
    .ok_or(AgentRegistryError::InvalidProjectId)
}

/// Require a workspace id that is non-empty after trimming and at most 512 bytes.
/// Identity validation imposes no prefix or character-set restriction.
/// Source: prefrontal 873870be8,
/// crates/prefrontal-core-store/src/agent_registry.rs:1388-1395.
pub fn validate_workspace_id(raw: &str) -> Result<String, AgentRegistryError> {
    validate_optional_scalar(
        Some(raw),
        MAX_SCOPE_BYTES,
        AgentRegistryError::InvalidWorkspaceId,
    )?
    .ok_or(AgentRegistryError::InvalidWorkspaceId)
}

/// Require positive numeric GitHub ids and non-empty required strings, including
/// optional strings when present, without trimming or checking credential syntax.
/// Source: prefrontal 873870be8,
/// crates/prefrontal-core-store/src/agent_registry.rs:1507-1552.
pub fn validate_github_identity(identity: &GithubIdentity) -> Result<(), AgentRegistryError> {
    let required_string = |value: &str, field| {
        (!value.is_empty())
            .then_some(())
            .ok_or(AgentRegistryError::InvalidGithubIdentity { field })
    };
    match identity {
        GithubIdentity::App {
            app_id,
            app_slug,
            installation_id,
            client_id,
            credential_ref,
            coauthor_line,
        } => {
            if *app_id <= 0 {
                return Err(AgentRegistryError::InvalidGithubIdentity { field: "app_id" });
            }
            required_string(app_slug, "app_slug")?;
            if installation_id.is_some_and(|installation_id| installation_id <= 0) {
                return Err(AgentRegistryError::InvalidGithubIdentity {
                    field: "installation_id",
                });
            }
            // Missing client_id is accepted because older bindings lack it,
            // but a present client_id must be non-empty.
            if let Some(client_id) = client_id {
                required_string(client_id, "client_id")?;
            }
            required_string(credential_ref, "credential_ref")?;
            required_string(coauthor_line, "coauthor_line")
        }
        GithubIdentity::UserToken {
            login,
            credential_ref,
            coauthor_line,
        } => {
            required_string(login, "login")?;
            required_string(credential_ref, "credential_ref")?;
            if let Some(coauthor_line) = coauthor_line {
                required_string(coauthor_line, "coauthor_line")?;
            }
            Ok(())
        }
    }
}

/// Decode null as an unset identity and tagged objects as a GitHub identity.
/// Malformed objects refuse `invalid_request`, separately from semantic validation.
/// Source: prefrontal 873870be8,
/// crates/prefrontal-core-module/src/agent_registry_ops.rs:844-851.
pub fn decode_github_identity(value: Value) -> Result<Option<GithubIdentity>, AgentRegistryError> {
    if value.is_null() {
        return Ok(None);
    }
    serde_json::from_value(value).map(Some).map_err(|error| {
        AgentRegistryError::invalid_request(format!("invalid github_identity: {error}"))
    })
}

/// Return the fixed genome length for the supported `creature.classic` layout,
/// refusing unknown avatar types rather than guessing their layout.
/// Source: prefrontal 873870be8,
/// crates/prefrontal-core-module/src/agent_registry_ops.rs:835-842.
fn avatar_layout_hex_chars(avatar_type: &str) -> Result<usize, AgentRegistryError> {
    match avatar_type {
        "creature.classic" => Ok(2048),
        _ => Err(AgentRegistryError::invalid_request(format!(
            "unknown avatar type '{avatar_type}'"
        ))),
    }
}

/// Require an explicit supported avatar type and a genome of its fixed byte
/// length, reporting missing types, unknown types and length mismatches as
/// `invalid_request` rather than accepting an ambiguous or incomplete avatar.
/// Source: prefrontal 873870be8,
/// crates/prefrontal-core-module/src/agent_registry_ops.rs:3276-3285.
pub fn validate_agent_avatar(
    genome: &str,
    avatar_type: Option<&str>,
) -> Result<(), AgentRegistryError> {
    let avatar_type = avatar_type.ok_or_else(|| {
        AgentRegistryError::invalid_request("agent.set_avatar requires type with genome")
    })?;
    let expected_hex_chars = avatar_layout_hex_chars(avatar_type)?;
    // Check only length, not hex characters: core accepts 2048-character genomes
    // with non-hex content, so a stricter check would refuse avatars it already
    // stores. The length measurement remains UTF-8 bytes, as in core.
    if genome.len() != expected_hex_chars {
        return Err(AgentRegistryError::invalid_request(format!(
            "avatar genome for type '{avatar_type}' must be exactly 1024 bytes ({expected_hex_chars} hex chars); received {} hex chars",
            genome.len()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // A tag containing only Unicode whitespace becomes empty after trimming and
    // must refuse invalid_tag rather than storing an invisible tag.
    // Source: prefrontal 873870be8,
    // crates/prefrontal-core-store/src/agent_registry.rs:1324-1330,843-873.
    #[test]
    fn tag_empty_after_unicode_trim_refuses_invalid_tag() {
        let error = validate_agent_tag("\u{3000}\u{00A0}\t").unwrap_err();
        assert_eq!(error, AgentRegistryError::InvalidTag);
        assert_eq!(error.code(), "invalid_tag");
    }

    // A 256-byte tag is accepted at the limit after edge whitespace is trimmed;
    // multibyte characters ensure the bound counts bytes rather than scalars.
    // Source: prefrontal 873870be8,
    // crates/prefrontal-core-store/src/agent_registry.rs:30,1324-1330.
    #[test]
    fn tag_256_bytes_passes_trimmed() {
        assert_eq!(
            validate_agent_tag(&format!("\u{3000}{}\u{00A0}", "é".repeat(128))),
            Ok("é".repeat(128))
        );
    }

    // A 257-byte tag is one byte over the 256-byte limit and refuses invalid_tag,
    // even though its multibyte characters occupy fewer than 256 scalars.
    // Source: prefrontal 873870be8,
    // crates/prefrontal-core-store/src/agent_registry.rs:30,1324-1330,843-873.
    #[test]
    fn tag_257_bytes_refuses_invalid_tag() {
        let error = validate_agent_tag(&format!("{}x", "é".repeat(128))).unwrap_err();
        assert_eq!(error, AgentRegistryError::InvalidTag);
        assert_eq!(error.code(), "invalid_tag");
    }

    // Both an empty list and 16 distinct labels are valid, pinning that labels
    // are optional and that the list-count bound is inclusive.
    // Source: prefrontal 873870be8,
    // crates/prefrontal-core-store/src/agent_registry.rs:31,1332-1361.
    #[test]
    fn labels_16_pass_and_empty_list_passes() {
        let labels: Vec<String> = (0..16).map(|i| format!("label-{i}")).collect();
        assert_eq!(validate_agent_labels(&labels), Ok(labels.clone()));
        assert_eq!(validate_agent_labels(&[]), Ok(vec![]));
    }

    // Seventeen labels exceed the 16-label limit and refuse invalid_labels before
    // validating individual labels, even when every label is otherwise valid.
    // Source: prefrontal 873870be8,
    // crates/prefrontal-core-store/src/agent_registry.rs:31,1332-1361,843-873.
    #[test]
    fn labels_17_refuse_invalid_labels() {
        let labels: Vec<String> = (0..17).map(|i| format!("label-{i}")).collect();
        let error = validate_agent_labels(&labels).unwrap_err();
        assert_eq!(
            error,
            AgentRegistryError::InvalidLabels {
                reason: InvalidAgentLabelReason::TooMany
            }
        );
        assert_eq!(error.code(), "invalid_labels");
    }

    // A 32-scalar label is accepted even when its UTF-8 encoding takes more than
    // 32 bytes, pinning the scalar bound rather than an accidental byte limit.
    // Source: prefrontal 873870be8,
    // crates/prefrontal-core-store/src/agent_registry.rs:32,1332-1361.
    #[test]
    fn label_32_scalars_passes_without_a_byte_bound() {
        assert_eq!(
            validate_agent_labels(&["é".repeat(32)]),
            Ok(vec!["é".repeat(32)])
        );
    }

    // A 33-scalar label is one scalar over the 32-scalar limit and refuses
    // invalid_labels without truncating the requested label.
    // Source: prefrontal 873870be8,
    // crates/prefrontal-core-store/src/agent_registry.rs:32,1332-1361,843-873.
    #[test]
    fn label_33_scalars_refuses_invalid_labels() {
        let error = validate_agent_labels(&["é".repeat(33)]).unwrap_err();
        assert_eq!(
            error,
            AgentRegistryError::InvalidLabels {
                reason: InvalidAgentLabelReason::TooLong
            }
        );
        assert_eq!(error.code(), "invalid_labels");
    }

    // A whitespace-only label becomes empty after Unicode trimming and refuses
    // invalid_labels instead of leaving an invisible entry in the list.
    // Source: prefrontal 873870be8,
    // crates/prefrontal-core-store/src/agent_registry.rs:1332-1361,843-873.
    #[test]
    fn label_empty_after_unicode_trim_refuses_invalid_labels() {
        let error = validate_agent_labels(&["\u{3000}\u{00A0}".into()]).unwrap_err();
        assert_eq!(
            error,
            AgentRegistryError::InvalidLabels {
                reason: InvalidAgentLabelReason::Empty
            }
        );
        assert_eq!(error.code(), "invalid_labels");
    }

    // Full ICU case folding makes these differently spelled pairs duplicates;
    // the list must refuse invalid_labels instead of storing duplicate lookup keys.
    // Source: prefrontal 873870be8,
    // crates/prefrontal-core-store/src/agent_registry.rs:1332-1361,843-873.
    #[test]
    fn case_fold_duplicate_labels_refuse_invalid_labels() {
        for pair in [[" Straße ", "STRASSE"], ["İ", "i\u{307}"]] {
            let error = validate_agent_labels(&pair.map(String::from)).unwrap_err();
            assert_eq!(
                error,
                AgentRegistryError::InvalidLabels {
                    reason: InvalidAgentLabelReason::Duplicate
                }
            );
            assert_eq!(error.code(), "invalid_labels");
        }
    }

    // Labels retain request order and spelling after edge trimming, including
    // composed/decomposed forms because label validation does not apply NFC.
    // Source: prefrontal 873870be8,
    // crates/prefrontal-core-store/src/agent_registry.rs:1332-1361.
    #[test]
    fn labels_preserve_trimmed_request_order_and_spelling() {
        assert_eq!(
            validate_agent_labels(&[
                "\u{3000}Zulu\u{00A0}".into(),
                " Cafe\u{301} ".into(),
                "Café".into()
            ]),
            Ok(vec!["Zulu".into(), "Cafe\u{301}".into(), "Café".into()])
        );
    }

    // A 2048-byte hex genome is exactly the creature.classic layout length and
    // passes with the supported type, pinning the accepted boundary.
    // Source: prefrontal 873870be8,
    // crates/prefrontal-core-module/src/agent_registry_ops.rs:835-842,3276-3285.
    #[test]
    fn avatar_2048_hex_bytes_pass() {
        assert_eq!(
            validate_agent_avatar(&"a".repeat(2048), Some("creature.classic")),
            Ok(())
        );
    }

    // A genome with non-hex content still passes at 2048 bytes, avoiding a
    // stricter content check than core; the multibyte case pins byte counting.
    // Source: prefrontal 873870be8,
    // crates/prefrontal-core-module/src/agent_registry_ops.rs:3276-3285.
    #[test]
    fn avatar_2048_non_hex_bytes_pass() {
        assert_eq!(
            validate_agent_avatar(&"z".repeat(2048), Some("creature.classic")),
            Ok(())
        );
        assert_eq!(
            validate_agent_avatar(&"é".repeat(1024), Some("creature.classic")),
            Ok(())
        );
    }

    // A 2047-byte genome is one byte short of the fixed 2048-byte layout and
    // refuses invalid_request instead of accepting a partial genome.
    // Source: prefrontal 873870be8,
    // crates/prefrontal-core-module/src/agent_registry_ops.rs:3276-3285.
    #[test]
    fn avatar_2047_bytes_refuses_invalid_request() {
        assert_eq!(
            validate_agent_avatar(&"a".repeat(2047), Some("creature.classic"))
                .unwrap_err()
                .code(),
            "invalid_request"
        );
    }

    // A 2049-byte genome is one byte over the fixed 2048-byte layout and refuses
    // invalid_request instead of ignoring the extra byte.
    // Source: prefrontal 873870be8,
    // crates/prefrontal-core-module/src/agent_registry_ops.rs:3276-3285.
    #[test]
    fn avatar_2049_bytes_refuses_invalid_request() {
        assert_eq!(
            validate_agent_avatar(&"a".repeat(2049), Some("creature.classic"))
                .unwrap_err()
                .code(),
            "invalid_request"
        );
    }

    // Even a correctly sized genome requires an explicit type so its layout is
    // known; omitting the type refuses invalid_request.
    // Source: prefrontal 873870be8,
    // crates/prefrontal-core-module/src/agent_registry_ops.rs:3276-3285.
    #[test]
    fn avatar_missing_type_refuses_invalid_request() {
        assert_eq!(
            validate_agent_avatar(&"a".repeat(2048), None)
                .unwrap_err()
                .code(),
            "invalid_request"
        );
    }

    // An unknown type refuses invalid_request despite a correctly sized genome,
    // because length alone cannot identify a supported avatar layout.
    // Source: prefrontal 873870be8,
    // crates/prefrontal-core-module/src/agent_registry_ops.rs:835-842.
    #[test]
    fn avatar_unknown_type_refuses_invalid_request() {
        assert_eq!(
            validate_agent_avatar(&"a".repeat(2048), Some("unknown"))
                .unwrap_err()
                .code(),
            "invalid_request"
        );
    }

    // The avatar reply omits absent type/version fields and includes a present
    // version, pinning omitted-vs-null behavior for reply consumers.
    // Source: prefrontal 873870be8,
    // crates/prefrontal-core-store/src/agent_registry.rs:638-647.
    #[test]
    fn avatar_reply_omits_absent_version_and_type() {
        let mut avatar = AgentAvatar {
            genome: "a".into(),
            avatar_type: Some("creature.classic".into()),
            version: None,
        };
        assert_eq!(
            serde_json::to_value(&avatar).unwrap(),
            json!({"genome":"a", "type":"creature.classic"})
        );
        avatar.version = Some(2);
        assert_eq!(
            serde_json::to_value(&avatar).unwrap(),
            json!({"genome":"a", "type":"creature.classic", "version":2})
        );
        avatar.avatar_type = None;
        avatar.version = None;
        assert_eq!(
            serde_json::to_value(&avatar).unwrap(),
            json!({"genome":"a"})
        );
    }

    // A valid App fixture names a credential reference and supplies its required
    // fields while omitting the optional installation and client ids.
    // Source: prefrontal 873870be8,
    // crates/prefrontal-core-store/src/agent_registry.rs:600-628.
    fn app() -> Value {
        json!({"kind":"app", "app_id":1, "app_slug":"bot", "credential_ref":"ckcred:bot", "coauthor_line":"Bot <bot@example.com>"})
    }

    // A valid user-token fixture requires a login and credential reference but
    // permits an absent coauthor line, keeping optionality distinct from emptiness.
    // Source: prefrontal 873870be8,
    // crates/prefrontal-core-store/src/agent_registry.rs:600-628.
    fn user() -> Value {
        json!({"kind":"user_token", "login":"alice", "credential_ref":"ckcred:alice"})
    }

    // Pin null as an unset identity, both tagged variants and their optional fields,
    // including whitespace-only strings because validity checks emptiness, not trim.
    // Source: prefrontal 873870be8,
    // crates/prefrontal-core-store/src/agent_registry.rs:600-628,1507-1552;
    // crates/prefrontal-core-module/src/agent_registry_ops.rs:844-851.
    #[test]
    fn github_valid_app_user_and_null_keep_core_surface() {
        assert_eq!(decode_github_identity(Value::Null), Ok(None));
        let identity = decode_github_identity(app()).unwrap().unwrap();
        assert_eq!(
            identity,
            GithubIdentity::App {
                app_id: 1,
                app_slug: "bot".into(),
                installation_id: None,
                client_id: None,
                credential_ref: "ckcred:bot".into(),
                coauthor_line: "Bot <bot@example.com>".into(),
            }
        );
        assert_eq!(validate_github_identity(&identity), Ok(()));
        assert_eq!(serde_json::to_value(identity).unwrap(), app());
        let mut value = app();
        value["installation_id"] = json!(1);
        value["client_id"] = json!("client");
        assert_eq!(
            validate_github_identity(&decode_github_identity(value).unwrap().unwrap()),
            Ok(())
        );
        let identity = decode_github_identity(user()).unwrap().unwrap();
        assert_eq!(
            identity,
            GithubIdentity::UserToken {
                login: "alice".into(),
                credential_ref: "ckcred:alice".into(),
                coauthor_line: None
            }
        );
        assert_eq!(validate_github_identity(&identity), Ok(()));
        assert_eq!(
            serde_json::to_value(identity).unwrap(),
            json!({"kind":"user_token", "login":"alice", "credential_ref":"ckcred:alice", "coauthor_line":null})
        );
        // Core checks emptiness, not trimming or credential syntax.
        let mut value = user();
        value["login"] = json!(" ");
        value["credential_ref"] = json!(" ");
        value["coauthor_line"] = json!(" ");
        assert_eq!(
            validate_github_identity(&decode_github_identity(value).unwrap().unwrap()),
            Ok(())
        );
    }

    // An unknown kind cannot decode as either tagged identity variant and refuses
    // invalid_request before semantic identity validation can run.
    // Source: prefrontal 873870be8,
    // crates/prefrontal-core-module/src/agent_registry_ops.rs:844-851.
    #[test]
    fn github_unknown_kind_refuses_invalid_request() {
        assert_eq!(
            decode_github_identity(json!({"kind":"unknown"}))
                .unwrap_err()
                .code(),
            "invalid_request"
        );
    }

    // Extra fields refuse invalid_request on either variant, preventing an
    // unrecognized field such as a secret from being silently accepted and ignored.
    // Source: prefrontal 873870be8,
    // crates/prefrontal-core-store/src/agent_registry.rs:600-628;
    // crates/prefrontal-core-module/src/agent_registry_ops.rs:844-851.
    #[test]
    fn github_unknown_field_refuses_invalid_request() {
        for mut value in [app(), user()] {
            value["secret"] = json!("not a credential reference");
            assert_eq!(
                decode_github_identity(value).unwrap_err().code(),
                "invalid_request"
            );
        }
    }

    // Each generated test decodes a valid-shaped identity with one invalid field,
    // pinning that field's semantic failure as invalid_github_identity, not a decode error.
    // Source: prefrontal 873870be8,
    // crates/prefrontal-core-store/src/agent_registry.rs:1507-1552,843-873.
    macro_rules! github_refusal {
        ($name:ident, $fixture:ident, $field:literal, $value:expr) => {
            #[test]
            fn $name() {
                let mut value = $fixture();
                value[$field] = json!($value);
                let identity = decode_github_identity(value).unwrap().unwrap();
                let error = validate_github_identity(&identity).unwrap_err();
                assert_eq!(
                    error,
                    AgentRegistryError::InvalidGithubIdentity { field: $field }
                );
                assert_eq!(error.code(), "invalid_github_identity");
            }
        };
    }

    github_refusal!(github_zero_app_id_refuses, app, "app_id", 0);
    github_refusal!(github_negative_app_id_refuses, app, "app_id", -1);
    github_refusal!(github_empty_app_slug_refuses, app, "app_slug", "");
    github_refusal!(
        github_zero_installation_id_refuses,
        app,
        "installation_id",
        0
    );
    github_refusal!(
        github_negative_installation_id_refuses,
        app,
        "installation_id",
        -1
    );
    github_refusal!(github_present_empty_client_id_refuses, app, "client_id", "");
    github_refusal!(
        github_empty_app_credential_ref_refuses,
        app,
        "credential_ref",
        ""
    );
    github_refusal!(
        github_empty_app_coauthor_line_refuses,
        app,
        "coauthor_line",
        ""
    );
    github_refusal!(github_empty_login_refuses, user, "login", "");
    github_refusal!(
        github_empty_user_credential_ref_refuses,
        user,
        "credential_ref",
        ""
    );
    github_refusal!(
        github_present_empty_user_coauthor_line_refuses,
        user,
        "coauthor_line",
        ""
    );

    // For both scope-id validators, pin empty-after-trim refusal, acceptance at
    // 512 bytes without a prefix/character restriction, and refusal at 513 bytes.
    // Source: prefrontal 873870be8,
    // crates/prefrontal-core-store/src/agent_registry.rs:33,1237-1256,1363-1395,843-873.
    macro_rules! scope_id_tests {
        ($empty:ident, $max:ident, $over:ident, $validate:ident, $error:ident, $code:literal, $message:literal) => {
            #[test]
            fn $empty() {
                for input in ["", "\u{3000}\u{00A0}\t"] {
                    let error = $validate(input).unwrap_err();
                    assert_eq!(error, AgentRegistryError::$error);
                    assert_eq!(error.code(), $code);
                    assert_eq!(error.to_string(), $message);
                }
            }

            #[test]
            fn $max() {
                // 512 bytes, arbitrary characters, no id prefix, trimmed at the edges.
                let id = "é/!?".repeat(102) + "xx";
                assert_eq!($validate(&format!("\u{3000}{id}\u{00A0}")), Ok(id));
                assert_eq!($validate("\u{200B}"), Ok("\u{200B}".into()));
            }

            #[test]
            fn $over() {
                let error = $validate(&("é/!?".repeat(102) + "xxx")).unwrap_err();
                assert_eq!(error, AgentRegistryError::$error);
                assert_eq!(error.code(), $code);
                assert_eq!(error.to_string(), $message);
            }
        };
    }

    scope_id_tests!(
        project_id_empty_refuses,
        project_id_512_bytes_passes,
        project_id_513_bytes_refuses,
        validate_project_id,
        InvalidProjectId,
        "invalid_role_shape",
        "invalid project_id"
    );
    scope_id_tests!(
        workspace_id_empty_refuses,
        workspace_id_512_bytes_passes,
        workspace_id_513_bytes_refuses,
        validate_workspace_id,
        InvalidWorkspaceId,
        "invalid_role_shape",
        "invalid workspace_id"
    );
}
