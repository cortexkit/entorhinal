use std::collections::BTreeSet;

use icu_casemap::CaseMapper;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{names::trim_unicode_15_1, AgentRegistryError, InvalidAgentLabelReason};

// From prefrontal 873870be8 crates/prefrontal-core-store/src/agent_registry.rs:30-33.
const MAX_TAG_BYTES: usize = 256;
const MAX_AGENT_LABELS: usize = 16;
const MAX_AGENT_LABEL_SCALARS: usize = 32;
const MAX_SCOPE_BYTES: usize = 512;

/// Tagged credential references, not credentials themselves. Ported from
/// prefrontal 873870be8 crates/prefrontal-core-store/src/agent_registry.rs:600-628.
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

/// Core's set-avatar reply surface, from prefrontal 873870be8
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

/// Ported from prefrontal 873870be8
/// crates/prefrontal-core-store/src/agent_registry.rs:1324-1330.
pub fn validate_agent_tag(raw: &str) -> Result<String, AgentRegistryError> {
    let tag = trim_unicode_15_1(raw);
    if tag.is_empty() || tag.len() > MAX_TAG_BYTES {
        return Err(AgentRegistryError::InvalidTag);
    }
    Ok(tag.to_owned())
}

/// Ported from prefrontal 873870be8
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

/// Ported from prefrontal 873870be8
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

/// Ported from prefrontal 873870be8
/// crates/prefrontal-core-store/src/agent_registry.rs:1379-1386.
pub fn validate_project_id(raw: &str) -> Result<String, AgentRegistryError> {
    validate_optional_scalar(
        Some(raw),
        MAX_SCOPE_BYTES,
        AgentRegistryError::InvalidProjectId,
    )?
    .ok_or(AgentRegistryError::InvalidProjectId)
}

/// Ported from prefrontal 873870be8
/// crates/prefrontal-core-store/src/agent_registry.rs:1388-1395.
pub fn validate_workspace_id(raw: &str) -> Result<String, AgentRegistryError> {
    validate_optional_scalar(
        Some(raw),
        MAX_SCOPE_BYTES,
        AgentRegistryError::InvalidWorkspaceId,
    )?
    .ok_or(AgentRegistryError::InvalidWorkspaceId)
}

/// Ported from prefrontal 873870be8
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
            // Present-but-empty is a defect; absent is a pre-correction row.
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

/// Ported from prefrontal 873870be8
/// crates/prefrontal-core-module/src/agent_registry_ops.rs:844-851.
/// Decoding and semantic validity are separate, with different refusal codes.
pub fn decode_github_identity(value: Value) -> Result<Option<GithubIdentity>, AgentRegistryError> {
    if value.is_null() {
        return Ok(None);
    }
    serde_json::from_value(value).map(Some).map_err(|error| {
        AgentRegistryError::invalid_request(format!("invalid github_identity: {error}"))
    })
}

/// Ported from prefrontal 873870be8
/// crates/prefrontal-core-module/src/agent_registry_ops.rs:835-842.
fn avatar_layout_hex_chars(avatar_type: &str) -> Result<usize, AgentRegistryError> {
    match avatar_type {
        "creature.classic" => Ok(2048),
        _ => Err(AgentRegistryError::invalid_request(format!(
            "unknown avatar type '{avatar_type}'"
        ))),
    }
}

/// Ported from prefrontal 873870be8
/// crates/prefrontal-core-module/src/agent_registry_ops.rs:3276-3285.
/// Despite core's hex terminology, this checks byte length, not hex content.
pub fn validate_agent_avatar(
    genome: &str,
    avatar_type: Option<&str>,
) -> Result<(), AgentRegistryError> {
    let avatar_type = avatar_type.ok_or_else(|| {
        AgentRegistryError::invalid_request("agent.set_avatar requires type with genome")
    })?;
    let expected_hex_chars = avatar_layout_hex_chars(avatar_type)?;
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

    // Expected values from prefrontal 873870be8
    // crates/prefrontal-core-store/src/agent_registry.rs:1324-1330,843-873.
    #[test]
    fn tag_empty_after_unicode_trim_refuses_invalid_tag() {
        let error = validate_agent_tag("\u{3000}\u{00A0}\t").unwrap_err();
        assert_eq!(error, AgentRegistryError::InvalidTag);
        assert_eq!(error.code(), "invalid_tag");
    }

    // Expected bound and byte counting from prefrontal 873870be8
    // crates/prefrontal-core-store/src/agent_registry.rs:30,1324-1330.
    #[test]
    fn tag_256_bytes_passes_trimmed() {
        assert_eq!(
            validate_agent_tag(&format!("\u{3000}{}\u{00A0}", "é".repeat(128))),
            Ok("é".repeat(128))
        );
    }

    // Expected refusal from prefrontal 873870be8
    // crates/prefrontal-core-store/src/agent_registry.rs:30,1324-1330,843-873.
    #[test]
    fn tag_257_bytes_refuses_invalid_tag() {
        let error = validate_agent_tag(&format!("{}x", "é".repeat(128))).unwrap_err();
        assert_eq!(error, AgentRegistryError::InvalidTag);
        assert_eq!(error.code(), "invalid_tag");
    }

    // Expected bound from prefrontal 873870be8
    // crates/prefrontal-core-store/src/agent_registry.rs:31,1332-1361.
    #[test]
    fn labels_16_pass_and_empty_list_passes() {
        let labels: Vec<String> = (0..16).map(|i| format!("label-{i}")).collect();
        assert_eq!(validate_agent_labels(&labels), Ok(labels.clone()));
        assert_eq!(validate_agent_labels(&[]), Ok(vec![]));
    }

    // Expected refusal from prefrontal 873870be8
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

    // Expected scalar bound from prefrontal 873870be8
    // crates/prefrontal-core-store/src/agent_registry.rs:32,1332-1361.
    #[test]
    fn label_32_scalars_passes_without_a_byte_bound() {
        assert_eq!(
            validate_agent_labels(&["é".repeat(32)]),
            Ok(vec!["é".repeat(32)])
        );
    }

    // Expected refusal from prefrontal 873870be8
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

    // Expected refusal from prefrontal 873870be8
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

    // Expected full ICU-fold duplicate refusal from prefrontal 873870be8
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

    // Expected order/trim (and no NFC step) from prefrontal 873870be8
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

    // Expected lengths/type from prefrontal 873870be8
    // crates/prefrontal-core-module/src/agent_registry_ops.rs:835-842,3276-3285.
    #[test]
    fn avatar_2048_hex_bytes_pass() {
        assert_eq!(
            validate_agent_avatar(&"a".repeat(2048), Some("creature.classic")),
            Ok(())
        );
    }

    // Core deliberately does not validate hex content; expected result from
    // prefrontal 873870be8 crates/prefrontal-core-module/src/agent_registry_ops.rs:3276-3285.
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

    // Expected refusal from prefrontal 873870be8
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

    // Expected refusal from prefrontal 873870be8
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

    // Expected refusal from prefrontal 873870be8
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

    // Expected refusal from prefrontal 873870be8
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

    // Expected serialization from prefrontal 873870be8
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

    // Fixture literal taken from the tagged enum in prefrontal 873870be8
    // crates/prefrontal-core-store/src/agent_registry.rs:600-628.
    fn app() -> Value {
        json!({"kind":"app", "app_id":1, "app_slug":"bot", "credential_ref":"ckcred:bot", "coauthor_line":"Bot <bot@example.com>"})
    }

    // Fixture literal taken from prefrontal 873870be8
    // crates/prefrontal-core-store/src/agent_registry.rs:600-628.
    fn user() -> Value {
        json!({"kind":"user_token", "login":"alice", "credential_ref":"ckcred:alice"})
    }

    // Expected decode and validation results from prefrontal 873870be8
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

    // Expected decoder refusal from prefrontal 873870be8
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

    // Expected deny_unknown_fields refusal from prefrontal 873870be8
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

    // Each generated test's expected field and code comes from prefrontal 873870be8
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

    // Each generated test's expectations are from prefrontal 873870be8
    // crates/prefrontal-core-store/src/agent_registry.rs:33,1237-1256,1363-1395,843-873.
    macro_rules! scope_id_tests {
        ($empty:ident, $max:ident, $over:ident, $validate:ident, $error:ident, $code:literal) => {
            #[test]
            fn $empty() {
                for input in ["", "\u{3000}\u{00A0}\t"] {
                    let error = $validate(input).unwrap_err();
                    assert_eq!(error, AgentRegistryError::$error);
                    assert_eq!(error.code(), $code);
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
            }
        };
    }

    scope_id_tests!(
        project_id_empty_refuses,
        project_id_512_bytes_passes,
        project_id_513_bytes_refuses,
        validate_project_id,
        InvalidProjectId,
        "invalid_project_id"
    );
    scope_id_tests!(
        workspace_id_empty_refuses,
        workspace_id_512_bytes_passes,
        workspace_id_513_bytes_refuses,
        validate_workspace_id,
        InvalidWorkspaceId,
        "invalid_workspace_id"
    );
}
