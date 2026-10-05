use icu_casemap::CaseMapper;
use icu_normalizer::ComposingNormalizer;

use super::{AgentRegistryError, InvalidNameReason};

// Version 1 pins the name normalization pipeline, and its length limit applies
// to both the display name and the folded lookup name in Unicode scalars.
// Source: prefrontal 873870be8, crates/prefrontal-core-store/src/agent_registry.rs:25-29.
pub const NAME_NORMALIZATION_VERSION: i64 = 1;
const MAX_NAME_SCALARS: usize = 24;

/// Keep the user's display spelling separately from the case-folded lookup key,
/// alongside the version that identifies how the two names were normalized.
/// Source: prefrontal 873870be8,
/// crates/prefrontal-core-store/src/agent_registry.rs:705-710.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedAgentName {
    pub stored_name: String,
    pub normalized_name: String,
    pub normalization_version: i64,
}

/// Recognize the fixed Unicode 15.1 whitespace set, so trimming does not change
/// when the Rust toolchain's Unicode data changes.
/// Source: prefrontal 873870be8,
/// crates/prefrontal-core-store/src/agent_registry.rs:1237-1252.
fn is_unicode_15_1_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'..='\u{000D}'
            | '\u{0020}'
            | '\u{0085}'
            | '\u{00A0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200A}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202F}'
            | '\u{205F}'
            | '\u{3000}'
    )
}

/// Remove only Unicode 15.1 whitespace at the edges, leaving internal spelling
/// unchanged and applying the same trim rule to all identity fields.
/// Source: prefrontal 873870be8,
/// crates/prefrontal-core-store/src/agent_registry.rs:1254-1256.
pub(super) fn trim_unicode_15_1(value: &str) -> &str {
    value.trim_matches(is_unicode_15_1_whitespace)
}

/// Refuse invisible formatting, bidirectional controls and similar code points
/// that could make two visually identical names differ in their lookup keys.
/// Source: prefrontal 873870be8 (predicate copied verbatim),
/// crates/prefrontal-core-store/src/agent_registry.rs:1258-1283.
fn is_disallowed_name_character(character: char) -> bool {
    matches!(
        character,
        '\u{00AD}'
            | '\u{0600}'..='\u{0605}'
            | '\u{061C}'
            | '\u{06DD}'
            | '\u{070F}'
            | '\u{0890}'..='\u{0891}'
            | '\u{08E2}'
            | '\u{180E}'
            | '\u{200B}'..='\u{200F}'
            | '\u{202A}'..='\u{202E}'
            | '\u{2060}'..='\u{2064}'
            | '\u{2066}'..='\u{206F}'
            | '\u{FEFF}'
            | '\u{FFF9}'..='\u{FFFB}'
            | '\u{110BD}'
            | '\u{110CD}'
            | '\u{13430}'..='\u{1343F}'
            | '\u{1BCA0}'..='\u{1BCA3}'
            | '\u{1D173}'..='\u{1D17A}'
            | '\u{E0001}'
            | '\u{E0020}'..='\u{E007F}'
    )
}

/// Normalize a registry name without losing the user's case and spelling in the
/// display form. ICU4X 1.5 compiled data is sourced from ICU 75, whose Unicode
/// data is 15.1.0; `fold_string` is the full, non-Turkic C/F mapping.
/// Source: prefrontal 873870be8,
/// crates/prefrontal-core-store/src/agent_registry.rs:1285-1322.
pub fn normalize_agent_name(raw: &str) -> Result<NormalizedAgentName, AgentRegistryError> {
    if let Some(character) = raw
        .chars()
        .find(|character| is_disallowed_name_character(*character))
    {
        return Err(AgentRegistryError::InvalidName {
            reason: InvalidNameReason::DisallowedCharacter {
                codepoint: character as u32,
            },
        });
    }
    let display_nfc = ComposingNormalizer::new_nfc().normalize(raw);
    let stored_name = trim_unicode_15_1(&display_nfc).to_owned();
    let folded = CaseMapper::new().fold_string(&display_nfc);
    let normalized_name = trim_unicode_15_1(&folded).to_owned();

    let stored_length = stored_name.chars().count();
    let normalized_length = normalized_name.chars().count();
    if stored_length == 0 || normalized_length == 0 {
        return Err(AgentRegistryError::InvalidName {
            reason: InvalidNameReason::Empty,
        });
    }
    if stored_length > MAX_NAME_SCALARS || normalized_length > MAX_NAME_SCALARS {
        return Err(AgentRegistryError::InvalidName {
            reason: InvalidNameReason::TooLong,
        });
    }

    Ok(NormalizedAgentName {
        stored_name,
        normalized_name,
        normalization_version: NAME_NORMALIZATION_VERSION,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // Pin NFC, full case folding and trimming, including the order that counts
    // scalars after composition/folding and never normalizes the fold output again.
    // Source: prefrontal 873870be8 (test copied verbatim),
    // crates/prefrontal-core-store/src/agent_registry.rs:7533-7580.
    #[test]
    fn normalization_pipeline_pins_nfc_case_fold_trim_and_scalar_order() {
        let decomposed = normalize_agent_name(" Cafe\u{301} ").unwrap();
        let composed = normalize_agent_name("CAFÉ").unwrap();
        assert_eq!(decomposed.stored_name, "Café");
        assert_eq!(decomposed.normalized_name, composed.normalized_name);

        let sharp_s = normalize_agent_name(" Straße ").unwrap();
        assert_eq!(sharp_s.stored_name, "Straße");
        assert_eq!(sharp_s.normalized_name, "strasse");
        assert_eq!(
            sharp_s.normalized_name,
            normalize_agent_name("STRASSE").unwrap().normalized_name
        );

        assert_eq!(
            normalize_agent_name("\u{3000}Alice\u{00A0}")
                .unwrap()
                .normalized_name,
            normalize_agent_name("alice").unwrap().normalized_name
        );
        assert_eq!(
            normalize_agent_name("İ").unwrap().normalized_name,
            "i\u{307}"
        );
        assert_eq!(
            normalize_agent_name("ẖ").unwrap().normalized_name,
            "h\u{331}",
            "fold output must not be normalized again"
        );

        let raw_over_limit = "e\u{301}".repeat(13);
        assert_eq!(raw_over_limit.chars().count(), 26);
        assert_eq!(
            normalize_agent_name(&raw_over_limit)
                .unwrap()
                .stored_name
                .chars()
                .count(),
            13
        );
        assert_eq!(
            normalize_agent_name(&"ß".repeat(13)),
            Err(AgentRegistryError::InvalidName {
                reason: InvalidNameReason::TooLong
            })
        );
    }

    // Whitespace-only names are empty and 25 scalars exceed the 24-scalar limit;
    // both must refuse with typed reasons rather than silently truncating a name.
    // Source: prefrontal 873870be8,
    // crates/prefrontal-core-store/src/agent_registry.rs:7618-7634.
    #[test]
    fn invalid_names_are_typed_and_never_truncated() {
        assert_eq!(
            normalize_agent_name("\u{3000}\u{00A0}"),
            Err(AgentRegistryError::InvalidName {
                reason: InvalidNameReason::Empty
            })
        );
        let too_long = normalize_agent_name(&"x".repeat(25));
        assert_eq!(
            too_long,
            Err(AgentRegistryError::InvalidName {
                reason: InvalidNameReason::TooLong
            })
        );
        assert_eq!(too_long.unwrap_err().code(), "invalid_name");
    }

    // Pin the reported disallowed code points, preserved Cyrillic/display spelling,
    // normalization version 1 and acceptance at the 24-scalar boundary.
    // Claim-path and row lifecycle assertions belong to the mutation implementation.
    // Source: prefrontal 873870be8,
    // crates/prefrontal-core-store/src/agent_registry.rs:7636-7661,7733-7748.
    #[test]
    fn names_pin_codepoints_display_and_normalization_version() {
        for (name, codepoint) in [
            ("ali\u{200B}ce", 0x200B),
            ("alice\u{202E}", 0x202E),
            ("alice\u{2060}", 0x2060),
        ] {
            let error = normalize_agent_name(name).unwrap_err();
            assert_eq!(
                error,
                AgentRegistryError::InvalidName {
                    reason: InvalidNameReason::DisallowedCharacter { codepoint }
                }
            );
            assert_eq!(error.code(), "invalid_name");
            assert!(error.to_string().contains(&format!("U+{codepoint:04X}")));
        }
        assert_eq!(normalize_agent_name("Алиса").unwrap().stored_name, "Алиса");
        assert_eq!(
            normalize_agent_name("  Cafe\u{301}  ").unwrap(),
            NormalizedAgentName {
                stored_name: "Café".into(),
                normalized_name: "café".into(),
                normalization_version: 1,
            }
        );
        assert_eq!(
            normalize_agent_name(&"é".repeat(24)).unwrap().stored_name,
            "é".repeat(24)
        );
    }
}
