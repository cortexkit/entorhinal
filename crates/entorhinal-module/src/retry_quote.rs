//! Shell-specific argument quoting for operator retries.

pub fn posix_word(word: &str) -> String {
    if !word.is_empty()
        && word
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_./:-".contains(&byte))
    {
        word.to_owned()
    } else {
        format!("'{}'", word.replace('\'', "'\\''"))
    }
}

pub fn powershell_word(word: &str) -> String {
    format!("'{}'", word.replace('\'', "''"))
}

pub fn render_arguments(words: &[&str], powershell: bool) -> String {
    words
        .iter()
        .map(|word| {
            if powershell {
                powershell_word(word)
            } else {
                posix_word(word)
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn render_retry(words: &[&str], powershell: bool) -> String {
    let prefix = if powershell {
        "retry (pwsh): ck agents "
    } else {
        "retry: ck agents "
    };
    format!("{prefix}{}", render_arguments(words, powershell))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn posix_vectors() {
        for (input, expected) in [
            ("a b", "'a b'"),
            ("a'b", "'a'\\''b'"),
            ("a\"b", "'a\"b'"),
            ("", "''"),
            ("a_./:-9", "a_./:-9"),
        ] {
            assert_eq!(posix_word(input), expected);
        }
    }

    #[test]
    fn powershell_vectors() {
        for (input, expected) in [
            ("a b", "'a b'"),
            ("a'b", "'a''b'"),
            ("a\"b", "'a\"b'"),
            ("", "''"),
            ("plain", "'plain'"),
        ] {
            assert_eq!(powershell_word(input), expected);
        }
    }

    #[test]
    fn retry_prefix_and_request_key() {
        let words = ["create", "--request-key", "key ' with space"];
        assert_eq!(
            render_retry(&words, true),
            "retry (pwsh): ck agents 'create' '--request-key' 'key '' with space'"
        );
        assert_eq!(
            render_retry(&words, false),
            "retry: ck agents create --request-key 'key '\\'' with space'"
        );
    }
}
