use super::{Command, Value};
use serde_json::json;

pub(super) fn usage() -> &'static str {
    "ck catalog — machine-local package registries\n\nusage: ck catalog registry <verb> [--subc <connection file>] [--json]\n\n  add <url> (--scope <@scope>... | --default) --key-id <id> --public-key <64 hex>\n  repin <url> --key-id <id> --public-key <64 hex> [--valid-from <RFC 3339 Z>]\n  remove <url>\n  list\n\nWrites require operator confirmation. Repin retains earlier keys; remove clears mappings and held state.\n\n  publisher trust <fingerprint> --agent <agent_id>\n\nPublisher trust grants install only, using authenticated pins. It refuses manifest_unavailable until the pin source is supplied.\n"
}
pub(super) fn build(command: &Command) -> Result<(String, Value), String> {
    let fail = || usage().to_string();
    if command.verb == "publisher" {
        if command.args.len() != 4 || command.args[0] != "trust" || command.args[2] != "--agent" {
            return Err(fail());
        }
        return Ok((
            crate::catalog_ops::PUBLISHER_TRUST.into(),
            json!({"publisher_fingerprint":command.args[1],"agent_id":command.args[3]}),
        ));
    }
    if command.verb != "registry" {
        return Err(fail());
    }
    let verb = command.args.first().ok_or_else(fail)?;
    if verb == "list" {
        if command.args.len() != 1 {
            return Err(fail());
        }
        return Ok((crate::catalog_ops::LIST.into(), json!({})));
    }
    let url = command
        .args
        .get(1)
        .filter(|s| !s.starts_with('-'))
        .ok_or_else(fail)?;
    let method = match verb.as_str() {
        "add" => crate::catalog_ops::ADD,
        "repin" => crate::catalog_ops::REPIN,
        "remove" => crate::catalog_ops::REMOVE,
        _ => return Err(fail()),
    };
    let mut params = json!({"url":url});
    let mut scopes = Vec::new();
    let mut flags = command.args[2..].iter();
    while let Some(flag) = flags.next() {
        let field = match flag.as_str() {
            "--default" if verb == "add" => "default",
            "--scope" if verb == "add" => "scopes",
            "--key-id" if verb != "remove" => "key_id",
            "--public-key" if verb != "remove" => "public_key",
            "--valid-from" if verb == "repin" => "valid_from",
            _ => return Err(fail()),
        };
        if field == "scopes" {
            scopes.push(flags.next().ok_or_else(fail)?.clone());
        } else {
            if params.get(field).is_some() {
                return Err(format!("duplicate {flag}"));
            }
            params[field] = if field == "default" {
                json!(true)
            } else {
                json!(flags.next().ok_or_else(fail)?)
            };
        }
    }
    if verb == "add" {
        if scopes.is_empty() != (params["default"] == true) {
            return Err(fail());
        }
        if !scopes.is_empty() {
            params["scopes"] = json!(scopes);
        }
    }
    if verb != "remove" && (params["key_id"].is_null() || params["public_key"].is_null()) {
        return Err(fail());
    }
    Ok((method.into(), params))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{face_from_argv0, parse, Face, Invocation};
    fn command(args: &[&str]) -> Result<(String, Value), String> {
        match parse(
            Face::Catalog,
            &args.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        ) {
            Invocation::Command(c) => build(&c),
            _ => Err("not a command".into()),
        }
    }
    #[test]
    fn catalog_cli_faces_and_wire() {
        for argv0 in ["/bin/ck-catalog", "CKDEV-CATALOG.EXE"] {
            assert_eq!(face_from_argv0(Some(argv0)), Face::Catalog);
        }
        let key = "ab".repeat(32);
        assert_eq!(
            command(&["publisher", "trust", &key, "--agent", "ag_test"]).unwrap(),
            (
                crate::catalog_ops::PUBLISHER_TRUST.into(),
                json!({"publisher_fingerprint":key,"agent_id":"ag_test"})
            )
        );
        assert!(command(&["publisher", "trust", &key]).is_err());
        assert_eq!(
            command(&[
                "registry",
                "add",
                "https://registry.test",
                "--scope",
                "@a",
                "--scope",
                "@b",
                "--key-id",
                "k",
                "--public-key",
                &key
            ])
            .unwrap(),
            (
                crate::catalog_ops::ADD.into(),
                json!({"url":"https://registry.test","scopes":["@a","@b"],"key_id":"k","public_key":key})
            )
        );
        assert_eq!(
            command(&[
                "registry",
                "repin",
                "https://registry.test",
                "--key-id",
                "k",
                "--public-key",
                &key,
                "--valid-from",
                "2026-01-01T00:00:00Z"
            ])
            .unwrap()
            .1["valid_from"],
            "2026-01-01T00:00:00Z"
        );
        assert!(command(&[
            "registry",
            "add",
            "https://registry.test",
            "--default",
            "--key-id",
            "k",
            "--public-key",
            &key
        ])
        .is_ok());
        assert!(command(&["registry", "remove", "https://registry.test"]).is_ok());
        assert!(command(&["registry", "list"]).is_ok());
        for args in [
            vec!["registry", "list", "extra"],
            vec!["registry", "remove", "u", "extra"],
            vec!["registry", "add", "u"],
            vec![
                "registry",
                "add",
                "u",
                "--default",
                "--scope",
                "@a",
                "--key-id",
                "k",
                "--public-key",
                &key,
            ],
        ] {
            assert!(command(&args).is_err());
        }
    }
}
