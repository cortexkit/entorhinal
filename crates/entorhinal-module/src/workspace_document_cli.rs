//! Two-step workspace editing: read the accepted base, show a diff, and write
//! only with --yes using that base revision for optimistic concurrency.
use super::*;
fn request_key() -> Result<String, String> {
    let mut bytes = [0; 16];
    getrandom::getrandom(&mut bytes).map_err(|e| format!("draw request key: {e}"))?;
    Ok(crate::log_client::encode_hex(&bytes))
}
pub(super) fn build(command: &Command, cwd: &str) -> Result<(String, Value), String> {
    let args = &command.args;
    let verb = args
        .first()
        .map(String::as_str)
        .ok_or("document needs set or show")?;
    let workspace = args.get(1).ok_or("document needs a workspace id")?;
    if verb == "show" {
        let revision = match &args[2..] {
            [] => None,
            [flag, n] if flag == "--revision" => Some(
                n.parse::<i64>()
                    .map_err(|_| "revision must be a positive integer")?,
            )
            .filter(|n| *n > 0)
            .ok_or("revision must be positive")
            .map(Some)?,
            _ => return Err("document show accepts only --revision N".into()),
        };
        return Ok((
            "workspace.document".into(),
            json!({"workspace_id":workspace,"revision":revision}),
        ));
    }
    if verb != "set" {
        return Err("document needs set or show".into());
    }
    let mut value = json!({"workspace_id":workspace,"yes":false,"request_key":request_key()?});
    let mut rest = args[2..].iter();
    while let Some(flag) = rest.next() {
        let field = match flag.as_str() {
            "--charter-file" => "charter",
            "--document-file" => "document",
            "--yes" if value["yes"] == false => {
                value["yes"] = json!(true);
                continue;
            }
            _ => return Err(format!("unknown or duplicate document flag {flag}")),
        };
        if value.get(field).is_some() {
            return Err(format!("{flag} may only be given once"));
        }
        let path = rest.next().ok_or_else(|| format!("{flag} needs a path"))?;
        let path = Path::new(cwd).join(path);
        let text =
            std::fs::read_to_string(&path).map_err(|e| format!("read {}: {e}", path.display()))?;
        value[field] = json!(text);
    }
    Ok(("workspace.document.set".into(), value))
}
fn unified(field: &str, old: &str, new: &str) -> String {
    if old == new {
        return String::new();
    }
    let old_lines: Vec<_> = old.split_inclusive('\n').collect();
    let new_lines: Vec<_> = new.split_inclusive('\n').collect();
    let mut out = format!(
        "--- accepted/{field}\n+++ draft/{field}\n@@ -{},{} +{},{} @@\n",
        usize::from(!old_lines.is_empty()),
        old_lines.len(),
        usize::from(!new_lines.is_empty()),
        new_lines.len()
    );
    for (prefix, lines) in [('-', old_lines), ('+', new_lines)] {
        for line in lines {
            out.push(prefix);
            out.push_str(line);
            if !line.ends_with('\n') {
                out.push_str("\n\\ No newline at end of file\n");
            }
        }
    }
    out
}
pub(super) async fn execute(
    caller: &dyn CliCaller,
    mut params: Value,
) -> Result<Value, (u8, String)> {
    let base = match caller
        .call(
            "workspace.document",
            json!({"workspace_id":params["workspace_id"]}),
            Default::default(),
        )
        .await
    {
        Ok(base) => base,
        Err(CallFailure::Refused { code, .. }) if code == "revision_unknown" => {
            json!({"revision":0,"charter":"","document":""})
        }
        Err(error) => return Err(error.display()),
    };
    let mut diff = String::new();
    for field in ["charter", "document"] {
        if params.get(field).is_none() {
            params[field] = base[field].clone();
        }
        diff.push_str(&unified(
            field,
            base[field].as_str().unwrap_or(""),
            params[field].as_str().unwrap_or(""),
        ));
    }
    println!(
        "{}",
        if diff.is_empty() {
            "No text changes.\n"
        } else {
            &diff
        }
    );
    let yes = params.as_object_mut().unwrap().remove("yes") == Some(json!(true));
    if !yes {
        return Ok(json!({"preview":true,"expected_revision":base["revision"],"diff":diff}));
    }
    params["expected_revision"] = base["revision"].clone();
    caller
        .call(
            "workspace.set_document",
            params,
            placement_call_options("workspace.set_document"),
        )
        .await
        .map_err(CallFailure::display)
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Caller {
        calls: std::sync::Mutex<Vec<(String, Value)>>,
    }
    #[async_trait::async_trait]
    impl CliCaller for Caller {
        async fn call(
            &self,
            method: &str,
            params: Value,
            options: subc_client_rs::CallOptions,
        ) -> Result<Value, CallFailure> {
            self.calls
                .lock()
                .unwrap()
                .push((method.into(), params.clone()));
            if method == "workspace.document" {
                Ok(json!({"revision":2,"charter":"Keep\n","document":"Old\n"}))
            } else {
                assert_eq!(options.timeout, Duration::from_secs(300));
                Ok(json!({"revision":3}))
            }
        }
    }
    #[tokio::test]
    async fn memory_document_cli_previews_without_write_and_yes_uses_accepted_revision() {
        for yes in [false, true] {
            let caller = Caller {
                calls: Default::default(),
            };
            let reply = execute(
                &caller,
                json!({"workspace_id":"W","document":"New\n","yes":yes,"request_key":"key"}),
            )
            .await
            .unwrap();
            let calls = caller.calls.lock().unwrap();
            assert_eq!(calls.len(), if yes { 2 } else { 1 });
            if yes {
                assert_eq!(calls[1].0, "workspace.set_document");
                assert_eq!(calls[1].1["expected_revision"], 2);
                assert_eq!(calls[1].1["charter"], "Keep\n");
                assert_eq!(reply["revision"], 3);
            } else {
                assert_eq!(reply["preview"], true);
                assert_eq!(
                    reply["diff"],
                    "--- accepted/document\n+++ draft/document\n@@ -1,1 +1,1 @@\n-Old\n+New\n"
                );
            }
        }
    }
    #[test]
    fn memory_cli_accepts_only_explicit_domain_clear_and_document_flags() {
        for (face, args, op) in [
            (
                Face::Projects,
                vec!["domain", "P", "--clear"],
                "project.set_domain",
            ),
            (
                Face::Projects,
                vec!["domain", "P", "@example/software"],
                "project.set_domain",
            ),
            (
                Face::Workspaces,
                vec!["document", "show", "W", "--revision", "1"],
                "workspace.document",
            ),
            (
                Face::Workspaces,
                vec!["document", "set", "W", "--yes"],
                "workspace.document.set",
            ),
        ] {
            let Invocation::Command(command) = parse(
                face,
                &args.into_iter().map(str::to_owned).collect::<Vec<_>>(),
            ) else {
                panic!("invalid parse")
            };
            let (actual, params) = build_at(&command, "/").unwrap();
            assert_eq!(actual, op);
            if actual == "project.set_domain" {
                assert!(params["request_key"].is_string());
            }
        }
        assert!(matches!(
            parse(
                Face::Workspaces,
                &[
                    "document".into(),
                    "set".into(),
                    "W".into(),
                    "--charter-file".into()
                ]
            ),
            Invocation::Refuse(_)
        ));
    }
}
