//! `--manifest` is how `ck fleet lint` reads a module offline, and it is the
//! only place lint can learn that entorhinal provides the capability other
//! modules declare `required`. So the test runs the real binary.

use std::process::Command;

use cortexkit_test_support::ckdev_binary;

#[test]
fn manifest_flag_prints_the_manifest_with_the_provided_capability() {
    let binary = ckdev_binary(env!("CARGO_BIN_EXE_ck-entorhinal"));
    let output = Command::new(binary)
        .arg("--manifest")
        // Offline inspection must not need supervision.
        .env_remove("SUBC_MODULE_ID")
        .env_remove("SUBC_LAUNCH_NONCE")
        .output()
        .expect("run ck-entorhinal --manifest");
    assert!(
        output.status.success(),
        "exit {:?}, stderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let manifest: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("--manifest prints JSON");
    assert_eq!(manifest["module_id"], "entorhinal");
    // Placement creates exactly these `ck-<name>` links to this binary and
    // removes any other link pointing at it, so the list is pinned exactly.
    // Every name must also be one the binary dispatches on, or a placed link
    // would start the module instead of a CLI face.
    assert_eq!(
        manifest["cli_faces"],
        serde_json::json!([
            {"name":"agents","binary":"ck-entorhinal"},
            {"name":"packages","binary":"ck-entorhinal"},
            {"name":"projects","binary":"ck-entorhinal"},
            {"name":"roles","binary":"ck-entorhinal"},
            {"name":"workspaces","binary":"ck-entorhinal"}
        ])
    );
    // The face is chosen from argv[0], so setting argv[0] exercises the same
    // dispatch a placed link does, without writing a second copy of the binary.
    // tests/ck_domain.rs already runs the agents, projects and workspaces faces
    // with stricter output checks, so only the other declared faces, including
    // any added later, are probed here.
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        const CHECKED_IN_CK_DOMAIN_TESTS: [&str; 3] = ["agents", "projects", "workspaces"];
        let binary = ckdev_binary(env!("CARGO_BIN_EXE_ck-entorhinal"));
        for face in manifest["cli_faces"].as_array().unwrap() {
            let name = face["name"].as_str().unwrap();
            if CHECKED_IN_CK_DOMAIN_TESTS.contains(&name) {
                continue;
            }
            let headline = Command::new(&binary)
                .arg0(format!("ckdev-{name}"))
                .arg("--ck-domain")
                .output()
                .unwrap();
            assert!(
                headline.status.success(),
                "{} is declared but the binary doesn't answer to it",
                face["name"]
            );
        }
    }
    // Pin the exact list: a capability appearing or disappearing changes what
    // the daemon tells consumers this module serves.
    assert_eq!(
        manifest["capabilities"]["provides"],
        serde_json::json!([
            "project-identity/v1",
            "agent-identity/v1",
            "bind-no-project/v1",
            "permissions/v1",
            "tool-provider/v1",
            "agent-data/v1",
            "call-modes/v1"
        ])
    );
    assert_eq!(
        manifest["provides"][2],
        serde_json::json!({
            "role":"internal_service",
            "service_id":"agent-data",
            "transport":"bulk",
            "agent_facing":false,
            "operations":["agent_data.describe","agent_data.busy","agent_data.status","agent_data.prepare","agent_data.export","agent_data.diverged","agent_data.import","agent_data.activate","agent_data.abort","agent_data.release"]
        })
    );
    let operations = manifest["provides"][0]["operations"].as_array().unwrap();
    let rebind: Vec<_> = operations
        .iter()
        .filter(|op| op["name"] == "rebind_root")
        .collect();
    assert_eq!(rebind.len(), 1);
    assert_eq!(rebind[0]["kind"], "mutate");
    for (name, kind) in [
        ("agent.preview_avatar_manifest", "query"),
        ("agent.apply_avatar_manifest", "mutate"),
    ] {
        let matched: Vec<_> = operations.iter().filter(|op| op["name"] == name).collect();
        assert_eq!(matched.len(), 1, "{name}");
        assert_eq!(matched[0]["kind"], kind);
        assert!(matched[0]["description"]
            .as_str()
            .unwrap()
            .contains("manifest"));
    }
    let mut descriptions = std::collections::BTreeSet::new();
    for operation in operations {
        let description = operation["description"].as_str().unwrap();
        assert!(!description.is_empty(), "{}", operation["name"]);
        assert!(
            descriptions.insert(description),
            "{} repeats a description",
            operation["name"]
        );
    }
}

/// The daemon can only show which source and wire version entorhinal runs, and
/// where its launch nonce came from, when the HELLO declares provenance.
/// Without it `ck provenance entorhinal` is empty and the fleet cannot confirm
/// entorhinal reads the nonce from the daemon's pipe.
#[test]
fn manifest_declares_build_provenance() {
    let binary = ckdev_binary(env!("CARGO_BIN_EXE_ck-entorhinal"));
    let output = Command::new(binary)
        .arg("--manifest")
        .env_remove("SUBC_MODULE_ID")
        .env_remove("SUBC_LAUNCH_NONCE")
        .env_remove("SUBC_LAUNCH_NONCE_FD")
        .output()
        .expect("run ck-entorhinal --manifest");
    assert!(output.status.success());
    let manifest: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("--manifest prints JSON");
    let provenance = &manifest["provenance"];
    assert_eq!(
        provenance["wire_crate_version"],
        subc_protocol::SUBC_PROTOCOL_CRATE_VERSION,
        "provenance: {provenance}"
    );
    // A test build runs from a git checkout, so the revision is either attested
    // (clean tree) or declined with its reason (dirty tree), never silently absent.
    assert!(
        provenance["build_git_sha"].is_string()
            || provenance["build_git_sha_absence_reason"].is_string(),
        "provenance: {provenance}"
    );
    if option_env!("ENTORHINAL_BUILD_PROFILE").is_none() {
        assert!(
            provenance["build_git_sha"].is_null(),
            "debug builds do not embed a git revision: {provenance}"
        );
        assert_eq!(
            provenance["build_git_sha_absence_reason"], "provenance_stamped_only_in_release_builds",
            "debug builds intentionally omit git provenance: {provenance}"
        );
    }
}
