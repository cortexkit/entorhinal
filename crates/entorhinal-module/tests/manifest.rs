//! `--manifest` is how `ck fleet lint` reads a module offline, and it is the
//! only place lint can learn that entorhinal provides the capability other
//! modules declare `required`. So the test runs the real binary.

use std::process::Command;

#[test]
fn manifest_flag_prints_the_manifest_with_the_provided_capability() {
    let output = Command::new(env!("CARGO_BIN_EXE_ck-entorhinal"))
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
    // Pin the exact list: a capability appearing or disappearing changes what
    // the daemon tells consumers this module serves.
    assert_eq!(
        manifest["capabilities"]["provides"],
        serde_json::json!(["project-identity/v1", "agent-identity/v1"])
    );
}

/// The daemon can only show which source and wire version entorhinal runs, and
/// where its launch nonce came from, when the HELLO declares provenance.
/// Without it `ck provenance entorhinal` is empty and the fleet cannot confirm
/// entorhinal reads the nonce from the daemon's pipe.
#[test]
fn manifest_declares_build_provenance() {
    let output = Command::new(env!("CARGO_BIN_EXE_ck-entorhinal"))
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
}
