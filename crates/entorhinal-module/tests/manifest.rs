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
    assert_eq!(
        manifest["capabilities"]["provides"],
        serde_json::json!(["project-identity/v1"])
    );
}
