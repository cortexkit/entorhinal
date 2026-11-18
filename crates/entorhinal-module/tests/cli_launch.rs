use cortexkit_test_support::{ckdev_binary, ScratchDir};
use std::process::Command;

fn child(scratch: &ScratchDir) -> Command {
    // On Unix the face comes from argv[0], so run the shared ckdev copy under
    // that name. Copying the binary again here would let another test's fork
    // inherit the still-open write descriptor, and exec would then fail with
    // "Text file busy". Windows has no argv[0] override, so it gets a copy
    // named for the face; its exec has no such race.
    #[cfg(unix)]
    let mut child = {
        use std::os::unix::process::CommandExt;
        let _ = scratch;
        let mut child = Command::new(ckdev_binary(env!("CARGO_BIN_EXE_ck-entorhinal")));
        child.arg0("ckdev-projects");
        child
    };
    #[cfg(windows)]
    let mut child = {
        let binary = scratch.path().join("ckdev-projects.exe");
        std::fs::copy(ckdev_binary(env!("CARGO_BIN_EXE_ck-entorhinal")), &binary).unwrap();
        Command::new(binary)
    };
    child.env_clear();
    #[cfg(windows)]
    for name in ["SYSTEMROOT", "PATH"] {
        if let Some(value) = std::env::var_os(name) {
            child.env(name, value);
        }
    }
    child
}

#[tokio::test]
async fn explicit_connection_failure_matches_the_sdk_display() {
    let scratch = ScratchDir::new("entorhinal-cli-discovery");
    let path = scratch.path().join("absent.json");
    let error =
        subc_client_rs::SubcConsumer::connect(&path, subc_client_rs::ConsumerOptions::default())
            .await
            .err()
            .expect("absent connection must fail");
    let output = child(&scratch)
        .args(["list", "--subc"])
        .arg(&path)
        .output()
        .expect("spawn projects CLI");
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        format!("connect {}: {error}\n", path.display())
    );
}

#[test]
fn non_unicode_argument_exits_with_usage_error() {
    #[cfg(unix)]
    let invalid = {
        use std::os::unix::ffi::OsStringExt;
        std::ffi::OsString::from_vec(vec![0xff])
    };
    #[cfg(windows)]
    let invalid = {
        use std::os::windows::ffi::OsStringExt;
        std::ffi::OsString::from_wide(&[0xd800])
    };
    let scratch = ScratchDir::new("entorhinal-cli-argument");
    let output = child(&scratch)
        .arg(invalid)
        .output()
        .expect("spawn non-Unicode argument");
    assert_eq!(output.status.code(), Some(64));
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        "argument_not_unicode\n"
    );
}
