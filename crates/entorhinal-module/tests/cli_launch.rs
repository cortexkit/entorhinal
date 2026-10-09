use cortexkit_test_support::{ckdev_binary, ScratchDir};
use std::process::Command;

fn child(scratch: &ScratchDir) -> Command {
    let binary = scratch.path().join(if cfg!(windows) {
        "ckdev-projects.exe"
    } else {
        "ckdev-projects"
    });
    std::fs::copy(ckdev_binary(env!("CARGO_BIN_EXE_ck-entorhinal")), &binary).unwrap();
    let mut child = Command::new(binary);
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
