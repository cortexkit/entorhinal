#![cfg(windows)]

#[path = "support/windows_acl.rs"]
mod windows_acl;

#[test]
fn deny_read_helper_restores_the_original_dacl() {
    let path = std::env::temp_dir().join(format!("entorhinal-acl-{}.txt", std::process::id()));
    std::fs::write(&path, "scratch").unwrap();
    let saved = windows_acl::deny_read_for_current_user(&path).unwrap();
    let error = std::fs::read(&path).unwrap_err();
    windows_acl::restore(saved).unwrap();
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    assert_eq!(std::fs::read(&path).unwrap(), b"scratch");
    std::fs::remove_file(path).unwrap();
}
