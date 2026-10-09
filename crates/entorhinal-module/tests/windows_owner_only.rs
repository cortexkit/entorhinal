#![cfg(windows)]

use std::{
    collections::BTreeSet,
    fs,
    io::{self, Read},
    mem::size_of,
    os::windows::ffi::OsStrExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    ptr,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use cortexkit_store_types::{Isolation, StorageBackend, StorageDescriptor};
use cortexkit_test_support::ckdev_binary;
use entorhinal_core::RegistryStore;
use windows_sys::Win32::{
    Foundation::{CloseHandle, LocalFree, GENERIC_ALL, HANDLE},
    Security::{
        Authorization::{
            GetNamedSecurityInfoW, SetEntriesInAclW, SetNamedSecurityInfoW, EXPLICIT_ACCESS_W,
            NO_MULTIPLE_TRUSTEE, SET_ACCESS, SE_FILE_OBJECT, TRUSTEE_IS_SID, TRUSTEE_IS_USER,
            TRUSTEE_W,
        },
        CreateWellKnownSid, EqualSid, GetAce, GetSecurityDescriptorControl, GetTokenInformation,
        IsValidSid, TokenUser, WinAuthenticatedUserSid, WinBuiltinAdministratorsSid,
        WinLocalSystemSid, ACCESS_ALLOWED_ACE, ACE_HEADER, ACL, DACL_SECURITY_INFORMATION,
        PROTECTED_DACL_SECURITY_INFORMATION, PSID, SECURITY_MAX_SID_SIZE, SE_DACL_PRESENT,
        SE_DACL_PROTECTED, TOKEN_QUERY, TOKEN_USER, WELL_KNOWN_SID_TYPE,
    },
    System::{
        SystemServices::{
            ACCESS_ALLOWED_ACE_TYPE, ACCESS_ALLOWED_CALLBACK_ACE_TYPE,
            ACCESS_ALLOWED_CALLBACK_OBJECT_ACE_TYPE, ACCESS_ALLOWED_COMPOUND_ACE_TYPE,
            ACCESS_ALLOWED_OBJECT_ACE_TYPE,
        },
        Threading::{GetCurrentProcess, OpenProcessToken},
    },
};

struct Scratch(PathBuf);

impl Scratch {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "entorhinal-owner-only-{label}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Allocation(*mut std::ffi::c_void);

impl Drop for Allocation {
    fn drop(&mut self) {
        unsafe { LocalFree(self.0) };
    }
}

struct Token(HANDLE);

impl Drop for Token {
    fn drop(&mut self) {
        unsafe { CloseHandle(self.0) };
    }
}

fn check_status(status: u32) -> io::Result<()> {
    if status == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(status as i32))
    }
}

fn wide(path: &Path) -> Vec<u16> {
    path.as_os_str().encode_wide().chain(Some(0)).collect()
}

struct Sid(Vec<usize>);

impl Sid {
    fn well_known(kind: WELL_KNOWN_SID_TYPE) -> Self {
        let mut size = SECURITY_MAX_SID_SIZE;
        let mut buffer = vec![0usize; (size as usize).div_ceil(size_of::<usize>())];
        assert_ne!(
            unsafe {
                CreateWellKnownSid(kind, ptr::null_mut(), buffer.as_mut_ptr().cast(), &mut size)
            },
            0,
            "CreateWellKnownSid: {}",
            io::Error::last_os_error()
        );
        Self(buffer)
    }

    fn as_ptr(&self) -> PSID {
        self.0.as_ptr().cast_mut().cast()
    }
}

struct CurrentUser(Vec<usize>);

impl CurrentUser {
    fn get() -> Self {
        let mut handle = ptr::null_mut();
        assert_ne!(
            unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut handle) },
            0,
            "OpenProcessToken: {}",
            io::Error::last_os_error()
        );
        let token = Token(handle);
        let mut size = 0;
        unsafe { GetTokenInformation(token.0, TokenUser, ptr::null_mut(), 0, &mut size) };
        assert!(
            size > 0,
            "GetTokenInformation: {}",
            io::Error::last_os_error()
        );
        // TOKEN_USER contains a pointer, so the buffer must be pointer-aligned.
        let mut buffer = vec![0usize; (size as usize).div_ceil(size_of::<usize>())];
        assert_ne!(
            unsafe {
                GetTokenInformation(
                    token.0,
                    TokenUser,
                    buffer.as_mut_ptr().cast(),
                    size,
                    &mut size,
                )
            },
            0,
            "GetTokenInformation: {}",
            io::Error::last_os_error()
        );
        Self(buffer)
    }

    fn sid(&self) -> PSID {
        unsafe { &*self.0.as_ptr().cast::<TOKEN_USER>() }.User.Sid
    }
}

fn owner_only(path: &Path, user: &CurrentUser) -> Result<(), String> {
    let mut descriptor = ptr::null_mut();
    let mut dacl: *mut ACL = ptr::null_mut();
    check_status(unsafe {
        GetNamedSecurityInfoW(
            wide(path).as_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION,
            ptr::null_mut(),
            ptr::null_mut(),
            &mut dacl,
            ptr::null_mut(),
            &mut descriptor,
        )
    })
    .map_err(|error| error.to_string())?;
    let descriptor = Allocation(descriptor);
    let mut control = 0;
    let mut revision = 0;
    if unsafe { GetSecurityDescriptorControl(descriptor.0, &mut control, &mut revision) } == 0 {
        return Err(io::Error::last_os_error().to_string());
    }
    if control & SE_DACL_PRESENT == 0 || dacl.is_null() {
        return Err("DACL must be present and non-null".into());
    }
    if control & SE_DACL_PROTECTED == 0 {
        return Err("DACL must be protected from inheritance".into());
    }
    let system = Sid::well_known(WinLocalSystemSid);
    let administrators = Sid::well_known(WinBuiltinAdministratorsSid);
    let allowed = [user.sid(), system.as_ptr(), administrators.as_ptr()];
    for index in 0..unsafe { (*dacl).AceCount } {
        let mut ace = ptr::null_mut();
        if unsafe { GetAce(dacl, index.into(), &mut ace) } == 0 {
            return Err(io::Error::last_os_error().to_string());
        }
        let kind = unsafe { (*ace.cast::<ACE_HEADER>()).AceType } as u32;
        if kind == ACCESS_ALLOWED_ACE_TYPE {
            let sid = unsafe { ptr::addr_of!((*ace.cast::<ACCESS_ALLOWED_ACE>()).SidStart) }
                .cast_mut()
                .cast();
            if unsafe { IsValidSid(sid) } == 0 {
                return Err(format!("access-allowed ACE {index} has an invalid SID"));
            }
            if !allowed
                .iter()
                .any(|allowed| unsafe { EqualSid(sid, *allowed) } != 0)
            {
                return Err(format!(
                    "access-allowed ACE {index} grants an unexpected SID"
                ));
            }
        } else if [
            ACCESS_ALLOWED_COMPOUND_ACE_TYPE,
            ACCESS_ALLOWED_OBJECT_ACE_TYPE,
            ACCESS_ALLOWED_CALLBACK_ACE_TYPE,
            ACCESS_ALLOWED_CALLBACK_OBJECT_ACE_TYPE,
        ]
        .contains(&kind)
        {
            // These ACE layouts need different SID offsets. Refuse them rather
            // than silently accepting a grant the checker did not inspect.
            return Err(format!("unsupported access-allowed ACE type {kind}"));
        }
    }
    Ok(())
}

fn snapshot(root: &Path) -> BTreeSet<PathBuf> {
    let mut paths = BTreeSet::new();
    for entry in fs::read_dir(root).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        assert!(paths.insert(path.clone()));
        if entry.file_type().unwrap().is_dir() {
            paths.extend(snapshot(&path));
        }
    }
    paths
}

#[test]
fn store_directory_sqlite_and_any_lease_have_protected_owner_only_dacls() {
    let scratch = Scratch::new("store");
    let before = snapshot(&scratch.0);
    assert!(before.is_empty());
    let database = scratch.0.join("store/registry.db");
    let descriptor = StorageDescriptor {
        module_id: "entorhinal".into(),
        backend: StorageBackend::Sqlite {
            path: database.to_str().unwrap().into(),
        },
        storage_namespace: "entorhinal-owner-only".into(),
        isolation: Isolation::Module,
    };
    let store = RegistryStore::open(&descriptor).unwrap();
    store
        .apply_entry("fixture", "{}", "owner-only", None, |tx| {
            tx.execute_batch("CREATE TABLE owner_only_write (value INTEGER); INSERT INTO owner_only_write VALUES (1);")
        })
        .unwrap();
    let after = snapshot(&scratch.0);
    let new: BTreeSet<_> = after.difference(&before).cloned().collect();
    assert!(new.contains(database.parent().unwrap()));
    assert!(new.contains(&database));
    let sqlite_files = [
        database.clone(),
        database.with_file_name("registry.db-wal"),
        database.with_file_name("registry.db-shm"),
    ];
    for path in &sqlite_files {
        assert!(
            new.contains(path),
            "SQLite did not create {}",
            path.display()
        );
    }
    let user = CurrentUser::get();
    for path in &new {
        let metadata = fs::symlink_metadata(path).unwrap();
        assert!(
            metadata.is_dir() || metadata.is_file(),
            "unexpected store entry: {}",
            path.display()
        );
        let kind = if metadata.is_dir() {
            "store directory"
        } else if sqlite_files.contains(path) {
            "SQLite file"
        } else {
            "lease file"
        };
        owner_only(path, &user)
            .unwrap_or_else(|error| panic!("{kind} {}: {error}", path.display()));
    }
    drop(store);
}

#[test]
fn module_mode_log_has_a_protected_owner_only_dacl() {
    let scratch = Scratch::new("log");
    let before = snapshot(&scratch.0);
    assert!(before.is_empty());
    let binary = ckdev_binary(env!("CARGO_BIN_EXE_ck-entorhinal"));
    let mut command = Command::new(binary);
    command.env_clear();
    for name in ["SYSTEMROOT", "PATH"] {
        command.env(
            name,
            std::env::var_os(name).expect("Windows child environment"),
        );
    }
    command
        .env("HOME", scratch.0.join("home"))
        .env("USERPROFILE", scratch.0.join("home"))
        .env("XDG_DATA_HOME", scratch.0.join("data"))
        .env("XDG_CONFIG_HOME", scratch.0.join("config"))
        .env("XDG_RUNTIME_DIR", scratch.0.join("runtime"))
        .env("LOCALAPPDATA", scratch.0.join("local-app-data"))
        .env("APPDATA", scratch.0.join("app-data"))
        .env("TMPDIR", &scratch.0)
        .env("TMP", &scratch.0)
        .env("TEMP", &scratch.0)
        .env("SUBC_MODULE_ID", "entorhinal")
        .args(["--subc", scratch.0.join("absent.json").to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn isolated module");
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll isolated module") {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("module did not exit on its own after the absent descriptor refusal");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();
    assert!(!status.success(), "module unexpectedly succeeded: {stderr}");
    assert!(
        stderr.contains("absent.json"),
        "module did not name the isolated descriptor: {stderr}"
    );
    let after = snapshot(&scratch.0);
    let logs: Vec<_> = after
        .difference(&before)
        .filter(|path| {
            path.is_file()
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("entorhinal.") && name.ends_with(".log"))
        })
        .collect();
    assert_eq!(
        logs.len(),
        1,
        "expected one entorhinal log under scratch: {after:?}; stderr: {stderr}"
    );
    owner_only(logs[0], &CurrentUser::get())
        .unwrap_or_else(|error| panic!("log {}: {error}", logs[0].display()));
}

fn allow(sid: PSID) -> EXPLICIT_ACCESS_W {
    EXPLICIT_ACCESS_W {
        grfAccessPermissions: GENERIC_ALL,
        grfAccessMode: SET_ACCESS,
        grfInheritance: 0,
        Trustee: TRUSTEE_W {
            pMultipleTrustee: ptr::null_mut(),
            MultipleTrusteeOperation: NO_MULTIPLE_TRUSTEE,
            TrusteeForm: TRUSTEE_IS_SID,
            TrusteeType: TRUSTEE_IS_USER,
            ptstrName: sid.cast(),
        },
    }
}

#[test]
fn owner_only_checker_rejects_authenticated_users_even_on_a_protected_dacl() {
    let scratch = Scratch::new("negative");
    let path = scratch.0.join("authenticated-users.txt");
    fs::write(&path, b"negative ACL fixture").unwrap();
    let user = CurrentUser::get();
    let authenticated_users = Sid::well_known(WinAuthenticatedUserSid);
    let entries = [allow(user.sid()), allow(authenticated_users.as_ptr())];
    let mut dacl = ptr::null_mut();
    check_status(unsafe {
        SetEntriesInAclW(
            entries.len() as u32,
            entries.as_ptr(),
            ptr::null(),
            &mut dacl,
        )
    })
    .unwrap();
    let dacl = Allocation(dacl.cast());
    check_status(unsafe {
        SetNamedSecurityInfoW(
            wide(&path).as_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
            ptr::null_mut(),
            ptr::null_mut(),
            dacl.0.cast(),
            ptr::null(),
        )
    })
    .unwrap();
    let error =
        owner_only(&path, &user).expect_err("Authenticated Users must not pass the checker");
    assert!(error.contains("unexpected SID"), "wrong rejection: {error}");
}
