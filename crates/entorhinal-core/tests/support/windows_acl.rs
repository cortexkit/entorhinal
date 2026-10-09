//! Scratch-file ACL changes that preserve and restore the original DACL.

use std::{io, os::windows::ffi::OsStrExt, path::Path, ptr};
use windows_sys::Win32::{
    Foundation::{CloseHandle, LocalFree, GENERIC_READ, HANDLE},
    Security::{
        Authorization::{
            GetNamedSecurityInfoW, SetEntriesInAclW, SetNamedSecurityInfoW, DENY_ACCESS,
            EXPLICIT_ACCESS_W, NO_MULTIPLE_TRUSTEE, SE_FILE_OBJECT, TRUSTEE_IS_SID,
            TRUSTEE_IS_USER, TRUSTEE_W,
        },
        GetSecurityDescriptorControl, GetTokenInformation, TokenUser, ACL,
        DACL_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION, SE_DACL_PROTECTED,
        TOKEN_QUERY, TOKEN_USER, UNPROTECTED_DACL_SECURITY_INFORMATION,
    },
    System::Threading::{GetCurrentProcess, OpenProcessToken},
};

struct Allocation(*mut std::ffi::c_void);
impl Drop for Allocation {
    fn drop(&mut self) {
        unsafe {
            LocalFree(self.0);
        }
    }
}

struct Token(HANDLE);
impl Drop for Token {
    fn drop(&mut self) {
        unsafe {
            CloseHandle(self.0);
        }
    }
}

/// The descriptor owns the original DACL until it has been restored.
pub struct SavedDacl {
    path: Vec<u16>,
    _descriptor: Allocation,
    dacl: *mut ACL,
    protection: u32,
    restored: bool,
}

fn check(status: u32) -> io::Result<()> {
    if status == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(status as i32))
    }
}

impl SavedDacl {
    fn restore(&mut self) -> io::Result<()> {
        if self.restored {
            return Ok(());
        }
        // The saved descriptor keeps this DACL pointer alive through the call.
        check(unsafe {
            SetNamedSecurityInfoW(
                self.path.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION | self.protection,
                ptr::null_mut(),
                ptr::null_mut(),
                self.dacl,
                ptr::null(),
            )
        })?;
        self.restored = true;
        Ok(())
    }
}

impl Drop for SavedDacl {
    fn drop(&mut self) {
        // Restore even if an assertion unwinds, so scratch-directory cleanup works.
        let _ = self.restore();
    }
}

pub fn deny_read_for_current_user(path: &Path) -> io::Result<SavedDacl> {
    let path: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let mut handle = ptr::null_mut();
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut handle) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let token = Token(handle);
    let mut size = 0;
    unsafe {
        GetTokenInformation(token.0, TokenUser, ptr::null_mut(), 0, &mut size);
    }
    if size == 0 {
        return Err(io::Error::last_os_error());
    }
    // A usize buffer provides the alignment TOKEN_USER requires.
    let mut user = vec![0usize; (size as usize).div_ceil(std::mem::size_of::<usize>())];
    if unsafe {
        GetTokenInformation(
            token.0,
            TokenUser,
            user.as_mut_ptr().cast(),
            size,
            &mut size,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    let sid = unsafe { &*user.as_ptr().cast::<TOKEN_USER>() }.User.Sid;
    let mut descriptor = ptr::null_mut();
    let mut dacl = ptr::null_mut();
    check(unsafe {
        GetNamedSecurityInfoW(
            path.as_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION,
            ptr::null_mut(),
            ptr::null_mut(),
            &mut dacl,
            ptr::null_mut(),
            &mut descriptor,
        )
    })?;
    let descriptor = Allocation(descriptor);
    let mut control = 0;
    let mut revision = 0;
    if unsafe { GetSecurityDescriptorControl(descriptor.0, &mut control, &mut revision) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let mut saved = SavedDacl {
        path,
        _descriptor: descriptor,
        dacl,
        restored: false,
        protection: if control & SE_DACL_PROTECTED != 0 {
            PROTECTED_DACL_SECURITY_INFORMATION
        } else {
            UNPROTECTED_DACL_SECURITY_INFORMATION
        },
    };
    let entry = EXPLICIT_ACCESS_W {
        grfAccessPermissions: GENERIC_READ,
        grfAccessMode: DENY_ACCESS,
        grfInheritance: 0,
        Trustee: TRUSTEE_W {
            pMultipleTrustee: ptr::null_mut(),
            MultipleTrusteeOperation: NO_MULTIPLE_TRUSTEE,
            TrusteeForm: TRUSTEE_IS_SID,
            TrusteeType: TRUSTEE_IS_USER,
            ptstrName: sid.cast(),
        },
    };
    let mut denied = ptr::null_mut();
    check(unsafe { SetEntriesInAclW(1, &entry, saved.dacl, &mut denied) })?;
    let denied = Allocation(denied.cast());
    check(unsafe {
        SetNamedSecurityInfoW(
            saved.path.as_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
            ptr::null_mut(),
            ptr::null_mut(),
            denied.0.cast(),
            ptr::null(),
        )
    })?;
    saved.restored = false;
    Ok(saved)
}

pub fn restore(mut saved: SavedDacl) -> io::Result<()> {
    saved.restore()
}
