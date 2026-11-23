//! Refuse stores that still share through engram before SQLite can change their files.

use std::{
    fmt::Write as _,
    fs, io,
    path::{Path, PathBuf},
};

use cortexkit_lease::{FileLeaseStore, LeaseKey, LeaseStore};
use cortexkit_store::{StorageDescriptor, StoreError};
use rusqlite::{Connection, OpenFlags, OptionalExtension};

use crate::{RegistryError, MIGRATION_NAMESPACE};

#[cfg(test)]
#[path = "legacy_sharing_tests.rs"]
mod tests;

#[cfg(feature = "legacy-store-for-tests")]
pub(crate) mod test_access {
    use std::{
        collections::HashSet,
        path::{Path, PathBuf},
        sync::{Mutex, OnceLock},
    };

    pub(super) const CHILD_PATH_ENV: &str = "ENTORHINAL_LEGACY_STORE_TEST_PATH";
    static ALLOWED: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();

    /// Allow an old-way test fixture at the path in its storage descriptor.
    /// This hook only keeps tests of engram sharing running while those paths
    /// are being removed. When the last of those paths is deleted, remove this
    /// hook and every use of it.
    /// For a child process, set `ENTORHINAL_LEGACY_STORE_TEST_PATH` to that path
    /// on the child command instead.
    pub fn legacy_sharing_test_allow(path: impl AsRef<Path>) {
        ALLOWED
            .get_or_init(Mutex::default)
            .lock()
            .expect("legacy store test allowlist lock poisoned")
            .insert(path.as_ref().to_path_buf());
    }

    pub(crate) fn is_allowed(path: &Path) -> bool {
        ALLOWED.get().is_some_and(|allowed| {
            allowed
                .lock()
                .expect("legacy store test allowlist lock poisoned")
                .contains(path)
        }) || std::env::var_os(CHILD_PATH_ENV).is_some_and(|allowed| Path::new(&allowed) == path)
    }
}

pub(crate) fn probe(
    descriptor: &StorageDescriptor,
    path: &str,
    chain_max: u32,
) -> Result<(), RegistryError> {
    let main = Path::new(path);
    let parent = main.parent().unwrap_or_else(|| Path::new("."));
    // Match the ordinary opener's lease directory and key. Do not tighten an
    // existing store directory until the ordinary opener is allowed to run.
    let on_disk_path = !path.is_empty() && path != ":memory:" && !path.starts_with("file:");
    if on_disk_path && !parent.as_os_str().is_empty() && !parent.try_exists().map_err(io_error)? {
        cortexkit_lease::create_private_dir(parent).map_err(io_error)?;
    }
    let key = LeaseKey::new(
        &descriptor.module_id,
        descriptor.backend.label(),
        &descriptor.storage_namespace,
    );
    let lease = FileLeaseStore::new(parent)
        .acquire(&key)
        .map_err(|error| RegistryError::Store(StoreError::Lease(error)))?;

    #[cfg(test)]
    tests::run_hook(tests::ProbeEvent::BeforeRead);

    // Daemon descriptors name files. Other SQLite names are left to the
    // ordinary opener and its writable-connection recheck.
    if on_disk_path && main.try_exists().map_err(io_error)? {
        let wal = sidecar(main, "-wal");
        let legacy = if wal.try_exists().map_err(io_error)? {
            let private = PrivateDirectory::new()?;
            let copy = private.0.join("store.db");
            fs::copy(main, &copy).map_err(io_error)?;
            fs::copy(wal, sidecar(&copy, "-wal")).map_err(io_error)?;
            let conn = Connection::open(copy)?;
            is_legacy(&conn, chain_max)?
        } else {
            is_legacy(&open_immutable(path)?, chain_max)?
        };
        refuse(legacy)?;
    }

    #[cfg(test)]
    tests::run_hook(tests::ProbeEvent::BeforeWritable);

    // This exclusive lock belongs to the lease handle, not the database
    // connections. open_sqlite acquires its own lock and cannot take a handle
    // from us, so release this one immediately before handing off to it.
    drop(lease);
    Ok(())
}

pub(crate) fn is_legacy(conn: &Connection, chain_max: u32) -> rusqlite::Result<bool> {
    if conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema
         WHERE type = 'table' AND name = 'cortexkit_schema_version')",
        [],
        |row| row.get::<_, bool>(0),
    )? {
        let recorded: u32 = conn.query_row(
            "SELECT COALESCE(MAX(version), 0) FROM cortexkit_schema_version WHERE namespace = ?1",
            [MIGRATION_NAMESPACE],
            |row| row.get(0),
        )?;
        if recorded > chain_max {
            return Ok(false);
        }
    }

    let columns = conn
        .prepare("PRAGMA table_info(identity_log_state)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if columns.is_empty() {
        return Ok(false);
    }
    // Only select names present in this schema. The oldest stores predate the
    // hub columns, and newer ones no longer have the legacy backfill column.
    let column = |name: &'static str| {
        if columns.iter().any(|column| column == name) {
            name
        } else {
            "NULL"
        }
    };
    let base = column("hub_log_base");
    let state = column("state");
    let enable_id = column("enable_id");
    let backfill = column("enable_backfill");
    let id = column("id");
    conn.query_row(
        &format!(
            "SELECT CASE WHEN {base} IS NULL AND (
                {state} IN ('enabled', 'joining')
                OR ({state} = 'enabling' AND {enable_id} IS NULL)
                OR {backfill} IS NOT NULL
             ) THEN 1 ELSE 0 END FROM identity_log_state WHERE {id} = 1"
        ),
        [],
        |row| row.get(0),
    )
    .optional()
    .map(|legacy| legacy.unwrap_or(false))
}

pub(crate) fn refuse(legacy: bool) -> Result<(), RegistryError> {
    if !legacy {
        return Ok(());
    }
    Err(RegistryError::Domain {
        code: "legacy_sharing_unsupported".into(),
        message: "This registry store shares through engram. This build carries over neither its \
                  shared data nor its local-only data. Move the store folder aside and keep it; \
                  do not delete it. With a new, empty registry, if this account's registry \
                  already lives on the hub, this computer will join it on its own, or you can run \
                  `ck projects log enable` to join at once. Otherwise, to start a new shared \
                  registry, import agents first and run `ck projects log enable`, or run \
                  `ck projects log enable --without-agents`, then confirm the prompt."
            .into(),
        detail: None,
    })
}

fn open_immutable(path: &str) -> rusqlite::Result<Connection> {
    // Escape the literal filename so a '?' or '#' in it cannot change the URI.
    let mut uri = String::from("file:");
    for byte in path.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
            uri.push(char::from(byte));
        } else {
            write!(uri, "%{byte:02X}").expect("writing a String cannot fail");
        }
    }
    uri.push_str("?immutable=1");
    Connection::open_with_flags(
        uri,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
}

fn sidecar(main: &Path, suffix: &str) -> PathBuf {
    let mut path = main.as_os_str().to_os_string();
    path.push(suffix);
    path.into()
}

fn io_error(error: io::Error) -> RegistryError {
    RegistryError::Store(StoreError::Io(error))
}

struct PrivateDirectory(PathBuf);

impl PrivateDirectory {
    fn new() -> Result<Self, RegistryError> {
        let mut random = [0; 16];
        getrandom::getrandom(&mut random)
            .map_err(|error| io_error(io::Error::other(error.to_string())))?;
        let path = std::env::temp_dir().join(format!(
            "entorhinal-store-probe-{:032x}",
            u128::from_ne_bytes(random)
        ));
        cortexkit_lease::create_private_dir(&path).map_err(io_error)?;
        Ok(Self(path))
    }
}

impl Drop for PrivateDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
