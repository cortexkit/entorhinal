use std::{
    cell::RefCell,
    io::{BufRead, BufReader, Read, Write},
    process::{Child, Command, Stdio},
};

use cortexkit_store::{open_sqlite, Isolation, StorageBackend};
use entorhinal_catalog::sha256_hex;

use super::*;
use crate::{RegistryStore, MIGRATIONS};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ProbeEvent {
    BeforeRead,
    BeforeWritable,
}

type ProbeHook = Box<dyn FnMut(ProbeEvent)>;
thread_local! {
    static PROBE_HOOK: RefCell<Option<ProbeHook>> = const { RefCell::new(None) };
}

pub(super) fn run_hook(event: ProbeEvent) {
    PROBE_HOOK.with_borrow_mut(|hook| {
        if let Some(hook) = hook {
            hook(event);
        }
    });
}

fn with_probe_hook<T>(hook: impl FnMut(ProbeEvent) + 'static, open: impl FnOnce() -> T) -> T {
    struct ClearHook;
    impl Drop for ClearHook {
        fn drop(&mut self) {
            PROBE_HOOK.replace(None);
        }
    }
    assert!(PROBE_HOOK.replace(Some(Box::new(hook))).is_none());
    let _clear = ClearHook;
    open()
}

const PRE_REMOVAL: u32 = 27;

struct Fixture {
    descriptor: StorageDescriptor,
    path: PathBuf,
    _scratch: PrivateDirectory,
}

impl Fixture {
    fn new() -> Self {
        let scratch = PrivateDirectory::new().unwrap();
        // An unescaped '#' would make SQLite probe a different file.
        let path = scratch.0.join("registry # %.db");
        Self {
            descriptor: descriptor(path.to_str().unwrap().into()),
            path,
            _scratch: scratch,
        }
    }

    fn at_version(version: u32) -> Self {
        let fixture = Self::new();
        let db = open_sqlite(&fixture.descriptor).unwrap();
        let count = MIGRATIONS.partition_point(|migration| migration.version <= version);
        let result = db
            .migrate(MIGRATION_NAMESPACE, &MIGRATIONS[..count])
            .unwrap();
        assert_eq!(result.recorded, version);
        fixture
    }

    fn connect(&self) -> Connection {
        Connection::open(&self.path).unwrap()
    }

    fn update(&self, assignments: &str) {
        update(&self.connect(), assignments);
    }

    fn copy_with_wal(&self, copy_shm: bool) -> Self {
        let copy = self.copy_main();
        for suffix in if copy_shm {
            &["-wal", "-shm"][..]
        } else {
            &["-wal"]
        } {
            fs::copy(sidecar(&self.path, suffix), sidecar(&copy.path, suffix)).unwrap();
        }
        copy
    }

    fn copy_main(&self) -> Self {
        let copy = Self::new();
        fs::copy(&self.path, &copy.path).unwrap();
        copy
    }

    fn open(&self) -> Result<RegistryStore, RegistryError> {
        RegistryStore::open(&self.descriptor)
    }
}

fn descriptor(path: String) -> StorageDescriptor {
    StorageDescriptor {
        module_id: "entorhinal-oldway-test".into(),
        storage_namespace: "oldway-test".into(),
        isolation: Isolation::Module,
        backend: StorageBackend::Sqlite { path },
    }
}

fn update(conn: &Connection, assignments: &str) {
    let changed = conn
        .execute(
            &format!("UPDATE identity_log_state SET {assignments} WHERE id = 1"),
            [],
        )
        .unwrap();
    assert_eq!(changed, 1);
}

fn state(conn: &Connection) -> String {
    conn.query_row(
        "SELECT state FROM identity_log_state WHERE id = 1",
        [],
        |r| r.get(0),
    )
    .unwrap()
}

fn digest(path: &Path) -> String {
    sha256_hex(fs::read(path).unwrap())
}

fn assert_refused(result: Result<RegistryStore, RegistryError>) -> String {
    match result.unwrap_err() {
        RegistryError::Domain {
            code,
            message,
            detail,
        } => {
            assert_eq!(code, "legacy_sharing_unsupported");
            assert!(detail.is_none());
            message
        }
        error => panic!("expected legacy_sharing_unsupported, got {error}"),
    }
}

fn assert_probe_refuses(fixture: &Fixture) -> String {
    assert_refused(with_probe_hook(
        |event| assert_eq!(event, ProbeEvent::BeforeRead, "reached the writable opener"),
        || fixture.open(),
    ))
}

#[test]
fn oldway_refusal_leaves_historical_stores_untouched() {
    for (version, assignments) in [
        (PRE_REMOVAL, "state='enabled'"),
        (PRE_REMOVAL, "state='enabled', engram_frozen='pending'"),
        (PRE_REMOVAL, "state='enabled', hub_quieting=1"),
        (PRE_REMOVAL, "state='joining'"),
        (PRE_REMOVAL, "state='enabling', enable_id=NULL"),
        (
            PRE_REMOVAL,
            "state='enabling', enable_id='stale', enable_backfill='{}'",
        ),
        (PRE_REMOVAL, "state='disabled', enable_backfill='{}'"),
        (25, "state='enabled'"),
        (25, "state='joining'"),
        (25, "state='enabling'"),
    ] {
        let fixture = Fixture::at_version(version);
        fixture.update(assignments);
        let before = digest(&fixture.path);
        let sidecars = ["-wal", "-shm", "-journal"].map(|suffix| sidecar(&fixture.path, suffix));
        assert!(sidecars.iter().all(|path| !path.exists()));
        let message = assert_probe_refuses(&fixture);
        for point in [
            "shares through engram",
            "neither its shared data nor its local-only data",
            "Move the store folder aside and keep it; do not delete it",
            "With a new, empty registry",
            "this account's registry already lives on the hub",
            "this computer will join it on its own",
            "`ck projects log enable` to join at once",
            "import agents first and run `ck projects log enable`",
            "`ck projects log enable --without-agents`, then confirm the prompt",
        ] {
            assert!(message.contains(point), "missing {point:?} in {message:?}");
        }
        assert_eq!(digest(&fixture.path), before, "v{version}: {assignments}");
        assert!(
            sidecars.iter().all(|path| !path.exists()),
            "v{version}: {assignments}"
        );
    }
}

#[test]
fn oldway_refusal_sees_wal_only_commits_without_touching_sidecars() {
    for with_shm in [true, false] {
        let source = Fixture::at_version(PRE_REMOVAL);
        let conn = source.connect();
        conn.execute_batch("PRAGMA wal_autocheckpoint=0").unwrap();
        update(&conn, "state='enabled'");
        let fixture = source.copy_with_wal(with_shm);
        assert_eq!(state(&fixture.copy_main().connect()), "disabled");
        let main_before = digest(&fixture.path);
        let wal = sidecar(&fixture.path, "-wal");
        let wal_before = fs::read(&wal).unwrap();
        let shm = sidecar(&fixture.path, "-shm");
        let shm_before = with_shm.then(|| fs::read(&shm).unwrap());

        assert_probe_refuses(&fixture);

        assert_eq!(digest(&fixture.path), main_before);
        assert_eq!(fs::read(wal).unwrap(), wal_before);
        assert_eq!(shm.exists(), with_shm);
        if let Some(shm_before) = shm_before {
            assert_eq!(fs::read(shm).unwrap(), shm_before);
        }
        assert!(!sidecar(&fixture.path, "-journal").exists());
        let replica = fixture.copy_with_wal(with_shm);
        assert_eq!(state(&replica.connect()), "enabled");
    }
}

#[test]
fn oldway_refusal_allows_a_wal_only_native_plan() {
    let source = Fixture::at_version(PRE_REMOVAL);
    let conn = source.connect();
    conn.execute_batch("PRAGMA wal_autocheckpoint=0").unwrap();
    update(&conn, "state='enabling', enable_id='native-plan'");
    let fixture = source.copy_with_wal(false);
    assert_eq!(state(&fixture.copy_main().connect()), "disabled");
    assert_eq!(
        fixture.open().unwrap().identity_log_status().unwrap().state,
        "enabling"
    );
}

#[test]
fn oldway_refusal_allows_supported_store_states() {
    for (version, assignments, expected) in [
        (0, "", "disabled"),
        (6, "", "disabled"),
        (PRE_REMOVAL, "", "disabled"),
        (
            PRE_REMOVAL,
            "state='enabling', enable_id='native-plan'",
            "enabling",
        ),
        (PRE_REMOVAL, "state='enabled', hub_log_base=0", "enabled"),
        (PRE_REMOVAL, "state='enabled', hub_log_base=7", "enabled"),
    ] {
        let fixture = if version == 0 {
            Fixture::new()
        } else {
            Fixture::at_version(version)
        };
        if !assignments.is_empty() {
            fixture.update(assignments);
        }
        assert_eq!(
            fixture.open().unwrap().identity_log_status().unwrap().state,
            expected
        );
    }
}

#[test]
fn oldway_refusal_preserves_store_ahead_error() {
    let fixture = Fixture::at_version(PRE_REMOVAL);
    let latest = MIGRATIONS.last().unwrap().version;
    let ahead = latest + 1;
    let conn = fixture.connect();
    update(&conn, "state='enabled'");
    conn.execute(
        "INSERT INTO cortexkit_schema_version (namespace, version, applied_at_unix) VALUES (?1, ?2, 0)",
        rusqlite::params![MIGRATION_NAMESPACE, ahead],
    ).unwrap();
    drop(conn);
    let error = fixture.open().unwrap_err();
    assert!(
        matches!(error, RegistryError::Database(ref message)
        if message.contains(&format!("schema version {ahead}, ahead of this binary's highest migration {latest}"))),
        "{error}"
    );
}

#[test]
fn oldway_refusal_excludes_both_competitors_under_the_probe_and_writer_leases() {
    for opener in ["registry", "ordinary"] {
        let fixture = Fixture::at_version(PRE_REMOVAL);
        let competitor = Competitor::hold(&fixture.descriptor, opener);
        let error = with_probe_hook(
            |event| panic!("probe ran while the competitor held the lease: {event:?}"),
            || fixture.open().unwrap_err(),
        );
        assert!(error.is_lease_held(), "{error}");
        competitor.release();

        let descriptor = fixture.descriptor.clone();
        let store = with_probe_hook(
            move |event| {
                if event == ProbeEvent::BeforeWritable {
                    Competitor::assert_held(&descriptor, opener);
                }
            },
            || fixture.open().unwrap(),
        );
        Competitor::assert_held(&fixture.descriptor, opener);
        drop(store);

        fixture.update("state='enabled'");
        assert_probe_refuses(&fixture);
        if opener == "registry" {
            // The new opener also refuses an old-way row. Reset that row so
            // the competitor can stay open once it acquires the released lease.
            fixture.update("state='disabled'");
        }
        Competitor::hold(&fixture.descriptor, opener).release();
    }
}

#[test]
fn oldway_refusal_rechecks_before_migrating_after_the_probe() {
    for opener in ["registry", "ordinary"] {
        let version = 26;
        let fixture = Fixture::at_version(version);
        let path = fixture.path.clone();
        let descriptor = fixture.descriptor.clone();
        assert_refused(with_probe_hook(
            move |event| {
                if event == ProbeEvent::BeforeWritable {
                    Competitor::assert_held(&descriptor, opener);
                    // Simulate a change across the handoff. A real SQLite
                    // writer here would first have to acquire the lease.
                    update(&Connection::open(&path).unwrap(), "state='enabled'");
                }
            },
            || fixture.open(),
        ));
        let conn = fixture.connect();
        assert_eq!(state(&conn), "enabled");
        let recorded: u32 = conn
            .query_row(
                "SELECT MAX(version) FROM cortexkit_schema_version WHERE namespace=?1",
                [MIGRATION_NAMESPACE],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(recorded, version, "a refused store must not migrate");
    }
}

const COMPETITOR_READY: &str = "ENTORHINAL_COMPETITOR_HOLDS_STORE";

struct Competitor(Child);

impl Competitor {
    fn command(descriptor: &StorageDescriptor, opener: &str, expect: &str) -> Command {
        let StorageBackend::Sqlite { path } = &descriptor.backend else {
            panic!("not SQLite")
        };
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--ignored",
                "--exact",
                "legacy_sharing::tests::oldway_refusal_competitor",
                "--nocapture",
            ])
            .env("ENTORHINAL_COMPETITOR_PATH", path)
            .env("ENTORHINAL_COMPETITOR_OPENER", opener)
            .env("ENTORHINAL_COMPETITOR_EXPECT", expect);
        command
    }

    fn hold(descriptor: &StorageDescriptor, opener: &str) -> Self {
        Self::start(Self::command(descriptor, opener, "open"), opener)
    }

    fn start(mut command: Command, opener: &str) -> Self {
        let mut child = Self(
            command
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap(),
        );
        let stdout = child.0.stdout.take().unwrap();
        let mut stdout = BufReader::new(stdout);
        let mut line = String::new();
        let mut output = String::new();
        while stdout.read_line(&mut line).unwrap() != 0 {
            if line.contains(COMPETITOR_READY) {
                // Keep stdout open so the child can print its test result.
                child.0.stdout = Some(stdout.into_inner());
                return child;
            }
            output.push_str(&line);
            line.clear();
        }
        panic!("{opener} competitor did not acquire the lease: {output}");
    }

    fn release(mut self) {
        self.0.stdin.take().unwrap().write_all(b"\n").unwrap();
        assert!(self.0.wait().unwrap().success());
    }

    fn assert_held(descriptor: &StorageDescriptor, opener: &str) {
        let output = Self::command(descriptor, opener, "held").output().unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success() && stdout.contains("1 passed"),
            "{opener} competitor did not see the lease held: {stdout}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

impl Drop for Competitor {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
#[ignore = "separate-process helper, invoked by the lease tests"]
fn oldway_refusal_competitor() {
    let Ok(path) = std::env::var("ENTORHINAL_COMPETITOR_PATH") else {
        return;
    };
    let descriptor = descriptor(path);
    let expect = std::env::var("ENTORHINAL_COMPETITOR_EXPECT").unwrap();
    let hold = || {
        assert_eq!(expect, "open", "competitor unexpectedly opened the store");
        println!("{COMPETITOR_READY}");
        std::io::stdout().flush().unwrap();
        std::io::stdin().read_exact(&mut [0]).unwrap();
    };
    let result = match std::env::var("ENTORHINAL_COMPETITOR_OPENER")
        .unwrap()
        .as_str()
    {
        "registry" => RegistryStore::open(&descriptor).map(|_store| hold()),
        // The ordinary opener is what older processes use to take the lease.
        "ordinary" => open_sqlite(&descriptor)
            .map_err(RegistryError::Store)
            .map(|_store| hold()),
        other => panic!("unexpected opener: {other}"),
    };
    if expect == "held" {
        let error = result.unwrap_err();
        assert!(error.is_lease_held(), "{error}");
    } else {
        result.unwrap();
    }
}
