//! A small fixed pool of WAL readers independent of the leased writer. Every
//! query closure runs on one reader inside one transaction, so it observes one
//! committed snapshot, including its journal generation. One slow read holds
//! only its own reader; the others keep answering.

use std::sync::{Arc, Condvar, Mutex};

use cortexkit_store_types::{StorageBackend, StorageDescriptor};
use rusqlite::{params_from_iter, types::Value, Connection, OpenFlags};

use crate::RegistryError;

/// How many read-only connections a store opens. A burst of more reads than
/// this waits for a free reader rather than opening another connection.
pub(crate) const READERS: usize = 4;

/// A test seam run inside every read snapshot while the read holds its reader.
pub(crate) type ReadHook = Arc<dyn Fn() + Send + Sync>;

pub(crate) struct ReadConnection {
    idle: Mutex<Vec<Connection>>,
    returned: Condvar,
    hook: Mutex<Option<ReadHook>>,
}

impl ReadConnection {
    pub(crate) fn open(descriptor: &StorageDescriptor) -> Result<Self, RegistryError> {
        let StorageBackend::Sqlite { path } = &descriptor.backend else {
            return Err(RegistryError::Database(
                "registry requires SQLite storage".into(),
            ));
        };
        let readers = (0..READERS)
            .map(|_| open_reader(path))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            idle: Mutex::new(readers),
            returned: Condvar::new(),
            hook: Mutex::new(None),
        })
    }

    pub(crate) fn read<T>(
        &self,
        query: impl FnOnce(&Connection) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<T> {
        let mut reader = self.checkout();
        let tx = reader.connection().transaction()?;
        let hook = self
            .hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(hook) = hook {
            hook();
        }
        let result = query(&tx)?;
        tx.commit()?;
        Ok(result)
    }

    pub(crate) fn set_hook(&self, hook: Option<ReadHook>) {
        *self
            .hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = hook;
    }

    /// Wait for a free reader. The returned guard puts it back when the read
    /// ends, including when the query panics (its transaction rolls back first).
    fn checkout(&self) -> Reader<'_> {
        let mut idle = self
            .idle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        loop {
            if let Some(connection) = idle.pop() {
                return Reader {
                    pool: self,
                    connection: Some(connection),
                };
            }
            idle = self
                .returned
                .wait(idle)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
}

fn open_reader(path: &str) -> Result<Connection, RegistryError> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let journal_mode: String = conn.query_row("PRAGMA journal_mode", [], |r| r.get(0))?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(RegistryError::Database(
            "registry reads require SQLite WAL".into(),
        ));
    }
    conn.execute_batch("PRAGMA query_only=ON; PRAGMA foreign_keys=ON;")?;
    Ok(conn)
}

struct Reader<'a> {
    pool: &'a ReadConnection,
    connection: Option<Connection>,
}

impl Reader<'_> {
    fn connection(&mut self) -> &mut Connection {
        self.connection.as_mut().expect("reader is checked out")
    }
}

impl Drop for Reader<'_> {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            self.pool
                .idle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(connection);
            self.pool.returned.notify_one();
        }
    }
}

/// Copy schema and cells without JSON conversion or a live write transaction.
/// The caller holds the read snapshot throughout the copy. Foreign keys are
/// enabled only after loading so cycles and existing drift can be inspected.
pub(crate) fn copy_snapshot(source: &Connection) -> rusqlite::Result<Connection> {
    let mut copy = Connection::open_in_memory()?;
    copy.execute_batch("PRAGMA foreign_keys=OFF;")?;
    let schema = source
        .prepare("SELECT type,name,sql FROM sqlite_schema WHERE sql IS NOT NULL ORDER BY CASE type WHEN 'table' THEN 0 ELSE 1 END,name")?
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let tx = copy.transaction()?;
    for (_, name, sql) in schema.iter().filter(|(kind, _, _)| kind == "table") {
        // AUTOINCREMENT creates sqlite_sequence itself; its cells are copied
        // below along with the user tables, rather than recreating its schema.
        if name != "sqlite_sequence" {
            tx.execute_batch(sql)?;
        }
    }
    for (_, name, _) in schema.iter().filter(|(kind, _, _)| kind == "table") {
        let quoted = format!("\"{}\"", name.replace('"', "\"\""));
        let mut rows = source.prepare(&format!("SELECT * FROM {quoted}"))?;
        let columns = rows.column_count();
        let placeholders = vec!["?"; columns].join(",");
        let mut insert = tx.prepare(&format!("INSERT INTO {quoted} VALUES ({placeholders})"))?;
        if name == "sqlite_sequence" {
            tx.execute("DELETE FROM sqlite_sequence", [])?;
        }
        for row in rows.query_map([], |r| {
            (0..columns)
                .map(|i| r.get::<_, Value>(i))
                .collect::<rusqlite::Result<Vec<_>>>()
        })? {
            insert.execute(params_from_iter(row?))?;
        }
    }
    // Install indexes, views and triggers after copying, so loading a committed
    // row cannot fire a trigger and alter the state being verified.
    for (_, _, sql) in schema.iter().filter(|(kind, _, _)| kind != "table") {
        tx.execute_batch(sql)?;
    }
    tx.commit()?;
    copy.execute_batch("PRAGMA foreign_keys=ON;")?;
    Ok(copy)
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{
            atomic::{AtomicU64, Ordering},
            mpsc, Arc, Barrier,
        },
        thread,
    };

    use super::*;
    use crate::{RegisterRequest, RegistryStore};
    use cortexkit_store_types::Isolation;

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct Fixture {
        store: RegistryStore,
        path: std::path::PathBuf,
        checkout: String,
        _cleanup: crate::scratch_cleanup::ScratchCleanup,
    }

    impl Fixture {
        fn new() -> Self {
            let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/read-connection")
                .join(format!(
                    "{}-{}",
                    std::process::id(),
                    COUNTER.fetch_add(1, Ordering::Relaxed)
                ));
            fs::create_dir_all(root.join("checkout")).unwrap();
            let path = root.join("store.db");
            let store = RegistryStore::open(&StorageDescriptor {
                module_id: "entorhinal".into(),
                storage_namespace: "test".into(),
                isolation: Isolation::Module,
                backend: StorageBackend::Sqlite {
                    path: path.to_string_lossy().into_owned(),
                },
            })
            .unwrap();
            let checkout =
                RegistryStore::canonical_mutation_root(root.join("checkout").to_str().unwrap())
                    .unwrap();
            Self {
                _cleanup: crate::scratch_cleanup::ScratchCleanup(root.clone()),
                store,
                path,
                checkout,
            }
        }

        fn register(&self, name: &str) {
            self.store.register(self.request(name)).unwrap();
        }

        fn request(&self, name: &str) -> RegisterRequest {
            RegisterRequest {
                project_id: Some("P".into()),
                name: name.into(),
                roots: vec![self.checkout.clone()],
                workspace_id: Some("W".into()),
                ..Default::default()
            }
        }
    }

    #[test]
    fn read_connection_is_read_only_and_snapshot_replies_track_committed_generations() {
        let f = Fixture::new();
        f.register("version-1");
        f.store
            .read(|conn| {
                assert!(conn
                    .execute("UPDATE project SET name='not-allowed'", [])
                    .is_err());
                Ok(())
            })
            .unwrap();

        // Force a commit between two statements, rather than hoping scheduling
        // happens to expose an untransactional reader during the stress loop.
        let (start_tx, start_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        thread::scope(|scope| {
            let f = &f;
            scope.spawn(move || {
                start_rx
                    .recv_timeout(std::time::Duration::from_secs(3))
                    .unwrap();
                f.register("version-2");
                done_tx.send(()).unwrap();
            });
            let (name, generation) = f
                .store
                .read(|conn| {
                    let name: String =
                        conn.query_row("SELECT name FROM project WHERE project_id='P'", [], |r| {
                            r.get(0)
                        })?;
                    start_tx.send(()).unwrap();
                    done_rx
                        .recv_timeout(std::time::Duration::from_secs(3))
                        .unwrap();
                    Ok((name, f.store.generation_from_connection(conn)?))
                })
                .unwrap();
            assert_eq!((name.as_str(), generation), ("version-1", 1));
        });

        let start = Barrier::new(2);
        thread::scope(|scope| {
            scope.spawn(|| {
                start.wait();
                for generation in 3..=252 {
                    f.register(&format!("version-{generation}"));
                }
            });
            start.wait();
            for _ in 0..500 {
                let enumerated = f.store.enumerate(None).unwrap();
                assert_eq!(enumerated.projects.len(), 1);
                assert_eq!(enumerated.workspaces.len(), 1);
                assert_eq!(
                    enumerated.projects[0].name,
                    format!("version-{}", enumerated.generation)
                );
                assert_eq!(enumerated.projects[0].roots, vec![f.checkout.clone()]);
                assert_eq!(enumerated.projects[0].workspace_id.as_deref(), Some("W"));
                let resolved = f.store.resolve(&f.checkout).unwrap();
                assert_eq!(
                    resolved.project_name,
                    Some(format!("version-{}", resolved.generation))
                );
                assert_eq!(resolved.workspace_id.as_deref(), Some("W"));
            }
        });
        assert_eq!(f.store.generation().unwrap(), 252);
    }

    #[test]
    fn a_read_parked_inside_its_snapshot_leaves_the_other_readers_answering() {
        let f = Fixture::new();
        f.register("parked");
        // Park the first read inside its snapshot, holding its reader.
        let armed = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let (parked_tx, parked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let release_rx = std::sync::Mutex::new(release_rx);
        let hook_armed = armed.clone();
        f.store.set_read_hook(Some(Arc::new(move || {
            if hook_armed.swap(false, Ordering::SeqCst) {
                parked_tx.send(()).unwrap();
                let _ = release_rx.lock().unwrap().recv();
            }
        })));
        thread::scope(|scope| {
            let f = &f;
            let parked = scope.spawn(move || f.store.generation().unwrap());
            parked_rx
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap();
            // Every other kind of read still answers while it is parked.
            let (done_tx, done_rx) = mpsc::channel();
            scope.spawn(move || {
                let resolved = f.store.resolve(&f.checkout).unwrap();
                let enumerated = f.store.enumerate(None).unwrap();
                let generation = f.store.generation().unwrap();
                done_tx
                    .send((resolved.project_name, enumerated.projects.len(), generation))
                    .unwrap();
            });
            let answered = done_rx.recv_timeout(std::time::Duration::from_secs(10));
            release_tx.send(()).unwrap();
            assert_eq!(
                answered.expect("reads answer while another read is parked"),
                (Some("parked".to_string()), 1, 1)
            );
            assert_eq!(parked.join().unwrap(), 1);
        });
        f.store.set_read_hook(None);
    }

    #[test]
    fn a_burst_of_reads_waits_for_a_free_reader_instead_of_opening_more() {
        let f = Fixture::new();
        // Park every read that enters a snapshot until the gate opens.
        let entered = Arc::new(AtomicU64::new(0));
        let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let (hook_entered, hook_gate) = (entered.clone(), gate.clone());
        f.store.set_read_hook(Some(Arc::new(move || {
            hook_entered.fetch_add(1, Ordering::SeqCst);
            let (open, opened) = &*hook_gate;
            let mut open = open.lock().unwrap();
            while !*open {
                open = opened.wait(open).unwrap();
            }
        })));
        thread::scope(|scope| {
            let reads = (0..READERS + 1)
                .map(|_| scope.spawn(|| f.store.generation().unwrap()))
                .collect::<Vec<_>>();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while entered.load(Ordering::SeqCst) < READERS as u64 {
                assert!(std::time::Instant::now() < deadline, "readers never filled");
                thread::yield_now();
            }
            // The extra read has no reader to enter a snapshot on.
            thread::sleep(std::time::Duration::from_millis(200));
            assert_eq!(entered.load(Ordering::SeqCst), READERS as u64);
            let (open, opened) = &*gate;
            *open.lock().unwrap() = true;
            opened.notify_all();
            for read in reads {
                assert_eq!(read.join().unwrap(), 0);
            }
        });
        assert_eq!(entered.load(Ordering::SeqCst), READERS as u64 + 1);
        f.store.set_read_hook(None);
    }

    #[test]
    fn verify_reports_drift_without_changing_live_database_bytes_or_mtimes() {
        let f = Fixture::new();
        f.register("original");
        f.store
            .db
            .with_conn_fenced(|tx| tx.execute("UPDATE project SET name='drift'", []))
            .unwrap();
        let paths = [
            f.path.clone(),
            std::path::PathBuf::from(format!("{}-wal", f.path.display())),
        ];
        let before = paths
            .iter()
            .map(|path| {
                (
                    fs::read(path).unwrap(),
                    fs::metadata(path).unwrap().modified().unwrap(),
                )
            })
            .collect::<Vec<_>>();
        let report = f.store.verify().unwrap();
        assert!(!report.ok);
        assert_eq!(report.generation, 1);
        assert_eq!(report.replay.tables.len(), 1);
        assert_eq!(report.replay.tables[0].table, "project");
        assert_eq!(report.replay.tables[0].missing, 1);
        assert_eq!(report.replay.tables[0].unexpected, 1);
        assert_eq!(
            f.store
                .resolve(&f.checkout)
                .unwrap()
                .project_name
                .as_deref(),
            Some("drift")
        );
        let after = paths
            .iter()
            .map(|path| {
                (
                    fs::read(path).unwrap(),
                    fs::metadata(path).unwrap().modified().unwrap(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            before, after,
            "verify must not write the live database or WAL"
        );
    }

    #[test]
    fn cloned_handles_serialize_mutations_and_preserve_request_key_idempotence() {
        let f = Fixture::new();
        let start = Arc::new(Barrier::new(2));
        let handles = (0..2)
            .map(|index| {
                let store = f.store.clone();
                let start = start.clone();
                let request = RegisterRequest {
                    project_id: Some(format!("P{index}")),
                    name: format!("Project {index}"),
                    request_key: Some("one-request".into()),
                    ..Default::default()
                };
                thread::spawn(move || {
                    start.wait();
                    store.register(request).unwrap()
                })
            })
            .collect::<Vec<_>>();
        let replies = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            replies[0], replies[1],
            "same-key register replays the first cached reply"
        );
        assert_eq!(f.store.generation().unwrap(), 1);
        assert_eq!(f.store.enumerate(None).unwrap().projects.len(), 1);

        // Register names are not unique. A shared root is the existing conflict
        // contract: exactly one owner commits, the other gets root_conflict.
        let start = Arc::new(Barrier::new(2));
        let handles = (0..2)
            .map(|index| {
                let store = f.store.clone();
                let start = start.clone();
                let request = RegisterRequest {
                    project_id: Some(format!("R{index}")),
                    name: "same-name".into(),
                    roots: vec![f.checkout.clone()],
                    ..Default::default()
                };
                thread::spawn(move || {
                    start.wait();
                    store.register(request)
                })
            })
            .collect::<Vec<_>>();
        let results = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert!(results.iter().any(|result| matches!(result, Err(RegistryError::Domain { code, .. }) if code == "root_conflict")));
        assert_eq!(f.store.generation().unwrap(), 2);
    }
}
