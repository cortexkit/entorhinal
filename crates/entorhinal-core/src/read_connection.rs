//! A WAL reader independent of the leased writer. Every query closure observes
//! one committed snapshot, including its journal generation.

use std::sync::Mutex;

use cortexkit_store_types::{StorageBackend, StorageDescriptor};
use rusqlite::{params_from_iter, types::Value, Connection, OpenFlags};

use crate::RegistryError;

pub(crate) struct ReadConnection(Mutex<Connection>);

impl ReadConnection {
    pub(crate) fn open(descriptor: &StorageDescriptor) -> Result<Self, RegistryError> {
        let StorageBackend::Sqlite { path } = &descriptor.backend else {
            return Err(RegistryError::Database(
                "registry requires SQLite storage".into(),
            ));
        };
        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        let journal_mode: String = conn.query_row("PRAGMA journal_mode", [], |r| r.get(0))?;
        if !journal_mode.eq_ignore_ascii_case("wal") {
            return Err(RegistryError::Database(
                "registry reads require SQLite WAL".into(),
            ));
        }
        conn.execute_batch("PRAGMA query_only=ON; PRAGMA foreign_keys=ON;")?;
        Ok(Self(Mutex::new(conn)))
    }

    pub(crate) fn read<T>(
        &self,
        query: impl FnOnce(&Connection) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<T> {
        let mut conn = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let tx = conn.transaction()?;
        let result = query(&tx)?;
        tx.commit()?;
        Ok(result)
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
        root: std::path::PathBuf,
        path: std::path::PathBuf,
        checkout: String,
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
                store,
                root,
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

    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.root).unwrap();
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
