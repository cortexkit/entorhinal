//! Rehearses the agent-identity switchover import on scratch copies, never on
//! a live store.
//!
//! Agent identities (names, roles, tags, retirements) were first kept by
//! prefrontal-core. At the switchover, entorhinal imports core's agent tables
//! once and becomes their owner; core then reads them from entorhinal. This
//! runs that one-time import against copies, so it can be checked before the
//! real switchover.
//!
//! Usage:
//!   cargo run -p entorhinal-core --example rehearse_import -- \
//!       <entorhinal store copy> <core agent snapshot>
//!
//! - `<entorhinal store copy>`: a copy of entorhinal's `store.db`, made with
//!   `sqlite3 <live store.db> ".backup <copy>"`. The live writer-lease file
//!   (`<hash>.lease` beside the live store) must be copied beside it too.
//!   Without it the copy claims a fresh writer epoch, and every write is
//!   refused as fenced by a newer writer.
//! - `<core agent snapshot>`: a SQLite file holding core's `agent` and
//!   `agent_name_claim` tables, exported table by table. A full online backup
//!   of core's store never finishes while core is writing.
//!
//! It runs the same `agent.import` the switchover runs, then checks the store
//! (`verify`), rebuilds it from its journal (`rebuild`), and prints one JSON
//! object on stdout: the import reply, both checks, the generation before and
//! after the rebuild, and every imported agent and name claim exactly as
//! entorhinal stores them, for a field-by-field comparison with core's rows.
//! It exits non-zero if the import is refused or either check fails.

use std::path::Path;
use std::process::ExitCode;

use cortexkit_store_types::{Isolation, StorageBackend, StorageDescriptor};
use entorhinal_core::RegistryStore;
use serde_json::json;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let [_, store, snapshot] = args.as_slice() else {
        eprintln!("usage: rehearse_import <entorhinal store copy> <core agent snapshot>");
        return ExitCode::from(2);
    };
    let store_dir = Path::new(store).parent().unwrap_or(Path::new("."));
    let has_lease = std::fs::read_dir(store_dir)
        .map(|entries| {
            entries.flatten().any(|entry| {
                entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "lease")
            })
        })
        .unwrap_or(false);
    if !has_lease {
        eprintln!(
            "no .lease file beside {store}: copy the live writer lease from \
             entorhinal's data directory next to the store copy"
        );
        return ExitCode::from(2);
    }

    let descriptor = StorageDescriptor {
        module_id: "entorhinal".into(),
        storage_namespace: "default".into(),
        isolation: Isolation::Module,
        backend: StorageBackend::Sqlite {
            path: store.clone(),
        },
    };
    let registry = match RegistryStore::open(&descriptor) {
        Ok(registry) => registry,
        Err(error) => {
            eprintln!("open {store}: {error}");
            return ExitCode::FAILURE;
        }
    };
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or_default();
    let import = match registry.agent_import(
        json!({"snapshot_path": snapshot, "request_key": "switchover-rehearsal"}),
        now_ms,
    ) {
        Ok(reply) => serde_json::from_slice::<serde_json::Value>(&reply)
            .unwrap_or_else(|_| json!(String::from_utf8_lossy(&reply))),
        Err(error) => {
            eprintln!("import refused: {error:?}");
            return ExitCode::FAILURE;
        }
    };

    let result = (|| -> Result<(serde_json::Value, bool), String> {
        let verify = registry.verify().map_err(|e| format!("verify: {e}"))?;
        let before = registry
            .generation()
            .map_err(|e| format!("generation: {e}"))?;
        let rebuild = registry.rebuild().map_err(|e| format!("rebuild: {e}"))?;
        let after = registry
            .generation()
            .map_err(|e| format!("generation: {e}"))?;
        let stored = registry
            .agent_snapshot()
            .map_err(|e| format!("agent snapshot: {e:?}"))?;
        // `rebuild.replay.ok` means replaying the journal reproduced the
        // tables as they were, so no imported row lives outside the journal.
        let ok = verify.ok && rebuild.replay.ok && before == after;
        Ok((
            json!({
                "import": import,
                "verify": verify,
                "rebuild": rebuild,
                "generation_before_rebuild": before,
                "generation_after_rebuild": after,
                "stored": stored,
            }),
            ok,
        ))
    })();
    match result {
        Ok((report, ok)) => {
            println!("{report}");
            if ok {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
