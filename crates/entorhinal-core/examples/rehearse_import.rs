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
//!       <entorhinal store copy> <core agent snapshot> [--rename <agent_id> <new name>]
//!
//! - `<entorhinal store copy>`: a copy of entorhinal's `store.db`, made with
//!   `sqlite3 <live store.db> ".backup <copy>"`. The live writer-lease file
//!   (`<hash>.lease` beside the live store) must be copied beside it too.
//!   Without it the copy claims a fresh writer epoch, and every write is
//!   refused as fenced by a newer writer.
//! - `<core agent snapshot>`: a SQLite file holding core's `agent` and
//!   `agent_name_claim` tables, exported table by table. A full online backup
//!   of core's store never finishes while core is writing.
//! - `--rename`: after the import, rename one agent the way a rename relayed
//!   by core lands (recorded under core's principal), and report the
//!   `agent.changes` page core's replica would read from the import's
//!   generation onward. Feeding that page to core's replica checks rename
//!   forwarding without a running entorhinal. The live service adds its
//!   process `incarnation` to that reply; this offline page has none.
//!
//! It runs the same `agent.import` the switchover runs, then checks the store
//! (`verify`), rebuilds it from its journal (`rebuild`), and prints one JSON
//! object on stdout: the import reply, both checks, the generation before and
//! after the rebuild, and every imported agent and name claim exactly as
//! entorhinal stores them, for a field-by-field comparison with core's rows.
//! With `--rename` it adds the rename reply, the changes page, and a second
//! `verify` after the rename. It exits non-zero if the import or rename is
//! refused or any check fails.

use std::path::Path;
use std::process::ExitCode;

use cortexkit_store_types::{Isolation, StorageBackend, StorageDescriptor};
use entorhinal_core::RegistryStore;
use serde_json::json;

/// The principal the daemon records for writes core relays to entorhinal.
const CORE_PRINCIPAL: &str = "reserved:prefrontal-core";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let (store, snapshot, rename) = match args.as_slice() {
        [_, store, snapshot] => (store, snapshot, None),
        [_, store, snapshot, flag, agent_id, name] if flag == "--rename" => {
            (store, snapshot, Some((agent_id, name)))
        }
        _ => {
            eprintln!(
                "usage: rehearse_import <entorhinal store copy> <core agent snapshot> \
                 [--rename <agent_id> <new name>]"
            );
            return ExitCode::from(2);
        }
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
        let mut ok = verify.ok && rebuild.replay.ok && before == after;
        let mut report = json!({
            "import": import,
            "verify": verify,
            "rebuild": rebuild,
            "generation_before_rebuild": before,
            "generation_after_rebuild": after,
            "stored": stored,
        });
        if let Some((agent_id, name)) = rename {
            let reply = registry
                .with_principal(CORE_PRINCIPAL)
                .agent_mutation(
                    "agent.rename",
                    json!({
                        "agent_id": agent_id,
                        "name": name,
                        "request_key": "switchover-rehearsal-rename",
                    }),
                    now_ms,
                )
                .map_err(|e| format!("rename refused: {e:?}"))?;
            // Core's replica resumes from the generation it last saw, which
            // after the import is the import's own generation.
            let changes = registry
                .agent_changes(after, None)
                .map_err(|e| format!("agent changes: {e:?}"))?;
            let verify_after = registry
                .verify()
                .map_err(|e| format!("verify after rename: {e}"))?;
            ok = ok && verify_after.ok;
            report["rename"] = json!({
                "reply": serde_json::from_slice::<serde_json::Value>(&reply)
                    .unwrap_or_else(|_| json!(String::from_utf8_lossy(&reply))),
                "changes_since_import": changes,
                "verify_after_rename": verify_after,
            });
        }
        Ok((report, ok))
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
