//! Build the local registry rows needed by the live Switch fixture.

use super::*;
use entorhinal_core::SessionRegistration;

pub(crate) fn add_agent(store: &RegistryStore, id: &str, name: &str) {
    store
        .with_principal("reserved:prefrontal")
        .agent_mutation_with_id(
            "agent.create",
            json!({"role":"assistant","name":name,"tag":"baseline","request_key":name}),
            NOW,
            Some(id),
        )
        .unwrap();
}

pub(crate) fn cutover(store: &RegistryStore) {
    // Import an empty snapshot to record the real local cutover marker.
    let path = store.store_folder().join("import.db");
    Connection::open(&path)
        .unwrap()
        .execute_batch("CREATE TABLE empty_snapshot(id INTEGER)")
        .unwrap();
    store
        .agent_import(
            json!({"snapshot_path":path,"request_key":"baseline-import"}),
            NOW,
        )
        .unwrap();
}

pub(crate) fn session(store: &RegistryStore, agent: &str) {
    store
        .register_session(&SessionRegistration {
            agent_id: agent,
            ref_kind: "broca-session",
            session_ref: agent,
            harness: "pi",
            session_id: agent,
            project_root: None,
            registered_by: "host",
            runner: Some("broca"),
            role: None,
            network: None,
            now_ms: NOW,
        })
        .unwrap();
}
