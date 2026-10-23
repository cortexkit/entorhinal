use std::sync::atomic::{AtomicU64, Ordering};

use rusqlite::params;
use serde_json::json;

use super::*;
use crate::move_store::AgentMove;

const AGENT: &str = "agent_0123456789abcdef";
static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    store: RegistryStore,
    _cleanup: crate::scratch_cleanup::ScratchCleanup,
}

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "session-carry-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let descriptor = cortexkit_store::StorageDescriptor {
            module_id: "entorhinal-test".into(),
            storage_namespace: "carry".into(),
            isolation: cortexkit_store::Isolation::Module,
            backend: cortexkit_store::StorageBackend::Sqlite {
                path: root.join("store.db").to_string_lossy().into_owned(),
            },
        };
        let store = RegistryStore::open_with_migrations(&descriptor, &crate::MIGRATIONS).unwrap();
        store.check_store_origin(Some("source-machine"), 1).unwrap();
        Self {
            store,
            _cleanup: crate::scratch_cleanup::ScratchCleanup(root),
        }
    }

    /// Insert a binding row and its history directly, as any earlier
    /// registration would have left them.
    fn bind(&self, agent: &str, binding: &CarriedBinding) {
        self.store
            .db
            .with_conn_fenced(|tx| {
                tx.execute(
                    "INSERT INTO session_binding(ref_kind,ref,agent_id,harness,session_id,project_root,
                         registered_by,registered_at_ms,runner,ended_at_ms)
                     VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                    params![
                        binding.ref_kind,
                        binding.session_ref,
                        agent,
                        binding.harness,
                        binding.session_id,
                        binding.project_root,
                        binding.registered_by,
                        binding.registered_at_ms,
                        binding.runner,
                        binding.ended_at_ms
                    ],
                )?;
                for entry in &binding.network_history {
                    tx.execute(
                        "INSERT INTO session_network(ref_kind,ref,network,since_ms) VALUES(?1,?2,?3,?4)",
                        params![binding.ref_kind, binding.session_ref, entry.network, entry.since_ms],
                    )?;
                }
                Ok(())
            })
            .unwrap();
    }

    fn rows(&self) -> Vec<CarriedBinding> {
        self.store.read(|conn| carried_rows(conn, AGENT)).unwrap()
    }
}

fn entry(network: &str, since_ms: i64) -> NetworkEntry {
    NetworkEntry {
        network: network.into(),
        since_ms,
    }
}

fn binding(session: &str, runner: Option<&str>, history: Vec<NetworkEntry>) -> CarriedBinding {
    CarriedBinding {
        ref_kind: "broca-session".into(),
        session_ref: format!("ref-{session}"),
        runner: runner.map(str::to_owned),
        harness: Some("broca".into()),
        session_id: Some(session.into()),
        project_root: "/work/project".into(),
        registered_by: "host".into(),
        registered_at_ms: 40,
        ended_at_ms: None,
        network_history: history,
        held_on: None,
    }
}

fn move_ref(binding: &CarriedBinding) -> MoveRef {
    MoveRef {
        ref_kind: binding.ref_kind.clone(),
        reference: binding.session_ref.clone(),
    }
}

fn agent_move(cut: &str, side: MoveSide, step: MoveStep, refs: &[MoveRef]) -> AgentMove {
    AgentMove {
        cut_id: cut.into(),
        agent_id: AGENT.into(),
        side,
        peer: "peer".into(),
        step,
        plan: json!({"participants":[
            {"module_id":"broca","refs":refs,"external":[]},
            {"module_id":"entorhinal","refs":[{"ref_kind":SESSION_BINDINGS_KIND,"ref":AGENT}],"external":[]}
        ]}),
        request_body: None,
        record_version: 1,
        claim_version: None,
        claim_take_body: None,
        claim_take_version: None,
        claim_landed_version: None,
        seal_sent: false,
        abort_requested: None,
        hold: None,
        cut_accept: None,
        reason: None,
        detail: None,
        request_key: None,
        attempts: 0,
        activated_at_ms: None,
        created_at_ms: 1,
        updated_at_ms: 1,
    }
}

#[test]
fn export_reads_every_binding_with_its_history_under_the_open_source_move() {
    let f = Fixture::new();
    let live = binding(
        "s1",
        Some("broca"),
        vec![entry("yes", 100), entry("no", 250)],
    );
    let mut ended = binding("s2", None, vec![]);
    ended.ended_at_ms = Some(90);
    for b in [&live, &ended] {
        f.bind(AGENT, b);
    }
    let refs = vec![move_ref(&live)];
    // Without an open source move nothing freezes the agent, so nothing is read.
    assert_eq!(
        f.store
            .export_session_bindings("cut", AGENT)
            .unwrap_err()
            .code,
        "invalid_state"
    );
    f.store
        .insert_agent_move(&agent_move(
            "cut",
            MoveSide::Source,
            MoveStep::Preparing,
            &refs,
        ))
        .unwrap();
    let exported = f.store.export_session_bindings("cut", AGENT).unwrap();
    ended.held_on = Some("source-machine".into());
    assert_eq!(exported, vec![live.clone(), ended.clone()]);
    assert_eq!(
        exported[0].network_history,
        vec![entry("yes", 100), entry("no", 250)]
    );
    // A plan naming a different set than the rows is refused, not carried.
    let other = agent_move("cut-2", MoveSide::Source, MoveStep::Preparing, &[]);
    f.store
        .db
        .with_conn_fenced(|tx| tx.execute("DELETE FROM agent_move", []))
        .unwrap();
    f.store.insert_agent_move(&other).unwrap();
    assert_eq!(
        f.store
            .export_session_bindings("cut-2", AGENT)
            .unwrap_err()
            .code,
        "invalid_state"
    );
}

fn activating(f: &Fixture, refs: &[MoveRef]) -> AgentMove {
    let row = agent_move("cut", MoveSide::Receiver, MoveStep::Activating, refs);
    f.store.insert_agent_move(&row).unwrap();
    let mut next = row;
    next.step = MoveStep::Activated;
    next.activated_at_ms = Some(5000);
    next
}

#[test]
fn activation_installs_carried_rows_and_history_verbatim() {
    let f = Fixture::new();
    let live = binding(
        "s1",
        Some("broca"),
        vec![entry("yes", 100), entry("no", 250)],
    );
    let mut ended = binding("s2", None, vec![entry("unknown", 7)]);
    ended.ended_at_ms = Some(90);
    let bare = binding("s3", None, vec![]);
    let refs = vec![move_ref(&live), move_ref(&ended), move_ref(&bare)];
    let next = activating(&f, &refs);
    let (_, outcome) = f
        .store
        .activate_agent_move_carrying(&next, &refs, &[live.clone(), ended.clone()])
        .unwrap();
    assert_eq!(
        outcome,
        CarriedInstall {
            installed: 2,
            placeholders: 0,
            kept: 0,
            skipped_finished: Vec::new(),
        }
    );
    let placeholder = CarriedBinding {
        runner: None,
        harness: None,
        session_id: None,
        project_root: String::new(),
        registered_by: "move".into(),
        registered_at_ms: 5000,
        ..bare
    };
    assert_eq!(f.rows(), vec![live, ended, placeholder]);
}

#[test]
fn activation_keeps_an_existing_local_binding_and_its_history() {
    let f = Fixture::new();
    let local = binding("s1", Some("broca"), vec![entry("no", 10)]);
    f.bind(AGENT, &local);
    let mut carried = local.clone();
    carried.project_root = "/elsewhere".into();
    carried.network_history = vec![entry("yes", 100), entry("no", 250)];
    let refs = vec![move_ref(&local)];
    let next = activating(&f, &refs);
    let (_, outcome) = f
        .store
        .activate_agent_move_carrying(&next, &refs, &[carried])
        .unwrap();
    assert_eq!(outcome.kept, 1);
    assert_eq!(outcome.installed, 0);
    assert_eq!(f.rows(), vec![local]);
}

#[test]
fn activation_downgrades_a_colliding_row_to_a_placeholder_and_still_activates() {
    let f = Fixture::new();
    // Another agent's bindings already hold host session s1 and runner
    // session s2 on this Mac.
    let mut holder = binding("s1", None, vec![]);
    holder.session_ref = "other-1".into();
    f.bind("agent_other", &holder);
    let mut runner_holder = binding("s2", Some("broca"), vec![]);
    runner_holder.session_ref = "other-2".into();
    runner_holder.harness = Some("pi".into());
    f.bind("agent_other", &runner_holder);
    let by_host = binding("s1", None, vec![entry("yes", 100), entry("no", 250)]);
    let mut by_runner = binding("s2", Some("broca"), vec![]);
    by_runner.session_ref = "ref-s2b".into();
    let refs = vec![move_ref(&by_host), move_ref(&by_runner)];
    let next = activating(&f, &refs);
    let (row, outcome) = f
        .store
        .activate_agent_move_carrying(&next, &refs, &[by_host.clone(), by_runner])
        .unwrap();
    assert_eq!(row.step, MoveStep::Activated);
    assert_eq!(
        outcome,
        CarriedInstall {
            installed: 0,
            placeholders: 2,
            kept: 0,
            skipped_finished: Vec::new(),
        }
    );
    let rows = f.rows();
    assert_eq!(rows.len(), 2);
    assert!(rows
        .iter()
        .all(|r| r.harness.is_none() && r.session_id.is_none() && r.registered_by == "move"));
    assert_eq!(
        rows.iter()
            .find(|r| r.session_ref == by_host.session_ref)
            .unwrap()
            .network_history,
        by_host.network_history
    );
}

#[test]
fn carried_rows_are_checked_against_the_receiving_schema() {
    let good = binding("s1", Some("broca"), vec![entry("yes", 1), entry("no", 2)]);
    assert_eq!(good.check(), Ok(()));
    let mut cases = Vec::new();
    let mut b = good.clone();
    b.registered_by = "other".into();
    cases.push(b);
    let mut b = good.clone();
    b.harness = None;
    cases.push(b);
    let mut b = good.clone();
    b.network_history = vec![entry("yes", 2), entry("no", 2)];
    cases.push(b);
    let mut b = good.clone();
    b.network_history = vec![entry("maybe", 2)];
    cases.push(b);
    let mut b = good.clone();
    b.ref_kind = SESSION_BINDINGS_KIND.into();
    cases.push(b);
    let mut b = good;
    b.network_history = (0..=NETWORK_HISTORY_MAX).map(|i| entry("yes", i)).collect();
    cases.push(b);
    for case in cases {
        assert!(case.check().is_err(), "{case:?}");
    }
}

#[test]
fn finished_metadata_keeps_original_holder_across_three_macs_and_clears_at_home() {
    fn hop(source: &Fixture, receiver: &Fixture, cut: &str) {
        source
            .store
            .insert_agent_move(&agent_move(cut, MoveSide::Source, MoveStep::Preparing, &[]))
            .unwrap();
        let rows = source.store.export_session_bindings(cut, AGENT).unwrap();
        receiver
            .store
            .insert_agent_move(&agent_move(
                cut,
                MoveSide::Receiver,
                MoveStep::Activating,
                &[],
            ))
            .unwrap();
        let mut activated = agent_move(cut, MoveSide::Receiver, MoveStep::Activated, &[]);
        activated.activated_at_ms = Some(5000);
        let (_, outcome) = receiver
            .store
            .activate_agent_move_carrying(&activated, &[], &rows)
            .unwrap();
        assert_eq!(outcome.installed, 1);
        assert_eq!(outcome.placeholders, 0);
        for f in [source, receiver] {
            f.store
                .db
                .with_conn_fenced(|tx| tx.execute("DELETE FROM agent_move", []))
                .unwrap();
        }
    }
    let a = Fixture::new();
    let b = Fixture::new();
    let c = Fixture::new();
    for (f, id) in [(&a, "a"), (&b, "b"), (&c, "c")] {
        f.store
            .db
            .with_conn_fenced(|tx| tx.execute("UPDATE store_origin SET machine_id=?1", [id]))
            .unwrap();
    }
    let mut finished = binding(
        "ended",
        Some("broca"),
        vec![entry("yes", 1), entry("no", 2)],
    );
    finished.ended_at_ms = Some(90);
    a.bind(AGENT, &finished);
    hop(&a, &b, "ab");
    assert_eq!(b.rows()[0].held_on.as_deref(), Some("a"));
    hop(&b, &c, "bc");
    assert_eq!(c.rows()[0].held_on.as_deref(), Some("a"));
    // Retention removes metadata, not the data still held on the original Mac.
    assert_eq!(
        a.store
            .prune_ended_sessions(91 + crate::SESSION_RETENTION_MS)
            .unwrap(),
        1
    );
    hop(&c, &a, "ca");
    assert_eq!(a.rows(), vec![finished]);
    assert_eq!(
        c.store
            .prune_ended_sessions(91 + crate::SESSION_RETENTION_MS)
            .unwrap(),
        1
    );
    assert!(c.rows().is_empty());
}

#[test]
fn finished_collision_skips_metadata_and_keeps_local_binding_and_history_identical() {
    let f = Fixture::new();
    let local = binding("s1", Some("broca"), vec![entry("no", 10)]);
    f.bind(AGENT, &local);
    let before = serde_json::to_vec(&f.rows()).unwrap();
    let mut remote = local.clone();
    remote.session_ref = "remote-finished".into();
    remote.ended_at_ms = Some(20);
    remote.held_on = Some("other-machine".into());
    remote.network_history = vec![entry("yes", 5)];
    let next = activating(&f, &[]);
    let (_, outcome) = f
        .store
        .activate_agent_move_carrying(&next, &[], &[remote.clone()])
        .unwrap();
    assert_eq!(
        outcome.skipped_finished,
        vec![(move_ref(&remote), move_ref(&local))]
    );
    assert_eq!(outcome.kept, 1);
    assert_eq!(outcome.placeholders, 0);
    assert_eq!(outcome.installed, 0);
    assert_eq!(serde_json::to_vec(&f.rows()).unwrap(), before);
    assert!(f
        .store
        .session_bindings_for_refs(&[("broca-session", "remote-finished")])
        .unwrap()[0]
        .is_none());
}
