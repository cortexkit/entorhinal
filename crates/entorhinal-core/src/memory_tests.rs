use super::*;
use crate::{
    agent::{AgentPrecheck, OperatorApproval, PlacementConfirmation},
    RegisterRequest,
};
use cortexkit_store_types::{Isolation, StorageBackend, StorageDescriptor};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    store: RegistryStore,
    root: PathBuf,
    _cleanup: crate::scratch_cleanup::ScratchCleanup,
}
impl Fixture {
    fn new() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/memory-context")
            .join(format!(
                "{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        std::fs::create_dir_all(root.join("checkout")).unwrap();
        let store = RegistryStore::open(&StorageDescriptor {
            module_id: "entorhinal".into(),
            storage_namespace: "test".into(),
            isolation: Isolation::Module,
            backend: StorageBackend::Sqlite {
                path: root.join("store.db").to_string_lossy().into(),
            },
        })
        .unwrap();
        let f = Self {
            store,
            root: std::fs::canonicalize(&root).unwrap(),
            _cleanup: crate::scratch_cleanup::ScratchCleanup(root),
        };
        f.store
            .register(RegisterRequest {
                project_id: Some("P".into()),
                name: "Project".into(),
                workspace_id: Some("W".into()),
                roots: vec![f.folder()],
                ..Default::default()
            })
            .unwrap();
        f
    }
    fn folder(&self) -> String {
        self.root.join("checkout").to_string_lossy().into()
    }
    fn approval(&self, op: &str, request: Value) -> PlacementConfirmation {
        let AgentPrecheck::Ready(summary) = self
            .store
            .precheck_agent_confirmation(op, request, 700)
            .unwrap()
        else {
            panic!("expected confirmation")
        };
        PlacementConfirmation::Approved(OperatorApproval::new(summary))
    }
    fn set(&self, req: SetWorkspaceDocumentRequest) -> WorkspaceDocument {
        let approval = self.approval("workspace.set_document", json!(req));
        self.store
            .with_principal("direct")
            .with_confirmation(approval)
            .set_workspace_document_at(req, 700)
            .unwrap();
        self.verify();
        self.store.workspace_document("W", None).unwrap()
    }
    fn verify(&self) {
        let report = self.store.verify().unwrap();
        assert!(report.ok, "{report:?}");
        self.store.rebuild().unwrap();
        assert!(self.store.verify().unwrap().ok);
    }
}
fn doc(expected_revision: i64) -> SetWorkspaceDocumentRequest {
    SetWorkspaceDocumentRequest {
        workspace_id: "W".into(),
        expected_revision,
        charter: "Charter\n".into(),
        document: format!("Document {expected_revision}\n"),
        request_key: format!("document-{expected_revision}"),
    }
}
fn code(error: RegistryError) -> String {
    match error {
        RegistryError::Domain { code, .. } => code,
        _ => panic!("{error}"),
    }
}
#[test]
fn memory_document_revisions_hash_conflicts_limits_and_replay() {
    let f = Fixture::new();
    assert_eq!(
        code(f.store.workspace_document("W", None).unwrap_err()),
        "revision_unknown"
    );
    let first = f.set(doc(0));
    let second = f.set(doc(1));
    assert_eq!(first.revision, 1);
    assert_eq!(second.revision, 2);
    assert_ne!(first.sha256, second.sha256);
    assert_eq!(f.store.workspace_document("W", Some(1)).unwrap(), first);
    assert_eq!(f.store.workspace_document("W", None).unwrap(), second);
    assert_eq!(
        code(f.store.workspace_document("W", Some(3)).unwrap_err()),
        "revision_unknown"
    );
    let stale = SetWorkspaceDocumentRequest {
        request_key: "stale".into(),
        ..doc(0)
    };
    let error = f
        .store
        .precheck_memory_confirmation("workspace.set_document", json!(stale), 700)
        .err()
        .unwrap();
    assert!(
        matches!(error,RegistryError::Domain{ref code,ref detail,..} if code=="revision_conflict"&&detail.as_ref().unwrap()["current_revision"]==2)
    );
    for (field, limit) in [("charter", CHARTER_LIMIT), ("document", DOCUMENT_LIMIT)] {
        let mut req = json!(doc(2));
        req[field] = json!("é".repeat(limit / 2 + 1));
        let error = f
            .store
            .precheck_memory_confirmation("workspace.set_document", req, 700)
            .err()
            .unwrap();
        assert!(
            matches!(error,RegistryError::Domain{ref code,ref detail,..} if code=="document_too_large"&&detail.as_ref().unwrap()["field"]==field&&detail.as_ref().unwrap()["limit"]==limit)
        );
        let mut req = json!(doc(2));
        req[field] = json!("é\0");
        let error = f
            .store
            .precheck_memory_confirmation("workspace.set_document", req, 700)
            .err()
            .unwrap();
        assert!(
            matches!(error,RegistryError::Domain{ref code,ref detail,..} if code=="document_invalid_text"&&detail.as_ref().unwrap()["field"]==field&&detail.as_ref().unwrap()["byte_offset"]==2)
        );
    }
    let req = SetWorkspaceDocumentRequest {
        charter: "\t".repeat(CHARTER_LIMIT),
        document: "\"".repeat(DOCUMENT_LIMIT),
        ..doc(2)
    };
    let third = f.set(req);
    assert!(
        crate::shared_entry::canonical_bytes(&document_entry(&third)).len() < SHARED_ENTRY_LIMIT
    );
    assert_eq!(
        document_sha256("ab", "c"),
        "0b38e4d533a7177179b91823642e24568bcc8148dfb704e0760812c0edbc9864"
    );
    assert_ne!(document_sha256("a", "bc"), document_sha256("ab", "c"));
    let mut huge = doc(3);
    huge.workspace_id = "w".repeat(SHARED_ENTRY_LIMIT);
    f.store
        .apply_entry("fixture", "{}", "test", None, |tx| {
            tx.execute(
                "INSERT INTO workspace(workspace_id,name) VALUES(?1,'Short')",
                [&huge.workspace_id],
            )
            .map(|_| ())
        })
        .unwrap();
    assert_eq!(
        code(
            f.store
                .precheck_memory_confirmation(
                    "workspace.set_document",
                    json!(SetWorkspaceDocumentRequest {
                        expected_revision: 0,
                        ..huge
                    }),
                    700
                )
                .err()
                .unwrap()
        ),
        "document_too_large"
    );
}
#[test]
fn memory_document_direct_only_and_approval_bound_to_text() {
    let f = Fixture::new();
    for principal in [
        "reserved:prefrontal-core",
        "agent",
        "entorhinal",
        "reserved:other",
    ] {
        assert_eq!(
            code(
                f.store
                    .with_principal(principal)
                    .set_workspace_document_at(doc(0), 700)
                    .unwrap_err()
            ),
            "direct_caller_required"
        );
    }
    assert_eq!(
        code(
            f.store
                .with_principal("direct")
                .set_workspace_document_at(doc(0), 700)
                .unwrap_err()
        ),
        "operator_approval_stale"
    );
    let approval = f.approval("workspace.set_document", json!(doc(0)));
    let mut changed = doc(0);
    changed.document.push('x');
    assert_eq!(
        code(
            f.store
                .with_principal("direct")
                .with_confirmation(approval)
                .set_workspace_document_at(changed, 700)
                .unwrap_err()
        ),
        "operator_approval_stale"
    );
    assert_eq!(f.store.generation().unwrap(), 1);
}
#[test]
fn memory_domain_canonical_shared_replay_and_direct_confirmation() {
    let f = Fixture::new();
    for value in [
        "no-scope",
        "@Scope/Name",
        "@scope/a_b",
        "@scope/a/extra",
        "@/name",
        "@scope/-name",
    ] {
        assert_eq!(
            code(validate_domain(Some(value)).unwrap_err()),
            "invalid_domain"
        );
        assert_eq!(
            code(
                f.store
                    .register(RegisterRequest {
                        domain: Some(value.into()),
                        ..Default::default()
                    })
                    .unwrap_err()
            ),
            "invalid_domain"
        );
    }
    for (n, domain) in [Some("@third-party/project"), None].into_iter().enumerate() {
        let req = SetProjectDomainRequest {
            project_id: "P".into(),
            domain: domain.map(str::to_owned),
            request_key: format!("domain-{n}"),
        };
        assert_eq!(
            code(
                f.store
                    .with_principal("direct")
                    .set_project_domain_at(req.clone(), 700)
                    .unwrap_err()
            ),
            "operator_approval_stale"
        );
        let approval = f.approval("project.set_domain", json!(req));
        f.store
            .with_principal("direct")
            .with_confirmation(approval)
            .set_project_domain_at(req, 700)
            .unwrap();
        assert_eq!(
            f.store.context_project(&f.folder(), "fixed").unwrap()["domain"],
            json!(domain)
        );
        f.verify();
    }
    let register = RegisterRequest {
        project_id: Some("P".into()),
        name: "Project".into(),
        domain: Some("@scope/new".into()),
        ..Default::default()
    };
    assert_eq!(
        code(
            f.store
                .with_principal("direct")
                .with_confirmation(PlacementConfirmation::NoPrompt)
                .register(register.clone())
                .unwrap_err()
        ),
        "operator_approval_stale"
    );
    assert_eq!(
        code(
            f.store
                .with_principal("direct")
                .register(register.clone())
                .unwrap_err()
        ),
        "operator_approval_stale"
    );
    let approval = f.approval("register", json!(register));
    f.store
        .with_principal("direct")
        .with_confirmation(approval)
        .register(register.clone())
        .unwrap();
    f.verify();
    assert!(matches!(
        f.store
            .precheck_agent_confirmation("register", json!(register), 700)
            .unwrap(),
        AgentPrecheck::NoPrompt
    ));
    f.store
        .with_principal("direct")
        .with_confirmation(PlacementConfirmation::NoPrompt)
        .register(RegisterRequest {
            project_id: Some("P".into()),
            name: "Project".into(),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        f.store.context_project(&f.folder(), "fixed").unwrap()["domain"],
        "@scope/new"
    );
    let mut req = SetProjectDomainRequest {
        project_id: "P".into(),
        domain: None,
        request_key: "long".into(),
    };
    f.store
        .register(RegisterRequest {
            project_id: Some("P".into()),
            name: "x".repeat(200),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        code(
            f.store
                .precheck_memory_confirmation("project.set_domain", json!(req), 700)
                .err()
                .unwrap()
        ),
        "operator_summary_too_long"
    );
    req.project_id = "missing".into();
    assert_eq!(
        code(
            f.store
                .with_principal("reserved:prefrontal-core")
                .set_project_domain_at(req, 700)
                .unwrap_err()
        ),
        "project_unknown"
    );
}

#[test]
fn memory_context_project_is_one_read_transaction() {
    let f = Fixture::new();
    f.set(doc(0));
    let before = f.store.generation().unwrap();
    let writer = f.store.clone();
    crate::context::CHECKPOINT.with(|c| {
        *c.borrow_mut() = Some(Box::new(move || {
            writer
                .register(RegisterRequest {
                    project_id: Some("P".into()),
                    name: "Changed".into(),
                    domain: Some("@scope/new".into()),
                    ..Default::default()
                })
                .unwrap();
        }))
    });
    let snapshot = f.store.context_project(&f.folder(), "fixed").unwrap();
    assert_eq!(snapshot["domain"], Value::Null);
    assert_eq!(snapshot["stamp"]["generation"], before);
    assert_eq!(snapshot["stamp"]["logPosition"], 0);
    assert_eq!(snapshot["members_digest"], sha256_hex("P"));
    assert_eq!(snapshot["document"]["revision"], 1);
    assert_eq!(
        f.store.context_project(&f.folder(), "fixed").unwrap()["domain"],
        "@scope/new"
    );
}

#[test]
fn memory_context_session_is_one_read_transaction() {
    let f = Fixture::new();
    let id = "agent_0000000000000001";
    // Insert a fixture agent without importing the executive's identity database:
    // this test isolates concurrent reads from that separate ownership transfer.
    // Context reads, like agent.where, work before agents can be mutated.
    f.store.apply_entry("fixture","{}","test",None,|tx|tx.execute("INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms) VALUES(?1,'Ada','','assistant',1,1)",[id]).map(|_|())).unwrap();
    let before = f.store.generation().unwrap();
    let writer = f.store.clone();
    crate::context::CHECKPOINT.with(|c|*c.borrow_mut()=Some(Box::new(move||{
  writer.apply_entry("fixture","{}","test",None,|tx|tx.execute("UPDATE agent SET terminal_reason='retired',terminal_at_ms=2 WHERE agent_id=?1",[id]).map(|_|())).unwrap();
 })));
    let snapshot = f.store.context_session(id, "fixed").unwrap();
    assert_eq!(snapshot["agent"]["status"], "live");
    assert_eq!(snapshot["where"]["status"], "unclaimed");
    assert_eq!(snapshot["stamp"]["generation"], before);
    assert_eq!(
        code(f.store.context_session(id, "fixed").unwrap_err()),
        "gone"
    );
    assert_eq!(
        code(f.store.context_session("missing", "fixed").unwrap_err()),
        "unknown_agent"
    );
}

#[test]
fn memory_context_session_includes_only_memory_persona_pins_and_live_generations() {
    let f = Fixture::new();
    let id = "agent_0000000000000001";
    f.store.apply_entry("fixture","{}","test",None,|tx|{
  tx.execute("INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms) VALUES(?1,'Ada','','assistant',1,1)",[id])?;
  for (n,state) in ["active","ready","degraded","superseded","installed"].into_iter().enumerate(){
   let plan=format!("plan-{n}");let generation=format!("generation-{n}");
   tx.execute("INSERT INTO catalog_plan(plan_id,agent_id,operation,spec,state,base_pin_digest,created_at_ms) VALUES(?1,?2,'install','{}','ready','',1)",params![plan,id])?;
   tx.execute("INSERT INTO catalog_generation(generation_id,agent_id,plan_id,state,closure,pin_digest,created_at_ms) VALUES(?1,?2,?3,?4,'[]','',1)",params![generation,id,plan,state])?;
  }Ok(())
 }).unwrap();
    let pins = [
        ("@example/memory", entorhinal_catalog::Kind::MemoryProfile),
        ("@example/persona", entorhinal_catalog::Kind::Persona),
        ("@example/skill", entorhinal_catalog::Kind::Skill),
    ]
    .into_iter()
    .map(|(name, kind)| crate::catalog_agent::CatalogPin {
        name: name.into(),
        kind,
        version: "1.0.0".into(),
        manifest_sha256: "a".repeat(64),
        archive_sha256: "b".repeat(64),
        publisher_fingerprint: "c".repeat(64),
        root: true,
        range: Some("*".into()),
    })
    .collect();
    f.store
        .with_principal("reserved:prefrontal-core")
        .catalog_set_pins(
            crate::catalog_agent::PinWriteRequest {
                agent_id: id.into(),
                pins,
                request_key: Some("pins".into()),
            },
            700,
        )
        .unwrap();
    let reply = f.store.context_session(id, "fixed").unwrap();
    assert_eq!(
        reply["catalog_generations"],
        json!([{"generation_id":"generation-0","state":"active"},{"generation_id":"generation-1","state":"ready"},{"generation_id":"generation-2","state":"degraded"}])
    );
    assert_eq!(reply["pins"].as_array().unwrap().len(), 2);
    assert_eq!(reply["pins"][0]["name"], "@example/memory");
    assert_eq!(reply["pins"][1]["kind"], "persona");
    let agent: Value = serde_json::from_slice(
        &f.store
            .agent_read("agent.resolve", json!({"agent_id":id}))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(reply["agent"], agent["result"]);
    assert_eq!(reply["where"], json!(f.store.agent_where(id).unwrap()));
}

#[test]
fn memory_registration_combines_domain_and_name_carry_in_one_approval() {
    let f = Fixture::new();
    let id = "agent_0000000000000001";
    f.store.apply_entry("agent.cutover","{}","test",None,|tx|{
  tx.execute("DELETE FROM project_workspace WHERE project_id='P'",[])?;
  tx.execute("DELETE FROM workspace_member WHERE project_id='P'",[])?;
  tx.execute("INSERT INTO agent(agent_id,name,tag,role,project_id,created_at_ms,updated_at_ms) VALUES(?1,'Ada','','head','P',1,1)",[id])?;
  tx.execute("INSERT INTO agent_name_claim(agent_id,namespace_kind,namespace_key,normalized_name,display_name,claimed_at_ms) VALUES(?1,'workspace','P','ada','Ada',1)",[id])?;
  Ok(())
 }).unwrap();
    let request = RegisterRequest {
        project_id: Some("P".into()),
        name: "Project".into(),
        workspace_id: Some("W".into()),
        domain: Some("@example/software".into()),
        ..Default::default()
    };
    let AgentPrecheck::Ready(text) = f
        .store
        .precheck_agent_confirmation("register", json!(request), 700)
        .unwrap()
    else {
        panic!("approval required")
    };
    assert!(text.contains("Ada"));
    assert!(text.contains("set domain of project Project to @example/software"));
    assert!(text.chars().count() <= 200);
    assert_eq!(
        code(
            f.store
                .with_principal("direct")
                .register(request.clone())
                .unwrap_err()
        ),
        "operator_approval_stale"
    );
    f.store
        .with_principal("direct")
        .with_confirmation(PlacementConfirmation::Approved(OperatorApproval::new(text)))
        .register_at(request, 700)
        .unwrap();
    assert_eq!(
        f.store.context_project(&f.folder(), "fixed").unwrap()["domain"],
        "@example/software"
    );
    assert!(f
        .store
        .agent_claims(id)
        .unwrap()
        .iter()
        .any(|c| c.namespace_key == "W" && c.released_at_ms.is_none()));
}
