use super::*;
use crate::{
    agent_ops::{OperatorConfirmError, OperatorConfirmer},
    tests::{flow_stamp, scratch_descriptor},
    RouteAdmission,
};
use async_trait::async_trait;
use base64::{
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
    Engine,
};
use cortexkit_claustrum_client::{
    CredentialErrorClass, CredentialGetTarget, CredentialResolverFuture,
};
use ed25519_dalek::{Signer, SigningKey};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};
use subc_protocol::Principal;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

const ROUTE: RouteKey = (1801, 1);
const KEY_ID: &str = "e59aba440dbdcfea";
const REGISTRY: &str = "https://registry.example";
fn vectors() -> Value {
    serde_json::from_str(include_str!(
        "../../../entorhinal-catalog/tests/vectors/ck-catalog-v1/record.json"
    ))
    .unwrap()
}
struct FakeVault {
    key: SigningKey,
    caller: String,
    nonce: Option<String>,
    attempts: AtomicUsize,
    audited: Mutex<Vec<Vec<u8>>>,
    errors: AtomicUsize,
    bad_signature: bool,
    bad_id: bool,
}
impl FakeVault {
    fn new() -> Self {
        let seed =
            catalog::decode_hex::<32>(vectors()["keys"]["trusted"]["seed_hex"].as_str().unwrap())
                .unwrap();
        Self {
            key: SigningKey::from_bytes(&seed),
            caller: "reserved:entorhinal".into(),
            nonce: Some("launch-secret".into()),
            attempts: AtomicUsize::new(0),
            audited: Mutex::new(vec![]),
            errors: AtomicUsize::new(0),
            bad_signature: false,
            bad_id: false,
        }
    }
    fn admit(&self, id: &str) -> Result<(), CredentialResolverError> {
        if self.caller != "reserved:entorhinal" || self.nonce.is_none() || id != CREDENTIAL {
            return Err(CredentialResolverError::new(
                CredentialErrorClass::CredentialAbsent,
                "not_found",
            ));
        }
        Ok(())
    }
    fn resolver(self: &Arc<Self>) -> Arc<dyn CredentialResolver> {
        Arc::new(ClaustrumCredentialResolver::with_target(self.clone(), 2))
    }
}
impl CredentialGetTarget for FakeVault {
    fn credential_get<'a>(
        &'a self,
        _: &'a str,
        _: Option<u64>,
        _: bool,
    ) -> CredentialResolverFuture<'a, Result<Value, CredentialResolverError>> {
        Box::pin(async { panic!("publisher must never request private key material") })
    }
    fn credential_public_key<'a>(
        &'a self,
        id: &'a str,
    ) -> CredentialResolverFuture<'a, Result<Value, CredentialResolverError>> {
        Box::pin(async move {
            self.admit(id)?;
            Ok(
                json!({"algorithm":"ed25519", "key_id":KEY_ID, "public_key_hex":catalog::hex(self.key.verifying_key().as_bytes())}),
            )
        })
    }
    fn credential_sign<'a>(
        &'a self,
        id: &'a str,
        payload: &'a [u8],
    ) -> CredentialResolverFuture<'a, Result<Value, CredentialResolverError>> {
        Box::pin(async move {
            self.admit(id)?;
            if payload.len() > 1_048_576 {
                return Err(CredentialResolverError::new(
                    CredentialErrorClass::Permanent,
                    "payload exceeds 1 MiB",
                ));
            }
            self.attempts.fetch_add(1, Ordering::SeqCst);
            if self
                .errors
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(CredentialResolverError::new(
                    CredentialErrorClass::Transient,
                    "store_error",
                ));
            }
            self.audited.lock().unwrap().push(payload.to_vec());
            let signature = if self.bad_signature {
                [0; 64]
            } else {
                self.key.sign(payload).to_bytes()
            };
            Ok(
                json!({"signature_b64": STANDARD.encode(signature), "key_id": if self.bad_id { "fedcba9876543210" } else { KEY_ID }}),
            )
        })
    }
}
struct Prompt {
    answer: Result<(), OperatorConfirmError>,
    calls: Mutex<Vec<String>>,
}
#[async_trait]
impl OperatorConfirmer for Prompt {
    async fn confirm_operator(
        &self,
        summary: &str,
        _: RouteKey,
    ) -> Result<(), OperatorConfirmError> {
        self.calls.lock().unwrap().push(summary.into());
        self.answer
    }
}
struct Fixture {
    root: PathBuf,
    package: PathBuf,
    handler: ProjectsHandler,
    vault: Arc<FakeVault>,
    prompt: Arc<Prompt>,
}
impl Fixture {
    fn new(vault: FakeVault, answer: Result<(), OperatorConfirmError>) -> Self {
        let (root, descriptor) = scratch_descriptor("catalog-publish");
        let store = entorhinal_core::RegistryStore::open(&descriptor).unwrap();
        store
            .catalog_registry_add(
                REGISTRY,
                &["@example".into()],
                false,
                "status-1",
                &catalog::hex(SigningKey::from_bytes(&[7; 32]).verifying_key().as_bytes()),
                0,
            )
            .unwrap();
        let package = root.join("package");
        std::fs::create_dir_all(&package).unwrap();
        for member in vectors()["packages"]["skill"]["members"]
            .as_array()
            .unwrap()
        {
            let path = package.join(member["path"].as_str().unwrap());
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, member["content"].as_str().unwrap()).unwrap();
        }
        let package = PathBuf::from(
            entorhinal_core::RegistryStore::canonical_mutation_root(package.to_str().unwrap())
                .unwrap(),
        );
        let mut handler =
            ProjectsHandler::with_runtime("publish-test".into(), || 1_767_225_600_000);
        *handler.store.lock().unwrap() = Some(store);
        handler.route_admissions().insert(
            ROUTE,
            RouteAdmission::from_bind(Some(Principal::Direct), None),
        );
        let vault = Arc::new(vault);
        handler.catalog_publish.vault = Some(vault.resolver());
        let prompt = Arc::new(Prompt {
            answer,
            calls: Mutex::new(vec![]),
        });
        handler.operator_confirmer = prompt.clone();
        Self {
            root,
            package,
            handler,
            vault,
            prompt,
        }
    }
    fn params(&self) -> Value {
        json!({"package_dir": self.package, "registry":REGISTRY, "scope":"@example"})
    }
    async fn call(&self) -> Result<Value, HandlerError> {
        let bytes = self
            .handler
            .catalog_publish(self.params(), ROUTE, Instant::now())
            .await?;
        Ok(serde_json::from_slice::<Value>(&bytes).unwrap()["result"].clone())
    }
    fn no_sign(&self) {
        assert_eq!(self.vault.attempts.load(Ordering::SeqCst), 0);
    }
    async fn registry(&mut self, responses: Vec<Response>) -> Registry {
        let registry = Registry::new(responses).await;
        self.handler.catalog_publish.client =
            Some(Arc::new(crate::catalog_network::RegistryClient::loopback(
                &registry.url,
                std::slice::from_ref(&registry.url),
                crate::catalog_network::Limits::MODULE,
            )));
        registry
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.handler.store.lock().unwrap().take();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
#[derive(Clone)]
enum Response {
    Success,
    Unavailable,
    Lost,
    VersionTaken,
    Malformed,
    BadStatus,
    WrongReply,
    Redirect(String),
}
#[derive(Clone, Debug, PartialEq)]
struct Upload {
    path: String,
    record: String,
    signature: String,
    body: Vec<u8>,
    content_type: String,
}
struct Registry {
    url: String,
    uploads: Arc<Mutex<Vec<Upload>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Registry {
    async fn new(responses: Vec<Response>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let uploads = Arc::new(Mutex::new(vec![]));
        let seen = uploads.clone();
        let task = tokio::spawn(async move {
            for response in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let upload = read_upload(&mut socket).await;
                seen.lock().unwrap().push(upload.clone());
                let (status, headers, body) = match response {
                    Response::Lost => continue,
                    Response::Unavailable => (503, "Retry-After: 0\r\n".into(), json!({"error":{"code":"publish_unavailable", "message":"busy", "detail":{}}}).to_string()),
                    Response::VersionTaken => (409, "".into(), json!({"error":{"code":"version_exists","message":"taken","detail":{}}}).to_string()),
                    Response::Malformed => (201, "".into(), "{}".into()),
                    Response::BadStatus => {
                        let mut reply = success(&upload); reply["status"]["signature"] = json!("00".repeat(64));
                        (201, "".into(), reply.to_string())
                    },
                    Response::WrongReply => {
                        let mut reply = success(&upload); reply["signature"] = json!("00".repeat(64));
                        (201, "".into(), reply.to_string())
                    },
                    Response::Redirect(location) => (307, format!("Location: {location}\r\n"), "".into()),
                    Response::Success => (201, "".into(), success(&upload).to_string()),
                };
                socket.write_all(format!("HTTP/1.1 {status} Reply\r\nContent-Length: {}\r\nConnection: close\r\n{headers}\r\n{body}", body.len()).as_bytes()).await.unwrap();
            }
        });
        Self { url, uploads, task }
    }
}
impl Drop for Registry {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn read_upload(socket: &mut TcpStream) -> Upload {
    let mut bytes = vec![];
    let end = loop {
        let mut byte = [0];
        socket.read_exact(&mut byte).await.unwrap();
        bytes.push(byte[0]);
        if bytes.ends_with(b"\r\n\r\n") {
            break bytes.len();
        }
    };
    let header = String::from_utf8(bytes[..end].to_vec()).unwrap();
    let field = |name: &str| {
        header
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.trim().to_string())
            .unwrap()
    };
    let len: usize = field("content-length").parse().unwrap();
    let mut body = vec![0; len];
    socket.read_exact(&mut body).await.unwrap();
    Upload {
        path: header.lines().next().unwrap().into(),
        record: field("ck-record"),
        signature: field("ck-signature"),
        content_type: field("content-type"),
        body,
    }
}
fn success(upload: &Upload) -> Value {
    let record: Value =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(&upload.record).unwrap()).unwrap();
    let mut reply = json!({"record":record, "signature":upload.signature,"status":{"record":{
        "schema":"ck-status/v1", "registry_key_id":"status-1", "name":record["name"], "version":record["version"],
        "archive_sha256":record["archive_sha256"],"publisher_key":catalog::key_fingerprint(record["public_key"].as_str().unwrap()).unwrap(),"sequence":1,"state":"available",
        "reason":null,"compromise_from":null,"published_at":"2026-01-01T00:00:00Z","changed_at":"2026-01-01T00:00:00Z"
    },"signature":"ab".repeat(64)}});
    let status = catalog::jcs::to_string(&reply["status"]["record"]).unwrap();
    reply["status"]["signature"] = json!(catalog::hex(
        &SigningKey::from_bytes(&[7; 32])
            .sign(&catalog::signing_input("ck-status/v1", &status))
            .to_bytes()
    ));
    reply
}

#[tokio::test]
async fn publish_happy_path() {
    let mut f = Fixture::new(FakeVault::new(), Ok(()));
    let registry = f.registry(vec![Response::Success]).await;
    let reply = f.call().await.unwrap();
    assert_eq!(reply["record"]["name"], "@example/hello-skill");
    assert_eq!(f.vault.audited.lock().unwrap().len(), 1);
    let uploads = registry.uploads.lock().unwrap();
    assert_eq!(uploads.len(), 1);
    assert_eq!(
        uploads[0].path,
        "PUT /v1/packages/example/hello-skill/1.0.0 HTTP/1.1"
    );
    assert_eq!(uploads[0].content_type, "application/x-tar");
    let v = vectors();
    assert_eq!(
        reply["status"]["record"]["publisher_key"],
        v["keys"]["trusted"]["fingerprint"]
    );
    assert_eq!(
        uploads[0].body,
        STANDARD
            .decode(v["packages"]["skill"]["archive_base64"].as_str().unwrap())
            .unwrap()
    );
    assert_eq!(
        URL_SAFE_NO_PAD.decode(&uploads[0].record).unwrap(),
        v["valid"][0]["canonical"].as_str().unwrap().as_bytes()
    );
    assert_eq!(uploads[0].signature, v["valid"][0]["signature"]);
    let summary = &f.prompt.calls.lock().unwrap()[0];
    for part in [
        "@example/hello-skill",
        "1.0.0",
        "skill",
        v["packages"]["skill"]["archive_sha256"].as_str().unwrap(),
    ] {
        assert!(summary.contains(part), "missing {part}");
    }
}
#[tokio::test]
async fn publish_vectors_exact_vault_bytes() {
    let v = vectors();
    let f = Fixture::new(FakeVault::new(), Ok(()));
    for row in v["valid"].as_array().unwrap() {
        let package = row["package"].as_str().unwrap();
        let archive = STANDARD
            .decode(v["packages"][package]["archive_base64"].as_str().unwrap())
            .unwrap();
        let checked = catalog::publish::check(&archive, "@example").unwrap();
        let signed = prepare(
            f.vault.resolver().as_ref(),
            &f.root,
            REGISTRY,
            "@example",
            &checked,
            &f.handler.catalog_sql,
        )
        .await
        .unwrap();
        assert_eq!(serde_json::to_value(&signed.record).unwrap(), row["record"]);
        assert_eq!(signed.canonical, row["canonical"]);
        assert_eq!(signed.signature, row["signature"]);
        assert_eq!(
            catalog::hex(f.vault.audited.lock().unwrap().last().unwrap()),
            row["signing_input_hex"]
        );
    }
}
#[tokio::test]
async fn publish_local_checks_before_card_or_vault() {
    let mut f = Fixture::new(FakeVault::new(), Ok(()));
    let _registry = f.registry(vec![Response::Success]).await;
    std::fs::write(f.package.join("SKILL.md"), "wrong content").unwrap();
    assert_eq!(f.call().await.unwrap_err().code, "files_mismatch");
    assert!(f.prompt.calls.lock().unwrap().is_empty());
    f.no_sign();
}
#[tokio::test]
async fn publish_decline_signs_nothing() {
    let mut f = Fixture::new(FakeVault::new(), Err(OperatorConfirmError::Declined));
    let _registry = f.registry(vec![Response::Success]).await;
    assert_eq!(f.call().await.unwrap_err().code, "operator_declined");
    f.no_sign();
}

#[test]
fn publish_preparation_is_off_runtime_counted_and_bounded() {
    use crate::catalog_sql::tests::{poll, responsive, HeldSql};
    tokio::runtime::Builder::new_current_thread()
        .enable_all().start_paused(true).max_blocking_threads(1).build().unwrap()
        .block_on(async {
            let f = Fixture::new(FakeVault::new(), Err(OperatorConfirmError::Declined));
            let store = f.handler.with_store(|s| Ok(s.clone())).unwrap();
            let held = HeldSql::new(store).await;
            let mut request = Box::pin(f.call());
            assert!(poll(request.as_mut()).is_pending(), "preparation must queue on the blocking pool");
            responsive(&f.handler).await;
            let mut drained = Box::pin(f.handler.catalog_sql.wait());
            assert!(poll(drained.as_mut()).is_pending(), "publish preparation must be counted until the blocking job exits");
            tokio::time::advance(std::time::Duration::from_millis(60_001)).await;
            assert!(matches!(poll(request.as_mut()), std::task::Poll::Ready(Err(e)) if e.code == "catalog_timeout"));
            assert!(poll(drained.as_mut()).is_pending());
            held.finish().await;
            drained.await;
            assert!(f.prompt.calls.lock().unwrap().is_empty());
            f.no_sign();
        });
}
#[tokio::test]
async fn publish_direct_only() {
    let mut f = Fixture::new(FakeVault::new(), Ok(()));
    let _registry = f.registry(vec![Response::Success]).await;
    f.handler.route_admissions().insert(
        ROUTE,
        RouteAdmission::from_bind(
            Some(Principal::Reserved {
                module_id: crate::WRITER_MODULE.into(),
            }),
            None,
        ),
    );
    assert_eq!(f.call().await.unwrap_err().code, "write_not_permitted");
    f.no_sign();
}
#[tokio::test]
async fn publish_flow_refused() {
    let mut f = Fixture::new(FakeVault::new(), Ok(()));
    let _registry = f.registry(vec![Response::Success]).await;
    f.handler.route_admissions().insert(
        ROUTE,
        RouteAdmission::from_bind(Some(Principal::Direct), Some(&flow_stamp(Some("flow_1")))),
    );
    assert_eq!(f.call().await.unwrap_err().code, "flow_scope_not_admitted");
    f.no_sign();
}
#[tokio::test]
async fn publish_relative_and_noncanonical_paths_refused() {
    let mut f = Fixture::new(FakeVault::new(), Ok(()));
    let _registry = f.registry(vec![Response::Success]).await;
    for path in ["relative".to_string(), format!("{}/.", f.package.display())] {
        let mut params = f.params();
        params["package_dir"] = json!(path);
        assert_eq!(
            f.handler
                .catalog_publish(params, ROUTE, Instant::now())
                .await
                .unwrap_err()
                .code,
            "not_canonical"
        );
    }
    f.no_sign();
}
#[tokio::test]
async fn publish_complete_summary_never_truncated() {
    let mut f = Fixture::new(FakeVault::new(), Ok(()));
    let _registry = f.registry(vec![Response::Success]).await;
    let mut manifest: Value =
        serde_json::from_slice(&std::fs::read(f.package.join("ck.json")).unwrap()).unwrap();
    manifest["version"] = json!(format!("1.0.0-{}", "a".repeat(180)));
    std::fs::write(
        f.package.join("ck.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    assert_eq!(f.call().await.unwrap_err().code, "invalid_params");
    f.no_sign();
    assert!(f.prompt.calls.lock().unwrap().is_empty());
}
#[tokio::test]
async fn publish_audit_store_error_retried() {
    let vault = FakeVault::new();
    vault.errors.store(1, Ordering::SeqCst);
    let mut f = Fixture::new(vault, Ok(()));
    let _registry = f.registry(vec![Response::Success]).await;
    f.call().await.unwrap();
    assert_eq!(f.vault.attempts.load(Ordering::SeqCst), 2);
    assert_eq!(f.vault.audited.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn publish_persistent_store_error_never_signed() {
    let vault = FakeVault::new();
    vault.errors.store(10, Ordering::SeqCst);
    let mut f = Fixture::new(vault, Ok(()));
    let registry = f.registry(vec![Response::Success]).await;
    assert_eq!(f.call().await.unwrap_err().code, "store_error");
    assert_eq!(f.vault.attempts.load(Ordering::SeqCst), 3);
    assert!(f.vault.audited.lock().unwrap().is_empty());
    assert!(registry.uploads.lock().unwrap().is_empty());
}
#[tokio::test]
async fn publish_bad_signature_before_upload() {
    let mut vault = FakeVault::new();
    vault.bad_signature = true;
    let mut f = Fixture::new(vault, Ok(()));
    let registry = f.registry(vec![Response::Success]).await;
    assert_eq!(f.call().await.unwrap_err().code, "signature_invalid");
    assert!(registry.uploads.lock().unwrap().is_empty());
}
#[tokio::test]
async fn publish_key_id_mismatch_before_upload() {
    let mut vault = FakeVault::new();
    vault.bad_id = true;
    let mut f = Fixture::new(vault, Ok(()));
    let registry = f.registry(vec![Response::Success]).await;
    assert_eq!(f.call().await.unwrap_err().code, "signature_invalid");
    assert!(registry.uploads.lock().unwrap().is_empty());
}
#[tokio::test]
async fn publish_unavailable_reuses_signed_bytes() {
    let mut f = Fixture::new(FakeVault::new(), Ok(()));
    let registry = f
        .registry(vec![Response::Unavailable, Response::Success])
        .await;
    assert_eq!(f.call().await.unwrap_err().code, "publish_unavailable");
    f.call().await.unwrap();
    let uploads = registry.uploads.lock().unwrap();
    assert_eq!(uploads.len(), 2);
    assert_eq!(uploads[0], uploads[1]);
    assert_eq!(f.vault.attempts.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn publish_lost_reply_durable_across_restart() {
    let mut f = Fixture::new(FakeVault::new(), Ok(()));
    let registry = f.registry(vec![Response::Lost, Response::Success]).await;
    assert!(f.call().await.is_err());
    f.handler.catalog_publish = Publisher {
        vault: Some(f.vault.resolver()),
        client: f.handler.catalog_publish.client.clone(),
        ..Default::default()
    };
    f.call().await.unwrap();
    let uploads = registry.uploads.lock().unwrap();
    assert_eq!(uploads.len(), 2);
    assert_eq!(uploads[0], uploads[1]);
    assert_eq!(f.vault.attempts.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn publish_version_exists_named() {
    let mut f = Fixture::new(FakeVault::new(), Ok(()));
    let _registry = f.registry(vec![Response::VersionTaken]).await;
    assert_eq!(f.call().await.unwrap_err().code, "version_exists");
}
#[tokio::test]
async fn publish_reply_invalid_outcome_unknown() {
    let mut f = Fixture::new(FakeVault::new(), Ok(()));
    let _registry = f.registry(vec![Response::Malformed]).await;
    let error = f.call().await.unwrap_err();
    assert_eq!(error.code, "invalid_reply");
    assert!(error.message.contains("outcome unknown"));
}
#[tokio::test]
async fn publish_cross_origin_redirect_never_sent() {
    let second = Registry::new(vec![Response::Success]).await;
    let location = format!("{}/stolen", second.url);
    let mut f = Fixture::new(FakeVault::new(), Ok(()));
    let first = f.registry(vec![Response::Redirect(location.clone())]).await;
    // Both origins are allowlisted, but the write must still refuse this redirect:
    // a publish may follow a redirect only within the registry's own origin, so
    // the signed package is never resent to a different server.
    f.handler.catalog_publish.client =
        Some(Arc::new(crate::catalog_network::RegistryClient::loopback(
            &first.url,
            &[first.url.clone(), location],
            crate::catalog_network::Limits::MODULE,
        )));
    assert_eq!(f.call().await.unwrap_err().code, "registry_not_allowlisted");
    assert!(second.uploads.lock().unwrap().is_empty());
}
#[tokio::test]
async fn publish_repacked_bytes_bind_cached_record() {
    let mut f = Fixture::new(FakeVault::new(), Ok(()));
    let registry = f
        .registry(vec![Response::Unavailable, Response::Success])
        .await;
    assert_eq!(f.call().await.unwrap_err().code, "publish_unavailable");
    let mut manifest: Value =
        serde_json::from_slice(&std::fs::read(f.package.join("ck.json")).unwrap()).unwrap();
    manifest["summary"] = json!("different package bytes");
    std::fs::write(
        f.package.join("ck.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    f.call().await.unwrap();
    let uploads = registry.uploads.lock().unwrap();
    assert_ne!(uploads[0].body, uploads[1].body);
    assert_ne!(uploads[0].signature, uploads[1].signature);
    assert_eq!(f.vault.attempts.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn publish_cached_tamper_refused() {
    let mut f = Fixture::new(FakeVault::new(), Ok(()));
    let registry = f
        .registry(vec![Response::Unavailable, Response::Success])
        .await;
    assert_eq!(f.call().await.unwrap_err().code, "publish_unavailable");
    let folder = f
        .handler
        .with_store(|s| Ok(s.store_folder().to_path_buf()))
        .unwrap();
    let path = storage::path(
        &folder,
        REGISTRY,
        "@example",
        &catalog::sha256_hex(pack::pack(&f.package).unwrap()),
    );
    let mut signed = storage::load(&path).unwrap().unwrap();
    signed.record.archive_sha256 = "00".repeat(32);
    storage::save(&path, &signed).unwrap();
    assert_eq!(f.call().await.unwrap_err().code, "signature_invalid");
    assert_eq!(registry.uploads.lock().unwrap().len(), 1);
    assert_eq!(f.vault.attempts.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn publish_fake_vault_contract() {
    let vault = Arc::new(FakeVault::new());
    assert!(vault.resolver().sign("unknown", b"bytes").await.is_err());
    assert!(vault
        .resolver()
        .sign(CREDENTIAL, &vec![0; 1_048_577])
        .await
        .is_err());
    for (caller, nonce) in [
        ("Direct", Some("secret")),
        ("reserved:other", Some("secret")),
        ("reserved:entorhinal", None),
    ] {
        let mut fake = FakeVault::new();
        fake.caller = caller.into();
        fake.nonce = nonce.map(String::from);
        assert!(Arc::new(fake)
            .resolver()
            .sign(CREDENTIAL, b"bytes")
            .await
            .is_err());
    }
    assert_eq!(vault.attempts.load(Ordering::SeqCst), 0);
}
#[test]
fn publish_cli_and_manifest_contract() {
    use crate::cli::{self, Face, Invocation};
    let args = [
        "publish",
        ".",
        "--registry",
        REGISTRY,
        "--scope",
        "@example",
    ]
    .map(String::from);
    assert!(matches!(
        cli::parse(Face::Catalog, &args),
        Invocation::Command(_)
    ));
    assert!(cli::usage(Face::Catalog)
        .contains("ck catalog publish <package dir> --registry <url> --scope <scope>"));
    let manifest = serde_json::to_value(crate::manifest()).unwrap();
    let ops = manifest["provides"][0]["operations"].as_array().unwrap();
    let op = ops.iter().find(|op| op["name"] == PUBLISH).unwrap();
    assert_eq!(op["kind"], "mutate");
    let description = op["description"].as_str().unwrap();
    assert_eq!(
        ops.iter()
            .filter(|op| op["description"] == description)
            .count(),
        1
    );
}

#[tokio::test]
async fn publish_reply_binds_request() {
    let mut f = Fixture::new(FakeVault::new(), Ok(()));
    let _registry = f.registry(vec![Response::WrongReply]).await;
    assert_eq!(f.call().await.unwrap_err().code, "invalid_reply");
}
#[tokio::test]
async fn publish_local_scope_refused_before_sign() {
    let mut f = Fixture::new(FakeVault::new(), Ok(()));
    let _registry = f.registry(vec![Response::Success]).await;
    let mut params = f.params();
    params["scope"] = json!("@other");
    assert_eq!(
        f.handler
            .catalog_publish(params, ROUTE, Instant::now())
            .await
            .unwrap_err()
            .code,
        "record_mismatch"
    );
    f.no_sign();
    assert!(f.prompt.calls.lock().unwrap().is_empty());
}
#[tokio::test]
async fn publish_extra_file_refused_before_sign() {
    let mut f = Fixture::new(FakeVault::new(), Ok(()));
    let _registry = f.registry(vec![Response::Success]).await;
    std::fs::write(f.package.join("extra.txt"), "unlisted").unwrap();
    assert_eq!(f.call().await.unwrap_err().code, "files_mismatch");
    f.no_sign();
}
#[test]
fn publish_flow_id_checked_after_file_digests() {
    let v = vectors();
    let mut members = v["packages"]["flow"]["members"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| {
            (
                m["path"].as_str().unwrap().to_string(),
                m["content"].as_str().unwrap().as_bytes().to_vec(),
            )
        })
        .collect::<Vec<_>>();
    let flow = members.iter_mut().find(|(p, _)| p == "flow.json").unwrap();
    flow.1 = b"{\"id\":\"wrong-id\",\"version\":3}".to_vec();
    let digest = catalog::sha256_hex(&flow.1);
    let len = flow.1.len();
    let manifest = members.iter_mut().find(|(p, _)| p == "ck.json").unwrap();
    let mut value: Value = serde_json::from_slice(&manifest.1).unwrap();
    value["files"][0]["sha256"] = json!(digest);
    value["files"][0]["bytes"] = json!(len);
    manifest.1 = serde_json::to_vec(&value).unwrap();
    let archive = catalog::archive::write(
        &members
            .iter()
            .map(|(p, b)| catalog::archive::Member::regular(p, b))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    assert_eq!(
        catalog::publish::check(&archive, "@example")
            .err()
            .unwrap()
            .code
            .as_str(),
        "flow_id_mismatch"
    );
}
#[tokio::test]
async fn publish_archive_member_type_refused() {
    let mut f = Fixture::new(FakeVault::new(), Ok(()));
    let _registry = f.registry(vec![Response::Success]).await;
    #[cfg(unix)]
    std::os::unix::fs::symlink("SKILL.md", f.package.join("link")).unwrap();
    #[cfg(windows)]
    std::os::windows::fs::symlink_file("SKILL.md", f.package.join("link")).unwrap();
    assert_eq!(f.call().await.unwrap_err().code, "archive_invalid");
    f.no_sign();
}
#[tokio::test]
async fn publish_same_origin_redirect_outside_base_refused() {
    let first = Registry::new(vec![Response::Redirect("/outside".into())]).await;
    let mut f = Fixture::new(FakeVault::new(), Ok(()));
    let base = format!("{}/base", first.url);
    f.handler.catalog_publish.client =
        Some(Arc::new(crate::catalog_network::RegistryClient::loopback(
            &base,
            std::slice::from_ref(&base),
            crate::catalog_network::Limits::MODULE,
        )));
    assert_eq!(f.call().await.unwrap_err().code, "registry_not_allowlisted");
    assert_eq!(first.uploads.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn publish_reply_cap_enforced() {
    let mut f = Fixture::new(FakeVault::new(), Ok(()));
    let registry = f.registry(vec![Response::Success]).await;
    let limits = crate::catalog_network::Limits {
        json_bytes: 8,
        ..crate::catalog_network::Limits::MODULE
    };
    f.handler.catalog_publish.client =
        Some(Arc::new(crate::catalog_network::RegistryClient::loopback(
            &registry.url,
            std::slice::from_ref(&registry.url),
            limits,
        )));
    assert_eq!(f.call().await.unwrap_err().code, "json_too_large");
}

#[tokio::test]
async fn publish_reply_status_authenticated() {
    let mut f = Fixture::new(FakeVault::new(), Ok(()));
    let _registry = f.registry(vec![Response::BadStatus]).await;
    assert_eq!(f.call().await.unwrap_err().code, "signature_invalid");
}

#[tokio::test]
async fn publish_cached_record_cap_refused() {
    let mut f = Fixture::new(FakeVault::new(), Ok(()));
    let _registry = f
        .registry(vec![Response::Unavailable, Response::Success])
        .await;
    assert_eq!(f.call().await.unwrap_err().code, "publish_unavailable");
    let folder = f
        .handler
        .with_store(|s| Ok(s.store_folder().to_path_buf()))
        .unwrap();
    let path = storage::path(
        &folder,
        REGISTRY,
        "@example",
        &catalog::sha256_hex(pack::pack(&f.package).unwrap()),
    );
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.resize(1_048_577, b' ');
    std::fs::write(&path, bytes).unwrap();
    assert_eq!(f.call().await.unwrap_err().code, "record_invalid");
}
