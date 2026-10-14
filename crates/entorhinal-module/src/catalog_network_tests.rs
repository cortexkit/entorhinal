use super::*;
use crate::fake_registry::{FakeRegistry, Reply};
use serde_json::Value;
use std::future::Future;
struct FixedClock(tokio::task::JoinHandle<()>);
impl Drop for FixedClock {
    fn drop(&mut self) {
        self.0.abort();
    }
}
fn hold_paused_clock() -> FixedClock {
    // OS socket readiness is not a timer. Prevent Tokio from auto-advancing a
    // paused clock while loopback I/O is pending; deadline tests advance it explicitly.
    FixedClock(tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    }))
}
use base64::Engine;
fn client(registry: &FakeRegistry) -> RegistryClient {
    RegistryClient::loopback(
        &registry.url,
        std::slice::from_ref(&registry.url),
        Limits::MODULE,
    )
}
#[test]
fn production_https_bundled_roots_and_limits() {
    for url in [
        "http://a.test",
        "https://user@a.test",
        "https://a.test?query",
        "https://a.test#fragment",
    ] {
        assert!(registry_url(url).is_err());
    }
    assert!(
        RegistryClient::new("https://a.test", &["https://a.test".into()], Limits::MODULE).is_ok()
    );
    assert!(
        RegistryClient::new("https://a.test", &["https://b.test".into()], Limits::MODULE).is_err()
    );
    let lock = include_str!("../../../Cargo.lock");
    for forbidden in ["openssl-sys", "native-tls", "security-framework"] {
        assert!(
            !lock.contains(&format!("name = \"{forbidden}\"")),
            "{forbidden}"
        );
    }
    assert_eq!(Limits::MODULE.connect, Duration::from_secs(10));
    assert_eq!(Limits::MODULE.archive_timeout, Duration::from_secs(120));
    assert_eq!(Limits::MODULE.json_timeout, Duration::from_secs(30));
    assert_eq!(
        (
            Limits::MODULE.archive_bytes,
            Limits::MODULE.manifest_bytes,
            Limits::MODULE.json_bytes
        ),
        (10_485_760, 65_536, 8_388_608)
    );
}
#[tokio::test(start_paused = true)]
async fn scoped_fetch_never_contacts_other_registry() {
    let _clock = hold_paused_clock();
    let a = FakeRegistry::start().await;
    let b = FakeRegistry::start().await;
    a.serve_vectors();
    b.serve_vectors();
    let (root, descriptor) = crate::tests::scratch_descriptor("catalog-network-scopes");
    let store = entorhinal_core::RegistryStore::open(&descriptor).unwrap();
    store
        .catalog_registry_add(&a.url, &["@a".into()], false, "k", "pk", 0)
        .unwrap();
    store
        .catalog_registry_add(&b.url, &["@b".into()], false, "k", "pk", 0)
        .unwrap();
    a.serve(
        "/v1/scopes/a/packages",
        Reply::bytes(br#"{"scope":"a","packages":[]}"#.to_vec()),
    );
    let c = RegistryClient::for_scope_loopback(&store, "@a", Limits::MODULE);
    let reply = c.fetch_json("v1/scopes/a/packages").await.unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&reply).unwrap()["scope"],
        "a"
    );
    assert_eq!(*a.calls.lock().unwrap(), vec!["/v1/scopes/a/packages"]);
    assert!(b.calls.lock().unwrap().is_empty());
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test(start_paused = true)]
async fn redirect_rechecks_host_port_and_path() {
    let _clock = hold_paused_clock();
    let a = FakeRegistry::start().await;
    let b = FakeRegistry::start().await;
    a.serve("/ok", Reply::bytes(b"ok".to_vec()));
    a.serve("/same", Reply::redirect("/ok"));
    assert_eq!(client(&a).fetch_json("same").await.unwrap(), b"ok");
    a.serve("/off", Reply::redirect(&format!("{}/ok", b.url)));
    assert_eq!(
        client(&a).fetch_json("off").await.unwrap_err().code(),
        "registry_not_allowlisted"
    );
    assert!(b.calls.lock().unwrap().is_empty());
    for hop in 0..=10 {
        let next = if hop == 10 {
            format!("{}/ok", b.url)
        } else {
            format!("/late/{}", hop + 1)
        };
        a.serve(&format!("/late/{hop}"), Reply::redirect(&next));
    }
    assert_eq!(
        client(&a).fetch_json("late/0").await.unwrap_err().code(),
        "registry_not_allowlisted"
    );
    assert!(b.calls.lock().unwrap().is_empty());
    a.serve("/prefix/off", Reply::redirect("/prefix-evil/ok"));
    let prefix = format!("{}/prefix", a.url);
    let c = RegistryClient::loopback(&prefix, std::slice::from_ref(&prefix), Limits::MODULE);
    assert_eq!(
        c.fetch_json("off").await.unwrap_err().code(),
        "registry_not_allowlisted"
    );
    assert!(!a
        .calls
        .lock()
        .unwrap()
        .contains(&"/prefix-evil/ok".to_string()));
}
#[tokio::test(start_paused = true)]
async fn archive_manifest_json_caps_and_chunked_cap() {
    let _clock = hold_paused_clock();
    let a = FakeRegistry::start().await;
    let c = client(&a);
    a.serve_vectors();
    let vectors: Value = serde_json::from_str(include_str!(
        "../../entorhinal-catalog/tests/vectors/ck-catalog-v1/archive.json"
    ))
    .unwrap();
    let small = RegistryClient::loopback(
        &a.url,
        std::slice::from_ref(&a.url),
        Limits {
            archive_bytes: 4607,
            ..Limits::MODULE
        },
    );
    assert_eq!(
        small
            .fetch_archive(vectors["valid"]["archive_sha256"].as_str().unwrap())
            .await
            .unwrap_err()
            .code(),
        "archive_too_large"
    );
    let digest = "ab".repeat(32);
    a.serve(
        &format!("/v1/archives/{digest}"),
        Reply::bytes(vec![0; 10_485_761]),
    );
    assert_eq!(
        c.fetch_archive(&digest).await.unwrap_err().code(),
        "archive_too_large"
    );
    a.serve("/manifest", Reply::bytes(vec![0; 65_537]));
    assert_eq!(
        c.fetch_manifest("manifest").await.unwrap_err().code(),
        "manifest_too_large"
    );
    a.serve("/json", Reply::bytes(vec![0; 8_388_609]));
    assert_eq!(
        c.fetch_json("json").await.unwrap_err().code(),
        "json_too_large"
    );
    a.serve(
        "/stream",
        Reply {
            chunked: true,
            ..Reply::bytes(vec![0; 65_537])
        },
    );
    assert_eq!(
        c.fetch_manifest("stream").await.unwrap_err().code(),
        "manifest_too_large"
    );
    a.serve("/exact", Reply::bytes(vec![0; 65_536]));
    assert_eq!(c.fetch_manifest("exact").await.unwrap().len(), 65_536);
}
#[tokio::test(start_paused = true)]
async fn registry_total_timeout_uses_tokio_clock() {
    let _clock = hold_paused_clock();
    let a = FakeRegistry::start().await;
    a.serve(
        "/blocked",
        Reply {
            release: Some(std::sync::Arc::new(tokio::sync::Notify::new())),
            ..Reply::bytes(vec![])
        },
    );
    let c = client(&a);
    let mut future = Box::pin(c.fetch_json("blocked"));
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(future.as_mut().poll(cx).is_pending()))
            .await
    );
    tokio::time::advance(Duration::from_secs(30)).await;
    let outcome = std::future::poll_fn(|cx| std::task::Poll::Ready(future.as_mut().poll(cx))).await;
    let std::task::Poll::Ready(outcome) = outcome else {
        panic!("total timeout must settle at the deadline");
    };
    assert_eq!(outcome.unwrap_err().code(), "registry_timeout");
}
#[tokio::test(start_paused = true)]
async fn frozen_archives_all_failure_vectors_through_transport() {
    let _clock = hold_paused_clock();
    let a = FakeRegistry::start().await;
    a.serve_vectors();
    let c = client(&a);
    let v: Value = serde_json::from_str(include_str!(
        "../../entorhinal-catalog/tests/vectors/ck-catalog-v1/archive.json"
    ))
    .unwrap();
    let base = base64::engine::general_purpose::STANDARD
        .decode(v["valid"]["archive_base64"].as_str().unwrap())
        .unwrap();
    assert_eq!(
        c.fetch_archive(v["valid"]["archive_sha256"].as_str().unwrap())
            .await
            .unwrap(),
        base
    );
    for case in v["cases"].as_array().unwrap() {
        let bytes = if let Some(encoded) = case["archive_base64"].as_str() {
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .unwrap()
        } else {
            let count = if case["name"] == "members_at_limit" {
                997
            } else {
                998
            };
            let mut out = base[..base.len() - 1024].to_vec();
            for i in 1..=count {
                let member = entorhinal_catalog::archive::write(&[
                    entorhinal_catalog::archive::Member::regular("ck.json", b""),
                    entorhinal_catalog::archive::Member::regular(&format!("pad/{i:04}"), b""),
                ])
                .unwrap();
                out.extend_from_slice(&member[512..1024]);
            }
            out.resize(out.len() + 1024, 0);
            out
        };
        assert_eq!(
            entorhinal_catalog::sha256_hex(&bytes),
            case["archive_sha256"]
        );
        let digest = case["archive_sha256"].as_str().unwrap();
        a.serve(&format!("/v1/archives/{digest}"), Reply::bytes(bytes));
        let reply = c.fetch_archive(digest).await;
        if case["expect"]["ok"] == true {
            assert!(reply.is_ok(), "{}", case["name"]);
        } else {
            let refusal = reply.unwrap_err();
            assert_eq!(refusal.code(), case["expect"]["code"], "{}", case["name"]);
            let mut expected = case["expect"].clone();
            expected.as_object_mut().unwrap().remove("ok");
            assert_eq!(
                refusal.detail().cloned().unwrap(),
                expected,
                "{}",
                case["name"]
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn registry_connect_timeout_bounds_tls_handshake() {
    let _clock = hold_paused_clock();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("https://{}", listener.local_addr().unwrap());
    let c = RegistryClient::new(&url, std::slice::from_ref(&url), Limits::MODULE).unwrap();
    let mut request = Box::pin(c.fetch_json("v1/registry/keys"));
    let (_socket, _) = tokio::select! {
        socket = listener.accept() => socket.unwrap(),
        reply = &mut request => panic!("connect completed before TLS peer replied: {reply:?}"),
    };
    tokio::time::advance(Duration::from_secs(10)).await;
    let outcome =
        std::future::poll_fn(|cx| std::task::Poll::Ready(request.as_mut().poll(cx))).await;
    let std::task::Poll::Ready(outcome) = outcome else {
        panic!("connect timeout must bound the TLS handshake");
    };
    assert_eq!(outcome.unwrap_err().code(), "registry_timeout");
}

#[tokio::test(start_paused = true)]
async fn archive_digest_and_redirect_hop_limit() {
    let _clock = hold_paused_clock();
    let a = FakeRegistry::start().await;
    let c = client(&a);
    let v: Value = serde_json::from_str(include_str!(
        "../../entorhinal-catalog/tests/vectors/ck-catalog-v1/archive.json"
    ))
    .unwrap();
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(v["valid"]["archive_base64"].as_str().unwrap())
        .unwrap();
    let wrong = "ab".repeat(32);
    a.serve(&format!("/v1/archives/{wrong}"), Reply::bytes(bytes));
    assert_eq!(
        c.fetch_archive(&wrong).await.unwrap_err().code(),
        "digest_mismatch"
    );
    a.calls.lock().unwrap().clear();
    a.serve("/loop", Reply::redirect("/loop"));
    assert_eq!(
        c.fetch_json("loop").await.unwrap_err().code(),
        "registry_redirect_limit"
    );
    assert_eq!(a.calls.lock().unwrap().len(), 11);
}

#[test]
fn retry_after_parses_caps_and_names_defaults() {
    use reqwest::header::HeaderValue;
    let at = 1_767_225_600_000;
    let parsed = |text: &str| retry_after(Some(&HeaderValue::from_str(text).unwrap()), at);
    assert_eq!(
        parsed("17"),
        RetryAfter {
            delay: Duration::from_secs(17),
            source: RetryAfterSource::DeltaSeconds
        }
    );
    assert_eq!(
        parsed("9999999999").delay,
        Duration::from_secs(MAX_RETRY_AFTER_SECONDS)
    );
    assert_eq!(
        parsed("Thu, 01 Jan 2026 00:00:10 GMT"),
        RetryAfter {
            delay: Duration::from_secs(10),
            source: RetryAfterSource::HttpDate
        }
    );
    assert_eq!(
        parsed("Thu, 01 Jan 2026 23:00:00 GMT").delay,
        Duration::from_secs(MAX_RETRY_AFTER_SECONDS)
    );
    assert_eq!(
        parsed("Wed, 31 Dec 2025 23:59:59 GMT").delay,
        Duration::ZERO
    );
    assert_eq!(
        retry_after(None, at),
        RetryAfter {
            delay: Duration::from_secs(DEFAULT_RETRY_AFTER_SECONDS),
            source: RetryAfterSource::DefaultMissing
        }
    );
    for invalid in ["", "-1", "not a date", "184467440737095516160"] {
        assert_eq!(
            parsed(invalid),
            RetryAfter {
                delay: Duration::from_secs(DEFAULT_RETRY_AFTER_SECONDS),
                source: RetryAfterSource::DefaultInvalid
            }
        );
    }
    let opaque = HeaderValue::from_bytes(&[0xff]).unwrap();
    assert_eq!(
        retry_after(Some(&opaque), at).source,
        RetryAfterSource::DefaultInvalid
    );
}

#[tokio::test(start_paused = true)]
async fn retryable_503_is_typed_and_never_retries_inside_client() {
    let _clock = hold_paused_clock();
    let a = FakeRegistry::start().await;
    let c = client(&a);
    let path = "/v1/scopes/example/status";
    for (code, expected) in [
        ("publish_unavailable", Unavailable::Publish),
        ("status_key_unavailable", Unavailable::StatusKey),
    ] {
        a.calls.lock().unwrap().clear();
        a.serve(
            path,
            Reply {
                status: 503,
                headers: vec![("Retry-After".into(), "17".into())],
                ..Reply::bytes(
                    serde_json::to_vec(
                        &serde_json::json!({"error":{"code":code,"message":"opaque display text"}}),
                    )
                    .unwrap(),
                )
            },
        );
        let refusal = c.fetch_json(&path[1..]).await.unwrap_err();
        assert_eq!(refusal.code(), code);
        let NetworkError::Retryable {
            unavailable,
            retry_after,
        } = refusal
        else {
            panic!("503 must be retryable");
        };
        assert_eq!(unavailable, expected);
        assert_eq!(
            retry_after,
            RetryAfter {
                delay: Duration::from_secs(17),
                source: RetryAfterSource::DeltaSeconds
            }
        );
        assert_eq!(*a.calls.lock().unwrap(), vec![path]);
        tokio::time::advance(retry_after.delay).await;
        assert_eq!(*a.calls.lock().unwrap(), vec![path]);
        a.serve(path, Reply::bytes(b"{}".to_vec()));
        assert_eq!(c.fetch_json(&path[1..]).await.unwrap(), b"{}");
        assert_eq!(*a.calls.lock().unwrap(), vec![path, path]);
    }
    for (header, source) in [
        (None, RetryAfterSource::DefaultMissing),
        (Some("bad"), RetryAfterSource::DefaultInvalid),
    ] {
        a.serve(
            path,
            Reply {
                status: 503,
                headers: header
                    .map(|h| vec![("Retry-After".into(), h.into())])
                    .unwrap_or_default(),
                ..Reply::bytes(br#"{"error":{"code":"status_key_unavailable"}}"#.to_vec())
            },
        );
        let NetworkError::Retryable { retry_after, .. } =
            c.fetch_json(&path[1..]).await.unwrap_err()
        else {
            panic!("missing or invalid Retry-After must remain retryable");
        };
        assert_eq!(
            retry_after,
            RetryAfter {
                delay: Duration::from_secs(DEFAULT_RETRY_AFTER_SECONDS),
                source
            }
        );
    }
}

#[tokio::test(start_paused = true)]
async fn old_feed_410_is_typed_without_hidden_fallback() {
    let _clock = hold_paused_clock();
    let a = FakeRegistry::start().await;
    let c = client(&a);
    let incremental = "/v1/scopes/example/status?after=1";
    a.serve(
        incremental,
        Reply {
            status: 410,
            ..Reply::bytes(
                br#"{"error":{"code":"feed_after_unavailable","message":"history expired"}}"#
                    .to_vec(),
            )
        },
    );
    a.serve_vectors();
    assert!(matches!(
        c.fetch_json(&incremental[1..]).await.unwrap_err(),
        NetworkError::FeedAfterUnavailable
    ));
    assert_eq!(*a.calls.lock().unwrap(), vec![incremental]);
    let full = c.fetch_json("v1/scopes/example/status").await.unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&full).unwrap()["schema"],
        "ck-status-feed/v1"
    );
    assert_eq!(
        *a.calls.lock().unwrap(),
        vec![incremental, "/v1/scopes/example/status"]
    );
    for (status, code) in [
        (503, "feed_after_unavailable"),
        (410, "gone"),
        (410, "publish_unavailable"),
        (503, "unknown"),
        (404, "status_key_unavailable"),
    ] {
        a.serve(
            "/other",
            Reply {
                status,
                ..Reply::bytes(
                    serde_json::to_vec(&serde_json::json!({"error":{"code":code}})).unwrap(),
                )
            },
        );
        assert_eq!(
            c.fetch_json("other").await.unwrap_err().code(),
            "registry_http_error"
        );
    }
}

#[test]
fn module_factory_passes_default_limits_and_module_clock() {
    let (root, descriptor) = crate::tests::scratch_descriptor("catalog-module-client");
    let store = entorhinal_core::RegistryStore::open(&descriptor).unwrap();
    store
        .catalog_registry_add(
            "https://a.test",
            &["@a".into()],
            false,
            "k",
            &"ab".repeat(32),
            0,
        )
        .unwrap();
    let handler = crate::ProjectsHandler::with_runtime("catalog-client-test".into(), || 999);
    *handler.store.lock().unwrap() = Some(store);
    let client = handler.catalog_registry_client("@a").unwrap();
    assert_eq!(client.registry.as_str(), "https://a.test/");
    assert_eq!(client.limits.connect, Duration::from_secs(10));
    assert_eq!(client.limits.archive_timeout, Duration::from_secs(120));
    assert_eq!(client.limits.json_timeout, Duration::from_secs(30));
    assert_eq!(
        (
            client.limits.archive_bytes,
            client.limits.manifest_bytes,
            client.limits.json_bytes
        ),
        (10_485_760, 65_536, 8_388_608)
    );
    assert_eq!((client.clock)(), 999);
    handler.store.lock().unwrap().take();
    std::fs::remove_dir_all(root).unwrap();
}
