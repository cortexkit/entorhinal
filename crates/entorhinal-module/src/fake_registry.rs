//! Loopback-only HTTP registry for transport tests. It never has a production constructor.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Clone)]
pub struct Reply {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub chunked: bool,
    pub release: Option<Arc<tokio::sync::Notify>>,
}
impl Reply {
    pub fn bytes(body: impl Into<Vec<u8>>) -> Self {
        Self {
            status: 200,
            headers: vec![],
            body: body.into(),
            chunked: false,
            release: None,
        }
    }
    pub fn redirect(url: &str) -> Self {
        Self {
            status: 302,
            headers: vec![("Location".into(), url.into())],
            ..Self::bytes(vec![])
        }
    }
}
pub struct FakeRegistry {
    pub url: String,
    pub replies: Arc<Mutex<BTreeMap<String, Reply>>>,
    pub calls: Arc<Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
}
impl FakeRegistry {
    pub async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let replies = Arc::new(Mutex::new(BTreeMap::<String, Reply>::new()));
        let calls = Arc::new(Mutex::new(vec![]));
        let (routes, requests) = (replies.clone(), calls.clone());
        let task = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let routes = routes.clone();
                let requests = requests.clone();
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    loop {
                        let mut buffer = [0; 1024];
                        let Ok(n) = socket.read(&mut buffer).await else {
                            return;
                        };
                        if n == 0 {
                            return;
                        }
                        request.extend_from_slice(&buffer[..n]);
                        if request.windows(4).any(|w| w == b"\r\n\r\n") {
                            break;
                        }
                        if request.len() > 8192 {
                            return;
                        }
                    }
                    let text = String::from_utf8_lossy(&request);
                    let path = text.split_whitespace().nth(1).unwrap_or("/").to_string();
                    requests.lock().unwrap().push(path.clone());
                    let reply = routes.lock().unwrap().get(&path).cloned().unwrap_or(Reply {
                        status: 404,
                        ..Reply::bytes(vec![])
                    });
                    if let Some(release) = &reply.release {
                        release.notified().await;
                    }
                    let mut header =
                        format!("HTTP/1.1 {} Fake\r\nConnection: close\r\n", reply.status);
                    for (key, value) in &reply.headers {
                        header.push_str(&format!("{key}: {value}\r\n"));
                    }
                    if reply.chunked {
                        header.push_str("Transfer-Encoding: chunked\r\n\r\n");
                    } else {
                        header.push_str(&format!("Content-Length: {}\r\n\r\n", reply.body.len()));
                    }
                    if socket.write_all(header.as_bytes()).await.is_err() {
                        return;
                    }
                    let body = if reply.chunked {
                        let mut bytes = format!("{:x}\r\n", reply.body.len()).into_bytes();
                        bytes.extend_from_slice(&reply.body);
                        bytes.extend_from_slice(b"\r\n0\r\n\r\n");
                        bytes
                    } else {
                        reply.body
                    };
                    let _ = socket.write_all(&body).await;
                });
            }
        });
        Self {
            url,
            replies,
            calls,
            task,
        }
    }
    pub fn serve(&self, path: &str, reply: Reply) {
        self.replies.lock().unwrap().insert(path.into(), reply);
    }
}
impl Drop for FakeRegistry {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl FakeRegistry {
    pub fn serve_vectors(&self) {
        use base64::Engine;
        use serde_json::{json, Value};
        let records: Value = serde_json::from_str(include_str!(
            "../../entorhinal-catalog/tests/vectors/ck-catalog-v1/record.json"
        ))
        .unwrap();
        let status: Value = serde_json::from_str(include_str!(
            "../../entorhinal-catalog/tests/vectors/ck-catalog-v1/status.json"
        ))
        .unwrap();
        for (kind, package_name) in [("skill", "hello-skill"), ("flow", "hello-flow")] {
            let package = &records["packages"][kind];
            let record = records["valid"]
                .as_array()
                .unwrap()
                .iter()
                .find(|v| v["record"]["name"] == format!("@example/{package_name}"))
                .unwrap();
            let latest = status["records"]
                .as_array()
                .unwrap()
                .iter()
                .find(|s| s["record"]["name"] == record["record"]["name"])
                .unwrap();
            let reply = json!({"name":record["record"]["name"],"versions":[{"version":"1.0.0","record":record["record"],"signature":record["signature"],"status":{"record":latest["record"],"signature":latest["signature"]}}]});
            self.serve(
                &format!("/v1/packages/example/{package_name}"),
                Reply::bytes(serde_json::to_vec(&reply).unwrap()),
            );
            self.serve(
                &format!("/v1/packages/example/{package_name}/1.0.0/ck.json"),
                Reply::bytes(
                    package["manifest_text"]
                        .as_str()
                        .unwrap()
                        .as_bytes()
                        .to_vec(),
                ),
            );
            self.serve(
                &format!(
                    "/v1/archives/{}",
                    package["archive_sha256"].as_str().unwrap()
                ),
                Reply::bytes(
                    base64::engine::general_purpose::STANDARD
                        .decode(package["archive_base64"].as_str().unwrap())
                        .unwrap(),
                ),
            );
        }
        for view in status["feed"]["views"].as_array().unwrap() {
            let path = view["request"]
                .as_str()
                .unwrap()
                .strip_prefix("GET ")
                .unwrap();
            let expected = &view["expect"];
            if expected["status"] == 410 {
                self.serve(
                    path,
                    Reply {
                        status: 410,
                        ..Reply::bytes(
                            serde_json::to_vec(&json!({"error":{"code":"feed_after_unavailable"}}))
                                .unwrap(),
                        )
                    },
                );
                continue;
            }
            let entries = expected["records"].as_array().unwrap().iter().map(|entry| {
                let document = status["records"].as_array().unwrap().iter().find(|s| s["id"] == entry["id"]).unwrap();
                json!({"feed_seq":entry["feed_seq"],"record":document["record"],"signature":document["signature"]})
            }).collect::<Vec<_>>();
            self.serve(path,Reply::bytes(serde_json::to_vec(&json!({"schema":"ck-status-feed/v1","scope":"example","feed_seq":expected["feed_seq"],"records":entries})).unwrap()));
        }
        let statements: Value = serde_json::from_str(include_str!(
            "../../entorhinal-catalog/tests/vectors/ck-catalog-v1/statements.json"
        ))
        .unwrap();
        let keys = statements["ck-registry-key/v1"]["chain"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| json!({"statement":s["statement"],"signature":s["signature"]}))
            .collect::<Vec<_>>();
        self.serve("/v1/registry/keys",Reply::bytes(serde_json::to_vec(&json!({"schema":"ck-registry-keys/v1","registry":"https://registry.example.invalid","keys":keys})).unwrap()));
        self.serve(
            "/v1/scopes/example",
            Reply::bytes(
                serde_json::to_vec(&json!({"scope":"example","visibility":"public","verified_by":"invitation","keys":[{
                    "public_key":records["keys"]["trusted"]["public_key_hex"],
                    "fingerprint":records["keys"]["trusted"]["fingerprint"],
                    "valid_from":"2026-01-01T00:00:00Z","valid_until":null,"revocation":null,"rotation":null,"cert":null
                }]}))
                    .unwrap(),
            ),
        );
    }
}
