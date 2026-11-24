use super::*;
use serde_json::Value;
use std::time::Duration;
struct EmptyHub;

#[async_trait::async_trait]
impl Probe for EmptyHub {
    fn activate(&self) {}
    async fn head(&self, _: Instant) -> Result<u64, HandlerError> {
        Ok(0)
    }
    async fn local_box(&self, _: Instant) -> Result<String, HandlerError> {
        unreachable!()
    }
}

#[tokio::test]
async fn hub_reader_regression_installed_switch_restarts_a_finished_tail() {
    use crate::hub_identity_log::{LocalReplan, SharedLog};

    struct Restart;

    #[async_trait::async_trait]
    impl SharedLog for Restart {
        async fn start_installed_tail(
            &self,
            _: Instant,
        ) -> Result<tokio::task::JoinHandle<()>, HandlerError> {
            Ok(tokio::spawn(std::future::pending()))
        }

        async fn applied(&self, _: Instant) -> Result<u64, HandlerError> {
            unreachable!()
        }

        async fn catch_up(&self, _: Instant) -> Result<(), HandlerError> {
            unreachable!()
        }

        async fn append_local(
            &self,
            _: &str,
            _: &str,
            _: u64,
            _: Value,
            _: Option<&entorhinal_core::HubContinuation>,
            _: LocalReplan,
            _: Instant,
        ) -> Result<Vec<u8>, HandlerError> {
            unreachable!()
        }
    }

    let switch = Switch::with_probe(Arc::new(EmptyHub));
    let handler = ProjectsHandler::with_runtime("switch-restart".into(), || 100);
    let runtime = Runtime::new(&handler);
    let mut finished = tokio::spawn(async {});
    (&mut finished).await.unwrap();
    *runtime.tail.lock().unwrap() = Some(finished);
    *runtime.shared.lock().unwrap() = Some(Arc::new(Restart));
    let deadline = Instant::now() + Duration::from_secs(1);
    switch.start_installed(&runtime, 0, deadline).await.unwrap();
    let id = runtime.tail.lock().unwrap().as_ref().unwrap().id();
    assert!(
        !runtime.tail.lock().unwrap().as_ref().unwrap().is_finished(),
        "finished tail was not replaced"
    );
    switch.start_installed(&runtime, 0, deadline).await.unwrap();
    let task = runtime.tail.lock().unwrap().take().unwrap();
    assert_eq!(task.id(), id, "a running tail was replaced");
    task.abort();
    let _ = task.await;
}
