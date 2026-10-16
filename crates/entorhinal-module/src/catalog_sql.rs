//! SQLite steps run independently of async network work. A reply deadline does
//! not cancel a transaction: the blocking owner retains its guards until commit.
use crate::{catalog_blob::CatalogError, HandlerError};
use tokio::time::{timeout_at, Instant};

pub(crate) async fn run<T, E>(work: impl FnOnce() -> Result<T, E> + Send + 'static) -> Result<T, E>
where
    T: Send + 'static,
    E: From<CatalogError> + Send + 'static,
{
    #[cfg(test)]
    let entered = tests::SQL_ENTERED.try_with(Clone::clone).ok();
    tokio::task::spawn_blocking(move || {
        #[cfg(test)]
        if let Some(entered) = entered {
            entered.notify_one();
        }
        // A current-thread test runtime must refuse an accidental inline SQL step.
        #[cfg(test)]
        tokio::task::block_in_place(|| {});
        work()
    })
    .await
    .map_err(|e| E::from(CatalogError::new("storage_error", e.to_string())))?
}

pub(crate) fn expired(operation: &str) -> HandlerError {
    HandlerError {
        code: "catalog_timeout".into(),
        message: format!("{operation} deadline expired; retry to observe the committed state"),
        detail: Some(serde_json::json!({"retryable": true})),
    }
}

pub(crate) async fn until<T: Send + 'static>(
    deadline: Instant,
    operation: &'static str,
    work: impl FnOnce() -> Result<T, HandlerError> + Send + 'static,
) -> Result<T, HandlerError> {
    timeout_at(deadline, run(work))
        .await
        .map_err(|_| expired(operation))?
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::ProjectsHandler;
    use entorhinal_core::RegistryStore;
    use std::{
        future::Future,
        pin::Pin,
        task::{Context, Poll, Waker},
    };
    use subc_client_rs::{HandlerOutcome, ModuleHandler};

    tokio::task_local! { pub(crate) static SQL_ENTERED: std::sync::Arc<tokio::sync::Notify>; }
    pub(crate) fn tracked<F: Future>(
        future: F,
    ) -> (
        impl Future<Output = F::Output>,
        std::sync::Arc<tokio::sync::Notify>,
    ) {
        let entered = std::sync::Arc::new(tokio::sync::Notify::new());
        (SQL_ENTERED.scope(entered.clone(), future), entered)
    }

    pub(crate) async fn started<F: Future>(mut future: Pin<&mut F>, entered: &tokio::sync::Notify) {
        finish(async {
            tokio::select! {
                biased;
                _ = entered.notified() => {},
                _ = &mut future => panic!("catalog operation finished before reaching its SQL worker"),
            }
        }).await;
    }
    pub(crate) struct HeldSql {
        release: Option<std::sync::mpsc::Sender<()>>,
        worker: Option<tokio::task::JoinHandle<()>>,
    }
    impl HeldSql {
        pub(crate) async fn new(store: RegistryStore) -> Self {
            assert_eq!(tokio::runtime::Handle::current().metrics().num_workers(), 1);
            let (ready, mut started) = tokio::sync::oneshot::channel();
            let (release, wait) = std::sync::mpsc::channel();
            let worker = tokio::task::spawn_blocking(move || {
                store
                    .apply_entry("test.catalog-held", "{}", "test", None, |_| {
                        ready.send(()).unwrap();
                        let _ = wait.recv();
                        Ok(())
                    })
                    .unwrap();
            });
            while started.try_recv().is_err() {
                tokio::task::yield_now().await;
            }
            Self {
                release: Some(release),
                worker: Some(worker),
            }
        }
        pub(crate) async fn finish(mut self) {
            self.release.take().unwrap().send(()).unwrap();
            finish(self.worker.take().unwrap()).await.unwrap();
        }
    }
    impl Drop for HeldSql {
        fn drop(&mut self) {
            // Even a failed assertion or an inline-SQL sentinel panic releases SQLite.
            if let Some(release) = self.release.take() {
                let _ = release.send(());
            }
        }
    }
    pub(crate) fn poll<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }
    pub(crate) async fn finish<F: Future>(future: F) -> F::Output {
        tokio::pin!(future);
        loop {
            tokio::select! {
                result = &mut future => return result,
                () = tokio::task::yield_now() => {},
            }
        }
    }
    pub(crate) async fn responsive(handler: &ProjectsHandler) {
        let key = (9876, 1234);
        handler.route_admissions().insert(
            key,
            crate::RouteAdmission::from_bind(Some(subc_protocol::Principal::Direct), None),
        );
        let mut health = Box::pin(handler.health());
        assert!(
            poll(health.as_mut()).is_ready(),
            "health waited for catalog SQL"
        );
        for method in ["enumerate", "agent.fleet_identity"] {
            let body =
                serde_json::to_vec(&serde_json::json!({"method":method,"params":{}})).unwrap();
            let mut read = Box::pin(handler.handle_request_wait(&body, key));
            assert!(
                matches!(
                    poll(read.as_mut()),
                    Poll::Ready(HandlerOutcome::Response(_))
                ),
                "{method} waited for catalog SQL"
            );
        }
    }
}
