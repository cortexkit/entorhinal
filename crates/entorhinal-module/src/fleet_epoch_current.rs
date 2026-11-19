//! Local read model for the verified hub epoch and committed vault keys.

use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Notify;

use crate::{encode_result, HandlerError, ProjectsHandler};

pub(super) const METHOD: &str = "fleet.epoch_current";

#[derive(Default, Serialize)]
struct Current {
    #[serde(skip_serializing_if = "Option::is_none")]
    current: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    highest_held: Option<u64>,
}

#[derive(Default)]
pub(super) struct Cache {
    view: Mutex<(Current, i64)>,
    pub(super) changed: Notify,
}

impl Cache {
    pub(super) fn generation(&self) -> i64 {
        self.view.lock().unwrap().1
    }

    fn update(&self, slot: impl FnOnce(&mut Current) -> &mut Option<u64>, value: Option<u64>) {
        let mut view = self.view.lock().unwrap();
        let field = slot(&mut view.0);
        if *field != value {
            *field = value;
            view.1 += 1;
            self.changed.notify_waiters();
        }
    }

    pub(super) fn set_current(&self, value: Option<u64>) {
        self.update(|view| &mut view.current, value);
    }

    pub(super) fn set_highest_held(&self, value: Option<u64>) {
        self.update(|view| &mut view.highest_held, value);
    }
}

impl ProjectsHandler {
    pub(super) fn fleet_epoch_current(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Params {}

        let _: Params = serde_json::from_value(params)
            .map_err(|_| HandlerError::new("invalid_request", "fleet.epoch_current expects {}"))?;
        encode_result(&self.health.fleet_epoch.view.lock().unwrap().0)
    }
}
