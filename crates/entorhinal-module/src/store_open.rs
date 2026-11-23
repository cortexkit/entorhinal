use std::sync::{Mutex, Weak};

use entorhinal_core::{RegistryError, RegistryStore};

use crate::HandlerError;

const LEGACY_SHARING_UNSUPPORTED: &str = "legacy_sharing_unsupported";

pub(super) struct OpenFailure {
    pub(super) log: String,
    pub(super) refusal: Option<HandlerError>,
}

impl OpenFailure {
    pub(super) fn from_open(context: &str, error: RegistryError) -> Self {
        let refusal = match &error {
            RegistryError::Domain { code, message, .. } if code == LEGACY_SHARING_UNSUPPORTED => {
                Some(HandlerError::new(code, message.clone()))
            }
            _ => None,
        };
        Self {
            log: format!("{context}: {error}"),
            refusal,
        }
    }
}

impl From<String> for OpenFailure {
    fn from(log: String) -> Self {
        Self { log, refusal: None }
    }
}

/// The last open refusal. Installation changes it under the store slot lock,
/// so a served lookup never observes an empty slot before its refusal is set.
/// Health can read the reason without taking the store slot lock.
#[derive(Default)]
pub(super) struct Refusal(Mutex<Option<HandlerError>>);

impl Refusal {
    pub(super) fn publish(&self, refusal: Option<HandlerError>) {
        *self.0.lock().unwrap_or_else(|e| e.into_inner()) = refusal;
    }

    pub(super) fn get(&self) -> Option<HandlerError> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    pub(super) fn lookup(
        &self,
        slot: &Mutex<Option<RegistryStore>>,
        unavailable: impl FnOnce() -> HandlerError,
    ) -> Result<RegistryStore, HandlerError> {
        let guard = slot.lock().unwrap_or_else(|e| e.into_inner());
        guard
            .clone()
            .ok_or_else(|| self.get().unwrap_or_else(unavailable))
    }

    pub(super) fn lookup_weak(
        &self,
        slot: &Weak<Mutex<Option<RegistryStore>>>,
        unavailable: impl FnOnce() -> HandlerError,
    ) -> Result<RegistryStore, HandlerError> {
        match slot.upgrade() {
            Some(slot) => self.lookup(&slot, unavailable),
            None => Err(unavailable()),
        }
    }
}
