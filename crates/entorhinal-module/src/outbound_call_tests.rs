use super::*;
use std::collections::BTreeMap;
use std::sync::Mutex;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::Layer;

#[derive(Debug)]
struct CapturedEvent {
    level: Level,
    fields: BTreeMap<String, String>,
}

impl Visit for CapturedEvent {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.fields
            .insert(field.name().into(), format!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields.insert(field.name().into(), value.into());
    }
}

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<CapturedEvent>>>);

impl<S: Subscriber> Layer<S> for Capture {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut captured = CapturedEvent {
            level: *event.metadata().level(),
            fields: BTreeMap::new(),
        };
        event.record(&mut captured);
        self.0.lock().unwrap().push(captured);
    }
}

#[test]
fn transport_warning_carries_target_method_variant_and_display_text() {
    let capture = Capture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    tracing::subscriber::with_default(subscriber, || {
        warn_route_call_error(
            &RouteTarget::InternalService {
                module_id: "engram".into(),
                service_id: "session-door".into(),
            },
            "cut.seal",
            &CallError::OutcomeUnknown("connection closed before reply".into()),
        );
        warn_route_call_error(
            &RouteTarget::InternalService {
                module_id: "participant-module".into(),
                service_id: "agent-data".into(),
            },
            "route.open",
            &CallError::NotSent("route open timed out".into()),
        );
    });

    let events = capture.0.lock().unwrap();
    assert_eq!(events.len(), 2);
    for (event, module, service, method, variant, error) in [
        (
            &events[0],
            "engram",
            "session-door",
            "cut.seal",
            "OutcomeUnknown",
            "request outcome unknown: connection closed before reply",
        ),
        (
            &events[1],
            "participant-module",
            "agent-data",
            "route.open",
            "NotSent",
            "request not sent: route open timed out",
        ),
    ] {
        assert_eq!(event.level, Level::WARN);
        assert_eq!(event.fields["module"], module);
        assert_eq!(event.fields["service"], service);
        assert_eq!(event.fields["method"], method);
        assert_eq!(event.fields["variant"], variant);
        assert_eq!(event.fields["error"], error);
        assert_eq!(event.fields["message"], "outbound subc call failed");
    }
}

#[test]
fn other_non_module_variants_are_named_without_discarding_the_error() {
    let cases = [
        (
            CallError::SubscriptionBackpressure("receiver full".into()),
            "SubscriptionBackpressure",
        ),
        (
            CallError::StaleRouteHandle(subc_client_rs::RouteHandle::detached(41, 1)),
            "StaleRouteHandle",
        ),
        (
            CallError::CapabilityUnprovided {
                capability: "agent-data".into(),
            },
            "CapabilityUnprovided",
        ),
        (
            CallError::CapabilityAmbiguous {
                capability: "agent-data".into(),
                claimants: vec!["one".into(), "two".into()],
            },
            "CapabilityAmbiguous",
        ),
        (
            CallError::InvalidCapabilityIdentifier {
                capability: "".into(),
            },
            "InvalidCapabilityIdentifier",
        ),
    ];
    let capture = Capture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    tracing::subscriber::with_default(subscriber, || {
        for (error, _) in &cases {
            warn_call_error("subc", "control", "catalog.list", error);
        }
    });
    let events = capture.0.lock().unwrap();
    assert_eq!(events.len(), cases.len());
    for (event, (error, variant)) in events.iter().zip(&cases) {
        assert_eq!(event.level, Level::WARN);
        assert_eq!(&event.fields["variant"], variant);
        assert_eq!(event.fields["error"], error.to_string());
    }
}

#[test]
fn module_refusals_do_not_emit_transport_warnings() {
    let capture = Capture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    tracing::subscriber::with_default(subscriber, || {
        for code in ["invalid_request", "cancelled"] {
            warn_call_error(
                "cingulate",
                "management_surface",
                "decide",
                &CallError::Module(subc_protocol::ErrorBody::new(code, "refused")),
            );
        }
    });
    assert!(capture.0.lock().unwrap().is_empty());
}
