use super::*;

const CACHED_HOLDER: &str = "11111111111111111111111111111111";

pub(super) fn unobserved_role(role: &str) -> Value {
    json!({"epoch":0,"held_here":false,"holder":null,"holder_name":null,
        "known":false,"role":role,"source":null,"stale":true})
}

pub(super) fn unobserved_fence(role: &str) -> Value {
    json!({"epoch":0,"held_here":false,"holder":null,"role":role,
        "stale":true,"subject":format!("role:{role}"),"version":0})
}

fn replace(path: &Path, method: &str, expected: &mut String, old: Value, new: Value) {
    let old = old.to_string();
    let new = new.to_string();
    assert_eq!(
        expected.matches(&old).count(),
        1,
        "{}: {method} must contain exactly one recorded exception: {old}",
        path.display()
    );
    *expected = expected.replacen(&old, &new, 1);
    eprintln!(
        "{}: {method}: {old} -> {new}",
        path.file_name().unwrap().display()
    );
}

pub(super) fn apply(path: &Path, name: &str, disabled: bool, expected: &mut String) {
    // Match the exact recorded cache values that the migration removes.
    // Untouched agents, requests, and envelope bytes still compare to the recording.
    let mut cached_claim = json!({"agent_id":BASELINE_AGENT,"cut_id":null,"epoch":3,
        "holder":CACHED_HOLDER,"holder_name":null,"known":true,"read_at_ms":NOW,
        "stale":false,"status":"held_elsewhere"});
    if name.starts_with("legacy-") {
        cached_claim["holder_box"] = json!(BOX_A);
    }
    replace(
        path,
        "agent.where_many",
        expected,
        cached_claim,
        json!({"agent_id":BASELINE_AGENT,"cut_id":null,"epoch":0,"holder":null,
            "holder_name":null,"known":disabled,"read_at_ms":null,"stale":!disabled,
            "status":"unclaimed"}),
    );
    replace(
        path,
        "agent.session_residence",
        expected,
        json!({"agent_id":BASELINE_AGENT,"epoch":3,"holder":CACHED_HOLDER,
            "read_at_ms":NOW,"state":"elsewhere"}),
        if disabled {
            json!({"agent_id":BASELINE_AGENT,"epoch":0,"state":"unclaimed"})
        } else {
            json!({"agent_id":BASELINE_AGENT,"state":"unknown"})
        },
    );

    for (role, source, holder, here, epoch, version) in [
        ("connector_home", "cloud", CACHED_HOLDER, false, 3, 5),
        ("local_role", "local", BOX_A, true, 1, 1),
    ] {
        if source == "local" && !disabled {
            continue;
        }
        replace(
            path,
            "role.read",
            expected,
            json!({"epoch":epoch,"held_here":here,"holder":holder,"holder_name":null,
                "known":true,"role":role,"source":source,"stale":true}),
            unobserved_role(role),
        );
        replace(
            path,
            "role.fence",
            expected,
            json!({"epoch":epoch,"held_here":here,"holder":holder,"role":role,
                "stale":true,"subject":format!("role:{role}"),"version":version}),
            unobserved_fence(role),
        );
    }
}
