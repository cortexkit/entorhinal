CREATE TABLE agent (
    agent_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    name_normalization_version INTEGER NOT NULL DEFAULT 1 CHECK(name_normalization_version = 1),
    tag TEXT NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('assistant', 'workspace_head', 'head', 'hiree')),
    project_id TEXT NULL,
    workspace_id TEXT NULL,
    persona_ref TEXT NULL,
    name_version INTEGER NOT NULL DEFAULT 1 CHECK(name_version > 0),
    wake_policy_json TEXT NULL,
    wake_policy_version INTEGER NOT NULL DEFAULT 1 CHECK(wake_policy_version > 0),
    residence_machine_id TEXT NULL REFERENCES machine_binding(machine_id),
    residence_harness TEXT NULL CHECK(residence_harness IN ('opencode', 'broca')),
    residence_address_json TEXT NULL,
    residence_epoch INTEGER NULL CHECK(residence_epoch >= 0),
    residence_state TEXT NULL CHECK(residence_state IN ('live', 'migrating', 'forced_flip_pending')),
    sleep INTEGER NOT NULL DEFAULT 0 CHECK(sleep IN (0, 1)),
    created_at_ms INTEGER NOT NULL CHECK(created_at_ms >= 0),
    updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms >= 0),
    terminal_reason TEXT NULL CHECK(terminal_reason IN ('deleted', 'merged')),
    terminal_at_ms INTEGER NULL CHECK(terminal_at_ms >= 0),
    merged_into_agent_id TEXT NULL REFERENCES agent(agent_id),
    CHECK (
        (role = 'assistant' AND project_id IS NULL AND workspace_id IS NULL)
        OR (role = 'workspace_head' AND project_id IS NULL AND workspace_id IS NOT NULL)
        OR (role IN ('head', 'hiree') AND project_id IS NOT NULL AND workspace_id IS NULL)
    ),
    CHECK ((terminal_reason IS NULL) = (terminal_at_ms IS NULL)),
    CHECK (
        (terminal_reason IS NULL AND merged_into_agent_id IS NULL)
        OR (
            terminal_reason IS NOT NULL
            AND terminal_reason = 'deleted'
            AND merged_into_agent_id IS NULL
        )
        OR (
            terminal_reason IS NOT NULL
            AND terminal_reason = 'merged'
            AND merged_into_agent_id IS NOT NULL
        )
    ),
    CHECK (
        (
            residence_machine_id IS NULL
            AND residence_harness IS NULL
            AND residence_address_json IS NULL
            AND residence_epoch IS NULL
            AND residence_state IS NULL
        )
        OR (
            residence_machine_id IS NOT NULL
            AND residence_harness IS NOT NULL
            AND residence_address_json IS NOT NULL
            AND residence_epoch IS NOT NULL
            AND residence_state IS NOT NULL
        )
    )
);

-- Terminal heads remain as history, so only rows still serving the role contend.
CREATE UNIQUE INDEX uq_live_head
ON agent(project_id)
WHERE role = 'head' AND terminal_reason IS NULL;

CREATE INDEX idx_agent_project
ON agent(project_id)
WHERE project_id IS NOT NULL;

CREATE INDEX idx_agent_workspace
ON agent(workspace_id)
WHERE workspace_id IS NOT NULL;

CREATE INDEX idx_agent_role ON agent(role);

CREATE TABLE agent_name_claim (
    claim_id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id TEXT NOT NULL REFERENCES agent(agent_id),
    namespace_kind TEXT NOT NULL CHECK(namespace_kind IN ('assistant', 'workspace')),
    namespace_key TEXT NOT NULL,
    name_normalization_version INTEGER NOT NULL DEFAULT 1 CHECK(name_normalization_version = 1),
    normalized_name TEXT NOT NULL,
    display_name TEXT NOT NULL,
    claimed_at_ms INTEGER NOT NULL CHECK(claimed_at_ms >= 0),
    released_at_ms INTEGER NULL CHECK(released_at_ms >= 0)
);

-- Released claims remain auditable without blocking reuse of their normalized name.
CREATE UNIQUE INDEX uq_active_claim
ON agent_name_claim(namespace_kind, namespace_key, name_normalization_version, normalized_name)
WHERE released_at_ms IS NULL;

-- Claim history may contain many rows, but an agent can hold only one current name.
CREATE UNIQUE INDEX uq_active_agent_claim
ON agent_name_claim(agent_id)
WHERE released_at_ms IS NULL;

CREATE INDEX idx_claim_agent ON agent_name_claim(agent_id);

CREATE TABLE machine_binding (
    machine_id TEXT PRIMARY KEY,
    binding_kind TEXT NOT NULL CHECK(binding_kind IN ('local', 'remote')),
    pairing_state TEXT NOT NULL CHECK(pairing_state IN ('local', 'never_paired', 'paired', 'revoked', 'remote_not_activated')),
    callo_peer_id TEXT NULL,
    binding_version INTEGER NULL CHECK(binding_version >= 0),
    transport_binding_digest TEXT NULL CHECK(transport_binding_digest IS NULL OR (length(transport_binding_digest) = 64 AND transport_binding_digest NOT GLOB '*[^0-9a-f]*')),
    source_locator TEXT NULL,
    observed_at_ms INTEGER NULL CHECK(observed_at_ms >= 0),
    CHECK (
        (
            binding_kind = 'local'
            AND pairing_state = 'local'
            AND callo_peer_id IS NULL
            AND binding_version IS NULL
            AND transport_binding_digest IS NULL
            AND source_locator IS NULL
            AND observed_at_ms IS NULL
        )
        OR (binding_kind = 'remote' AND pairing_state <> 'local')
    )
);

-- Remote bindings may be plural; the local authority must have one unambiguous home.
CREATE UNIQUE INDEX uq_local_binding
ON machine_binding(binding_kind)
WHERE binding_kind = 'local';

CREATE TABLE agent_delivery (
    agent_id TEXT NOT NULL REFERENCES agent(agent_id),
    delivery_id TEXT NOT NULL,
    canonical_request_hash TEXT NOT NULL CHECK(length(canonical_request_hash) = 64 AND canonical_request_hash NOT GLOB '*[^0-9a-f]*'),
    canonical_body_bytes BLOB NOT NULL,
    urgency TEXT NOT NULL,
    expected_residence_epoch INTEGER NULL CHECK(expected_residence_epoch >= 0),
    state TEXT NOT NULL CHECK(state IN ('prepared', 'accepted_delivered', 'accepted_queued')),
    accepted_disposition TEXT NULL CHECK(accepted_disposition IN ('delivered', 'queued')),
    committed_order INTEGER NULL CHECK(committed_order >= 0),
    PRIMARY KEY(agent_id, delivery_id),
    CHECK (
        (state = 'prepared' AND accepted_disposition IS NULL AND committed_order IS NULL)
        OR (
            state = 'accepted_delivered'
            AND accepted_disposition IS NOT NULL
            AND accepted_disposition = 'delivered'
            AND committed_order IS NOT NULL
        )
        OR (
            state = 'accepted_queued'
            AND accepted_disposition IS NOT NULL
            AND accepted_disposition = 'queued'
            AND committed_order IS NOT NULL
        )
    )
);

-- Prepared deliveries have no order yet and must not contend before acceptance.
CREATE UNIQUE INDEX uq_agent_delivery_order
ON agent_delivery(agent_id, committed_order)
WHERE committed_order IS NOT NULL;

CREATE TABLE residence_flip (
    agent_id TEXT NOT NULL REFERENCES agent(agent_id),
    flip_id TEXT NOT NULL,
    canonical_input_hash TEXT NOT NULL CHECK(length(canonical_input_hash) = 64 AND canonical_input_hash NOT GLOB '*[^0-9a-f]*'),
    starting_epoch INTEGER NOT NULL CHECK(starting_epoch >= 0),
    source_machine_id TEXT NOT NULL REFERENCES machine_binding(machine_id),
    source_harness TEXT NOT NULL CHECK(source_harness IN ('opencode', 'broca')),
    source_address_json TEXT NOT NULL,
    destination_machine_id TEXT NOT NULL REFERENCES machine_binding(machine_id),
    destination_harness TEXT NOT NULL CHECK(destination_harness IN ('opencode', 'broca')),
    destination_address_json TEXT NOT NULL,
    mode TEXT NOT NULL CHECK(mode IN ('graceful', 'forced')),
    phase TEXT NOT NULL CHECK(phase IN ('prepared', 'source_drained', 'source_revoked', 'destination_minted', 'destination_materialized', 'completed', 'quarantined', 'failed')),
    quarantine_locator TEXT NULL,
    quarantine_digest TEXT NULL CHECK(quarantine_digest IS NULL OR (length(quarantine_digest) = 64 AND quarantine_digest NOT GLOB '*[^0-9a-f]*')),
    terminal_status TEXT NULL,
    terminal_payload_json TEXT NULL,
    completed_agent_digest_json TEXT NULL,
    started_at_ms INTEGER NOT NULL CHECK(started_at_ms >= 0),
    updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms >= 0),
    PRIMARY KEY(agent_id, flip_id),
    CHECK ((quarantine_locator IS NULL) = (quarantine_digest IS NULL)),
    CHECK ((phase = 'quarantined') = (quarantine_locator IS NOT NULL)),
    CHECK (
        phase IN ('completed', 'quarantined', 'failed')
        OR (terminal_status IS NULL AND terminal_payload_json IS NULL)
    ),
    CHECK (completed_agent_digest_json IS NULL OR phase = 'completed')
);

-- Terminal attempts remain durable while only one recoverable flip can be in flight.
CREATE UNIQUE INDEX uq_live_flip
ON residence_flip(agent_id)
WHERE phase NOT IN ('completed', 'quarantined', 'failed');

CREATE TABLE agent_registry_activation_marker (
    marker_id INTEGER PRIMARY KEY CHECK(marker_id = 1),
    activated_at_ms INTEGER NOT NULL CHECK(activated_at_ms >= 0)
);
