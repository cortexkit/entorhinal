-- Wake delivery persistence and the workspace rung repair.
--
-- Project-bound agents already carried a workspace name-claim stamp because
-- names are unique per resolved workspace. Copy that durable placement into
-- agent.workspace_id when it exists. Older rows without such a claim remain
-- NULL; the migration does not invent membership.
PRAGMA defer_foreign_keys = ON;

CREATE TABLE agent_wake_workspace (
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
    merged_into_agent_id TEXT NULL REFERENCES agent_wake_workspace(agent_id),
    github_identity_json TEXT NULL,
    CHECK (
        (role = 'assistant' AND project_id IS NULL AND workspace_id IS NULL)
        OR (role = 'workspace_head' AND project_id IS NULL AND workspace_id IS NOT NULL)
        OR (role IN ('head', 'hiree') AND project_id IS NOT NULL)
    ),
    CHECK ((terminal_reason IS NULL) = (terminal_at_ms IS NULL)),
    CHECK (
        (terminal_reason IS NULL AND merged_into_agent_id IS NULL)
        OR (terminal_reason = 'deleted' AND merged_into_agent_id IS NULL)
        OR (terminal_reason = 'merged' AND merged_into_agent_id IS NOT NULL)
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

INSERT INTO agent_wake_workspace (
    agent_id, name, name_normalization_version, tag, role, project_id, workspace_id,
    persona_ref, name_version, wake_policy_json, wake_policy_version,
    residence_machine_id, residence_harness, residence_address_json, residence_epoch,
    residence_state, sleep, created_at_ms, updated_at_ms, terminal_reason,
    terminal_at_ms, merged_into_agent_id, github_identity_json
)
SELECT
    a.agent_id, a.name, a.name_normalization_version, a.tag, a.role, a.project_id,
    COALESCE(
        a.workspace_id,
        CASE WHEN a.role IN ('head', 'hiree') THEN (
            SELECT c.namespace_key
            FROM agent_name_claim c
            WHERE c.agent_id = a.agent_id AND c.namespace_kind = 'workspace'
            ORDER BY c.claim_id DESC
            LIMIT 1
        ) END
    ),
    a.persona_ref, a.name_version, a.wake_policy_json, a.wake_policy_version,
    a.residence_machine_id, a.residence_harness, a.residence_address_json,
    a.residence_epoch, a.residence_state, a.sleep, a.created_at_ms, a.updated_at_ms,
    a.terminal_reason, a.terminal_at_ms, a.merged_into_agent_id, a.github_identity_json
FROM agent a;

DROP TABLE agent;
ALTER TABLE agent_wake_workspace RENAME TO agent;

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

-- Each source input carries separate prose and machine-value fields. The latter
-- is canonical JSON and is rendered only by the module's value interpolator.
CREATE TABLE wake_fire (
    fire_id TEXT PRIMARY KEY
        CHECK (length(fire_id) = 19 AND fire_id GLOB 'wf_[0-9a-f]*'),
    agent_id TEXT NOT NULL REFERENCES agent(agent_id),
    source_kind TEXT NOT NULL
        CHECK (source_kind IN ('schedule', 'external_event', 'condition', 'idleness')),
    source_key TEXT NOT NULL DEFAULT '',
    input_key TEXT NOT NULL CHECK (length(input_key) > 0),
    user_prose TEXT NOT NULL,
    machine_value_json TEXT NOT NULL,
    delivery_action TEXT NOT NULL
        CHECK (delivery_action IN ('wake', 'piggyback', 'silent')),
    due_at INTEGER NOT NULL CHECK (due_at >= 0),
    created_at INTEGER NOT NULL CHECK (created_at >= 0),
    delivered_at INTEGER NULL CHECK (delivered_at >= 0),
    acked_at INTEGER NULL CHECK (acked_at >= 0),
    effect_id TEXT NULL,
    payload_hash TEXT NULL
        CHECK (payload_hash IS NULL OR (length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*')),
    UNIQUE (agent_id, source_kind, input_key),
    CHECK ((delivered_at IS NULL) = (effect_id IS NULL)),
    CHECK ((delivered_at IS NULL) = (payload_hash IS NULL))
);

CREATE INDEX idx_wake_fire_pending
ON wake_fire(agent_id, due_at, fire_id)
WHERE delivered_at IS NULL AND acked_at IS NULL;
CREATE INDEX idx_wake_fire_history
ON wake_fire(agent_id, delivered_at, created_at, fire_id);
CREATE INDEX idx_wake_fire_source_delivery
ON wake_fire(agent_id, source_kind, source_key, delivered_at)
WHERE delivered_at IS NOT NULL;
