ALTER TABLE agent
ADD COLUMN generation INTEGER NOT NULL DEFAULT 1 CHECK(generation > 0);

-- A token snapshots the whole registry row, not one selected field. Bump the
-- watermark whenever durable agent identity or routing metadata changes so a
-- verifier can reject assertions minted from any older row image.
CREATE TRIGGER agent_generation_on_mutation
AFTER UPDATE OF
    name,
    name_normalization_version,
    tag,
    role,
    project_id,
    workspace_id,
    persona_ref,
    name_version,
    wake_policy_json,
    wake_policy_version,
    github_identity_json,
    residence_machine_id,
    residence_harness,
    residence_address_json,
    residence_epoch,
    residence_state,
    sleep,
    terminal_reason,
    terminal_at_ms,
    merged_into_agent_id
ON agent
FOR EACH ROW
WHEN NEW.generation = OLD.generation
 AND (
    NEW.name IS NOT OLD.name
    OR NEW.name_normalization_version IS NOT OLD.name_normalization_version
    OR NEW.tag IS NOT OLD.tag
    OR NEW.role IS NOT OLD.role
    OR NEW.project_id IS NOT OLD.project_id
    OR NEW.workspace_id IS NOT OLD.workspace_id
    OR NEW.persona_ref IS NOT OLD.persona_ref
    OR NEW.name_version IS NOT OLD.name_version
    OR NEW.wake_policy_json IS NOT OLD.wake_policy_json
    OR NEW.wake_policy_version IS NOT OLD.wake_policy_version
    OR NEW.github_identity_json IS NOT OLD.github_identity_json
    OR NEW.residence_machine_id IS NOT OLD.residence_machine_id
    OR NEW.residence_harness IS NOT OLD.residence_harness
    OR NEW.residence_address_json IS NOT OLD.residence_address_json
    OR NEW.residence_epoch IS NOT OLD.residence_epoch
    OR NEW.residence_state IS NOT OLD.residence_state
    OR NEW.sleep IS NOT OLD.sleep
    OR NEW.terminal_reason IS NOT OLD.terminal_reason
    OR NEW.terminal_at_ms IS NOT OLD.terminal_at_ms
    OR NEW.merged_into_agent_id IS NOT OLD.merged_into_agent_id
 )
BEGIN
    UPDATE agent
       SET generation = OLD.generation + 1
     WHERE agent_id = OLD.agent_id;
END;
