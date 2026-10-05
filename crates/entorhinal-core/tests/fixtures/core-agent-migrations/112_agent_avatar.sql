-- The genome is an ASSIGNED APPEARANCE: prefrontal seeds it deterministically at agent creation by
-- calling the avatar library as a linked function, then the operator owns any later re-roll. After a
-- re-roll no derivation reproduces this row, so the store is the only truth and NOTHING may key
-- avatar artifacts by agent_id alone; caches key on genome bytes + type + version, with any future
-- mood field joining the key because mood changes geometry as well as paint.
--
-- avatar_type pins the positional layout while avatar_version selects rendering within that type.
-- Types are named for character: a redesign is a separately chosen type such as creature.wild,
-- never a numbered or silent replacement. Versions only fix procedural-generation bugs within a
-- type; version changes require explicit consent, rows retain their stored version, and renderers
-- draw that version rather than their own newest. A point-count-floor fix changed version to 2
-- without changing the layout. Version alone therefore cannot identify the bits:
-- (genome, type) makes the bits interpretable; (genome, type, version) makes them pixels.
ALTER TABLE agent ADD COLUMN avatar_genome TEXT NULL;
ALTER TABLE agent ADD COLUMN avatar_type TEXT NULL
    CHECK ((avatar_genome IS NULL) = (avatar_type IS NULL));
ALTER TABLE agent ADD COLUMN avatar_version INTEGER NULL;

-- Avatar state is part of the registry row image covered by assertion generations.
DROP TRIGGER agent_generation_on_mutation;
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
    merged_into_agent_id,
    avatar_genome,
    avatar_type,
    avatar_version
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
    OR NEW.avatar_genome IS NOT OLD.avatar_genome
    OR NEW.avatar_type IS NOT OLD.avatar_type
    OR NEW.avatar_version IS NOT OLD.avatar_version
 )
BEGIN
    UPDATE agent
       SET generation = OLD.generation + 1
     WHERE agent_id = OLD.agent_id;
END;
