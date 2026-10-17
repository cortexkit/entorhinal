//! Memory context answers are assembled on the read-only connection in one WAL
//! transaction. Filesystem resolution delegates to `RegistryStore::resolve_in`,
//! the same implementation used by `RegistryStore::resolve`.
use crate::{
    agent, claim_cache::agent_where_in, memory::load_document, RegistryError, RegistryStore,
};
use entorhinal_catalog::sha256_hex;
use rusqlite::{Connection, OptionalExtension};
use serde_json::{json, Value};

fn stamp(store: &RegistryStore, conn: &Connection, incarnation: &str) -> rusqlite::Result<Value> {
    let log_position: i64 = conn.query_row("SELECT CASE WHEN state='disabled' THEN 0 ELSE last_applied_position END FROM identity_log_state WHERE id=1", [], |r|r.get(0))?;
    Ok(
        json!({"incarnation":incarnation,"generation":store.generation_from_connection(conn)?,"logPosition":log_position}),
    )
}
impl RegistryStore {
    pub fn context_project(&self, folder: &str, incarnation: &str) -> Result<Value, RegistryError> {
        self.read(|conn| Ok(self.context_project_in(conn, folder, incarnation)))?
    }
    fn context_project_in(
        &self,
        conn: &Connection,
        folder: &str,
        incarnation: &str,
    ) -> Result<Value, RegistryError> {
        let resolved = self.resolve_in(conn, folder, None)?;
        #[cfg(test)]
        checkpoint();
        let domain: Option<Option<String>> = conn
            .query_row(
                "SELECT domain FROM project WHERE project_id=?1",
                [&resolved.project_id],
                |r| r.get(0),
            )
            .optional()?;
        let project_id = domain.as_ref().map(|_| &resolved.project_id);
        let workspace = resolved.workspace_id.as_deref();
        let document = workspace.map(|w| load_document(conn,w,None)).transpose()?.flatten().map(|d| json!({"revision":d.revision,"sha256":d.sha256,"charter_bytes":d.charter.len(),"document_bytes":d.document.len()}));
        let members_digest = workspace.map(|w| {
            let ids = conn.prepare("SELECT project_id FROM project_workspace WHERE workspace_id=?1 ORDER BY project_id COLLATE BINARY")?.query_map([w],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
            Ok::<_,rusqlite::Error>(sha256_hex(ids.join("\n")))
        }).transpose()?;
        Ok(
            json!({"project_id":project_id,"workspace_id":workspace,"domain":domain.flatten(),"via":resolved.via,"document":document,"members_digest":members_digest,"stamp":stamp(self,conn,incarnation)?}),
        )
    }
    pub fn context_session(
        &self,
        agent_id: &str,
        incarnation: &str,
    ) -> Result<Value, RegistryError> {
        self.read(|conn| Ok(self.context_session_in(conn, agent_id, incarnation)))?
    }
    fn context_session_in(
        &self,
        conn: &Connection,
        id: &str,
        incarnation: &str,
    ) -> Result<Value, RegistryError> {
        let whereabouts = agent_where_in(conn, id)?;
        #[cfg(test)]
        checkpoint();
        let agent = agent::resolve_agent_in(conn, id, self.generation_from_connection(conn)?)?;
        let generations = conn.prepare("SELECT generation_id,state FROM catalog_generation WHERE agent_id=?1 AND state IN ('active','degraded','ready') ORDER BY created_at_ms,generation_id")?.query_map([id],|r| Ok(json!({"generation_id":r.get::<_,String>(0)?,"state":r.get::<_,String>(1)?})))?.collect::<rusqlite::Result<Vec<_>>>()?;
        let pins = conn.prepare("SELECT name,version,kind,manifest_sha256,archive_sha256,publisher_fingerprint FROM catalog_pin WHERE agent_id=?1 AND kind IN ('memory-profile','persona') ORDER BY name COLLATE BINARY")?.query_map([id],|r| Ok(json!({"name":r.get::<_,String>(0)?,"version":r.get::<_,String>(1)?,"kind":r.get::<_,String>(2)?,"manifest_sha256":r.get::<_,String>(3)?,"archive_sha256":r.get::<_,String>(4)?,"publisher_fingerprint":r.get::<_,String>(5)?})))?.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(
            json!({"agent":agent,"catalog_generations":generations,"pins":pins,"where":whereabouts,"stamp":stamp(self,conn,incarnation)?}),
        )
    }
}

#[cfg(test)]
thread_local! {
    pub(crate) static CHECKPOINT: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}
#[cfg(test)]
fn checkpoint() {
    let callback = CHECKPOINT.with(|c| c.borrow_mut().take());
    if let Some(callback) = callback {
        callback();
    }
}
