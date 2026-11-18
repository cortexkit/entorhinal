//! Journal-derived mapping from an identity-log author to a daemon box id.

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{claim_cache, mutations::append, shared_entry, RegistryError, RegistryStore};

pub(crate) const V25_MEMBER_BOX: &str = "CREATE TABLE member_box (
    author TEXT PRIMARY KEY NOT NULL CHECK(length(author)=32 AND author NOT GLOB '*[^0-9a-f]*'),
    box_id TEXT NOT NULL CHECK(length(box_id)=32 AND box_id NOT GLOB '*[^0-9a-f]*')
);";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberBox {
    pub author: String,
    pub box_id: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    op: String,
    #[serde(flatten)]
    row: MemberBox,
}

fn invalid(message: &str) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(message.to_owned())))
}

fn valid_id(id: &str) -> bool {
    id.len() == 32
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

impl MemberBox {
    pub(crate) fn validate(&self) -> rusqlite::Result<()> {
        if !valid_id(&self.author) || !valid_id(&self.box_id) {
            return Err(invalid("invalid member box author or box id"));
        }
        Ok(())
    }

    pub(crate) fn restore(&self, tx: &Transaction<'_>) -> rusqlite::Result<()> {
        self.validate()?;
        tx.execute(
            "INSERT INTO member_box(author,box_id) VALUES(?1,?2) ON CONFLICT(author) DO UPDATE SET box_id=excluded.box_id",
            [&self.author, &self.box_id],
        )?;
        Ok(())
    }
}

pub(crate) fn decode(value: Value, log_author: Option<&str>) -> rusqlite::Result<MemberBox> {
    let entry: Entry = serde_json::from_value(value).map_err(shared_entry::decode_error)?;
    if entry.op != "member.set_box" {
        return Err(invalid("invalid member box op"));
    }
    entry.row.validate()?;
    if log_author.is_some_and(|author| author != entry.row.author) {
        return Err(invalid("member box author differs from log author"));
    }
    Ok(entry.row)
}

pub(crate) fn replay(tx: &Transaction<'_>, value: Value) -> rusqlite::Result<()> {
    decode(value, None)?.restore(tx)
}

pub(crate) fn lookup(conn: &Connection, author: Option<&str>) -> rusqlite::Result<Option<String>> {
    if let Some(author) = author {
        conn.query_row(
            "SELECT box_id FROM member_box WHERE author=?1",
            [author],
            |r| r.get(0),
        )
        .optional()
    } else {
        Ok(None)
    }
}

pub(crate) fn own_row(conn: &Connection) -> rusqlite::Result<Option<MemberBox>> {
    conn.query_row(
        "SELECT author,box_id FROM member_box WHERE author=(SELECT self_author FROM self_identity WHERE id=1)",
        [],
        |r| Ok(MemberBox { author: r.get(0)?, box_id: r.get(1)? }),
    ).optional()
}

impl RegistryStore {
    pub fn own_member_box(&self) -> Result<Option<String>, RegistryError> {
        Ok(self.read(own_row)?.map(|row| row.box_id))
    }

    pub fn learn_verified_self_author(
        &self,
        author: &str,
        now: i64,
    ) -> Result<bool, RegistryError> {
        if !valid_id(author) {
            return Err(invalid("invalid self author").into());
        }
        self.with_where_changes(|tx| claim_cache::learn_self_author(tx, author, now))
            .map_err(RegistryError::Store)
    }

    pub fn set_own_member_box(&self, box_id: String) -> Result<Vec<u8>, RegistryError> {
        let error = std::cell::RefCell::new(None);
        self.with_where_changes(|tx| {
            let write = || -> Result<_, RegistryError> {
                let author =
                    claim_cache::self_author(tx)?.ok_or_else(|| invalid("self author unknown"))?;
                let row = MemberBox { author, box_id };
                row.validate()?;
                let body = shared_entry::canonical_bytes(&Entry {
                    op: "member.set_box".into(),
                    row: row.clone(),
                });
                let seq = append(
                    tx,
                    "member.set_box",
                    &serde_json::from_slice(&body).map_err(shared_entry::decode_error)?,
                    "entorhinal",
                    None,
                    crate::now_unix_millis(),
                    "entorhinal",
                )?;
                row.restore(tx)?;
                tx.execute(
                    "UPDATE registry_journal SET stream='shared',entry=?1 WHERE seq=?2",
                    params![body, seq],
                )?;
                crate::pending::finish(tx, Some(seq))?;
                Ok(Vec::new())
            };
            write().map_err(|cause| {
                let sql_error = invalid(&cause.to_string());
                *error.borrow_mut() = Some(cause);
                sql_error
            })
        })
        .map_err(|cause| error.into_inner().unwrap_or(RegistryError::Store(cause)))
    }
}
