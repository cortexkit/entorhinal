//! Pins how the bundled SQLite is compiled. `.cargo/config.toml` sets
//! `LIBSQLITE3_FLAGS=-DSQLITE_DEFAULT_MEMSTATUS=0` to turn off SQLite's
//! process-wide allocation counter, whose mutex parallel tests contend on. The
//! flag only reaches the build through that config file, so a moved or
//! deleted file would silently bring the cost back; this test fails instead.

#[test]
fn bundled_sqlite_is_built_without_memory_accounting() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    let used: i64 = conn
        .query_row(
            "SELECT sqlite_compileoption_used('DEFAULT_MEMSTATUS=0')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        used, 1,
        "SQLITE_DEFAULT_MEMSTATUS=0 is missing; check LIBSQLITE3_FLAGS in .cargo/config.toml"
    );
}
