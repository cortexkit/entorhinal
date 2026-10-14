mod catalog_tests {
    use super::*;
    #[test]
    fn catalog_v10_migration_and_local_rebuild() {
        let scratch = TestStore::new("catalog-migration");
        let descriptor = legacy_descriptor(&scratch, "v10.db");
        drop(RegistryStore::open_with_migrations(&descriptor, &MIGRATIONS[..10]).unwrap());
        let store = RegistryStore::open(&descriptor).unwrap();
        let expected: &[(&str, &[&str])] = &[
            ("catalog_registry", &["url", "added_at_ms", "is_default"]),
            ("catalog_scope_map", &["scope", "registry_url"]),
            (
                "catalog_registry_key",
                &[
                    "registry_url",
                    "chain_index",
                    "key_id",
                    "public_key",
                    "statement",
                    "valid_from",
                ],
            ),
            (
                "catalog_feed",
                &["scope", "registry_url", "feed_seq", "etag"],
            ),
            (
                "catalog_status",
                &[
                    "name",
                    "version",
                    "registry_url",
                    "sequence",
                    "changed_at",
                    "published_at",
                    "signer_index",
                    "canonical",
                    "signature",
                ],
            ),
            (
                "catalog_publisher_grant",
                &[
                    "id",
                    "publisher_fingerprint",
                    "operation",
                    "placement_agent_id",
                    "ceiling_json",
                    "granted_at_ms",
                    "card_ref",
                    "revoked_at_ms",
                ],
            ),
            (
                "catalog_audit",
                &["id", "at_ms", "kind", "grant_id", "detail_json"],
            ),
        ];
        store
            .read(|conn| {
                for (table, columns) in expected {
                    let actual = conn
                        .prepare(&format!("PRAGMA table_info({table})"))?
                        .query_map([], |r| r.get::<_, String>(1))?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    assert_eq!(actual, *columns, "{table}");
                }
                Ok(())
            })
            .unwrap();
        store
            .catalog_registry_add(
                "https://a.test",
                &["@a".into()],
                false,
                "k",
                &"ab".repeat(32),
                1,
            )
            .unwrap();
        store.db.with_conn_fenced(|tx| {
            tx.execute_batch("INSERT INTO catalog_feed VALUES('@a','https://a.test',1,'etag');
                INSERT INTO catalog_status VALUES('@a/p','1.0.0','https://a.test',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z',0,'{}','sig');
                INSERT INTO catalog_publisher_grant VALUES('g','fp','install','agent','[]',1,NULL,NULL);
                INSERT INTO catalog_audit VALUES(1,1,'grant_insert','g','{}');")?;
            assert!(tx.execute("INSERT INTO catalog_publisher_grant VALUES('h','fp','install','agent','[]',1,NULL,NULL)",[]).is_err());
            Ok(())
        }).unwrap();
        let before = store.catalog_registries().unwrap();
        assert_eq!(store.generation().unwrap(), 0);
        let verified = serde_json::to_value(store.verify().unwrap()).unwrap();
        assert!(
            verified["replay"]["tables"].as_array().unwrap().is_empty(),
            "{verified}"
        );
        store.rebuild().unwrap();
        assert_eq!(
            serde_json::to_value(store.catalog_registries().unwrap()).unwrap(),
            serde_json::to_value(before).unwrap()
        );
        store
            .read(|conn| {
                for (table, _) in expected {
                    assert_eq!(
                        conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r
                            .get::<_, i64>(0))?,
                        1,
                        "{table}"
                    );
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(store.generation().unwrap(), 0);
    }
    #[test]
    fn catalog_scope_default_collision_and_remove() {
        let scratch = TestStore::new("catalog-lifecycle");
        let store = &scratch.store;
        store
            .catalog_registry_add(
                "https://a.test",
                &["@a".into()],
                false,
                "k",
                &"ab".repeat(32),
                1,
            )
            .unwrap();
        store
            .catalog_registry_add("https://b.test", &[], true, "k", &"ab".repeat(32), 1)
            .unwrap();
        assert_eq!(
            store.catalog_registry_for_scope("@a").unwrap().unwrap(),
            "https://a.test"
        );
        assert_eq!(
            store.catalog_registry_for_scope("@b").unwrap().unwrap(),
            "https://b.test"
        );
        assert_eq!(
            store
                .catalog_registry_add("https://c.test", &["@a".into()], false, "k", "pk", 1)
                .unwrap_err()
                .to_string(),
            "scope_already_mapped"
        );
        assert_eq!(
            store
                .catalog_registry_add("https://c.test", &[], true, "k", "pk", 1)
                .unwrap_err()
                .to_string(),
            "default_already_set"
        );
        store
            .catalog_registry_repin(
                "https://a.test",
                "k2",
                &"cd".repeat(32),
                "2026-01-01T00:00:00Z",
            )
            .unwrap();
        for (id, key) in [("k", "ef".repeat(32)), ("new", "ab".repeat(32))] {
            assert_eq!(
                store
                    .catalog_registry_repin("https://a.test", id, &key, "2026-01-01T00:00:00Z")
                    .unwrap_err()
                    .to_string(),
                "key_conflict"
            );
        }
        let keys = &store.catalog_registries().unwrap()[0].keys;
        assert_eq!(keys.len(), 2);
        assert!(keys[0].valid_from.is_none());
        assert_eq!(keys[1].valid_from.as_deref(), Some("2026-01-01T00:00:00Z"));
        store.db.with_conn_fenced(|tx| tx.execute_batch("INSERT INTO catalog_feed VALUES('@a','https://a.test',99,NULL); INSERT INTO catalog_status VALUES('@a/p','1','https://a.test',99,'t','t',0,'{}','s');")).unwrap();
        store.catalog_registry_remove("https://a.test").unwrap();
        store
            .read(|conn| {
                for table in ["catalog_feed", "catalog_status"] {
                    assert_eq!(
                        conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r
                            .get::<_, i64>(0))?,
                        0
                    );
                }
                Ok(())
            })
            .unwrap();
        store.catalog_registry_remove("https://b.test").unwrap();
        assert!(store.catalog_registry_for_scope("@a").unwrap().is_none());
        store
            .catalog_registry_add(
                "https://a.test",
                &["@a".into()],
                false,
                "replacement",
                "newpk",
                1,
            )
            .unwrap();
        assert_eq!(store.catalog_registries().unwrap()[0].keys.len(), 1);
        assert_eq!(store.generation().unwrap(), 0);
    }
}
