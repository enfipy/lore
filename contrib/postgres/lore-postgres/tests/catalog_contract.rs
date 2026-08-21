// Copyright 2026 David
// SPDX-License-Identifier: MIT

use std::any::Any;
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Address;
use lore_base::types::Context;
use lore_base::types::Hash;
use lore_postgres::PostgresFragmentCatalog;
use lore_postgres::PostgresFragmentCatalogConfig;
use lore_storage::fragment_catalog::BeginObliteration;
use lore_storage::fragment_catalog::FragmentCatalog;
use sha2::Digest;
use sha2::Sha256;
use tokio::sync::Barrier;
use tokio_postgres::NoTls;

async fn in_test_context(future: impl Future<Output = ()>) {
    let context: Arc<dyn Any + Send + Sync> = Arc::new(());
    LORE_CONTEXT.scope(context, future).await;
}

#[tokio::test]
async fn postgres_catalog_satisfies_fragment_catalog_contract() {
    in_test_context(async {
        let catalog = PostgresFragmentCatalog::connect_for_test()
            .await
            .expect("failed to create isolated PostgreSQL catalog");
        lore_storage::fragment_catalog::contract::run(&catalog).await;
    })
    .await;
}

#[tokio::test]
async fn migration_upgrades_a_populated_v1_catalog() {
    in_test_context(async {
        let connection_string = std::env::var("LORE_POSTGRES_TEST_URL")
            .expect("LORE_POSTGRES_TEST_URL is required");
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock precedes Unix epoch")
            .as_nanos();
        let schema = format!("lore_v1_{}_{nonce}", std::process::id());
        let config = tokio_postgres::Config::from_str(&connection_string)
            .expect("invalid PostgreSQL test connection");
        let (client, connection) = config.connect(NoTls).await.expect("connect to PostgreSQL");
        let connection_task = lore_base::lore_spawn!(async move {
            connection.await.expect("PostgreSQL connection failed");
        });
        let v1 = include_str!("../migrations/0001_fragment_catalog.sql");
        let checksum: String = Sha256::digest(v1.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        client
            .batch_execute(&format!(
                "CREATE SCHEMA \"{schema}\";
                 SET search_path TO \"{schema}\";
                 CREATE TABLE schema_migrations (
                     version INTEGER PRIMARY KEY,
                     checksum TEXT NOT NULL,
                     applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                 );
                 {v1}
                 INSERT INTO schema_migrations (version, checksum) VALUES (1, '{checksum}');
                 INSERT INTO fragment_state (hash, state) VALUES (decode(repeat('42', 32), 'hex'), 0);"
            ))
            .await
            .expect("create populated v1 catalog");

        let mut catalog_config = PostgresFragmentCatalogConfig::new(connection_string);
        catalog_config.schema = schema.clone();
        PostgresFragmentCatalog::connect(catalog_config)
            .await
            .expect("upgrade v1 catalog");

        let row = client
            .query_one(
                &format!(
                    "SELECT generation > 0,
                            (SELECT count(*) FROM \"{schema}\".schema_migrations)
                     FROM \"{schema}\".fragment_state
                     WHERE hash = decode(repeat('42', 32), 'hex')"
                ),
                &[],
            )
            .await
            .expect("inspect upgraded catalog");
        assert!(row.try_get::<_, bool>(0).expect("read generation"));
        assert_eq!(row.try_get::<_, i64>(1).expect("read migration count"), 2);
        connection_task.abort();
    })
    .await;
}

#[tokio::test]
async fn stale_missing_report_cannot_erase_a_later_publication() {
    in_test_context(async {
        let catalog = PostgresFragmentCatalog::connect_for_test()
            .await
            .expect("failed to create isolated PostgreSQL catalog");
        let partition = Context::from([0x31; 16]).into();
        let address = Address {
            hash: Hash::from([0x32; 32]),
            context: Context::from([0x33; 16]),
        };

        catalog
            .publish(partition, address)
            .await
            .expect("first publish");
        let observed = catalog
            .resolve(partition, address)
            .await
            .expect("observe stored payload");
        assert!(observed.associated);
        let observed_generation = observed.generation().expect("observed generation");

        catalog
            .publish(partition, address)
            .await
            .expect("later payload confirmation");
        catalog
            .repair_missing_payload(address.hash, observed_generation)
            .await
            .expect("stale missing report");

        assert_eq!(
            catalog
                .resolve(partition, address)
                .await
                .expect("resolve after stale report")
                .state(),
            observed.state(),
            "a report based on an older observation erased a later publication"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn obliteration_marker_rejects_concurrent_associations() {
    in_test_context(async {
        let catalog = Arc::new(
            PostgresFragmentCatalog::connect_for_test()
                .await
                .expect("failed to create isolated PostgreSQL catalog"),
        );
        let partition = Context::from([0x61; 16]).into();
        let address = Address {
            hash: Hash::from([0x62; 32]),
            context: Context::from([0x63; 16]),
        };
        catalog.publish(partition, address).await.expect("publish");
        assert_eq!(
            catalog
                .begin_obliteration(partition, address)
                .await
                .expect("begin obliteration"),
            BeginObliteration::PayloadUnreferenced
        );

        let task_count = 32;
        let barrier = Arc::new(Barrier::new(task_count + 1));
        let mut tasks = Vec::with_capacity(task_count);
        for index in 0..task_count {
            let catalog = catalog.clone();
            let barrier = barrier.clone();
            tasks.push(lore_base::lore_spawn!(async move {
                let mut partition_bytes = [0x70; 16];
                partition_bytes[0] = index as u8;
                let mut context_bytes = [0x71; 16];
                context_bytes[0] = index as u8;
                barrier.wait().await;
                catalog
                    .publish(
                        Context::from(partition_bytes).into(),
                        Address {
                            hash: address.hash,
                            context: Context::from(context_bytes),
                        },
                    )
                    .await
            }));
        }
        barrier.wait().await;
        for task in tasks {
            assert!(task.await.expect("association task panicked").is_err());
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_concurrent_begin_call_claims_the_payload() {
    in_test_context(async {
        let catalog = Arc::new(
            PostgresFragmentCatalog::connect_for_test()
                .await
                .expect("failed to create isolated PostgreSQL catalog"),
        );
        let partition = Context::from([0x81; 16]).into();
        let address = Address {
            hash: Hash::from([0x82; 32]),
            context: Context::from([0x83; 16]),
        };
        catalog.publish(partition, address).await.expect("publish");

        let task_count = 32;
        let barrier = Arc::new(Barrier::new(task_count + 1));
        let mut tasks = Vec::with_capacity(task_count);
        for _ in 0..task_count {
            let catalog = catalog.clone();
            let barrier = barrier.clone();
            tasks.push(lore_base::lore_spawn!(async move {
                barrier.wait().await;
                catalog.begin_obliteration(partition, address).await
            }));
        }
        barrier.wait().await;

        let mut claims = 0;
        for task in tasks {
            match task
                .await
                .expect("begin task panicked")
                .expect("begin failed")
            {
                BeginObliteration::PayloadUnreferenced => claims += 1,
                BeginObliteration::ResumePayloadDeletion => {}
                unexpected => panic!("unexpected begin result: {unexpected:?}"),
            }
        }
        assert_eq!(claims, 1);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hash_guard_serializes_catalog_mutations() {
    in_test_context(async {
        let catalog = Arc::new(
            PostgresFragmentCatalog::connect_for_test()
                .await
                .expect("failed to create isolated PostgreSQL catalog"),
        );
        let partition = Context::from([0x91; 16]).into();
        let address = Address {
            hash: Hash::from([0x92; 32]),
            context: Context::from([0x93; 16]),
        };
        let guard = catalog
            .lock_hash(address.hash)
            .await
            .expect("acquire hash guard");
        let started = Arc::new(Barrier::new(2));
        let publisher = {
            let catalog = catalog.clone();
            let started = started.clone();
            lore_base::lore_spawn!(async move {
                started.wait().await;
                catalog.publish(partition, address).await
            })
        };

        started.wait().await;
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        assert!(
            !publisher.is_finished(),
            "a publication crossed an active guard for the same hash"
        );

        guard.unlock().await.expect("release hash guard");
        publisher
            .await
            .expect("publication task panicked")
            .expect("publish after guard release");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_hash_lock_acquisition_does_not_poison_the_pool() {
    in_test_context(async {
        let connection_string =
            std::env::var("LORE_POSTGRES_TEST_URL").expect("LORE_POSTGRES_TEST_URL is required");
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock precedes Unix epoch")
            .as_nanos();
        let mut config = PostgresFragmentCatalogConfig::new(connection_string.clone());
        config.schema = format!("lore_cancel_{}_{nonce}", std::process::id());
        config.max_connections = 1;
        let catalog = Arc::new(
            PostgresFragmentCatalog::connect(config)
                .await
                .expect("create isolated catalog"),
        );

        let pg_config = tokio_postgres::Config::from_str(&connection_string)
            .expect("invalid PostgreSQL test connection");
        let (blocker, connection) = pg_config.connect(NoTls).await.expect("connect blocker");
        let connection_task = lore_base::lore_spawn!(async move {
            connection.await.expect("blocker connection failed");
        });
        let hash = Hash::from([0xa4; 32]);
        let bytes = hash.data();
        let first = i32::from_be_bytes(bytes[..4].try_into().unwrap());
        let second = i32::from_be_bytes(bytes[4..8].try_into().unwrap());
        blocker
            .query_one("SELECT pg_advisory_lock($1, $2)", &[&first, &second])
            .await
            .expect("take blocking advisory lock");

        let acquisition = {
            let catalog = catalog.clone();
            lore_base::lore_spawn!(async move { catalog.lock_hash(hash).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        acquisition.abort();
        let _ = acquisition.await;
        blocker
            .query_one("SELECT pg_advisory_unlock($1, $2)", &[&first, &second])
            .await
            .expect("release blocking advisory lock");
        tokio::time::sleep(Duration::from_millis(50)).await;

        let guard = tokio::time::timeout(Duration::from_secs(1), catalog.lock_hash(hash))
            .await
            .expect("catalog pool was poisoned by a cancelled lock")
            .expect("acquire hash after cancellation");
        guard
            .unlock()
            .await
            .expect("release hash after cancellation");
        let available: bool = blocker
            .query_one("SELECT pg_try_advisory_lock($1, $2)", &[&first, &second])
            .await
            .expect("probe advisory lock")
            .try_get(0)
            .expect("read advisory lock probe");
        assert!(available, "cancelled acquisition leaked an advisory lock");
        blocker
            .query_one("SELECT pg_advisory_unlock($1, $2)", &[&first, &second])
            .await
            .expect("release advisory lock probe");
        connection_task.abort();
    })
    .await;
}
