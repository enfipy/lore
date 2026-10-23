// Copyright 2026 David
// SPDX-License-Identifier: MIT

use std::any::Any;
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;

use futures::StreamExt;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Hash;
use lore_base::types::KeyType;
use lore_base::types::Partition;
use lore_postgres::PostgresMutableStore;
use lore_postgres::PostgresStoreConfig;
use lore_storage::MutableStore;
use lore_storage::mutable_conformance::Capabilities;
use lore_storage::mutable_conformance::verify_mutable_store;
use tokio::sync::Barrier;

async fn in_test_context(future: impl Future<Output = ()>) {
    let context: Arc<dyn Any + Send + Sync> = Arc::new(());
    LORE_CONTEXT.scope(context, future).await;
}

async fn store() -> Arc<PostgresMutableStore> {
    Arc::new(
        PostgresMutableStore::connect_for_test()
            .await
            .expect("failed to create isolated PostgreSQL mutable store"),
    )
}

fn test_url() -> String {
    std::env::var("LORE_POSTGRES_TEST_URL").expect("LORE_POSTGRES_TEST_URL is required")
}

/// Another Lore server on the same schema as `store`.
async fn connect_to_schema(
    connection_string: String,
    store: &PostgresMutableStore,
) -> Arc<PostgresMutableStore> {
    let mut config = PostgresStoreConfig::new(connection_string);
    config.schema = store.schema().to_string();
    Arc::new(
        PostgresMutableStore::connect(config)
            .await
            .expect("connect to an existing mutable store schema"),
    )
}

fn nonzero_hash() -> Hash {
    let mut bytes = rand::random::<[u8; 32]>();
    bytes[1] |= 1;
    Hash::from(bytes)
}

fn partition() -> Partition {
    Partition::from(rand::random::<[u8; 16]>())
}

#[tokio::test]
async fn postgres_mutable_store_satisfies_conformance_battery() {
    in_test_context(async {
        verify_mutable_store(store().await, Capabilities::new("PostgresMutableStore")).await;
    })
    .await;
}

/// The DynamoDB store's regression: `branch::create` writes a zero through compare-and-swap, and
/// the first push then swaps from zero. That swap must happen rather than report a false success.
#[tokio::test]
async fn zero_expected_swap_replaces_a_stored_zero() {
    in_test_context(async {
        let store = store().await;
        let (partition, key, value) = (partition(), nonzero_hash(), nonzero_hash());

        let previous = store
            .clone()
            .compare_and_swap(
                partition,
                key,
                Hash::default(),
                Hash::default(),
                KeyType::BranchId,
            )
            .await
            .expect("initialize the key with zero");
        assert_eq!(previous, Hash::default());
        assert!(
            store
                .clone()
                .load(partition, key, KeyType::BranchId)
                .await
                .expect_err("a stored zero reads as absent")
                .is_address_not_found()
        );

        let previous = store
            .clone()
            .compare_and_swap(partition, key, Hash::default(), value, KeyType::BranchId)
            .await
            .expect("swap from the stored zero");
        assert_eq!(previous, Hash::default());
        assert_eq!(
            store
                .clone()
                .load(partition, key, KeyType::BranchId)
                .await
                .expect("load the swapped value"),
            value
        );

        let stale = nonzero_hash();
        let previous = store
            .clone()
            .compare_and_swap(partition, key, Hash::default(), stale, KeyType::BranchId)
            .await
            .expect("a zero-expected swap over a live value reports it");
        assert_eq!(previous, value);
    })
    .await;
}

/// A non-zero value stored with compare-and-swap reports a stored zero, not an absent key, to the
/// next swap that expects something else.
#[tokio::test]
async fn failed_swap_reports_a_stored_zero() {
    in_test_context(async {
        let store = store().await;
        let (partition, key) = (partition(), nonzero_hash());
        store
            .clone()
            .compare_and_swap(
                partition,
                key,
                Hash::default(),
                Hash::default(),
                KeyType::BranchId,
            )
            .await
            .expect("initialize the key with zero");

        let previous = store
            .clone()
            .compare_and_swap(
                partition,
                key,
                nonzero_hash(),
                nonzero_hash(),
                KeyType::BranchId,
            )
            .await
            .expect("mismatched swap");
        assert_eq!(previous, Hash::default());
        let listed: Vec<_> = store
            .clone()
            .list(partition, KeyType::BranchId)
            .await
            .expect("list")
            .collect()
            .await;
        assert!(listed.is_empty(), "a stored zero is not listed: {listed:?}");
    })
    .await;
}

#[tokio::test]
async fn concurrent_swaps_from_one_value_admit_exactly_one_writer() {
    in_test_context(async {
        const WRITERS: usize = 32;
        let store = store().await;
        let (partition, key) = (partition(), nonzero_hash());
        let barrier = Arc::new(Barrier::new(WRITERS));
        let mut tasks = Vec::with_capacity(WRITERS);
        for _ in 0..WRITERS {
            let store = store.clone();
            let barrier = barrier.clone();
            let value = nonzero_hash();
            tasks.push(lore_base::lore_spawn!(async move {
                barrier.wait().await;
                let previous = store
                    .compare_and_swap(partition, key, Hash::default(), value, KeyType::BranchId)
                    .await
                    .expect("concurrent swap");
                (previous, value)
            }));
        }

        let mut winners = Vec::new();
        for task in tasks {
            let (previous, value) = task.await.expect("swap task");
            if previous == Hash::default() {
                winners.push(value);
            }
        }
        assert_eq!(winners.len(), 1, "exactly one swap from absent may succeed");
        assert_eq!(
            store
                .clone()
                .load(partition, key, KeyType::BranchId)
                .await
                .expect("load the winning value"),
            winners[0]
        );
    })
    .await;
}

#[tokio::test]
async fn concurrent_swap_chains_lose_no_update() {
    in_test_context(async {
        const WRITERS: usize = 16;
        const STEPS: u64 = 8;
        let store = store().await;
        let (partition, key) = (partition(), nonzero_hash());
        let barrier = Arc::new(Barrier::new(WRITERS));
        let mut tasks = Vec::with_capacity(WRITERS);
        for _ in 0..WRITERS {
            let store = store.clone();
            let barrier = barrier.clone();
            tasks.push(lore_base::lore_spawn!(async move {
                barrier.wait().await;
                for _ in 0..STEPS {
                    let mut expected = Hash::default();
                    loop {
                        let next = Hash::from_u64(expected.to_u64() + 1);
                        let previous = store
                            .clone()
                            .compare_and_swap(partition, key, expected, next, KeyType::BranchId)
                            .await
                            .expect("counter swap");
                        if previous == expected {
                            break;
                        }
                        expected = previous;
                    }
                }
            }));
        }
        for task in tasks {
            task.await.expect("counter task");
        }
        let total = store
            .clone()
            .load(partition, key, KeyType::BranchId)
            .await
            .expect("load counter");
        assert_eq!(total.to_u64(), WRITERS as u64 * STEPS);
    })
    .await;
}

/// No write-behind buffer: a second server sees each acknowledged write at once.
#[tokio::test]
async fn acknowledged_writes_are_visible_to_another_server() {
    in_test_context(async {
        let first = store().await;
        let second = connect_to_schema(test_url(), &first).await;
        let (partition, key, value) = (partition(), nonzero_hash(), nonzero_hash());

        first
            .clone()
            .store(partition, key, value, KeyType::BranchMetadata)
            .await
            .expect("store on the first server");
        assert_eq!(
            second
                .clone()
                .load(partition, key, KeyType::BranchMetadata)
                .await
                .expect("load on the second server"),
            value
        );

        let next = nonzero_hash();
        assert_eq!(
            second
                .clone()
                .compare_and_swap(partition, key, value, next, KeyType::BranchMetadata)
                .await
                .expect("swap on the second server"),
            value
        );
        let listed: Vec<_> = first
            .clone()
            .list(partition, KeyType::BranchMetadata)
            .await
            .expect("list on the first server")
            .collect()
            .await;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].1, next);
        // The listed key is the stored, typed key, as the DynamoDB store reports it.
        assert_eq!(listed[0].0.data()[0], KeyType::BranchMetadata as u8);
        assert_eq!(listed[0].0.data()[1..], key.data()[1..]);
    })
    .await;
}

/// A multi-host connection string skips a host that is down and lands on the writable one.
#[tokio::test]
async fn multi_host_connection_string_reaches_the_writable_host() {
    in_test_context(async {
        let parsed = tokio_postgres::Config::from_str(&test_url())
            .expect("invalid PostgreSQL test connection");
        let host = match parsed.get_hosts().first() {
            Some(tokio_postgres::config::Host::Tcp(host)) => host.clone(),
            other => panic!("test connection must use a TCP host, got {other:?}"),
        };
        let port = parsed.get_ports().first().copied().unwrap_or(5432);
        let mut multi_host = format!(
            "host=127.0.0.1,{host} port=1,{port} target_session_attrs=read-write sslmode=disable"
        );
        if let Some(user) = parsed.get_user() {
            multi_host.push_str(&format!(" user={user}"));
        }
        if let Some(password) = parsed.get_password() {
            multi_host.push_str(&format!(
                " password={}",
                String::from_utf8(password.to_vec()).expect("UTF-8 password")
            ));
        }
        if let Some(dbname) = parsed.get_dbname() {
            multi_host.push_str(&format!(" dbname={dbname}"));
        }

        let first = store().await;
        let store = connect_to_schema(multi_host, &first).await;
        let (partition, key, value) = (partition(), nonzero_hash(), nonzero_hash());
        store
            .clone()
            .store(partition, key, value, KeyType::BranchId)
            .await
            .expect("store through the multi-host pool");
        assert_eq!(
            first
                .clone()
                .load(partition, key, KeyType::BranchId)
                .await
                .expect("load through the single-host pool"),
            value
        );
    })
    .await;
}
