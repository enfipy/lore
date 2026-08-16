// Copyright 2026 David
// SPDX-License-Identifier: MIT

use std::any::Any;
use std::future::Future;
use std::sync::Arc;

use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Address;
use lore_base::types::Context;
use lore_base::types::Hash;
use lore_postgres::PostgresFragmentCatalog;
use lore_storage::fragment_catalog::BeginObliteration;
use lore_storage::fragment_catalog::FragmentCatalog;
use tokio::sync::Barrier;

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
        catalog.publish(address.hash).await.expect("publish");
        catalog
            .associate(partition, address)
            .await
            .expect("associate");
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
                    .associate(
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
        catalog.publish(address.hash).await.expect("publish");
        catalog
            .associate(partition, address)
            .await
            .expect("associate");

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
