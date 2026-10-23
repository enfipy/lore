// Copyright 2026 David
// SPDX-License-Identifier: MIT

use std::any::Any;
use std::future::Future;
use std::sync::Arc;

use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::BranchId;
use lore_base::types::Hash;
use lore_base::types::LockResource;
use lore_base::types::RepositoryId;
use lore_postgres::PostgresLockStore;
use lore_revision::lock::LockError;
use lore_revision::lock::LockQuery;
use lore_revision::lock::LockStore;
use tokio::sync::Barrier;

async fn in_test_context(future: impl Future<Output = ()>) {
    let context: Arc<dyn Any + Send + Sync> = Arc::new(());
    LORE_CONTEXT.scope(context, future).await;
}

async fn store() -> Arc<PostgresLockStore> {
    Arc::new(
        PostgresLockStore::connect_for_test()
            .await
            .expect("failed to create isolated PostgreSQL lock store"),
    )
}

fn resource(branch: BranchId, description: &str) -> LockResource {
    LockResource {
        branch,
        hash: Hash::from(rand::random::<[u8; 32]>()),
        description: description.to_string(),
    }
}

fn ids() -> (RepositoryId, BranchId) {
    (
        RepositoryId::from(rand::random::<[u8; 16]>()),
        BranchId::from(rand::random::<[u8; 16]>()),
    )
}

#[tokio::test]
async fn lock_batches_are_all_or_nothing() {
    in_test_context(async {
        let store = store().await;
        let (repository, branch) = ids();
        let contended = resource(branch, "Contended.umap");
        let free = resource(branch, "Free.umap");

        let locks = store
            .lock_resources("user_a", repository, std::slice::from_ref(&contended))
            .await
            .expect("the first owner takes the lock");
        assert_eq!(locks.len(), 1);
        assert_eq!(locks[0].owner, "user_a");
        assert_eq!(locks[0].resource, contended);
        assert!(locks[0].locked_at > 0);

        let error = store
            .lock_resources("user_b", repository, &[free.clone(), contended.clone()])
            .await
            .expect_err("another owner is refused");
        assert!(matches!(error, LockError::LockNotOwned(_)), "{error:?}");

        let held = store
            .query_locks(LockQuery::Repository(repository))
            .await
            .expect("query held locks");
        assert_eq!(held.len(), 1, "the refused batch left a lock behind");
        assert_eq!(held[0].owner, "user_a");

        // Relocking what the owner holds succeeds and reports only the new locks.
        let locks = store
            .lock_resources("user_a", repository, &[contended.clone(), free.clone()])
            .await
            .expect("the owner extends its batch");
        assert_eq!(locks.len(), 1);
        assert_eq!(locks[0].resource, free);
    })
    .await;
}

#[tokio::test]
async fn queries_select_by_every_key() {
    in_test_context(async {
        let store = store().await;
        let (repository, branch) = ids();
        let (_, other_branch) = ids();
        let map = resource(branch, "Map.umap");
        let mesh = resource(other_branch, "Mesh.uasset");
        store
            .lock_resources("user_a", repository, std::slice::from_ref(&map))
            .await
            .expect("lock map");
        store
            .lock_resources("user_b", repository, std::slice::from_ref(&mesh))
            .await
            .expect("lock mesh");

        let owners = |locks: Vec<lore_base::types::LockData>| {
            let mut owners: Vec<String> = locks.into_iter().map(|lock| lock.owner).collect();
            owners.sort();
            owners
        };
        let cases = [
            (LockQuery::Hash(map.hash), vec!["user_a"]),
            (
                LockQuery::HashRepository(map.hash, repository),
                vec!["user_a"],
            ),
            (
                LockQuery::HashRepositoryBranch(map.hash, repository, other_branch),
                vec![],
            ),
            (LockQuery::Owner("user_b".to_string()), vec!["user_b"]),
            (
                LockQuery::OwnerRepository("user_b".to_string(), repository),
                vec!["user_b"],
            ),
            (
                LockQuery::OwnerRepositoryBranch("user_b".to_string(), repository, branch),
                vec![],
            ),
            (LockQuery::Repository(repository), vec!["user_a", "user_b"]),
            (
                LockQuery::RepositoryBranch(repository, other_branch),
                vec!["user_b"],
            ),
            (
                LockQuery::RepositoryBranchDescription(repository, branch, "Map.umap".to_string()),
                vec!["user_a"],
            ),
        ];
        for (query, expected) in cases {
            let locks = store.query_locks(query.clone()).await.expect("query locks");
            assert_eq!(owners(locks), expected, "{query:?}");
        }

        let status = store
            .check_locks_status(
                repository,
                &[map.clone(), resource(branch, "Unlocked.umap"), map.clone()],
            )
            .await
            .expect("check lock status");
        assert_eq!(status.len(), 1);
        assert_eq!(status[0].resource, map);
    })
    .await;
}

#[tokio::test]
async fn unlock_checks_the_owner_unless_told_not_to() {
    in_test_context(async {
        let store = store().await;
        let (repository, branch) = ids();
        let map = resource(branch, "Map.umap");
        let mesh = resource(branch, "Mesh.uasset");
        store
            .lock_resources("user_a", repository, &[map.clone(), mesh.clone()])
            .await
            .expect("lock both");

        let error = store
            .unlock_resources("user_b", true, repository, std::slice::from_ref(&map))
            .await
            .expect_err("another owner cannot unlock");
        assert!(matches!(error, LockError::LockNotOwned(_)), "{error:?}");

        let missing = resource(branch, "Missing.umap");
        let error = store
            .unlock_resources("user_a", true, repository, &[map.clone(), missing.clone()])
            .await
            .expect_err("a batch with an unlocked resource fails");
        assert!(matches!(error, LockError::LockNotFound(_)), "{error:?}");
        assert_eq!(
            store
                .check_locks_status(repository, std::slice::from_ref(&map))
                .await
                .expect("status")
                .len(),
            1,
            "a failed unlock batch released a lock"
        );

        let unlocked = store
            .unlock_resources("user_a", true, repository, &[map.clone(), map.clone()])
            .await
            .expect("the owner unlocks");
        assert_eq!(unlocked, vec![map.clone()]);

        // Without validation the lock goes regardless of owner, and absent locks are ignored.
        let unlocked = store
            .unlock_resources("admin", false, repository, &[mesh.clone(), missing])
            .await
            .expect("unvalidated unlock");
        assert_eq!(unlocked.len(), 2);
        assert!(
            store
                .query_locks(LockQuery::Repository(repository))
                .await
                .expect("query")
                .is_empty()
        );
    })
    .await;
}

#[tokio::test]
async fn concurrent_owners_race_for_one_lock() {
    in_test_context(async {
        const OWNERS: usize = 32;
        let store = store().await;
        let (repository, branch) = ids();
        let contended = resource(branch, "Contended.umap");
        let barrier = Arc::new(Barrier::new(OWNERS));
        let mut tasks = Vec::with_capacity(OWNERS);
        for owner in 0..OWNERS {
            let store = store.clone();
            let barrier = barrier.clone();
            let contended = contended.clone();
            tasks.push(lore_base::lore_spawn!(async move {
                barrier.wait().await;
                store
                    .lock_resources(&format!("user_{owner}"), repository, &[contended])
                    .await
            }));
        }
        let mut winners = 0;
        for task in tasks {
            match task.await.expect("lock task") {
                Ok(locks) => {
                    assert_eq!(locks.len(), 1);
                    winners += 1;
                }
                Err(error) => assert!(matches!(error, LockError::LockNotOwned(_)), "{error:?}"),
            }
        }
        assert_eq!(winners, 1);
    })
    .await;
}
