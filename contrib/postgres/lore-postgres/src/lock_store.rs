// Copyright 2026 David
// SPDX-License-Identifier: MIT
//! PostgreSQL lock store with the semantics of Lore's DynamoDB lock store.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use deadpool_postgres::Pool;
use lore_base::error::LockNotFound;
use lore_base::error::LockNotOwned;
use lore_base::error::SlowDown;
use lore_base::types::BranchId;
use lore_base::types::Hash;
use lore_base::types::LockData;
use lore_base::types::LockResource;
use lore_base::types::RepositoryId;
use lore_revision::lock::LockError;
use lore_revision::lock::LockQuery;
use lore_revision::lock::LockStore;
use lore_storage::StoreError;
use tokio_postgres::Row;
use tokio_postgres::types::ToSql;
use tracing::debug;

use crate::config::PostgresStoreConfig;
use crate::connection::Migrations;
use crate::connection::database_error;

const MIGRATIONS: Migrations = Migrations {
    ledger: "lock_store_migrations",
    lock_name: "lore-lock-store",
    steps: &[(
        1,
        include_str!("../migrations/lock_store/0001_resource_lock.sql"),
    )],
};

/// Attempts a lock batch makes when locks it found held by its own owner are released while it
/// runs.
const LOCK_ATTEMPTS: usize = 8;

const LOCK_COLUMNS: &str = "branch, hash, description, owner_id, locked_at_millis";

/// PostgreSQL implementation of Lore's resource lock store.
pub struct PostgresLockStore {
    pool: Pool,
    schema: Arc<str>,
}

impl fmt::Debug for PostgresLockStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresLockStore")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

/// One requested lock per branch and hash; a repeated resource keeps its last description.
fn deduplicate(resources: &[LockResource]) -> BTreeMap<(BranchId, Hash), &LockResource> {
    resources
        .iter()
        .map(|resource| ((resource.branch, resource.hash), resource))
        .collect()
}

fn key_arrays<'a>(
    keys: impl Iterator<Item = &'a (BranchId, Hash)>,
) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    keys.map(|(branch, hash)| (branch.as_ref().to_vec(), hash.as_ref().to_vec()))
        .unzip()
}

fn lock_error(error: StoreError, context: &'static str) -> LockError {
    match error {
        StoreError::SlowDown(slow_down) => LockError::SlowDown(slow_down),
        error => LockError::internal_with_context(error, context),
    }
}

fn lock_database_error(error: tokio_postgres::Error, context: &'static str) -> LockError {
    lock_error(database_error(error, context), context)
}

impl PostgresLockStore {
    /// Connect, verify the pool, and apply lock-store migrations transactionally.
    pub async fn connect(config: PostgresStoreConfig) -> Result<Self, StoreError> {
        config.validate().map_err(|error| {
            StoreError::internal_with_context(error, "Invalid PostgreSQL lock store configuration")
        })?;
        let pool = crate::connection::build_pool(config.pool_settings("lore-lock-store"))?;
        let store = Self {
            pool,
            schema: Arc::from(config.schema),
        };
        drop(crate::connection::connection(&store.pool).await?);
        crate::connection::migrate(&store.pool, &store.schema, &MIGRATIONS).await?;
        Ok(store)
    }

    /// Create a uniquely namespaced store using `LORE_POSTGRES_TEST_URL`.
    #[doc(hidden)]
    pub async fn connect_for_test() -> Result<Self, StoreError> {
        let mut config = PostgresStoreConfig::new(crate::connection::test_connection_string()?);
        config.schema = crate::connection::test_schema("lore_lock_test")?;
        Self::connect(config).await
    }

    async fn connection(&self) -> Result<deadpool_postgres::Client, LockError> {
        crate::connection::connection(&self.pool)
            .await
            .map_err(|error| {
                lock_error(error, "Failed to acquire PostgreSQL lock store connection")
            })
    }

    fn table(&self) -> String {
        format!("\"{}\".resource_lock", self.schema)
    }

    async fn select_locks(
        &self,
        filter: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<LockData>, LockError> {
        self.connection()
            .await?
            .query(
                &format!(
                    "SELECT {LOCK_COLUMNS} FROM {} WHERE {filter}
                     ORDER BY repository, branch, hash",
                    self.table()
                ),
                params,
            )
            .await
            .map_err(|error| lock_database_error(error, "Failed to query resource locks"))?
            .iter()
            .map(lock_from_row)
            .collect()
    }
}

#[async_trait]
impl LockStore for PostgresLockStore {
    /// Lock every resource in one transaction, or none of them.
    ///
    /// As in the DynamoDB store, a resource the owner already holds is left as it is and is not
    /// returned, and a resource held by anyone else fails the whole batch with `LockNotOwned`.
    #[tracing::instrument(name = "PostgresLockStore::lock_resources", skip_all)]
    async fn lock_resources(
        &self,
        owner_id: &str,
        repository: RepositoryId,
        resources: &[LockResource],
    ) -> Result<Vec<LockData>, LockError> {
        let requested = deduplicate(resources);
        if requested.is_empty() {
            return Ok(Vec::new());
        }
        let (branches, hashes) = key_arrays(requested.keys());
        let descriptions: Vec<&str> = requested
            .values()
            .map(|resource| resource.description.as_str())
            .collect();
        let locked_at = lore_revision::util::time::timestamp();
        let locked_at_millis = i64::try_from(locked_at)
            .map_err(|error| LockError::internal_with_context(error, "Lock time out of range"))?;
        let table = self.table();

        let mut client = self.connection().await?;
        for _ in 0..LOCK_ATTEMPTS {
            let transaction = client.transaction().await.map_err(|error| {
                lock_database_error(error, "Failed to begin resource lock transaction")
            })?;
            let inserted = transaction
                .query(
                    &format!(
                        "INSERT INTO {table}
                             (repository, branch, hash, description, owner_id, locked_at_millis)
                         SELECT $1, input.branch, input.hash, input.description, $2, $3
                         FROM unnest($4::bytea[], $5::bytea[], $6::text[])
                             AS input(branch, hash, description)
                         ON CONFLICT DO NOTHING
                         RETURNING {LOCK_COLUMNS}"
                    ),
                    &[
                        &repository.as_ref(),
                        &owner_id,
                        &locked_at_millis,
                        &branches,
                        &hashes,
                        &descriptions,
                    ],
                )
                .await
                .map_err(|error| lock_database_error(error, "Failed to insert resource locks"))?;
            let locks = inserted
                .iter()
                .map(lock_from_row)
                .collect::<Result<Vec<_>, _>>()?;

            if locks.len() < requested.len() {
                let mut held = requested.clone();
                for lock in &locks {
                    held.remove(&(lock.resource.branch, lock.resource.hash));
                }
                let (held_branches, held_hashes) = key_arrays(held.keys());
                let holders = transaction
                    .query(
                        &format!(
                            "SELECT owner_id FROM {table}
                             WHERE repository = $1
                               AND (branch, hash) IN (
                                   SELECT * FROM unnest($2::bytea[], $3::bytea[])
                               )
                             FOR SHARE"
                        ),
                        &[&repository.as_ref(), &held_branches, &held_hashes],
                    )
                    .await
                    .map_err(|error| {
                        lock_database_error(error, "Failed to inspect held resource locks")
                    })?;
                for holder in &holders {
                    let holder: &str = holder.try_get(0).map_err(|error| {
                        lock_database_error(error, "Failed to read resource lock owner")
                    })?;
                    if holder != owner_id {
                        // Dropping the transaction rolls back every lock this batch inserted.
                        return Err(LockNotOwned.into());
                    }
                }
                if holders.len() < held.len() {
                    // A conflicting lock was released after the insert; try the batch again.
                    debug!(%repository, "Resource lock batch raced an unlock; retrying");
                    continue;
                }
            }

            transaction.commit().await.map_err(|error| {
                lock_database_error(error, "Failed to commit resource lock transaction")
            })?;
            return Ok(locks);
        }
        Err(SlowDown.into())
    }

    #[tracing::instrument(name = "PostgresLockStore::query_locks", skip_all)]
    async fn query_locks(&self, query: LockQuery) -> Result<Vec<LockData>, LockError> {
        match &query {
            LockQuery::Hash(hash) => self.select_locks("hash = $1", &[&hash.as_ref()]).await,
            LockQuery::HashRepository(hash, repository) => {
                self.select_locks(
                    "hash = $1 AND repository = $2",
                    &[&hash.as_ref(), &repository.as_ref()],
                )
                .await
            }
            LockQuery::HashRepositoryBranch(hash, repository, branch) => {
                self.select_locks(
                    "hash = $1 AND repository = $2 AND branch = $3",
                    &[&hash.as_ref(), &repository.as_ref(), &branch.as_ref()],
                )
                .await
            }
            LockQuery::Owner(owner) => self.select_locks("owner_id = $1", &[&owner.as_str()]).await,
            LockQuery::OwnerRepository(owner, repository) => {
                self.select_locks(
                    "owner_id = $1 AND repository = $2",
                    &[&owner.as_str(), &repository.as_ref()],
                )
                .await
            }
            LockQuery::OwnerRepositoryBranch(owner, repository, branch) => {
                self.select_locks(
                    "owner_id = $1 AND repository = $2 AND branch = $3",
                    &[&owner.as_str(), &repository.as_ref(), &branch.as_ref()],
                )
                .await
            }
            LockQuery::Repository(repository) => {
                self.select_locks("repository = $1", &[&repository.as_ref()])
                    .await
            }
            LockQuery::RepositoryBranch(repository, branch) => {
                self.select_locks(
                    "repository = $1 AND branch = $2",
                    &[&repository.as_ref(), &branch.as_ref()],
                )
                .await
            }
            LockQuery::RepositoryBranchDescription(repository, branch, description) => {
                self.select_locks(
                    "repository = $1 AND branch = $2 AND description = $3",
                    &[
                        &repository.as_ref(),
                        &branch.as_ref(),
                        &description.as_str(),
                    ],
                )
                .await
            }
        }
    }

    #[tracing::instrument(name = "PostgresLockStore::check_locks_status", skip_all)]
    async fn check_locks_status(
        &self,
        repository: RepositoryId,
        resources: &[LockResource],
    ) -> Result<Vec<LockData>, LockError> {
        let requested = deduplicate(resources);
        if requested.is_empty() {
            return Ok(Vec::new());
        }
        let (branches, hashes) = key_arrays(requested.keys());
        self.select_locks(
            "repository = $1
             AND (branch, hash) IN (SELECT * FROM unnest($2::bytea[], $3::bytea[]))",
            &[&repository.as_ref(), &branches, &hashes],
        )
        .await
    }

    /// Unlock every resource in one transaction, or none of them.
    ///
    /// As in the DynamoDB store, `validate_user` fails the batch with `LockNotOwned` when another
    /// owner holds a resource and with `LockNotFound` when a resource is not locked; without it,
    /// unlocking a resource that is not locked succeeds.
    #[tracing::instrument(name = "PostgresLockStore::unlock_resources", skip_all)]
    async fn unlock_resources(
        &self,
        owner_id: &str,
        validate_user: bool,
        repository: RepositoryId,
        resources: &[LockResource],
    ) -> Result<Vec<LockResource>, LockError> {
        let mut resources = resources.to_vec();
        resources.sort();
        resources.dedup();
        let keys: Vec<(BranchId, Hash)> = resources
            .iter()
            .map(|resource| (resource.branch, resource.hash))
            .collect();
        let (branches, hashes) = key_arrays(keys.iter());
        let table = self.table();

        let mut client = self.connection().await?;
        let transaction = client.transaction().await.map_err(|error| {
            lock_database_error(error, "Failed to begin resource unlock transaction")
        })?;
        let removed = transaction
            .query(
                &format!(
                    "DELETE FROM {table}
                     WHERE repository = $1
                       AND (branch, hash) IN (SELECT * FROM unnest($2::bytea[], $3::bytea[]))
                     RETURNING owner_id"
                ),
                &[&repository.as_ref(), &branches, &hashes],
            )
            .await
            .map_err(|error| lock_database_error(error, "Failed to delete resource locks"))?;
        if validate_user {
            for holder in &removed {
                let holder: &str = holder.try_get(0).map_err(|error| {
                    lock_database_error(error, "Failed to read resource lock owner")
                })?;
                if holder != owner_id {
                    // Dropping the transaction restores every lock this batch deleted.
                    return Err(LockNotOwned.into());
                }
            }
            let mut distinct_keys = keys;
            distinct_keys.dedup();
            if removed.len() < distinct_keys.len() {
                return Err(LockNotFound.into());
            }
        }
        transaction.commit().await.map_err(|error| {
            lock_database_error(error, "Failed to commit resource unlock transaction")
        })?;
        Ok(resources)
    }
}

fn lock_from_row(row: &Row) -> Result<LockData, LockError> {
    let branch: &[u8] = row
        .try_get(0)
        .map_err(|error| lock_database_error(error, "Failed to read lock branch"))?;
    let hash: &[u8] = row
        .try_get(1)
        .map_err(|error| lock_database_error(error, "Failed to read lock hash"))?;
    let description: String = row
        .try_get(2)
        .map_err(|error| lock_database_error(error, "Failed to read lock description"))?;
    let owner: String = row
        .try_get(3)
        .map_err(|error| lock_database_error(error, "Failed to read lock owner"))?;
    let locked_at: i64 = row
        .try_get(4)
        .map_err(|error| lock_database_error(error, "Failed to read lock time"))?;
    let branch: [u8; 16] = branch
        .try_into()
        .map_err(|_| LockError::internal("PostgreSQL lock branch is not 16 bytes"))?;
    let hash: [u8; 32] = hash
        .try_into()
        .map_err(|_| LockError::internal("PostgreSQL lock hash is not 32 bytes"))?;
    Ok(LockData {
        resource: LockResource {
            branch: BranchId::from(branch),
            hash: Hash::from(hash),
            description,
        },
        owner,
        locked_at: u64::try_from(locked_at)
            .map_err(|error| LockError::internal_with_context(error, "Lock time out of range"))?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_checksum_is_sha256() {
        for &(_, sql) in MIGRATIONS.steps {
            assert_eq!(crate::connection::migration_checksum(sql).len(), 64);
        }
    }

    #[test]
    fn deduplicate_keeps_one_lock_per_branch_and_hash() {
        let resource = |description: &str| LockResource {
            branch: BranchId::from([1; 16]),
            hash: Hash::from([2; 32]),
            description: description.to_string(),
        };
        let other = LockResource {
            hash: Hash::from([3; 32]),
            ..resource("Other.umap")
        };
        let resources = [resource("Map.umap"), other, resource("Renamed.umap")];

        let requested = deduplicate(&resources);
        assert_eq!(requested.len(), 2);
        assert_eq!(
            requested[&(BranchId::from([1; 16]), Hash::from([2; 32]))].description,
            "Renamed.umap"
        );
    }
}
