// Copyright 2026 David
// SPDX-License-Identifier: MIT
//! PostgreSQL mutable store with the semantics of Lore's DynamoDB mutable store.
//!
//! Every write is a committed PostgreSQL statement or transaction before it returns, so the store
//! has no write-behind buffer and `flush` has nothing to do.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use deadpool_postgres::Pool;
use lore_base::error::AddressNotFound;
use lore_base::error::SlowDown;
use lore_base::types::Address;
use lore_base::types::Hash;
use lore_base::types::KeyType;
use lore_base::types::Partition;
use lore_storage::KeyValueStream;
use lore_storage::MutableStore;
use lore_storage::StoreError;
use lore_storage::local::store_lock::StoreGuard;
use tokio_postgres::Row;
use tracing::debug;

use crate::config::PostgresStoreConfig;
use crate::connection::Migrations;
use crate::connection::database_error;

const MIGRATIONS: Migrations = Migrations {
    ledger: "mutable_store_migrations",
    lock_name: "lore-mutable-store",
    steps: &[(
        1,
        include_str!("../migrations/mutable_store/0001_mutable_value.sql"),
    )],
};

/// Lookups a zero-expected swap repeats when concurrent writers keep inserting and deleting the
/// key between its lookup and its insert.
const SWAP_ATTEMPTS: usize = 8;

/// PostgreSQL implementation of Lore's mutable store.
pub struct PostgresMutableStore {
    pool: Pool,
    schema: Arc<str>,
}

impl fmt::Debug for PostgresMutableStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresMutableStore")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl PostgresMutableStore {
    /// Connect, verify the pool, and apply mutable-store migrations transactionally.
    pub async fn connect(config: PostgresStoreConfig) -> Result<Self, StoreError> {
        config.validate().map_err(|error| {
            StoreError::internal_with_context(
                error,
                "Invalid PostgreSQL mutable store configuration",
            )
        })?;
        let pool = crate::connection::build_pool(config.pool_settings("lore-mutable-store"))?;
        let store = Self {
            pool,
            schema: Arc::from(config.schema),
        };
        drop(store.connection().await?);
        crate::connection::migrate(&store.pool, &store.schema, &MIGRATIONS).await?;
        Ok(store)
    }

    /// Create a uniquely namespaced store using `LORE_POSTGRES_TEST_URL`.
    #[doc(hidden)]
    pub async fn connect_for_test() -> Result<Self, StoreError> {
        let mut config = PostgresStoreConfig::new(crate::connection::test_connection_string()?);
        config.schema = crate::connection::test_schema("lore_mutable_test")?;
        Self::connect(config).await
    }

    /// PostgreSQL schema holding this store's table.
    pub fn schema(&self) -> &str {
        &self.schema
    }

    async fn connection(&self) -> Result<deadpool_postgres::Client, StoreError> {
        crate::connection::connection(&self.pool).await
    }

    fn table(&self) -> String {
        format!("\"{}\".mutable_value", self.schema)
    }

    /// The stored key: the key type replaces the key's first byte, as in the DynamoDB store.
    fn typed_key(mut key: Hash, key_type: KeyType) -> Hash {
        key.data_mut()[0] = key_type as u8;
        key
    }

    /// Inclusive bounds of the keys of one type.
    fn key_range(key_type: KeyType) -> ([u8; 32], [u8; 32]) {
        let mut start = [0u8; 32];
        start[0] = key_type as u8;
        let mut end = [0xFFu8; 32];
        end[0] = key_type as u8;
        (start, end)
    }

    async fn load_typed(&self, partition: Partition, key: Hash) -> Result<Hash, StoreError> {
        let row = self
            .connection()
            .await?
            .query_opt(
                &format!(
                    "SELECT value FROM {} WHERE partition = $1 AND key = $2",
                    self.table()
                ),
                &[&partition.as_ref(), &key.as_ref()],
            )
            .await
            .map_err(|error| database_error(error, "Failed to load mutable value"))?;
        match row.map(|row| hash_from_row(&row, 0)).transpose()? {
            // A stored zero reads as absent, matching the local and DynamoDB stores.
            Some(value) if !value.is_zero() => Ok(value),
            _ => Err(StoreError::from(AddressNotFound::from(
                Address::zero_context_hash(key),
            ))),
        }
    }

    async fn store_typed(
        &self,
        partition: Partition,
        key: Hash,
        value: Hash,
    ) -> Result<(), StoreError> {
        let client = self.connection().await?;
        if value.is_zero() {
            client
                .execute(
                    &format!(
                        "DELETE FROM {} WHERE partition = $1 AND key = $2",
                        self.table()
                    ),
                    &[&partition.as_ref(), &key.as_ref()],
                )
                .await
                .map_err(|error| database_error(error, "Failed to delete mutable value"))?;
        } else {
            client
                .execute(
                    &format!(
                        "INSERT INTO {} (partition, key, value) VALUES ($1, $2, $3)
                         ON CONFLICT (partition, key) DO UPDATE SET value = EXCLUDED.value",
                        self.table()
                    ),
                    &[&partition.as_ref(), &key.as_ref(), &value.as_ref()],
                )
                .await
                .map_err(|error| database_error(error, "Failed to store mutable value"))?;
        }
        Ok(())
    }

    /// Swap under a row lock and return the value seen under it, or zero for an absent key.
    ///
    /// As in the DynamoDB store, a zero `expected` matches both an absent key and a stored zero,
    /// and a non-zero `expected` never creates a key.
    async fn compare_and_swap_typed(
        &self,
        partition: Partition,
        key: Hash,
        expected: Hash,
        value: Hash,
    ) -> Result<Hash, StoreError> {
        let table = self.table();
        let mut client = self.connection().await?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| database_error(error, "Failed to begin mutable swap"))?;
        for _ in 0..SWAP_ATTEMPTS {
            let current = transaction
                .query_opt(
                    &format!(
                        "SELECT value FROM {table} WHERE partition = $1 AND key = $2 FOR UPDATE"
                    ),
                    &[&partition.as_ref(), &key.as_ref()],
                )
                .await
                .map_err(|error| database_error(error, "Failed to lock mutable value"))?
                .map(|row| hash_from_row(&row, 0))
                .transpose()?;
            let previous = match current {
                Some(current) => {
                    if current == expected {
                        transaction
                            .execute(
                                &format!(
                                    "UPDATE {table} SET value = $3
                                     WHERE partition = $1 AND key = $2"
                                ),
                                &[&partition.as_ref(), &key.as_ref(), &value.as_ref()],
                            )
                            .await
                            .map_err(|error| {
                                database_error(error, "Failed to swap mutable value")
                            })?;
                    }
                    current
                }
                None if !expected.is_zero() => Hash::default(),
                None => {
                    let inserted = transaction
                        .execute(
                            &format!(
                                "INSERT INTO {table} (partition, key, value) VALUES ($1, $2, $3)
                                 ON CONFLICT (partition, key) DO NOTHING"
                            ),
                            &[&partition.as_ref(), &key.as_ref(), &value.as_ref()],
                        )
                        .await
                        .map_err(|error| database_error(error, "Failed to insert mutable value"))?;
                    if inserted == 0 {
                        // Another writer created the key after the lookup. The next lookup is a
                        // new statement, so it sees that row and locks it.
                        debug!(%key, "Mutable swap raced a concurrent insert; retrying lookup");
                        continue;
                    }
                    Hash::default()
                }
            };
            transaction
                .commit()
                .await
                .map_err(|error| database_error(error, "Failed to commit mutable swap"))?;
            return Ok(previous);
        }
        Err(StoreError::from(SlowDown))
    }

    /// List one consistent snapshot; a null partition lists every partition.
    async fn list_typed(
        &self,
        partition: Partition,
        key_type: KeyType,
    ) -> Result<KeyValueStream, StoreError> {
        let (stream, sender) = KeyValueStream::new();
        if key_type == KeyType::Untyped {
            return Ok(stream);
        }

        let (start, end) = Self::key_range(key_type);
        let client = self.connection().await?;
        let rows = if partition.is_zero() {
            client
                .query(
                    &format!(
                        "SELECT key, value FROM {} WHERE key BETWEEN $1 AND $2
                         ORDER BY partition, key",
                        self.table()
                    ),
                    &[&start.as_slice(), &end.as_slice()],
                )
                .await
        } else {
            client
                .query(
                    &format!(
                        "SELECT key, value FROM {} WHERE partition = $1 AND key BETWEEN $2 AND $3
                         ORDER BY key",
                        self.table()
                    ),
                    &[&partition.as_ref(), &start.as_slice(), &end.as_slice()],
                )
                .await
        }
        .map_err(|error| database_error(error, "Failed to list mutable values"))?;
        drop(client);

        for row in rows {
            let key = hash_from_row(&row, 0)?;
            let value = hash_from_row(&row, 1)?;
            if value.is_zero() {
                continue;
            }
            if let Err(error) = sender.send((key, value)) {
                debug!(%error, "Mutable list receiver closed");
                break;
            }
        }
        Ok(stream)
    }
}

#[async_trait]
impl MutableStore for PostgresMutableStore {
    /// Nothing on disk to claim: the values live in PostgreSQL.
    async fn hold_for_command(self: Arc<Self>) -> Result<Option<StoreGuard>, StoreError> {
        Ok(None)
    }

    #[tracing::instrument(name = "PostgresMutableStore::load", skip(self))]
    async fn load(
        self: Arc<Self>,
        partition: Partition,
        key: Hash,
        key_type: KeyType,
    ) -> Result<Hash, StoreError> {
        self.load_typed(partition, Self::typed_key(key, key_type))
            .await
    }

    #[tracing::instrument(name = "PostgresMutableStore::store", skip(self))]
    async fn store(
        self: Arc<Self>,
        partition: Partition,
        key: Hash,
        value: Hash,
        key_type: KeyType,
    ) -> Result<(), StoreError> {
        self.store_typed(partition, Self::typed_key(key, key_type), value)
            .await
    }

    #[tracing::instrument(name = "PostgresMutableStore::compare_and_swap", skip(self))]
    async fn compare_and_swap(
        self: Arc<Self>,
        partition: Partition,
        key: Hash,
        expected: Hash,
        value: Hash,
        key_type: KeyType,
    ) -> Result<Hash, StoreError> {
        self.compare_and_swap_typed(partition, Self::typed_key(key, key_type), expected, value)
            .await
    }

    #[tracing::instrument(name = "PostgresMutableStore::list", skip(self))]
    async fn list(
        self: Arc<Self>,
        partition: Partition,
        key_type: KeyType,
    ) -> Result<KeyValueStream, StoreError> {
        self.list_typed(partition, key_type).await
    }

    async fn flush(self: Arc<Self>, _sync_data: bool) -> Result<(), StoreError> {
        // Every acknowledged write is already committed.
        Ok(())
    }
}

fn hash_from_row(row: &Row, index: usize) -> Result<Hash, StoreError> {
    let bytes: &[u8] = row
        .try_get(index)
        .map_err(|error| database_error(error, "Failed to read mutable store hash"))?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| StoreError::internal("PostgreSQL mutable store hash is not 32 bytes"))?;
    Ok(Hash::from(bytes))
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
    fn typed_key_replaces_the_first_byte() {
        let key = Hash::from([0xAB; 32]);
        let typed = PostgresMutableStore::typed_key(key, KeyType::BranchId);
        assert_eq!(typed.data()[0], KeyType::BranchId as u8);
        assert_eq!(typed.data()[1..], key.data()[1..]);
    }

    #[test]
    fn key_range_covers_exactly_one_key_type() {
        let (start, end) = PostgresMutableStore::key_range(KeyType::BranchMetadata);
        let mut below = [0xFF; 32];
        below[0] = KeyType::Untyped as u8;
        let mut above = [0u8; 32];
        above[0] = KeyType::BranchId as u8;
        let typed =
            PostgresMutableStore::typed_key(Hash::from([0x5A; 32]), KeyType::BranchMetadata);

        // bytea compares bytewise, as Rust slices do.
        assert!(start.as_slice() <= typed.as_ref() && typed.as_ref() <= end.as_slice());
        assert!(below.as_slice() < start.as_slice());
        assert!(above.as_slice() > end.as_slice());
    }
}
