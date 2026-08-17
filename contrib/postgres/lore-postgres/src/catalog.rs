// Copyright 2026 David
// SPDX-License-Identifier: MIT

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use deadpool_postgres::Manager;
use deadpool_postgres::ManagerConfig;
use deadpool_postgres::Pool;
use deadpool_postgres::RecyclingMethod;
use deadpool_postgres::Runtime;
use lore_base::error::SlowDown;
use lore_base::types::Address;
use lore_base::types::Hash;
use lore_base::types::Partition;
use lore_storage::StoreError;
use lore_storage::fragment_catalog::BeginObliteration;
use lore_storage::fragment_catalog::CatalogGeneration;
use lore_storage::fragment_catalog::CatalogPublication;
use lore_storage::fragment_catalog::CatalogResolution;
use lore_storage::fragment_catalog::FragmentCatalog;
use lore_storage::fragment_catalog::FragmentState;
use serde::Deserialize;
use sha2::Digest;
use sha2::Sha256;
use tokio_postgres::Row;
use tokio_postgres::config::SslMode;
use tokio_postgres::error::SqlState;
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::warn;

const DEFAULT_SCHEMA: &str = "lore";
const DEFAULT_MAX_CONNECTIONS: usize = 16;
const DEFAULT_CONNECT_TIMEOUT_MILLIS: u64 = 5_000;
const DEFAULT_BATCH_SIZE: usize = 10_000;
const MIGRATIONS: &[(i32, &str)] = &[
    (1, include_str!("../migrations/0001_fragment_catalog.sql")),
    (
        2,
        include_str!("../migrations/0002_fragment_generation.sql"),
    ),
];

#[derive(Debug, thiserror::Error)]
#[error("{details}")]
struct NativeCertificateErrors {
    details: String,
    #[source]
    first: Option<rustls_native_certs::Error>,
}

impl From<Vec<rustls_native_certs::Error>> for NativeCertificateErrors {
    fn from(errors: Vec<rustls_native_certs::Error>) -> Self {
        let details = errors
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ");
        Self {
            details: format!("{} native certificate error(s): {details}", errors.len()),
            first: errors.into_iter().next(),
        }
    }
}

/// Invalid PostgreSQL fragment-catalog configuration.
#[derive(Debug, thiserror::Error)]
pub enum PostgresFragmentCatalogConfigError {
    /// Schema is not a safe unquoted SQL identifier.
    #[error("PostgreSQL schema must be a 1-63 character SQL identifier")]
    InvalidSchema,
    /// A pool with no connections cannot serve catalog operations.
    #[error("PostgreSQL max_connections must be greater than zero")]
    ZeroConnections,
    /// A zero-sized batch cannot resolve any address.
    #[error("PostgreSQL max_batch_size must be greater than zero")]
    ZeroBatchSize,
}

fn default_schema() -> String {
    DEFAULT_SCHEMA.to_string()
}

fn default_max_connections() -> usize {
    DEFAULT_MAX_CONNECTIONS
}

fn default_connect_timeout_millis() -> u64 {
    DEFAULT_CONNECT_TIMEOUT_MILLIS
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

/// Connection and namespace settings for a PostgreSQL fragment catalog.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresFragmentCatalogConfig {
    /// libpq-style connection string or PostgreSQL URL.
    #[serde(alias = "url")]
    pub connection_string: String,
    /// Isolated PostgreSQL schema owned by this Lore deployment.
    #[serde(default = "default_schema")]
    pub schema: String,
    /// Maximum number of pooled PostgreSQL connections.
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    /// Connection and pool checkout timeout.
    #[serde(default = "default_connect_timeout_millis")]
    pub connect_timeout_millis: u64,
    /// Maximum address count accepted by one batch lookup.
    #[serde(default = "default_batch_size")]
    pub max_batch_size: usize,
}

impl fmt::Debug for PostgresFragmentCatalogConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresFragmentCatalogConfig")
            .field("connection_string", &"<redacted>")
            .field("schema", &self.schema)
            .field("max_connections", &self.max_connections)
            .field("connect_timeout_millis", &self.connect_timeout_millis)
            .field("max_batch_size", &self.max_batch_size)
            .finish()
    }
}

impl PostgresFragmentCatalogConfig {
    /// Create settings for a connection string using production-safe defaults.
    pub fn new(connection_string: String) -> Self {
        Self {
            connection_string,
            schema: default_schema(),
            max_connections: default_max_connections(),
            connect_timeout_millis: default_connect_timeout_millis(),
            max_batch_size: default_batch_size(),
        }
    }

    /// Validate settings without opening a database connection.
    pub fn validate(&self) -> Result<(), PostgresFragmentCatalogConfigError> {
        validate_identifier(&self.schema)?;
        if self.max_connections == 0 {
            return Err(PostgresFragmentCatalogConfigError::ZeroConnections);
        }
        if self.max_batch_size == 0 {
            return Err(PostgresFragmentCatalogConfigError::ZeroBatchSize);
        }
        Ok(())
    }
}

/// PostgreSQL implementation of Lore's fragment lifecycle and association catalog.
#[derive(Clone)]
pub struct PostgresFragmentCatalog {
    pool: Pool,
    schema: Arc<str>,
    max_batch_size: usize,
}

impl fmt::Debug for PostgresFragmentCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresFragmentCatalog")
            .field("schema", &self.schema)
            .field("max_batch_size", &self.max_batch_size)
            .finish_non_exhaustive()
    }
}

impl PostgresFragmentCatalog {
    /// Connect, verify the pool, and apply catalog migrations transactionally.
    pub async fn connect(config: PostgresFragmentCatalogConfig) -> Result<Self, StoreError> {
        config.validate().map_err(|error| {
            StoreError::internal_with_context(error, "Invalid PostgreSQL catalog configuration")
        })?;

        let mut pg_config =
            tokio_postgres::Config::from_str(&config.connection_string).map_err(|error| {
                StoreError::internal_with_context(
                    error,
                    "Failed to parse PostgreSQL connection settings",
                )
            })?;
        pg_config.connect_timeout(Duration::from_millis(config.connect_timeout_millis));
        pg_config.application_name("lore-fragment-catalog");

        let _ = rustls::crypto::ring::default_provider().install_default();
        let tls = match pg_config.get_ssl_mode() {
            SslMode::Disable => MakeRustlsConnect::new(
                rustls::ClientConfig::builder()
                    .with_root_certificates(rustls::RootCertStore::empty())
                    .with_no_client_auth(),
            ),
            SslMode::Prefer | SslMode::Require => {
                let (connector, certificate_errors) = MakeRustlsConnect::with_native_certs()
                    .map_err(|errors| {
                        StoreError::internal_with_context(
                            NativeCertificateErrors::from(errors),
                            "Failed to load PostgreSQL TLS trust roots",
                        )
                    })?;
                if !certificate_errors.is_empty() {
                    warn!(
                        errors = ?certificate_errors,
                        "Some PostgreSQL TLS trust roots could not be loaded"
                    );
                }
                connector
            }
            _ => {
                return Err(StoreError::internal(
                    "Unsupported PostgreSQL sslmode; use disable, prefer, or require",
                ));
            }
        };

        let manager = Manager::from_config(
            pg_config,
            tls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Verified,
            },
        );
        let pool = Pool::builder(manager)
            .runtime(Runtime::Tokio1)
            .max_size(config.max_connections)
            .wait_timeout(Some(Duration::from_millis(config.connect_timeout_millis)))
            .build()
            .map_err(|error| {
                StoreError::internal_with_context(error, "Failed to build PostgreSQL pool")
            })?;

        let catalog = Self {
            pool,
            schema: Arc::from(config.schema),
            max_batch_size: config.max_batch_size,
        };
        drop(catalog.connection().await?);
        catalog.migrate().await?;
        Ok(catalog)
    }

    /// Create a uniquely namespaced catalog using `LORE_POSTGRES_TEST_URL`.
    #[doc(hidden)]
    pub async fn connect_for_test() -> Result<Self, StoreError> {
        let connection_string = std::env::var("LORE_POSTGRES_TEST_URL").map_err(|error| {
            StoreError::internal_with_context(
                error,
                "LORE_POSTGRES_TEST_URL is required for PostgreSQL integration tests",
            )
        })?;
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| {
                StoreError::internal_with_context(error, "System clock precedes Unix epoch")
            })?
            .as_nanos();
        let mut config = PostgresFragmentCatalogConfig::new(connection_string);
        config.schema = format!("lore_test_{}_{nonce}", std::process::id());
        Self::connect(config).await
    }

    async fn connection(&self) -> Result<deadpool_postgres::Client, StoreError> {
        self.pool.get().await.map_err(|error| {
            warn!(?error, "Failed to acquire PostgreSQL catalog connection");
            StoreError::from(SlowDown)
        })
    }

    fn state_table(&self) -> String {
        format!("\"{}\".fragment_state", self.schema)
    }

    fn association_table(&self) -> String {
        format!("\"{}\".fragment_association", self.schema)
    }

    async fn migrate(&self) -> Result<(), StoreError> {
        let mut client = self.connection().await?;
        let transaction = client.transaction().await.map_err(|error| {
            database_error(error, "Failed to begin PostgreSQL catalog migration")
        })?;
        transaction
            .query_one(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                &[&format!("lore-fragment-catalog:{}", self.schema)],
            )
            .await
            .map_err(|error| database_error(error, "Failed to lock catalog migrations"))?;

        transaction
            .batch_execute(&format!(
                "CREATE SCHEMA IF NOT EXISTS \"{schema}\";
                 CREATE TABLE IF NOT EXISTS \"{schema}\".schema_migrations (
                     version INTEGER PRIMARY KEY,
                     checksum TEXT NOT NULL,
                     applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                 );",
                schema = self.schema,
            ))
            .await
            .map_err(|error| database_error(error, "Failed to create migration catalog"))?;

        transaction
            .batch_execute(&format!("SET LOCAL search_path TO \"{}\"", self.schema))
            .await
            .map_err(|error| database_error(error, "Failed to select catalog schema"))?;
        for &(version, sql) in MIGRATIONS {
            let checksum = migration_checksum(sql);
            let existing = transaction
                .query_opt(
                    &format!(
                        "SELECT checksum FROM \"{}\".schema_migrations WHERE version = $1",
                        self.schema
                    ),
                    &[&version],
                )
                .await
                .map_err(|error| database_error(error, "Failed to inspect catalog migrations"))?;

            if let Some(row) = existing {
                let recorded: String = row.try_get(0).map_err(|error| {
                    database_error(error, "Failed to read catalog migration checksum")
                })?;
                if recorded != checksum {
                    return Err(StoreError::internal(format!(
                        "PostgreSQL catalog migration {version} checksum mismatch"
                    )));
                }
                continue;
            }
            transaction
                .batch_execute(sql)
                .await
                .map_err(|error| database_error(error, "Failed to apply catalog migration"))?;
            transaction
                .execute(
                    &format!(
                        "INSERT INTO \"{}\".schema_migrations (version, checksum) VALUES ($1, $2)",
                        self.schema
                    ),
                    &[&version, &checksum],
                )
                .await
                .map_err(|error| database_error(error, "Failed to record catalog migration"))?;
        }

        transaction
            .commit()
            .await
            .map_err(|error| database_error(error, "Failed to commit catalog migrations"))
    }

    async fn locked_state(
        transaction: &deadpool_postgres::Transaction<'_>,
        table: &str,
        hash: Hash,
    ) -> Result<Option<(FragmentState, CatalogGeneration)>, StoreError> {
        let row = transaction
            .query_opt(
                &format!("SELECT state, generation FROM {table} WHERE hash = $1 FOR UPDATE"),
                &[&hash.as_ref()],
            )
            .await
            .map_err(|error| database_error(error, "Failed to lock fragment state"))?;
        let Some(row) = row else {
            return Ok(None);
        };
        let state = row
            .try_get(0)
            .map_err(|error| database_error(error, "Failed to read locked fragment state"))?;
        let generation = row
            .try_get(1)
            .map_err(|error| database_error(error, "Failed to read publication generation"))?;
        Ok(Some((
            state_from_i16(state)?,
            CatalogGeneration::new(generation),
        )))
    }

    async fn lock_hash(
        transaction: &deadpool_postgres::Transaction<'_>,
        hash: Hash,
    ) -> Result<(), StoreError> {
        let bytes = hash.data();
        let first = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let second = i32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        transaction
            .query_one("SELECT pg_advisory_xact_lock($1, $2)", &[&first, &second])
            .await
            .map(|_| ())
            .map_err(|error| database_error(error, "Failed to lock fragment publication"))
    }
}

#[async_trait]
impl FragmentCatalog for PostgresFragmentCatalog {
    async fn resolve(
        &self,
        partition: Partition,
        address: Address,
    ) -> Result<CatalogResolution, StoreError> {
        let client = self.connection().await?;
        let row = client
            .query_one(
                &format!(
                    "SELECT
                        EXISTS (
                            SELECT 1 FROM {association}
                            WHERE hash = $1 AND partition = $2 AND context = $3
                        ),
                        (SELECT state FROM {state} WHERE hash = $1),
                        (SELECT generation FROM {state} WHERE hash = $1)",
                    association = self.association_table(),
                    state = self.state_table(),
                ),
                &[
                    &address.hash.as_ref(),
                    &partition.as_ref(),
                    &address.context.as_ref(),
                ],
            )
            .await
            .map_err(|error| database_error(error, "Failed to resolve fragment catalog"))?;
        resolution_from_row(&row)
    }

    async fn resolve_partition(
        &self,
        partition: Partition,
        hash: Hash,
    ) -> Result<CatalogResolution, StoreError> {
        let client = self.connection().await?;
        let row = client
            .query_one(
                &format!(
                    "SELECT
                        EXISTS (
                            SELECT 1 FROM {association}
                            WHERE hash = $1 AND partition = $2
                        ),
                        (SELECT state FROM {state} WHERE hash = $1),
                        (SELECT generation FROM {state} WHERE hash = $1)",
                    association = self.association_table(),
                    state = self.state_table(),
                ),
                &[&hash.as_ref(), &partition.as_ref()],
            )
            .await
            .map_err(|error| database_error(error, "Failed to resolve partition catalog"))?;
        resolution_from_row(&row)
    }

    async fn resolve_batch(
        &self,
        partition: Partition,
        addresses: &[Address],
    ) -> Result<Vec<CatalogResolution>, StoreError> {
        if addresses.len() > self.max_batch_size {
            return Err(StoreError::internal(format!(
                "PostgreSQL catalog batch has {} addresses; maximum is {}",
                addresses.len(),
                self.max_batch_size
            )));
        }
        if addresses.is_empty() {
            return Ok(Vec::new());
        }

        let hashes: Vec<Vec<u8>> = addresses
            .iter()
            .map(|address| address.hash.as_ref().to_vec())
            .collect();
        let contexts: Vec<Vec<u8>> = addresses
            .iter()
            .map(|address| address.context.as_ref().to_vec())
            .collect();
        let client = self.connection().await?;
        let rows = client
            .query(
                &format!(
                    "SELECT
                        EXISTS (
                            SELECT 1 FROM {association} a
                            WHERE a.hash = input.hash
                              AND a.partition = $1
                              AND a.context = input.context
                        ),
                        (SELECT state FROM {state} s WHERE s.hash = input.hash),
                        (SELECT generation FROM {state} s WHERE s.hash = input.hash)
                     FROM unnest($2::bytea[], $3::bytea[])
                         WITH ORDINALITY AS input(hash, context, ordinality)
                     ORDER BY input.ordinality",
                    association = self.association_table(),
                    state = self.state_table(),
                ),
                &[&partition.as_ref(), &hashes, &contexts],
            )
            .await
            .map_err(|error| database_error(error, "Failed to batch resolve fragment catalog"))?;

        if rows.len() != addresses.len() {
            return Err(StoreError::internal(
                "PostgreSQL batch query returned an unexpected row count",
            ));
        }
        rows.iter().map(resolution_from_row).collect()
    }

    async fn publish(&self, partition: Partition, address: Address) -> Result<(), StoreError> {
        let mut client = self.connection().await?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| database_error(error, "Failed to begin fragment publication"))?;
        Self::lock_hash(&transaction, address.hash).await?;
        let table = self.state_table();
        let published = transaction
            .execute(
                &format!(
                    "INSERT INTO {table} AS current (hash, state) VALUES ($1, $2)
                     ON CONFLICT (hash) DO UPDATE
                     SET state = EXCLUDED.state,
                         generation = nextval('\"{schema}\".fragment_generation_seq')
                     WHERE current.state <> $3",
                    schema = self.schema,
                ),
                &[
                    &address.hash.as_ref(),
                    &state_to_i16(FragmentState::Stored),
                    &state_to_i16(FragmentState::Obliterating),
                ],
            )
            .await
            .map_err(|error| database_error(error, "Failed to publish fragment state"))?;
        if published == 0 {
            return Err(StoreError::from(SlowDown));
        }
        transaction
            .execute(
                &format!(
                    "INSERT INTO {} (hash, partition, context) VALUES ($1, $2, $3)
                     ON CONFLICT DO NOTHING",
                    self.association_table()
                ),
                &[
                    &address.hash.as_ref(),
                    &partition.as_ref(),
                    &address.context.as_ref(),
                ],
            )
            .await
            .map_err(|error| database_error(error, "Failed to associate fragment"))?;
        transaction
            .commit()
            .await
            .map_err(|error| database_error(error, "Failed to commit fragment publication"))
    }

    async fn repair_missing_payload(
        &self,
        hash: Hash,
        generation: CatalogGeneration,
    ) -> Result<(), StoreError> {
        let client = self.connection().await?;
        client
            .execute(
                &format!(
                    "DELETE FROM {} WHERE hash = $1 AND state = $2 AND generation = $3",
                    self.state_table()
                ),
                &[
                    &hash.as_ref(),
                    &state_to_i16(FragmentState::Stored),
                    &generation.value(),
                ],
            )
            .await
            .map(|_| ())
            .map_err(|error| database_error(error, "Failed to clear lost fragment state"))
    }

    async fn begin_obliteration(
        &self,
        partition: Partition,
        address: Address,
    ) -> Result<BeginObliteration, StoreError> {
        let mut client = self.connection().await?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| database_error(error, "Failed to begin obliteration transaction"))?;
        Self::lock_hash(&transaction, address.hash).await?;
        let state_table = self.state_table();
        let Some((state, _generation)) =
            Self::locked_state(&transaction, &state_table, address.hash).await?
        else {
            transaction
                .execute(
                    &format!(
                        "DELETE FROM {} WHERE hash = $1 AND partition = $2 AND context = $3",
                        self.association_table()
                    ),
                    &[
                        &address.hash.as_ref(),
                        &partition.as_ref(),
                        &address.context.as_ref(),
                    ],
                )
                .await
                .map_err(|error| {
                    database_error(error, "Failed to release orphaned fragment association")
                })?;
            transaction.commit().await.map_err(|error| {
                database_error(error, "Failed to commit empty obliteration lookup")
            })?;
            return Ok(BeginObliteration::NoState);
        };
        if state == FragmentState::Obliterated {
            transaction.commit().await.map_err(|error| {
                database_error(error, "Failed to commit terminal obliteration lookup")
            })?;
            return Ok(BeginObliteration::AlreadyObliterated);
        }
        if state == FragmentState::Obliterating {
            transaction.commit().await.map_err(|error| {
                database_error(error, "Failed to commit resumed obliteration lookup")
            })?;
            return Ok(BeginObliteration::ResumePayloadDeletion);
        }

        transaction
            .execute(
                &format!(
                    "DELETE FROM {} WHERE hash = $1 AND partition = $2 AND context = $3",
                    self.association_table()
                ),
                &[
                    &address.hash.as_ref(),
                    &partition.as_ref(),
                    &address.context.as_ref(),
                ],
            )
            .await
            .map_err(|error| database_error(error, "Failed to release fragment association"))?;
        transaction
            .execute(
                &format!("UPDATE {state_table} SET state = $2 WHERE hash = $1"),
                &[
                    &address.hash.as_ref(),
                    &state_to_i16(FragmentState::Obliterating),
                ],
            )
            .await
            .map_err(|error| database_error(error, "Failed to mark fragment obliterating"))?;
        let remains: bool = transaction
            .query_one(
                &format!(
                    "SELECT EXISTS (SELECT 1 FROM {} WHERE hash = $1)",
                    self.association_table()
                ),
                &[&address.hash.as_ref()],
            )
            .await
            .map_err(|error| database_error(error, "Failed to inspect fragment associations"))?
            .try_get(0)
            .map_err(|error| database_error(error, "Failed to read fragment associations"))?;

        let result = if remains {
            transaction
                .execute(
                    &format!("UPDATE {state_table} SET state = $2 WHERE hash = $1"),
                    &[&address.hash.as_ref(), &state_to_i16(FragmentState::Stored)],
                )
                .await
                .map_err(|error| database_error(error, "Failed to release obliteration marker"))?;
            BeginObliteration::ReferencesRemain
        } else {
            BeginObliteration::PayloadUnreferenced
        };
        transaction
            .commit()
            .await
            .map_err(|error| database_error(error, "Failed to commit obliteration transition"))?;
        Ok(result)
    }

    async fn finalize_obliteration(&self, hash: Hash) -> Result<(), StoreError> {
        let mut client = self.connection().await?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| database_error(error, "Failed to begin obliteration finalization"))?;
        let table = self.state_table();
        let (state, _generation) = Self::locked_state(&transaction, &table, hash)
            .await?
            .ok_or_else(|| StoreError::internal("Cannot finalize a missing fragment state"))?;
        match state {
            FragmentState::Obliterated => {}
            FragmentState::Obliterating => {
                transaction
                    .execute(
                        &format!("UPDATE {table} SET state = $2 WHERE hash = $1"),
                        &[&hash.as_ref(), &state_to_i16(FragmentState::Obliterated)],
                    )
                    .await
                    .map_err(|error| {
                        database_error(error, "Failed to finalize fragment obliteration")
                    })?;
            }
            FragmentState::Stored => {
                return Err(StoreError::internal(
                    "Cannot finalize a fragment that is not obliterating",
                ));
            }
        }
        transaction
            .commit()
            .await
            .map_err(|error| database_error(error, "Failed to commit obliteration finalization"))
    }

    fn max_query_batch(&self) -> Option<usize> {
        Some(self.max_batch_size)
    }
}

fn resolution_from_row(row: &Row) -> Result<CatalogResolution, StoreError> {
    let associated = row
        .try_get(0)
        .map_err(|error| database_error(error, "Failed to read association resolution"))?;
    let state: Option<i16> = row
        .try_get(1)
        .map_err(|error| database_error(error, "Failed to read state resolution"))?;
    let generation: Option<i64> = row
        .try_get(2)
        .map_err(|error| database_error(error, "Failed to read generation resolution"))?;
    if state.is_some() != generation.is_some() {
        return Err(StoreError::internal(
            "PostgreSQL fragment state has no publication generation",
        ));
    }
    Ok(CatalogResolution {
        associated,
        publication: state
            .zip(generation)
            .map(|(state, generation)| {
                Ok::<_, StoreError>(CatalogPublication {
                    state: state_from_i16(state)?,
                    generation: CatalogGeneration::new(generation),
                })
            })
            .transpose()?,
    })
}

fn state_to_i16(state: FragmentState) -> i16 {
    match state {
        FragmentState::Stored => 0,
        FragmentState::Obliterating => 1,
        FragmentState::Obliterated => 2,
    }
}

fn state_from_i16(state: i16) -> Result<FragmentState, StoreError> {
    match state {
        0 => Ok(FragmentState::Stored),
        1 => Ok(FragmentState::Obliterating),
        2 => Ok(FragmentState::Obliterated),
        _ => Err(StoreError::internal(format!(
            "PostgreSQL fragment state {state} is invalid"
        ))),
    }
}

fn validate_identifier(identifier: &str) -> Result<(), PostgresFragmentCatalogConfigError> {
    let valid_length = !identifier.is_empty() && identifier.len() <= 63;
    let mut characters = identifier.bytes();
    let valid_first = characters
        .next()
        .is_some_and(|value| value == b'_' || value.is_ascii_alphabetic());
    let valid_rest = characters.all(|value| value == b'_' || value.is_ascii_alphanumeric());
    if valid_length && valid_first && valid_rest {
        Ok(())
    } else {
        Err(PostgresFragmentCatalogConfigError::InvalidSchema)
    }
}

fn database_error(error: tokio_postgres::Error, context: &'static str) -> StoreError {
    let retryable = error.is_closed()
        || error.as_db_error().is_some_and(|database_error| {
            matches!(
                database_error.code(),
                &SqlState::T_R_SERIALIZATION_FAILURE
                    | &SqlState::T_R_DEADLOCK_DETECTED
                    | &SqlState::QUERY_CANCELED
                    | &SqlState::CONNECTION_EXCEPTION
                    | &SqlState::CONNECTION_FAILURE
                    | &SqlState::SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION
            )
        });
    warn!(
        ?error,
        context, "PostgreSQL fragment catalog operation failed"
    );
    if retryable {
        StoreError::from(SlowDown)
    } else {
        StoreError::internal_with_context(error, context)
    }
}

fn migration_checksum(sql: &str) -> String {
    Sha256::digest(sql.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_checksum_is_sha256() {
        for &(_, sql) in MIGRATIONS {
            assert_eq!(migration_checksum(sql).len(), 64);
        }
    }
}
