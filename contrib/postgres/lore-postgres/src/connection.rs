// Copyright 2026 David
// SPDX-License-Identifier: MIT
//! Connection pooling, error mapping, and migrations shared by Lore's PostgreSQL stores.

use std::str::FromStr;
use std::time::Duration;

use deadpool_postgres::Manager;
use deadpool_postgres::ManagerConfig;
use deadpool_postgres::Pool;
use deadpool_postgres::RecyclingMethod;
use deadpool_postgres::Runtime;
use lore_base::error::SlowDown;
use lore_error_set::prelude::*;
use lore_storage::StoreError;
use sha2::Digest;
use sha2::Sha256;
use tokio_postgres::config::SslMode;
use tokio_postgres::error::SqlState;
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::warn;

/// Recycling query run before a pooled session is handed out again.
///
/// `target_session_attrs=read-write` only applies while connecting. A pooled session can still end
/// up read-only, for example when a failover manager fences the old primary or a proxy re-routes
/// its sessions, so the check fails such a session and the pool replaces it with a new connection.
const WRITABLE_SESSION_CHECK: &str = "DO $$ BEGIN
    IF current_setting('transaction_read_only')::boolean THEN
        RAISE EXCEPTION 'PostgreSQL session is read-only'
            USING ERRCODE = 'read_only_sql_transaction';
    END IF;
END $$";

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

/// Settings for one pool of writable PostgreSQL sessions.
pub(crate) struct PoolSettings<'a> {
    pub connection_string: &'a str,
    pub application_name: &'a str,
    pub max_connections: usize,
    pub connect_timeout: Duration,
    /// Server-side limit for each statement, including row-lock waits.
    pub statement_timeout: Option<Duration>,
}

/// Build a pool whose sessions are verified writable each time they are checked out.
pub(crate) fn build_pool(settings: PoolSettings<'_>) -> Result<Pool, StoreError> {
    let mut pg_config =
        tokio_postgres::Config::from_str(settings.connection_string).map_err(|error| {
            StoreError::internal_with_context(
                error,
                "Failed to parse PostgreSQL connection settings",
            )
        })?;
    pg_config.connect_timeout(settings.connect_timeout);
    pg_config.application_name(settings.application_name);
    if let Some(statement_timeout) = settings.statement_timeout {
        let options = with_statement_timeout(pg_config.get_options(), statement_timeout);
        pg_config.options(&options);
    }

    let _ = rustls::crypto::ring::default_provider().install_default();
    let tls = match pg_config.get_ssl_mode() {
        SslMode::Disable => MakeRustlsConnect::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth(),
        ),
        SslMode::Prefer | SslMode::Require => {
            let (connector, certificate_errors) =
                MakeRustlsConnect::with_native_certs().map_err(|errors| {
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
            recycling_method: RecyclingMethod::Custom(WRITABLE_SESSION_CHECK.to_string()),
        },
    );
    Pool::builder(manager)
        .runtime(Runtime::Tokio1)
        .max_size(settings.max_connections)
        .wait_timeout(Some(settings.connect_timeout))
        .build()
        .map_err(|error| {
            StoreError::internal_with_context(error, "Failed to build PostgreSQL pool")
        })
}

/// Append a `statement_timeout` session default to any `options` from the connection string.
fn with_statement_timeout(options: Option<&str>, statement_timeout: Duration) -> String {
    let setting = format!("-c statement_timeout={}", statement_timeout.as_millis());
    match options {
        Some(options) if !options.trim().is_empty() => format!("{options} {setting}"),
        _ => setting,
    }
}

/// Check a session out of the pool.
pub(crate) async fn connection(pool: &Pool) -> Result<deadpool_postgres::Client, StoreError> {
    pool.get().await.map_err(pool_error)
}

/// Checksummed migrations recorded in their own ledger table.
pub(crate) struct Migrations {
    /// Ledger table inside the configured schema.
    pub ledger: &'static str,
    /// Advisory-lock namespace that serializes migrations of one schema.
    pub lock_name: &'static str,
    pub steps: &'static [(i32, &'static str)],
}

/// Create the schema if absent, then apply or verify each migration in one transaction.
pub(crate) async fn migrate(
    pool: &Pool,
    schema: &str,
    migrations: &Migrations,
) -> Result<(), StoreError> {
    let ledger = format!("\"{schema}\".{}", migrations.ledger);
    let mut client = connection(pool).await?;
    let transaction = client
        .transaction()
        .await
        .map_err(|error| database_error(error, "Failed to begin PostgreSQL migration"))?;
    transaction
        .query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            &[&format!("{}:{schema}", migrations.lock_name)],
        )
        .await
        .map_err(|error| database_error(error, "Failed to lock PostgreSQL migrations"))?;

    transaction
        .batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS \"{schema}\";
             CREATE TABLE IF NOT EXISTS {ledger} (
                 version INTEGER PRIMARY KEY,
                 checksum TEXT NOT NULL,
                 applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
             );"
        ))
        .await
        .map_err(|error| database_error(error, "Failed to create migration ledger"))?;

    transaction
        .batch_execute(&format!("SET LOCAL search_path TO \"{schema}\""))
        .await
        .map_err(|error| database_error(error, "Failed to select migration schema"))?;
    for &(version, sql) in migrations.steps {
        let checksum = migration_checksum(sql);
        let existing = transaction
            .query_opt(
                &format!("SELECT checksum FROM {ledger} WHERE version = $1"),
                &[&version],
            )
            .await
            .map_err(|error| database_error(error, "Failed to inspect PostgreSQL migrations"))?;

        if let Some(row) = existing {
            let recorded: String = row
                .try_get(0)
                .map_err(|error| database_error(error, "Failed to read migration checksum"))?;
            if recorded != checksum {
                return Err(StoreError::internal(format!(
                    "PostgreSQL migration {version} in {ledger} checksum mismatch"
                )));
            }
            continue;
        }
        transaction
            .batch_execute(sql)
            .await
            .map_err(|error| database_error(error, "Failed to apply PostgreSQL migration"))?;
        transaction
            .execute(
                &format!("INSERT INTO {ledger} (version, checksum) VALUES ($1, $2)"),
                &[&version, &checksum],
            )
            .await
            .map_err(|error| database_error(error, "Failed to record PostgreSQL migration"))?;
    }

    transaction
        .commit()
        .await
        .map_err(|error| database_error(error, "Failed to commit PostgreSQL migrations"))
}

pub(crate) fn migration_checksum(sql: &str) -> String {
    Sha256::digest(sql.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Whether `identifier` is a safe unquoted SQL identifier of at most 63 bytes.
pub(crate) fn is_identifier(identifier: &str) -> bool {
    let valid_length = !identifier.is_empty() && identifier.len() <= 63;
    let mut characters = identifier.bytes();
    let valid_first = characters
        .next()
        .is_some_and(|value| value == b'_' || value.is_ascii_alphabetic());
    let valid_rest = characters.all(|value| value == b'_' || value.is_ascii_alphanumeric());
    valid_length && valid_first && valid_rest
}

pub(crate) fn database_error(error: tokio_postgres::Error, context: &'static str) -> StoreError {
    // A read-only session or a shutting-down server is what a failover looks like to a client;
    // the caller retries on a fresh session from the pool.
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
                    | &SqlState::READ_ONLY_SQL_TRANSACTION
                    | &SqlState::ADMIN_SHUTDOWN
                    | &SqlState::CRASH_SHUTDOWN
                    | &SqlState::CANNOT_CONNECT_NOW
            )
        });
    warn!(?error, context, "PostgreSQL operation failed");
    if retryable {
        retryable_error(error, context)
    } else {
        StoreError::internal_with_context(error, context)
    }
}

pub(crate) fn pool_error(error: deadpool_postgres::PoolError) -> StoreError {
    const CONTEXT: &str = "Failed to acquire PostgreSQL connection";
    warn!(?error, "{CONTEXT}");
    match error {
        deadpool_postgres::PoolError::Backend(error) => database_error(error, CONTEXT),
        error @ deadpool_postgres::PoolError::Timeout(_) => retryable_error(error, CONTEXT),
        error => StoreError::internal_with_context(error, CONTEXT),
    }
}

fn retryable_error(
    error: impl std::error::Error + Send + Sync + 'static,
    context: &'static str,
) -> StoreError {
    let source = StoreError::internal_with_context(error, context);
    StoreError::SlowDown(SlowDown.chain_err_from(source, "retryable PostgreSQL failure"))
}

/// Unique schema name for one integration test.
pub(crate) fn test_schema(prefix: &str) -> Result<String, StoreError> {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| {
            StoreError::internal_with_context(error, "System clock precedes Unix epoch")
        })?
        .as_nanos();
    Ok(format!("{prefix}_{}_{nonce}", std::process::id()))
}

/// `LORE_POSTGRES_TEST_URL`, which PostgreSQL integration tests connect to.
pub(crate) fn test_connection_string() -> Result<String, StoreError> {
    std::env::var("LORE_POSTGRES_TEST_URL").map_err(|error| {
        StoreError::internal_with_context(
            error,
            "LORE_POSTGRES_TEST_URL is required for PostgreSQL integration tests",
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_timeouts_preserve_their_trace_as_slow_down() {
        let error = pool_error(deadpool_postgres::PoolError::Timeout(
            deadpool_postgres::TimeoutType::Wait,
        ));
        assert!(matches!(error, StoreError::SlowDown(_)));
        assert!(error.trace().len() >= 2);
    }

    #[test]
    fn permanent_pool_configuration_errors_are_internal() {
        let error = pool_error(deadpool_postgres::PoolError::NoRuntimeSpecified);
        assert!(matches!(error, StoreError::Internal(_)));
    }

    #[test]
    fn statement_timeout_keeps_connection_string_options() {
        let timeout = Duration::from_millis(1_500);
        assert_eq!(
            with_statement_timeout(None, timeout),
            "-c statement_timeout=1500"
        );
        assert_eq!(
            with_statement_timeout(Some("-c search_path=lore"), timeout),
            "-c search_path=lore -c statement_timeout=1500"
        );
    }

    #[test]
    fn identifiers_are_unquoted_sql_names() {
        assert!(is_identifier("lore_mutable_store"));
        assert!(is_identifier("_lore1"));
        assert!(!is_identifier(""));
        assert!(!is_identifier("1lore"));
        assert!(!is_identifier("not-safe;drop"));
        assert!(!is_identifier(&"a".repeat(64)));
    }

    #[test]
    fn multi_host_read_write_connection_strings_parse() {
        let config = tokio_postgres::Config::from_str(
            "host=pg1.internal,pg2.internal,pg3.internal port=5432 dbname=lore user=lore \
             target_session_attrs=read-write",
        )
        .expect("multi-host connection string should parse");
        assert_eq!(config.get_hosts().len(), 3);
        assert_eq!(
            config.get_target_session_attrs(),
            tokio_postgres::config::TargetSessionAttrs::ReadWrite
        );
    }
}

#[cfg(all(test, feature = "integration-tests"))]
mod integration_tests {
    use super::*;

    fn test_pool(max_connections: usize) -> Pool {
        build_pool(PoolSettings {
            connection_string: &test_connection_string().expect("test connection string"),
            application_name: "lore-postgres-test",
            max_connections,
            connect_timeout: Duration::from_secs(5),
            statement_timeout: Some(Duration::from_secs(5)),
        })
        .expect("test pool")
    }

    #[tokio::test]
    async fn pool_discards_a_session_that_turned_read_only() {
        let pool = test_pool(1);
        let client = connection(&pool).await.expect("first session");
        let first_pid: i32 = client
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .expect("read backend pid")
            .get(0);
        // The same effect as a fenced primary: every new transaction in the session is read-only.
        client
            .batch_execute("SET SESSION default_transaction_read_only = on")
            .await
            .expect("make session read-only");
        let error = client
            .batch_execute("CREATE TEMPORARY TABLE lore_read_only_probe (id INTEGER)")
            .await
            .expect_err("a read-only session rejects writes");
        assert_eq!(
            error.code(),
            Some(&SqlState::READ_ONLY_SQL_TRANSACTION),
            "unexpected error: {error:?}"
        );
        assert!(matches!(
            database_error(error, "write on a read-only session"),
            StoreError::SlowDown(_)
        ));
        drop(client);

        let client = connection(&pool).await.expect("replacement session");
        let row = client
            .query_one(
                "SELECT pg_backend_pid(), current_setting('transaction_read_only')",
                &[],
            )
            .await
            .expect("inspect replacement session");
        let pid: i32 = row.get(0);
        let read_only: String = row.get(1);
        assert_ne!(pid, first_pid, "the read-only session was reused");
        assert_eq!(read_only, "off");
    }

    #[tokio::test]
    async fn statement_timeout_applies_to_pooled_sessions() {
        let pool = test_pool(1);
        let client = connection(&pool).await.expect("session");
        let timeout: String = client
            .query_one("SHOW statement_timeout", &[])
            .await
            .expect("show statement timeout")
            .get(0);
        assert_eq!(timeout, "5s");
    }
}
