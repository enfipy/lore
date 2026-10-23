// Copyright 2026 David
// SPDX-License-Identifier: MIT

use std::fmt;
use std::time::Duration;

use serde::Deserialize;

const DEFAULT_SCHEMA: &str = "lore";
const DEFAULT_MAX_CONNECTIONS: usize = 16;
const DEFAULT_CONNECT_TIMEOUT_MILLIS: u64 = 5_000;
const DEFAULT_STATEMENT_TIMEOUT_MILLIS: u64 = 30_000;

/// Invalid PostgreSQL mutable- or lock-store configuration.
#[derive(Debug, thiserror::Error)]
pub enum PostgresStoreConfigError {
    /// Schema is not a safe unquoted SQL identifier.
    #[error("PostgreSQL schema must be a 1-63 character SQL identifier")]
    InvalidSchema,
    /// A pool with no connections cannot serve store operations.
    #[error("PostgreSQL max_connections must be greater than zero")]
    ZeroConnections,
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

fn default_statement_timeout_millis() -> u64 {
    DEFAULT_STATEMENT_TIMEOUT_MILLIS
}

/// Connection and namespace settings for a PostgreSQL mutable or lock store.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresStoreConfig {
    /// libpq-style connection string or PostgreSQL URL.
    #[serde(alias = "url")]
    pub connection_string: String,
    /// PostgreSQL schema holding this store's tables and migration ledger.
    #[serde(default = "default_schema")]
    pub schema: String,
    /// Maximum number of pooled PostgreSQL connections.
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    /// Connection and pool checkout timeout.
    #[serde(default = "default_connect_timeout_millis")]
    pub connect_timeout_millis: u64,
    /// Server-side limit for one statement, including row-lock waits. Zero disables it.
    #[serde(default = "default_statement_timeout_millis")]
    pub statement_timeout_millis: u64,
}

impl fmt::Debug for PostgresStoreConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresStoreConfig")
            .field("connection_string", &"<redacted>")
            .field("schema", &self.schema)
            .field("max_connections", &self.max_connections)
            .field("connect_timeout_millis", &self.connect_timeout_millis)
            .field("statement_timeout_millis", &self.statement_timeout_millis)
            .finish()
    }
}

impl PostgresStoreConfig {
    /// Create settings for a connection string using production-safe defaults.
    pub fn new(connection_string: String) -> Self {
        Self {
            connection_string,
            schema: default_schema(),
            max_connections: default_max_connections(),
            connect_timeout_millis: default_connect_timeout_millis(),
            statement_timeout_millis: default_statement_timeout_millis(),
        }
    }

    /// Validate settings without opening a database connection.
    pub fn validate(&self) -> Result<(), PostgresStoreConfigError> {
        if !crate::connection::is_identifier(&self.schema) {
            return Err(PostgresStoreConfigError::InvalidSchema);
        }
        if self.max_connections == 0 {
            return Err(PostgresStoreConfigError::ZeroConnections);
        }
        Ok(())
    }

    pub(crate) fn pool_settings<'a>(
        &'a self,
        application_name: &'a str,
    ) -> crate::connection::PoolSettings<'a> {
        crate::connection::PoolSettings {
            connection_string: &self.connection_string,
            application_name,
            max_connections: self.max_connections,
            connect_timeout: Duration::from_millis(self.connect_timeout_millis),
            statement_timeout: (self.statement_timeout_millis > 0)
                .then(|| Duration::from_millis(self.statement_timeout_millis)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_fill_omitted_fields_and_debug_redacts_credentials() {
        let config: PostgresStoreConfig =
            toml::from_str(r#"connection_string = "host=localhost password=do-not-log""#)
                .expect("minimal config");
        assert_eq!(config.schema, DEFAULT_SCHEMA);
        assert_eq!(config.max_connections, DEFAULT_MAX_CONNECTIONS);
        assert_eq!(
            config.pool_settings("test").statement_timeout,
            Some(Duration::from_millis(DEFAULT_STATEMENT_TIMEOUT_MILLIS))
        );
        let debug = format!("{config:?}");
        assert!(!debug.contains("do-not-log"));
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn zero_statement_timeout_disables_it() {
        let mut config = PostgresStoreConfig::new("host=localhost".to_string());
        config.statement_timeout_millis = 0;
        assert_eq!(config.pool_settings("test").statement_timeout, None);
    }

    #[test]
    fn validation_rejects_unsafe_schema_and_empty_pool() {
        let mut config = PostgresStoreConfig::new("host=localhost".to_string());
        config.schema = "not-safe;drop".to_string();
        assert!(matches!(
            config.validate(),
            Err(PostgresStoreConfigError::InvalidSchema)
        ));
        config.schema = "lore_mutable_store".to_string();
        config.max_connections = 0;
        assert!(matches!(
            config.validate(),
            Err(PostgresStoreConfigError::ZeroConnections)
        ));
    }
}
