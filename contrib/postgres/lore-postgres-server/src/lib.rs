// Copyright 2026 David
// SPDX-License-Identifier: MIT
//! Derived Lore server with a PostgreSQL catalog and S3-compatible payload storage, plus
//! PostgreSQL mutable and lock stores.

use std::sync::Arc;

use lore_aws::store::immutable_store::AwsImmutableStore;
use lore_aws::store::immutable_store::ObjectStoreImmutableStoreSettings;
use lore_base::error::PluginConfigError;
use lore_base::error::PluginInitError;
use lore_base::runtime::runtime;
use lore_postgres::PostgresFragmentCatalog;
use lore_postgres::PostgresFragmentCatalogConfig;
use lore_postgres::PostgresLockStore;
use lore_postgres::PostgresMutableStore;
use lore_postgres::PostgresStoreConfig;
use lore_revision::lock::LockStore;
use lore_server::plugins::ImmutableStorePluginFactory;
use lore_server::plugins::LockStorePluginFactory;
use lore_server::plugins::MutableStorePluginFactory;
use lore_server::plugins::PluginError;
use lore_server::plugins::PluginRegistry;
use lore_server::plugins::aws::S3ImmutableStorePluginConfig;
use lore_storage::ImmutableStore;
use lore_storage::MutableStore;
use lore_storage::StoreError;
use serde::Deserialize;
use tracing::info;

/// Configuration name used in `[immutable_store]`, `[mutable_store]`, `[lock_store]`, and
/// `[plugins]`.
pub const PLUGIN_NAME: &str = "postgres_s3";

/// PostgreSQL catalog plus S3-compatible payload-store configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresS3ImmutableStorePluginConfig {
    /// Shared S3-compatible payload-store settings.
    #[serde(flatten)]
    pub s3: S3ImmutableStorePluginConfig,
    /// PostgreSQL catalog connection and namespace.
    pub postgres: PostgresFragmentCatalogConfig,
}

/// Creates an immutable store backed by PostgreSQL metadata and S3-compatible payloads.
pub struct PostgresS3ImmutableStorePluginFactory;

impl PostgresS3ImmutableStorePluginFactory {
    fn parse(
        &self,
        config: &toml::Value,
    ) -> Result<PostgresS3ImmutableStorePluginConfig, PluginError> {
        let parsed: PostgresS3ImmutableStorePluginConfig =
            config.clone().try_into().map_err(|error| {
                PluginError::from(PluginConfigError {
                    plugin_name: PLUGIN_NAME.to_string(),
                    message: format!("Failed to deserialize PostgreSQL/S3 config: {error}"),
                })
            })?;
        parsed.s3.validate().map_err(|error| {
            PluginError::from(PluginConfigError {
                plugin_name: PLUGIN_NAME.to_string(),
                message: error.to_string(),
            })
        })?;
        parsed.postgres.validate().map_err(|error| {
            PluginError::from(PluginConfigError {
                plugin_name: PLUGIN_NAME.to_string(),
                message: error.to_string(),
            })
        })?;
        Ok(parsed)
    }
}

impl ImmutableStorePluginFactory for PostgresS3ImmutableStorePluginFactory {
    fn name(&self) -> &'static str {
        PLUGIN_NAME
    }

    fn validate_config(&self, config: &toml::Value) -> Result<(), PluginError> {
        self.parse(config).map(|_| ())
    }

    fn create(&self, config: &toml::Value) -> Result<Arc<dyn ImmutableStore>, PluginError> {
        let plugin_config = self.parse(config)?;
        info!(
            plugin_name = PLUGIN_NAME,
            s3_bucket = %plugin_config.s3.s3_bucket,
            postgres_schema = %plugin_config.postgres.schema,
            object_versioning = ?plugin_config.s3.s3_object_versioning,
            "Creating PostgreSQL catalog with S3-compatible payload storage"
        );

        let s3_client = plugin_config.s3.create_client(PLUGIN_NAME)?;

        // PostgreSQL connection is asynchronous behind the synchronous plugin hook.
        #[allow(clippy::disallowed_methods)]
        let catalog = tokio::task::block_in_place(|| {
            runtime().block_on(Box::pin(async {
                PostgresFragmentCatalog::connect(plugin_config.postgres.clone())
                    .await
                    .map_err(|error| {
                        PluginError::from(PluginInitError {
                            plugin_name: PLUGIN_NAME.to_string(),
                            message: format!("Failed to create PostgreSQL catalog: {error}"),
                        })
                    })
            }))
        })?;

        let settings = ObjectStoreImmutableStoreSettings::new(
            plugin_config.s3.store_settings(),
            plugin_config.s3.force_write,
        );
        Ok(Arc::new(AwsImmutableStore::with_catalog(
            s3_client,
            Arc::new(catalog),
            &settings,
        )))
    }
}

/// PostgreSQL mutable- or lock-store configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresStorePluginConfig {
    /// PostgreSQL connection and namespace.
    pub postgres: PostgresStoreConfig,
}

fn parse_store_config(
    config: &toml::Value,
    store: &str,
) -> Result<PostgresStorePluginConfig, PluginError> {
    let parsed: PostgresStorePluginConfig = config.clone().try_into().map_err(|error| {
        PluginError::from(PluginConfigError {
            plugin_name: PLUGIN_NAME.to_string(),
            message: format!("Failed to deserialize PostgreSQL {store} config: {error}"),
        })
    })?;
    parsed.postgres.validate().map_err(|error| {
        PluginError::from(PluginConfigError {
            plugin_name: PLUGIN_NAME.to_string(),
            message: error.to_string(),
        })
    })?;
    Ok(parsed)
}

/// Connect a PostgreSQL store from inside a synchronous plugin hook.
fn connect_store<T>(
    store: &str,
    connect: impl std::future::Future<Output = Result<T, StoreError>>,
) -> Result<T, PluginError> {
    // Plugin construction runs once at startup, one plugin at a time.
    #[allow(clippy::disallowed_methods)]
    tokio::task::block_in_place(|| {
        runtime().block_on(Box::pin(async {
            connect.await.map_err(|error| {
                PluginError::from(PluginInitError {
                    plugin_name: PLUGIN_NAME.to_string(),
                    message: format!("Failed to create PostgreSQL {store}: {error}"),
                })
            })
        }))
    })
}

/// Creates a mutable store whose branch heads and other mutable values live in PostgreSQL.
pub struct PostgresMutableStorePluginFactory;

impl MutableStorePluginFactory for PostgresMutableStorePluginFactory {
    fn name(&self) -> &'static str {
        PLUGIN_NAME
    }

    fn validate_config(&self, config: &toml::Value) -> Result<(), PluginError> {
        parse_store_config(config, "mutable store").map(|_| ())
    }

    fn create(
        &self,
        config: &toml::Value,
        _immutable_store: Arc<dyn ImmutableStore>,
    ) -> Result<Arc<dyn MutableStore>, PluginError> {
        let plugin_config = parse_store_config(config, "mutable store")?;
        info!(
            plugin_name = PLUGIN_NAME,
            postgres_schema = %plugin_config.postgres.schema,
            "Creating PostgreSQL mutable store"
        );
        let store = connect_store(
            "mutable store",
            PostgresMutableStore::connect(plugin_config.postgres),
        )?;
        Ok(Arc::new(store))
    }
}

/// Creates a resource lock store backed by PostgreSQL.
pub struct PostgresLockStorePluginFactory;

impl LockStorePluginFactory for PostgresLockStorePluginFactory {
    fn name(&self) -> &'static str {
        PLUGIN_NAME
    }

    fn validate_config(&self, config: &toml::Value) -> Result<(), PluginError> {
        parse_store_config(config, "lock store").map(|_| ())
    }

    fn create(&self, config: &toml::Value) -> Result<Arc<dyn LockStore>, PluginError> {
        let plugin_config = parse_store_config(config, "lock store")?;
        info!(
            plugin_name = PLUGIN_NAME,
            postgres_schema = %plugin_config.postgres.schema,
            "Creating PostgreSQL lock store"
        );
        let store = connect_store(
            "lock store",
            PostgresLockStore::connect(plugin_config.postgres),
        )?;
        Ok(Arc::new(store))
    }
}

/// Register the PostgreSQL/S3 immutable-store plugin and the PostgreSQL mutable- and lock-store
/// plugins.
pub fn register(registry: &mut PluginRegistry) {
    registry.register_immutable_store_plugin(Box::new(PostgresS3ImmutableStorePluginFactory));
    registry.register_mutable_store_plugin(Box::new(PostgresMutableStorePluginFactory));
    registry.register_lock_store_plugin(Box::new(PostgresLockStorePluginFactory));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> toml::Value {
        toml::from_str(
            r#"
s3_bucket = "lore-test"
s3_endpoint_url = "https://example.r2.cloudflarestorage.com"
s3_region = "auto"
s3_object_versioning = "unversioned"

[postgres]
connection_string = "host=localhost user=lore password=do-not-log"
schema = "lore_catalog"
"#,
        )
        .expect("valid test config")
    }

    #[test]
    fn parses_r2_config_and_redacts_postgres_credentials() {
        let factory = PostgresS3ImmutableStorePluginFactory;
        let parsed = factory.parse(&config()).expect("config should validate");

        assert_eq!(
            parsed.s3.s3_object_versioning,
            lore_aws::store::immutable_store::S3ObjectVersioning::Unversioned
        );
        let debug = format!("{parsed:?}");
        assert!(!debug.contains("do-not-log"));
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn rejects_invalid_postgres_schema_without_connecting() {
        let mut config = config();
        config["postgres"]["schema"] = toml::Value::String("not-safe;drop".to_string());

        let error = PostgresS3ImmutableStorePluginFactory
            .validate_config(&config)
            .expect_err("unsafe identifier should fail validation");
        assert!(error.to_string().contains("identifier"));
    }

    #[test]
    fn register_adds_postgres_s3_plugins() {
        let mut registry = PluginRegistry::new();
        register(&mut registry);

        assert_eq!(
            registry.list_immutable_store_plugins(),
            vec![PLUGIN_NAME.to_string()]
        );
        assert_eq!(
            registry.list_mutable_store_plugins(),
            vec![PLUGIN_NAME.to_string()]
        );
        assert_eq!(
            registry.list_lock_store_plugins(),
            vec![PLUGIN_NAME.to_string()]
        );
    }

    /// The server hands a store plugin its `[plugins.postgres_s3.<store>]` table.
    fn store_config(store: &str) -> toml::Value {
        let plugins: std::collections::HashMap<String, toml::Value> = toml::from_str(
            r#"
[postgres_s3.immutable_store]
s3_bucket = "lore-test"

[postgres_s3.mutable_store.postgres]
connection_string = "host=pg1,pg2,pg3 user=lore password=do-not-log target_session_attrs=read-write"
schema = "lore_mutable_store"
max_connections = 8

[postgres_s3.lock_store.postgres]
connection_string = "host=pg1,pg2,pg3 user=lore target_session_attrs=read-write"
schema = "lore_lock_store"
statement_timeout_millis = 0
"#,
        )
        .expect("valid test config");
        lore_server::store::resolve_plugin_config_with_fallback(&plugins, PLUGIN_NAME, store)
            .expect("store config resolves")
    }

    #[test]
    fn parses_mutable_and_lock_store_configs_and_redacts_credentials() {
        let mutable = PostgresMutableStorePluginFactory;
        mutable
            .validate_config(&store_config("mutable_store"))
            .expect("mutable store config should validate");
        let parsed = parse_store_config(&store_config("mutable_store"), "mutable store")
            .expect("mutable store config should parse");
        assert_eq!(parsed.postgres.schema, "lore_mutable_store");
        assert_eq!(parsed.postgres.max_connections, 8);
        assert!(!format!("{parsed:?}").contains("do-not-log"));

        PostgresLockStorePluginFactory
            .validate_config(&store_config("lock_store"))
            .expect("lock store config should validate");
        let parsed = parse_store_config(&store_config("lock_store"), "lock store")
            .expect("lock store config should parse");
        assert_eq!(parsed.postgres.schema, "lore_lock_store");
        assert_eq!(parsed.postgres.statement_timeout_millis, 0);
    }

    #[test]
    fn example_configs_select_and_validate_every_postgres_store() {
        for example in [
            include_str!("../../config-hetzner-consul.toml"),
            include_str!("../../config-r2-consul.toml"),
        ] {
            let config: toml::Table = toml::from_str(example).expect("valid example config");
            for store in ["immutable_store", "mutable_store", "lock_store"] {
                assert_eq!(
                    config[store]["mode"].as_str(),
                    Some(if store == "immutable_store" {
                        "composite"
                    } else {
                        PLUGIN_NAME
                    })
                );
            }
            let plugins: std::collections::HashMap<String, toml::Value> =
                config["plugins"].clone().try_into().expect("plugins table");
            let resolve = |store| {
                lore_server::store::resolve_plugin_config_with_fallback(
                    &plugins,
                    PLUGIN_NAME,
                    store,
                )
                .expect("store config resolves")
            };
            PostgresS3ImmutableStorePluginFactory
                .validate_config(&resolve("immutable_store"))
                .expect("immutable store config validates");
            PostgresMutableStorePluginFactory
                .validate_config(&resolve("mutable_store"))
                .expect("mutable store config validates");
            PostgresLockStorePluginFactory
                .validate_config(&resolve("lock_store"))
                .expect("lock store config validates");
        }
    }

    #[test]
    fn rejects_unknown_and_unsafe_store_settings_without_connecting() {
        let mut config = store_config("mutable_store");
        config["postgres"]
            .as_table_mut()
            .expect("postgres table")
            .insert("max_batch_size".to_string(), toml::Value::Integer(10));
        let error = PostgresMutableStorePluginFactory
            .validate_config(&config)
            .expect_err("catalog-only setting should be rejected");
        assert!(error.to_string().contains("max_batch_size"));

        let mut config = store_config("lock_store");
        config["postgres"]["schema"] = toml::Value::String("not-safe;drop".to_string());
        let error = PostgresLockStorePluginFactory
            .validate_config(&config)
            .expect_err("unsafe identifier should fail validation");
        assert!(error.to_string().contains("identifier"));
    }
}
