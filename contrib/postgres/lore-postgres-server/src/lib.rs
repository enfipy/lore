// Copyright 2026 David
// SPDX-License-Identifier: MIT
//! Derived Lore server with a PostgreSQL catalog and S3-compatible payload storage.

use std::sync::Arc;

use lore_aws::store::immutable_store::AwsImmutableStore;
use lore_aws::store::immutable_store::ObjectStoreImmutableStoreSettings;
use lore_base::error::PluginConfigError;
use lore_base::error::PluginInitError;
use lore_base::runtime::runtime;
use lore_postgres::PostgresFragmentCatalog;
use lore_postgres::PostgresFragmentCatalogConfig;
use lore_server::plugins::ImmutableStorePluginFactory;
use lore_server::plugins::PluginError;
use lore_server::plugins::PluginRegistry;
use lore_server::plugins::aws::S3ImmutableStorePluginConfig;
use lore_storage::ImmutableStore;
use serde::Deserialize;
use tracing::info;

/// Configuration name used in `[immutable_store]` and `[plugins]`.
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

/// Register the PostgreSQL/S3 immutable-store plugin.
pub fn register(registry: &mut PluginRegistry) {
    registry.register_immutable_store_plugin(Box::new(PostgresS3ImmutableStorePluginFactory));
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
    fn register_adds_postgres_s3_plugin() {
        let mut registry = PluginRegistry::new();
        register(&mut registry);

        assert_eq!(
            registry.list_immutable_store_plugins(),
            vec![PLUGIN_NAME.to_string()]
        );
    }
}
