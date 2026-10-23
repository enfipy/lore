// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// Copyright 2026 David
// SPDX-License-Identifier: MIT
//! AWS store plugin factories.
//!
//! This module provides plugin factories for AWS-backed stores:
//! - [`AwsImmutableStorePluginFactory`] - Creates S3/`DynamoDB`-backed immutable stores
//! - [`AwsMutableStorePluginFactory`] - Creates `DynamoDB`-backed mutable stores
//! - [`AwsLockStorePluginFactory`] - Creates `DynamoDB`-backed lock stores

use std::sync::Arc;
use std::time::Duration;

use lore_aws::clients::AwsClientBuilder;
use lore_aws::clients::HttpClientSettings;
use lore_aws::clients::TimeoutConfig;
use lore_aws::store::immutable_store::AwsImmutableStore;
use lore_aws::store::immutable_store::AwsImmutableStoreSettings;
use lore_aws::store::immutable_store::DynamoDbImmutableStoreSettings;
use lore_aws::store::immutable_store::S3ObjectVersioning;
use lore_aws::store::immutable_store::S3StoreSettings;
use lore_aws::store::lock_store::DynamoDbLockStore;
use lore_aws::store::mutable_store::AwsMutableStore;
use lore_aws::store::mutable_store::AwsMutableStoreSettings;
use lore_aws::store::mutable_store::DynamoDbMutableStoreSettings;
use lore_aws::telemetry::AWSResourceDetector;
use lore_base::error::PluginConfigError;
use lore_base::error::PluginInitError;
use lore_base::runtime::runtime;
use lore_revision::lock::LockStore;
use lore_storage::ImmutableStore;
use lore_storage::MutableStore;
use opentelemetry_sdk::resource::ResourceDetector;
use serde::Deserialize;
use tracing::info;

use crate::plugins::ImmutableStorePluginFactory;
use crate::plugins::LockStorePluginFactory;
use crate::plugins::MutableStorePluginFactory;
use crate::plugins::PluginError;
use crate::plugins::PluginRegistry;

#[lore_macro::test_pub]
const PLUGIN_NAME: &str = "aws";

// =============================================================================
// Configuration Structs
// =============================================================================

/// Reusable configuration for S3-compatible immutable payload storage.
#[derive(Debug, Clone, Deserialize)]
pub struct S3ImmutableStorePluginConfig {
    /// HTTP client settings for object-store operations.
    #[serde(default)]
    pub http: HttpClientSettings,
    /// S3 bucket name for storing fragment payloads.
    pub s3_bucket: String,
    /// Optional S3 endpoint URL (for `LocalStack` or other S3-compatible services).
    #[serde(default)]
    pub s3_endpoint_url: Option<String>,
    /// Optional S3 region.
    #[serde(default)]
    pub s3_region: Option<String>,
    /// Whether the S3-compatible backend retains historical object versions.
    ///
    /// Defaults to `versioned`, which preserves the existing conservative behavior. Set this to
    /// `unversioned` only for a backend such as Cloudflare R2 where one exact-key `DeleteObject`
    /// permanently removes the payload.
    #[serde(default)]
    pub s3_object_versioning: S3ObjectVersioning,

    /// Slow operation threshold in milliseconds for S3 operations.
    #[serde(default = "default_slow_threshold")]
    pub s3_slow_operation_threshold_millis: u64,

    /// Timeout in milliseconds for object-store operations.
    #[serde(default = "default_timeout")]
    pub timeout_millis: u64,

    /// Force write mode (bypasses some safety checks).
    #[serde(default)]
    pub force_write: bool,

    /// Force path-style S3 addressing for compatible stores behind non-AWS hostnames.
    #[serde(default)]
    pub s3_force_path_style: bool,
}

/// Invalid shared S3-compatible immutable-store configuration.
#[derive(Debug, thiserror::Error)]
pub enum S3ImmutableStoreConfigError {
    /// An object store cannot address an unnamed bucket.
    #[error("s3_bucket must not be empty")]
    EmptyBucket,
    /// A zero operation timeout would fail every request immediately.
    #[error("timeout_millis must be greater than zero")]
    ZeroTimeout,
}

impl S3ImmutableStorePluginConfig {
    /// Validate settings shared by all S3-compatible immutable-store plugins.
    pub fn validate(&self) -> Result<(), S3ImmutableStoreConfigError> {
        if self.s3_bucket.trim().is_empty() {
            return Err(S3ImmutableStoreConfigError::EmptyBucket);
        }
        if self.timeout_millis == 0 {
            return Err(S3ImmutableStoreConfigError::ZeroTimeout);
        }
        Ok(())
    }

    /// Build and verify the configured S3-compatible client at plugin startup.
    pub fn create_client(&self, plugin_name: &str) -> Result<lore_aws::s3::S3, PluginError> {
        let config = self.clone();
        // Plugin construction is synchronous and runs once at startup. The plugin trait does not
        // expose an asynchronous construction hook.
        #[allow(clippy::disallowed_methods)]
        tokio::task::block_in_place(|| {
            runtime().block_on(Box::pin(async move {
                let client = AwsClientBuilder::builder()
                    .with_http_settings(&config.http)
                    .maybe_endpoint(config.s3_endpoint_url)
                    .maybe_region(config.s3_region)
                    .with_timeout_config(
                        TimeoutConfig::builder()
                            .operation_timeout(Duration::from_millis(config.timeout_millis))
                            .build(),
                    )
                    .build_config()
                    .await
                    .with_slow_operation_threshold(config.s3_slow_operation_threshold_millis)
                    .s3_with_path_style(config.s3_force_path_style)
                    .ensure_bucket(config.s3_bucket.clone())
                    .build()
                    .await
                    .map_err(|error| {
                        PluginError::from(PluginInitError {
                            plugin_name: plugin_name.to_string(),
                            message: format!("Failed to create S3-compatible client: {error}"),
                        })
                    })?;
                client
                    .validate_bucket_versioning(&config.s3_bucket, config.s3_object_versioning)
                    .await
                    .map_err(|error| {
                        PluginError::from(PluginInitError {
                            plugin_name: plugin_name.to_string(),
                            message: format!("Invalid S3 bucket versioning: {error}"),
                        })
                    })?;
                Ok(client)
            }))
        })
    }

    /// Convert plugin settings to the immutable store's object-store settings.
    pub fn store_settings(&self) -> S3StoreSettings {
        S3StoreSettings {
            bucket: self.s3_bucket.clone(),
            endpoint_url: self.s3_endpoint_url.clone(),
            region: self.s3_region.clone(),
            object_versioning: self.s3_object_versioning,
            slow_operation_threshold_millis: self.s3_slow_operation_threshold_millis,
            timeout_millis: self.timeout_millis,
        }
    }
}

/// Configuration for the AWS immutable store plugin.
#[derive(Debug, Clone, Deserialize)]
//#[serde(deny_unknown_fields)]
pub struct AwsImmutableStorePluginConfig {
    /// S3-compatible payload-store settings.
    #[serde(flatten)]
    pub s3: S3ImmutableStorePluginConfig,

    /// `DynamoDB` table name for storing fragment associations.
    pub dynamodb_fragments_table: String,

    /// `DynamoDB` table name for storing fragment state, where a row's presence means the hash
    /// exists.
    ///
    /// Required, and deliberately without an alias: this table is its own, distinct from the one
    /// that held fragment metadata, and which table serves it is a decision rather than something
    /// to inherit.
    pub dynamodb_fragment_state_table: String,

    /// Optional `DynamoDB` table to read fragments from for objects written before they moved onto
    /// the S3 object.
    ///
    /// Accepts the older `dynamodb_metadata_table` spelling, which is what a configuration written
    /// before that change already points at — that table holds fragment metadata, and reading it is
    /// exactly what it is still needed for.
    ///
    /// Leaving it unset declares that no object predating the change exists, so an object carrying
    /// no metadata of its own is reported as damaged rather than described from a row that cannot
    /// be about it. Set it only where such objects may still exist; once a backfill has given them
    /// all their own metadata, removing it retires the fallback read.
    #[serde(default, alias = "dynamodb_metadata_table")]
    pub dynamodb_fragment_metadata_table: Option<String>,

    /// Optional `DynamoDB` endpoint URL (for `LocalStack` or other `DynamoDB`-compatible services).
    #[serde(default)]
    pub dynamodb_endpoint_url: Option<String>,

    /// Optional `DynamoDB` region.
    #[serde(default)]
    pub dynamodb_region: Option<String>,

    /// Slow operation threshold in milliseconds for `DynamoDB` operations.
    #[serde(default = "default_slow_threshold")]
    pub dynamodb_slow_operation_threshold_millis: u64,
}

/// Configuration for the AWS mutable store plugin.
///
/// This configuration is deserialized from TOML and contains all settings
/// needed to create an [`AwsMutableStore`].
#[derive(Debug, Clone, Deserialize)]
//#[serde(deny_unknown_fields)]
pub struct AwsMutableStorePluginConfig {
    /// HTTP client settings for AWS operations.
    #[serde(default)]
    pub http: HttpClientSettings,

    /// `DynamoDB` table name for storing mutable data.
    pub dynamodb_table: String,

    /// Optional `DynamoDB` endpoint URL (for `LocalStack` or other `DynamoDB`-compatible services).
    #[serde(default)]
    pub dynamodb_endpoint_url: Option<String>,

    /// Optional `DynamoDB` region.
    #[serde(default)]
    pub dynamodb_region: Option<String>,

    /// Slow operation threshold in milliseconds for `DynamoDB` operations.
    #[serde(default = "default_slow_threshold")]
    pub dynamodb_slow_operation_threshold_millis: u64,

    /// Timeout in milliseconds for AWS operations.
    #[serde(default = "default_timeout")]
    pub timeout_millis: u64,

    /// Force write mode (bypasses some safety checks).
    #[serde(default)]
    pub force_write: bool,
}

/// Configuration for the AWS lock store plugin.
///
/// This configuration is deserialized from TOML and contains all settings
/// needed to create a [`DynamoDbLockStore`].
#[derive(Debug, Clone, Deserialize)]
//#[serde(deny_unknown_fields)]
pub struct AwsLockStorePluginConfig {
    /// HTTP client settings for AWS operations.
    #[serde(default)]
    pub http: HttpClientSettings,

    /// `DynamoDB` table name for storing locks.
    pub dynamodb_table: String,

    /// Optional `DynamoDB` endpoint URL (for `LocalStack` or other `DynamoDB`-compatible services).
    #[serde(default)]
    pub dynamodb_endpoint_url: Option<String>,

    /// Optional `DynamoDB` region.
    #[serde(default)]
    pub dynamodb_region: Option<String>,

    /// Slow operation threshold in milliseconds for `DynamoDB` operations.
    #[serde(default = "default_slow_threshold")]
    pub dynamodb_slow_operation_threshold_millis: u64,

    /// Timeout in milliseconds for AWS operations.
    #[serde(default = "default_timeout")]
    pub timeout_millis: u64,
}

fn default_slow_threshold() -> u64 {
    u64::MAX
}

fn default_timeout() -> u64 {
    5000
}

// =============================================================================
// Plugin Factory Implementations
// =============================================================================

/// Plugin factory for creating AWS immutable stores.
///
/// This factory creates [`AwsImmutableStore`] instances backed by S3 (for payloads)
/// and `DynamoDB` (for fragment associations and metadata).
pub struct AwsImmutableStorePluginFactory;

impl ImmutableStorePluginFactory for AwsImmutableStorePluginFactory {
    fn name(&self) -> &'static str {
        PLUGIN_NAME
    }

    fn validate_config(&self, config: &toml::Value) -> Result<(), PluginError> {
        let plugin_name = self.name();

        // Deserialize and validate configuration without creating AWS clients
        let plugin_config: AwsImmutableStorePluginConfig =
            config.clone().try_into().map_err(|e| {
                PluginError::from(PluginConfigError {
                    plugin_name: plugin_name.to_string(),
                    message: format!("Failed to deserialize AWS immutable store config: {e}"),
                })
            })?;
        plugin_config.s3.validate().map_err(|error| {
            PluginError::from(PluginConfigError {
                plugin_name: plugin_name.to_string(),
                message: error.to_string(),
            })
        })?;

        Ok(())
    }

    fn create(&self, config: &toml::Value) -> Result<Arc<dyn ImmutableStore>, PluginError> {
        let plugin_name = self.name();

        // Deserialize configuration
        let plugin_config: AwsImmutableStorePluginConfig =
            config.clone().try_into().map_err(|e| {
                PluginError::from(PluginConfigError {
                    plugin_name: plugin_name.to_string(),
                    message: format!("Failed to deserialize AWS immutable store config: {e}"),
                })
            })?;

        info!(
            plugin_name = plugin_name,
            s3_bucket = %plugin_config.s3.s3_bucket,
            fragments_table = %plugin_config.dynamodb_fragments_table,
            fragment_state_table = %plugin_config.dynamodb_fragment_state_table,
            "Creating AWS immutable store: {plugin_config:?}"
        );

        let s3_client = plugin_config.s3.create_client(plugin_name)?;

        // DynamoDB construction is also asynchronous behind a synchronous plugin hook.
        #[allow(clippy::disallowed_methods)]
        let dynamodb_client = tokio::task::block_in_place(|| {
            runtime().block_on(Box::pin(async {
                let dynamodb_client_builder = Box::pin(
                    AwsClientBuilder::builder()
                        .with_http_settings(&plugin_config.s3.http)
                        .maybe_endpoint(plugin_config.dynamodb_endpoint_url.clone())
                        .maybe_region(plugin_config.dynamodb_region.clone())
                        .with_timeout_config(
                            TimeoutConfig::builder()
                                .operation_timeout(Duration::from_millis(
                                    plugin_config.s3.timeout_millis,
                                ))
                                .build(),
                        )
                        .build_config(),
                )
                .await
                .with_slow_operation_threshold(
                    plugin_config.dynamodb_slow_operation_threshold_millis,
                )
                .dynamodb()
                .ensure_table(&plugin_config.dynamodb_fragments_table)
                .ensure_table(&plugin_config.dynamodb_fragment_state_table);

                let dynamodb_client =
                    Box::pin(dynamodb_client_builder.build())
                        .await
                        .map_err(|e| {
                            PluginError::from(PluginInitError {
                                plugin_name: plugin_name.to_string(),
                                message: format!("Failed to create DynamoDB client: {e}"),
                            })
                        })?;

                Ok::<_, PluginError>(dynamodb_client)
            }))
        })?;

        let s3_settings = plugin_config.s3.store_settings();

        let dynamodb_settings = DynamoDbImmutableStoreSettings {
            fragments_table_name: plugin_config.dynamodb_fragments_table,
            fragment_state_table_name: plugin_config.dynamodb_fragment_state_table,
            fragment_metadata_table_name: plugin_config.dynamodb_fragment_metadata_table,
            endpoint_url: plugin_config.dynamodb_endpoint_url,
            region: plugin_config.dynamodb_region,
            slow_operation_threshold_millis: plugin_config.dynamodb_slow_operation_threshold_millis,
            timeout_millis: plugin_config.s3.timeout_millis,
        };

        let store_settings = AwsImmutableStoreSettings::new(
            s3_settings,
            dynamodb_settings,
            plugin_config.s3.force_write,
        );

        let store = AwsImmutableStore::new(s3_client, dynamodb_client, &store_settings);

        Ok(Arc::new(store))
    }
}

/// Plugin factory for creating AWS mutable stores.
///
/// This factory creates [`AwsMutableStore`] instances backed by `DynamoDB`.
pub struct AwsMutableStorePluginFactory;

impl MutableStorePluginFactory for AwsMutableStorePluginFactory {
    fn name(&self) -> &'static str {
        PLUGIN_NAME
    }

    fn validate_config(&self, config: &toml::Value) -> Result<(), PluginError> {
        let plugin_name = self.name();

        // Deserialize and validate configuration without creating AWS clients
        let _plugin_config: AwsMutableStorePluginConfig =
            config.clone().try_into().map_err(|e| {
                PluginError::from(PluginConfigError {
                    plugin_name: plugin_name.to_string(),
                    message: format!("Failed to deserialize AWS mutable store config: {e}"),
                })
            })?;

        Ok(())
    }

    fn create(
        &self,
        config: &toml::Value,
        immutable_store: Arc<dyn ImmutableStore>,
    ) -> Result<Arc<dyn MutableStore>, PluginError> {
        let plugin_name = self.name();

        // Deserialize configuration
        let plugin_config: AwsMutableStorePluginConfig =
            config.clone().try_into().map_err(|e| {
                PluginError::from(PluginConfigError {
                    plugin_name: plugin_name.to_string(),
                    message: format!("Failed to deserialize AWS mutable store config: {e}"),
                })
            })?;

        info!(
            plugin_name = plugin_name,
            dynamodb_table = %plugin_config.dynamodb_table,
            "Creating AWS mutable store: {plugin_config:?}"
        );

        // Plugin construction is a synchronous trait method. It runs once at startup, one plugin
        // at a time, so at most one core is handed off at a time.
        #[allow(clippy::disallowed_methods)]
        let dynamodb_client = tokio::task::block_in_place(|| {
            runtime().block_on(Box::pin(async {
                let builder = Box::pin(
                    AwsClientBuilder::builder()
                        .with_http_settings(&plugin_config.http)
                        .maybe_endpoint(plugin_config.dynamodb_endpoint_url.clone())
                        .maybe_region(plugin_config.dynamodb_region.clone())
                        .with_timeout_config(
                            TimeoutConfig::builder()
                                .operation_timeout(Duration::from_millis(
                                    plugin_config.timeout_millis,
                                ))
                                .build(),
                        )
                        .build_config(),
                )
                .await
                .with_slow_operation_threshold(
                    plugin_config.dynamodb_slow_operation_threshold_millis,
                )
                .dynamodb()
                .ensure_table(&plugin_config.dynamodb_table);

                Box::pin(builder.build()).await.map_err(|e| {
                    PluginError::from(PluginInitError {
                        plugin_name: plugin_name.to_string(),
                        message: format!("Failed to create DynamoDB client: {e}"),
                    })
                })
            }))
        })?;

        // Create settings
        let dynamodb_settings = DynamoDbMutableStoreSettings {
            mutable_store_table_name: plugin_config.dynamodb_table,
            endpoint_url: plugin_config.dynamodb_endpoint_url,
            region: plugin_config.dynamodb_region,
            slow_operation_threshold_millis: plugin_config.dynamodb_slow_operation_threshold_millis,
            timeout_millis: plugin_config.timeout_millis,
        };

        let store_settings =
            AwsMutableStoreSettings::new(dynamodb_settings, plugin_config.force_write);

        let store = AwsMutableStore::new(dynamodb_client, &store_settings, immutable_store);

        Ok(Arc::new(store))
    }
}

/// Plugin factory for creating `DynamoDB` lock stores.
///
/// This factory creates [`DynamoDbLockStore`] instances backed by `DynamoDB`.
pub struct AwsLockStorePluginFactory;

impl LockStorePluginFactory for AwsLockStorePluginFactory {
    fn name(&self) -> &'static str {
        PLUGIN_NAME
    }

    fn validate_config(&self, config: &toml::Value) -> Result<(), PluginError> {
        let plugin_name = self.name();

        // Deserialize and validate configuration without creating AWS clients
        let _plugin_config: AwsLockStorePluginConfig = config.clone().try_into().map_err(|e| {
            PluginError::from(PluginConfigError {
                plugin_name: plugin_name.to_string(),
                message: format!("Failed to deserialize DynamoDB lock store config: {e}"),
            })
        })?;

        Ok(())
    }

    fn create(&self, config: &toml::Value) -> Result<Arc<dyn LockStore>, PluginError> {
        let plugin_name = self.name();

        // Deserialize configuration
        let plugin_config: AwsLockStorePluginConfig = config.clone().try_into().map_err(|e| {
            PluginError::from(PluginConfigError {
                plugin_name: plugin_name.to_string(),
                message: format!("Failed to deserialize DynamoDB lock store config: {e}"),
            })
        })?;

        info!(
            plugin_name = plugin_name,
            dynamodb_table = %plugin_config.dynamodb_table,
            "Creating DynamoDB lock store: {plugin_config:?}"
        );

        // Plugin construction is a synchronous trait method. It runs once at startup, one plugin
        // at a time, so at most one core is handed off at a time.
        #[allow(clippy::disallowed_methods)]
        let dynamodb_client = tokio::task::block_in_place(|| {
            runtime().block_on(async {
                let builder = Box::pin(
                    AwsClientBuilder::builder()
                        .with_http_settings(&plugin_config.http)
                        .maybe_endpoint(plugin_config.dynamodb_endpoint_url.clone())
                        .maybe_region(plugin_config.dynamodb_region.clone())
                        .with_timeout_config(
                            TimeoutConfig::builder()
                                .operation_timeout(Duration::from_millis(
                                    plugin_config.timeout_millis,
                                ))
                                .build(),
                        )
                        .build_config(),
                )
                .await
                .with_slow_operation_threshold(
                    plugin_config.dynamodb_slow_operation_threshold_millis,
                )
                .dynamodb()
                .ensure_table(&plugin_config.dynamodb_table);

                Box::pin(builder.build()).await.map_err(|e| {
                    PluginError::from(PluginInitError {
                        plugin_name: plugin_name.to_string(),
                        message: format!("Failed to create DynamoDB client: {e}"),
                    })
                })
            })
        })?;

        let store = DynamoDbLockStore::new(dynamodb_client, plugin_config.dynamodb_table);

        Ok(Arc::new(store))
    }
}

// =============================================================================
// Registration
// =============================================================================

/// Registers the AWS plugin factories and resource detector with the given
/// registry.
///
/// The resource detector is registered by the module rather than through the
/// store factories: it describes the AWS deployment environment and is
/// independent of which (if any) AWS store is the configured backend.
pub fn register(registry: &mut PluginRegistry) {
    registry.register_immutable_store_plugin(Box::new(AwsImmutableStorePluginFactory));
    registry.register_mutable_store_plugin(Box::new(AwsMutableStorePluginFactory));
    registry.register_lock_store_plugin(Box::new(AwsLockStorePluginFactory));
    registry.register_resource_detector(|runtime_handle| {
        Box::new(AWSResourceDetector::new(runtime_handle)) as Box<dyn ResourceDetector>
    });
}

// =============================================================================
// Tests
// =============================================================================
