// Copyright 2026 David
// SPDX-License-Identifier: MIT
//! Backend-neutral lifecycle and association catalog for object-backed stores.
//!
//! Payload bytes and their immutable [`Fragment`] description belong to the object-store adapter.
//! This seam owns only mutable lifecycle state and partition/context associations.

use async_trait::async_trait;

use crate::Address;
use crate::Hash;
use crate::Partition;
use crate::StoreError;

/// Mutable lifecycle of an object-store payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FragmentState {
    /// The payload is stored and readable.
    Stored,
    /// An obliteration currently owns the hash.
    Obliterating,
    /// The payload was permanently removed; the row is retained as a tombstone.
    Obliterated,
}

/// Catalog facts needed to resolve one address.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CatalogResolution {
    /// Whether this exact partition/context association exists.
    pub associated: bool,
    /// Current lifecycle state, or `None` when the hash was never published.
    pub state: Option<FragmentState>,
}

/// Result of releasing one association and claiming obliteration when possible.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BeginObliteration {
    /// No lifecycle row exists, so nothing was changed.
    NoState,
    /// A previous request completed payload deletion and finalization.
    AlreadyObliterated,
    /// A previous request marked the hash but did not finalize payload deletion; resume it.
    ResumePayloadDeletion,
    /// The requested association was removed, but other associations still reference the payload.
    ReferencesRemain,
    /// The requested association was removed and the caller may delete the payload.
    PayloadUnreferenced,
}

/// Mutable catalog used by an object-backed immutable store.
///
/// Implementations own concurrency control. In particular, `associate` must not make an
/// obliterating hash active, and `begin_obliteration` must serialize against associations so a
/// `PayloadUnreferenced` result is safe to act on.
#[async_trait]
pub trait FragmentCatalog: Send + Sync {
    /// Resolve lifecycle and association facts for one address.
    async fn resolve(
        &self,
        partition: Partition,
        address: Address,
    ) -> Result<CatalogResolution, StoreError>;

    /// Resolve a batch in input order.
    async fn resolve_batch(
        &self,
        partition: Partition,
        addresses: &[Address],
    ) -> Result<Vec<CatalogResolution>, StoreError>;

    /// Check only the exact association, for a payload read already validated by object storage.
    async fn association_exists(
        &self,
        partition: Partition,
        address: Address,
    ) -> Result<bool, StoreError>;

    /// Check whether a partition has any association for a payload, regardless of context.
    async fn partition_association_exists(
        &self,
        partition: Partition,
        hash: Hash,
    ) -> Result<bool, StoreError>;

    /// Publish an uploaded payload, reviving a terminal tombstone but rejecting active deletion.
    async fn publish(&self, hash: Hash) -> Result<(), StoreError>;

    /// Clear a stored state whose payload was lost, allowing the next put to repair it.
    async fn repair_missing_payload(&self, hash: Hash) -> Result<(), StoreError>;

    /// Add an exact association idempotently, rejecting non-stored states.
    async fn associate(&self, partition: Partition, address: Address) -> Result<(), StoreError>;

    /// Remove one association and claim the hash when no references remain.
    async fn begin_obliteration(
        &self,
        partition: Partition,
        address: Address,
    ) -> Result<BeginObliteration, StoreError>;

    /// Replace the in-progress marker with a terminal tombstone.
    async fn finalize_obliteration(&self, hash: Hash) -> Result<(), StoreError>;

    /// Maximum supported batch size, when bounded by the backend.
    fn max_query_batch(&self) -> Option<usize> {
        None
    }
}

/// Backend-independent behavioral checks used by catalog integration tests.
#[cfg(any(test, feature = "catalog-contract-tests"))]
pub mod contract {
    use super::*;
    use crate::Context;

    /// Exercise publication, associations, batch ordering, obliteration, repair, and revival.
    pub async fn run(catalog: &dyn FragmentCatalog) {
        let first_partition: Partition = Context::from([0x11; 16]).into();
        let second_partition: Partition = Context::from([0x12; 16]).into();
        let address = Address {
            hash: Hash::from([0x13; 32]),
            context: Context::from([0x14; 16]),
        };
        let miss = catalog
            .resolve(first_partition, address)
            .await
            .expect("resolve absent");
        assert_eq!(miss, CatalogResolution::default());

        catalog.publish(address.hash).await.expect("publish");
        catalog
            .associate(first_partition, address)
            .await
            .expect("associate");
        catalog
            .associate(first_partition, address)
            .await
            .expect("idempotent associate");
        assert!(catalog
            .partition_association_exists(first_partition, address.hash)
            .await
            .expect("partition association exists"));
        assert!(!catalog
            .partition_association_exists(second_partition, address.hash)
            .await
            .expect("partition association absent"));
        assert_eq!(
            catalog
                .resolve(first_partition, address)
                .await
                .expect("resolve stored"),
            CatalogResolution {
                associated: true,
                state: Some(FragmentState::Stored),
            }
        );

        let absent = Address {
            hash: Hash::from([0x15; 32]),
            context: Context::from([0x16; 16]),
        };
        let batch = catalog
            .resolve_batch(first_partition, &[address, absent, address])
            .await
            .expect("batch resolve");
        assert_eq!(batch.len(), 3);
        assert!(batch[0].associated);
        assert_eq!(batch[1], CatalogResolution::default());
        assert_eq!(batch[2], batch[0]);

        catalog
            .associate(second_partition, address)
            .await
            .expect("second association");
        assert_eq!(
            catalog
                .begin_obliteration(first_partition, address)
                .await
                .expect("release first"),
            BeginObliteration::ReferencesRemain
        );
        assert_eq!(
            catalog
                .begin_obliteration(second_partition, address)
                .await
                .expect("claim last"),
            BeginObliteration::PayloadUnreferenced
        );
        assert_eq!(
            catalog
                .begin_obliteration(second_partition, address)
                .await
                .expect("resume interrupted deletion"),
            BeginObliteration::ResumePayloadDeletion
        );
        assert!(catalog.associate(first_partition, address).await.is_err());
        catalog
            .finalize_obliteration(address.hash)
            .await
            .expect("finalize");
        assert_eq!(
            catalog
                .begin_obliteration(first_partition, address)
                .await
                .expect("retry finalized deletion"),
            BeginObliteration::AlreadyObliterated
        );
        catalog
            .publish(address.hash)
            .await
            .expect("revive tombstone");
        catalog
            .associate(first_partition, address)
            .await
            .expect("associate revived");
        catalog
            .repair_missing_payload(address.hash)
            .await
            .expect("repair");
        let repaired = catalog
            .resolve(first_partition, address)
            .await
            .expect("resolve repaired");
        assert!(repaired.associated);
        assert_eq!(repaired.state, None);
    }
}
