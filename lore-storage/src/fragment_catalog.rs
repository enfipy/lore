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

/// Opaque publication generation used to reject stale cross-store observations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CatalogGeneration(i64);

impl CatalogGeneration {
    /// Construct a generation from a catalog backend's native value.
    pub const fn new(value: i64) -> Self {
        Self(value)
    }

    /// Return the value for persistence by a catalog backend.
    pub const fn value(self) -> i64 {
        self.0
    }
}

/// One complete payload publication.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CatalogPublication {
    /// Current lifecycle state.
    pub state: FragmentState,
    /// Generation that identifies this exact publication.
    pub generation: CatalogGeneration,
}

/// Catalog facts needed to resolve one address.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CatalogResolution {
    /// Whether this exact partition/context association exists.
    pub associated: bool,
    /// Current publication, or `None` when the hash was never published.
    pub publication: Option<CatalogPublication>,
}

impl CatalogResolution {
    /// Current lifecycle state, when a publication exists.
    pub fn state(self) -> Option<FragmentState> {
        self.publication.map(|publication| publication.state)
    }

    /// Generation of the current publication.
    pub fn generation(self) -> Option<CatalogGeneration> {
        self.publication.map(|publication| publication.generation)
    }
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

/// Exclusive catalog ownership for one payload hash.
///
/// The guard is the seam that lets an object-store adapter keep payload I/O and its catalog
/// transition ordered across every Lore process. Implementations must release ownership when
/// [`FragmentCatalogGuard::unlock`] succeeds and must fail closed if a guard is dropped.
#[async_trait]
pub trait FragmentCatalogGuard: Send {
    /// Resolve one address while ownership is held.
    async fn resolve(
        &mut self,
        partition: Partition,
        address: Address,
    ) -> Result<CatalogResolution, StoreError>;

    /// Publish the guarded payload and add its exact association.
    async fn publish(&mut self, partition: Partition, address: Address) -> Result<(), StoreError>;

    /// Clear a missing payload if the guarded publication is still the observed generation.
    async fn repair_missing_payload(
        &mut self,
        generation: CatalogGeneration,
    ) -> Result<(), StoreError>;

    /// Remove one association and claim the guarded hash when no references remain.
    async fn begin_obliteration(
        &mut self,
        partition: Partition,
        address: Address,
    ) -> Result<BeginObliteration, StoreError>;

    /// Replace the guarded in-progress marker with a terminal tombstone.
    async fn finalize_obliteration(&mut self) -> Result<(), StoreError>;

    /// Release ownership explicitly so failures are observable.
    async fn unlock(self: Box<Self>) -> Result<(), StoreError>;
}

/// Mutable catalog used by an object-backed immutable store.
///
/// Implementations own concurrency control. In particular, `publish` must atomically advance the
/// payload generation and add its association without reviving an active deletion, and
/// `begin_obliteration` must serialize against publication so a `PayloadUnreferenced` result is
/// safe to act on.
#[async_trait]
pub trait FragmentCatalog: Send + Sync {
    /// Take exclusive ownership of mutations for one payload hash.
    async fn lock_hash(&self, hash: Hash) -> Result<Box<dyn FragmentCatalogGuard>, StoreError>;

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

    /// Atomically publish or confirm a payload and add its exact association.
    async fn publish(&self, partition: Partition, address: Address) -> Result<(), StoreError>;

    /// Clear a missing payload only if its publication has not changed since it was observed.
    async fn repair_missing_payload(
        &self,
        hash: Hash,
        generation: CatalogGeneration,
    ) -> Result<(), StoreError>;

    /// Resolve lifecycle state and whether a partition has any association for a payload.
    async fn resolve_partition(
        &self,
        partition: Partition,
        hash: Hash,
    ) -> Result<CatalogResolution, StoreError>;

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

        catalog
            .publish(first_partition, address)
            .await
            .expect("publish");
        let first = catalog
            .resolve(first_partition, address)
            .await
            .expect("resolve stored");
        assert!(first.associated);
        assert_eq!(
            first.publication.map(|publication| publication.state),
            Some(FragmentState::Stored)
        );
        let first_generation = first.publication.expect("stored publication").generation;
        catalog
            .publish(first_partition, address)
            .await
            .expect("idempotent publish");
        catalog
            .repair_missing_payload(address.hash, first_generation)
            .await
            .expect("stale repair");
        let current = catalog
            .resolve(first_partition, address)
            .await
            .expect("resolve after stale repair");
        assert_eq!(
            current.publication.map(|publication| publication.state),
            Some(FragmentState::Stored)
        );
        assert_ne!(
            current
                .publication
                .map(|publication| publication.generation),
            Some(first_generation)
        );
        let partition = catalog
            .resolve_partition(first_partition, address.hash)
            .await
            .expect("resolve partition association");
        assert!(partition.associated);
        assert_eq!(partition.publication, current.publication);
        assert!(
            !catalog
                .resolve_partition(second_partition, address.hash)
                .await
                .expect("resolve absent partition association")
                .associated
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
            .publish(second_partition, address)
            .await
            .expect("second publication");
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
        assert!(catalog.publish(first_partition, address).await.is_err());
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
            .publish(first_partition, address)
            .await
            .expect("revive tombstone");
        let generation = catalog
            .resolve(first_partition, address)
            .await
            .expect("resolve revived")
            .publication
            .expect("revived publication")
            .generation;
        catalog
            .repair_missing_payload(address.hash, generation)
            .await
            .expect("repair");
        let repaired = catalog
            .resolve(first_partition, address)
            .await
            .expect("resolve repaired");
        assert!(repaired.associated);
        assert_eq!(repaired.publication, None);

        assert_eq!(
            catalog
                .begin_obliteration(first_partition, address)
                .await
                .expect("obliterate repaired association"),
            BeginObliteration::NoState
        );
        let replacement = Address {
            hash: address.hash,
            context: Context::from([0x17; 16]),
        };
        catalog
            .publish(second_partition, replacement)
            .await
            .expect("publish replacement association");
        assert!(
            !catalog
                .resolve(first_partition, address)
                .await
                .expect("resolve obliterated repaired association")
                .associated
        );
    }
}
