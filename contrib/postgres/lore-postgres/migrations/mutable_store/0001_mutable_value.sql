-- Copyright 2026 David
-- SPDX-License-Identifier: MIT
-- Canonical schema for checksum/review. The implementation substitutes a validated schema name.

-- `key` carries its key type in its first byte, as in Lore's DynamoDB mutable store, so a key
-- type is one contiguous key range within a partition.
CREATE TABLE mutable_value (
    partition BYTEA NOT NULL CHECK (octet_length(partition) = 16),
    key BYTEA NOT NULL CHECK (octet_length(key) = 32),
    value BYTEA NOT NULL CHECK (octet_length(value) = 32),
    PRIMARY KEY (partition, key)
);
