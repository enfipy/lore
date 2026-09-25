-- Copyright 2026 David
-- SPDX-License-Identifier: MIT
-- Canonical schema for checksum/review. The implementation substitutes a validated schema name.

CREATE TABLE fragment_state (
    hash BYTEA PRIMARY KEY CHECK (octet_length(hash) = 32),
    state SMALLINT NOT NULL CHECK (state BETWEEN 0 AND 2)
);

CREATE TABLE fragment_association (
    hash BYTEA NOT NULL CHECK (octet_length(hash) = 32),
    partition BYTEA NOT NULL CHECK (octet_length(partition) = 16),
    context BYTEA NOT NULL CHECK (octet_length(context) = 16),
    PRIMARY KEY (hash, partition, context)
);
