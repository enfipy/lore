-- Copyright 2026 David
-- SPDX-License-Identifier: MIT
-- Canonical schema for checksum/review. The implementation substitutes a validated schema name.

CREATE TABLE resource_lock (
    repository BYTEA NOT NULL CHECK (octet_length(repository) = 16),
    branch BYTEA NOT NULL CHECK (octet_length(branch) = 16),
    hash BYTEA NOT NULL CHECK (octet_length(hash) = 32),
    description TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    locked_at_millis BIGINT NOT NULL,
    PRIMARY KEY (repository, branch, hash)
);

-- The lock queries that do not start with the repository and branch.
CREATE INDEX resource_lock_hash ON resource_lock (hash, repository, branch);
CREATE INDEX resource_lock_owner ON resource_lock (owner_id, repository, branch);
