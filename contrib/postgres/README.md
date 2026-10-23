<!-- Copyright 2026 David; SPDX-License-Identifier: MIT -->

# Lore with PostgreSQL, S3-compatible storage, and Consul

This derived server keeps each system on the job it is designed for:

- PostgreSQL stores fragment lifecycle state and repository associations, Lore's mutable values
  (branch heads, branch and repository metadata), and resource locks.
- Hetzner Object Storage or Cloudflare R2 stores fragment payload bytes through its S3-compatible
  API.
- Consul discovers Lore peers. Consul KV does not store Lore data.

The catalog interface and its obliteration state contract live in the backend-neutral
`lore-storage` crate. `lore-postgres` depends on that interface directly and has no dependency on
`lore-aws`. The derived `lore-postgres-server` binary is the composition point that pairs the
PostgreSQL catalog adapter with Lore's S3-compatible payload adapter, and registers the PostgreSQL
mutable and lock stores under the same `postgres_s3` plugin name.

The example installs the PostgreSQL catalog and S3-compatible payload store as the durable tier
behind Lore's local cache. This also gives the Consul topology a live subscriber, so peer discovery
drives composite-store replication. Payload network calls never run inside PostgreSQL transactions.
Registration, association, and obliteration transitions use short row-locking transactions plus a
per-hash PostgreSQL advisory guard, so object I/O is ordered across Lore processes without holding
a database transaction open over the network.

Keep the example's `replica_factory` section whenever Consul is enabled for a composite store.
It supplies the client factory used to turn discovered peers into replication targets; omitting it
would leave discovery active but unable to materialize peer connections.

## Build

Run these commands from this directory:

```sh
cargo build --release -p lore-postgres-server
cargo test -p lore-postgres-server --lib
```

The binary is `target/release/loreserver-postgres`. This workspace restates the Lore workspace's
`[patch.crates-io]` table and builds `release` with `panic = "abort"`, as Lore's own release builds
do. On `aarch64-unknown-linux-gnu` the repository's `.cargo/config.toml` builds for the generic
Armv8-A baseline, so the binary also runs on Neoverse N1 (Ampere Altra, Graviton2).

## Configure

Start with [`config-hetzner-consul.toml`](config-hetzner-consul.toml) for Hetzner or
[`config-r2-consul.toml`](config-r2-consul.toml) for R2. Copy it to `local.toml` in a dedicated
configuration directory and keep credentials outside the file:

```sh
export AWS_ACCESS_KEY_ID='<S3 access key>'
export AWS_SECRET_ACCESS_KEY='<S3 secret key>'
export LORE__PLUGINS__POSTGRES_S3__IMMUTABLE_STORE__POSTGRES__CONNECTION_STRING='host=postgres.internal dbname=lore user=lore sslmode=require password=<secret>'
export LORE__PLUGINS__POSTGRES_S3__MUTABLE_STORE__POSTGRES__CONNECTION_STRING='host=postgres.internal dbname=lore user=lore sslmode=require target_session_attrs=read-write password=<secret>'
export LORE__PLUGINS__POSTGRES_S3__LOCK_STORE__POSTGRES__CONNECTION_STRING='host=postgres.internal dbname=lore user=lore sslmode=require target_session_attrs=read-write password=<secret>'
export CONSUL_HTTP_ADDR='http://consul.internal:8500'
export CONSUL_HTTP_TOKEN='<Consul ACL token>'

install -m 0600 config-hetzner-consul.toml /etc/lore/local.toml
target/release/loreserver-postgres --config /etc/lore
```

For Hetzner, enable bucket versioning and use `s3_object_versioning = "versioned"`. Lore deletes
every exact-key version and delete marker, repeats the listing until empty, and verifies that the
current object is absent before committing its catalog tombstone. Do not enable Object Lock or a
retention policy on the Lore bucket: either can intentionally reject permanent obliteration.

For R2, use `s3_object_versioning = "unversioned"`. Lore then permanently obliterates a payload
with one exact-key `DeleteObject` request. At startup Lore queries the bucket's actual versioning
state and refuses a configuration that contradicts the provider response.

## Mutable and lock stores

The examples keep mutable values and resource locks in PostgreSQL as well:

```toml
[mutable_store]
mode = "postgres_s3"

[lock_store]
mode = "postgres_s3"

[plugins.postgres_s3.mutable_store.postgres]
connection_string = "host=pg1,pg2,pg3 dbname=lore user=lore sslmode=require target_session_attrs=read-write"
schema = "lore_mutable_store"        # default "lore"
max_connections = 16                 # default 16
connect_timeout_millis = 5_000       # connect and pool checkout; default 5000
statement_timeout_millis = 30_000    # per statement, including row-lock waits; 0 disables

[plugins.postgres_s3.lock_store.postgres]
# The same keys as the mutable store.
connection_string = "host=pg1,pg2,pg3 dbname=lore user=lore sslmode=require target_session_attrs=read-write"
schema = "lore_lock_store"
```

These replace Lore's DynamoDB mutable and lock tables with the same semantics. Every write is a
committed PostgreSQL statement or transaction before Lore acknowledges it; unlike the local mutable
store there is no write-behind buffer, so a Lore server can stop at any moment, or start again on
another machine, without losing an acknowledged branch-head update or lock. Compare-and-swap locks
the row it compares, so of several concurrent writers, including Lore servers sharing the database,
each either swaps or is told the value that beat it. Lock batches are all-or-nothing.
The mutable store differs from the DynamoDB store in one respect: `list` with a null partition
returns every partition, as the trait and the local store specify.

Each store keeps its tables and its migration ledger apart, so the catalog, mutable store, and lock
store may share one schema or use one schema each.

For a highly available PostgreSQL, list every candidate host and set
`target_session_attrs=read-write`; a new connection then skips standbys and unreachable hosts. A
pooled connection, the catalog's included, is re-checked before each use and discarded if it has become read-only, and a
write that meets a read-only session, a shutting-down server, or a closed connection fails with
Lore's retryable `SlowDown`. Set TCP keepalives (`keepalives_idle`, `keepalives_interval`,
`keepalives_retries`) or `tcp_user_timeout` in the connection string so a vanished host fails a
statement quickly. This client reads `tcp_user_timeout` in seconds, whereas libpq reads it in
milliseconds.

## PostgreSQL lifecycle

On startup each adapter (catalog, mutable store, lock store):

1. validates the schema identifier and pool limits;
2. verifies a pooled connection using TLS when requested by the connection string;
3. takes a transaction-scoped advisory lock;
4. creates the schema and its own migration ledger (`schema_migrations`,
   `mutable_store_migrations`, or `lock_store_migrations`) if absent; and
5. applies or verifies its checksummed migrations.

The mutable store keeps one row per value in `mutable_value` (`partition`, `key`, `value`, all
`BYTEA`, keyed by partition and key; the key's first byte is its key type, as in the DynamoDB
table). The lock store keeps one row per lock in `resource_lock`, keyed by repository, branch, and
resource hash.

The database role needs `CONNECT`, `USAGE`/`CREATE` for the configured schemas, and DML privileges
on the tables. Back up PostgreSQL and R2 together according to a documented recovery point.
The catalog is authoritative for payload reachability, so restoring only one side can leave orphaned
bytes or missing payloads. With the PostgreSQL mutable store, branch heads live beside the catalog,
so one PostgreSQL backup holds both the heads and the catalog entries they reach.

## Integration tests

Tests create a unique schema and intentionally do not remove it:

```sh
export LORE_POSTGRES_TEST_URL='host=127.0.0.1 dbname=lore_test user=lore_test sslmode=disable'
cargo test -p lore-postgres --features integration-tests -- --nocapture
```

The catalog contract covers idempotent publication, ordered batch lookup, association, payload
repair, resumable obliteration, retained references, tombstone revival, and terminal finalization.
Concurrency tests race 32 associations against obliteration and 32 simultaneous obliteration
starts.

The mutable store runs Lore's mutable-store conformance battery with no known violations, the
DynamoDB store's zero-expected swap regression, 32 racing swaps from one value, 16 writers
incrementing a counter by swap without losing an update, visibility of every acknowledged write to
a second store on the same schema, and a multi-host connection string whose first host is down.
The lock store covers all-or-nothing lock and unlock batches, every lock query, owner validation,
and 32 owners racing for one lock. The connection tests check that a pooled session made read-only
is replaced and that `statement_timeout` reaches pooled sessions.
