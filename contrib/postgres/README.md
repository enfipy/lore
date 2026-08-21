<!-- Copyright 2026 David; SPDX-License-Identifier: MIT -->

# Lore with PostgreSQL, S3-compatible storage, and Consul

This derived server keeps each system on the job it is designed for:

- PostgreSQL stores fragment lifecycle state and repository associations.
- Hetzner Object Storage or Cloudflare R2 stores fragment payload bytes through its S3-compatible
  API.
- Consul discovers Lore peers. Consul KV does not store Lore data.

The catalog interface and its obliteration state contract live in the backend-neutral
`lore-storage` crate. `lore-postgres` depends on that interface directly and has no dependency on
`lore-aws`. The derived `lore-postgres-server` binary is the composition point that pairs the
PostgreSQL catalog adapter with Lore's S3-compatible payload adapter.

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

The binary is `target/release/loreserver-postgres`.

## Configure

Start with [`config-hetzner-consul.toml`](config-hetzner-consul.toml) for Hetzner or
[`config-r2-consul.toml`](config-r2-consul.toml) for R2. Copy it to `local.toml` in a dedicated
configuration directory and keep credentials outside the file:

```sh
export AWS_ACCESS_KEY_ID='<S3 access key>'
export AWS_SECRET_ACCESS_KEY='<S3 secret key>'
export LORE__PLUGINS__POSTGRES_S3__IMMUTABLE_STORE__POSTGRES__CONNECTION_STRING='host=postgres.internal dbname=lore user=lore sslmode=require password=<secret>'
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

The example uses local mutable and lock stores, which is appropriate only for a single primary.
In a multi-node deployment, use a shared implementation for mutable state and locks, or direct all
mutating work to one primary and use Lore's remote/replicated edge pattern. PostgreSQL in this
workspace replaces the two DynamoDB tables used by the immutable fragment catalog; it does not
silently change the semantics of the other store types.

## PostgreSQL lifecycle

On startup the adapter:

1. validates the schema identifier and pool limits;
2. verifies a pooled connection using TLS when requested by the connection string;
3. takes a transaction-scoped advisory lock;
4. creates the isolated schema and migration ledger if absent; and
5. applies or verifies the checksummed catalog migration.

The database role needs `CONNECT`, `USAGE`/`CREATE` for the configured schema, and DML privileges
on the catalog tables. Back up PostgreSQL and R2 together according to a documented recovery point.
The catalog is authoritative for payload reachability, so restoring only one side can leave orphaned
bytes or missing payloads.

## Integration tests

Tests create a unique schema and intentionally do not remove it:

```sh
export LORE_POSTGRES_TEST_URL='host=127.0.0.1 dbname=lore_test user=lore_test sslmode=disable'
cargo test -p lore-postgres --features integration-tests --test catalog_contract -- --nocapture
```

The contract covers idempotent publication, ordered batch lookup, association, payload repair,
resumable obliteration, retained references, tombstone revival, and terminal finalization.
Concurrency tests race 32 associations against obliteration and 32 simultaneous obliteration
starts.
