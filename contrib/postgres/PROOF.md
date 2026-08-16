<!-- Copyright 2026 David; SPDX-License-Identifier: MIT -->

# PostgreSQL + R2 + Consul implementation proof

**Current exact-branch result: BLOCKED**

The PostgreSQL contract and a current-commit R2 round trip/obliteration pass. The complete original
618 MB recovery, interruption, outage, and cache-loss matrix was run against the DynamoDB
composition, not repeated after the PostgreSQL rebase. Those gaps are reported below rather than
being inherited as PostgreSQL results.

Run date: 2026-08-16. Primary host: `enfis1`. Run root:
`/var/tmp/lore-pg-r2-consul-20260816T141340Z`.

This proof uses PostgreSQL as Lore's semantic fragment catalog, Cloudflare R2 as the durable
unversioned payload store, and Consul as active topology discovery for a composite Lore store.
All resources are uniquely named, test-owned, and bound to loopback where applicable. No resource
was pruned during or after the proof.

## Original task acceptance

| Criterion | Result | Evidence |
| --- | --- | --- |
| Recovered source and representative Linux image are byte-identical | **BLOCKED** | The historical DynamoDB + R2 run recovered 895,923,061 bytes, including a 618,370,560-byte image. Exact PostgreSQL code commit `27caeb0` round-tripped a new 4 MiB payload with SHA-256 `ba05721353e28615767938ede7da127eefb56c02a36f6f1db18a59954581f83f`, but did not repeat the Linux image. |
| No acknowledged push references a missing fragment | **BLOCKED** | The current isolated push had 61 catalog hashes and 61/61 successful pre-obliteration R2 HEADs. Missing-payload repair can still race an association/publish path; no test establishes this invariant under that race. |
| Complete recovery works with an empty server and cache | **BLOCKED** | Passed historically with DynamoDB + R2; not repeated for the exact PostgreSQL composition. |
| Interrupted operations are resumable and expose no partial revision | **BLOCKED** | The PostgreSQL state contract proves a resumable obliteration marker, and the pre-rebase R2 outage retry passed. Interrupted upload/commit recovery was not repeated at exact code commit `27caeb0`. |
| Cache loss causes no durable data loss | **BLOCKED** | Passed historically with DynamoDB + R2; not repeated for the exact PostgreSQL composition. |
| Obliteration passes or is reported as blocking; no bypass | **PASS** | Exact code commit `27caeb0` obliterated one unfragmented, single-association hash proven absent from 10,349 prior DynamoDB hashes and 19 prior PostgreSQL hashes. It completed in 0.62 s, left state `2`, zero associations, and R2 HEAD 404. The unit test also makes `ListObjectVersions` fail and proves the unversioned branch never calls it. |

## Architecture follow-up results

| Criterion | Result | Evidence |
| --- | --- | --- |
| Catalog seam is backend-neutral | **PASS** | The interface and state contract live in `lore-storage`; `cargo tree -p lore-postgres` contains no `lore-aws`. |
| Existing DynamoDB behavior is preserved without a duplicate adapter | **PASS** | After rebasing onto current upstream, DynamoDB remains in its existing AWS store; `cargo test -p lore-aws --lib`: 133 passed. |
| PostgreSQL satisfies the catalog state-machine contract | **PASS** | The rebased state-only contract passed against a fresh isolated PostgreSQL schema on `enfis1`. |
| Association and obliteration races serialize correctly | **PASS** | 32 association attempts were rejected after the marker; among 32 simultaneous starts exactly one claimed deletion and the others observed its resumable marker. |
| R2 uses explicit unversioned exact-key deletion | **PASS** | Remote obliteration finished in 0.52 s; 9 terminal catalog hashes were all HTTP 404 in R2; no `ListObjectVersions`/HTTP 501 appeared. |
| Interrupted obliteration is resumable | **PASS** | Unit coverage verifies an existing `PayloadObliterating` marker resumes; the previous isolated R2 outage proof remains green. |
| Consul is active topology, not catalog storage | **PASS** | The corrected composite server made 328 recorded health-service queries; final server log has 0 error lines. |
| End-to-end data integrity | **PASS** | Create, stage, commit, push, and cold clone passed. The 1 MiB payload matched SHA-256 `360d9dee65c1cec3353f3309ee6bfb45eef65313b0e98c39f0f32e5416212028`. |
| PostgreSQL and R2 remain consistent | **PASS** | Before obliteration, all 19 PostgreSQL hashes had successful exact-key R2 HEADs and total payload bytes matched (`525598`). |
| Existing test data was untouched | **PASS** | The 9 newly obliterated hashes had zero overlap with all 10,349 hashes in the older isolated catalog. |
| Credentials are not logged | **PASS** | PostgreSQL config Debug output is redacted; current server logs contain no credentials or error lines. |

## Live resources

- PostgreSQL container: `lore-pg-catalog-20260816t153000z`, loopback port `15439`.
- PostgreSQL schema: `lore_live_composite_20260816t141340z`.
- Consul container: `lore-consul-20260816t141340z`, loopback port `18500`.
- Server ports: `44237` and `44239`, loopback only.
- R2 bucket used: the earlier test-owned `lore-r2-proof-20260816t095204z` with short-lived,
  bucket-scoped credentials. Only newly generated, non-overlapping content hashes were removed.
- R2 bucket `lore-pg-r2-consul-20260816t141340z` was also created through `cf` but remained empty
  because the available short-lived S3 credential was scoped to the earlier test bucket.

The run root, database schemas, bucket, containers, server process, source tree, and evidence remain
in place to comply with the no-prune requirement.

## Reproducible tests

Run from the repository root:

```sh
cargo +nightly fmt --all -- --check
cargo test -p lore-aws --lib
cargo test -p lore-server plugins::hashicorp::tests --lib
git diff --check

cd contrib/postgres
cargo +nightly fmt -p lore-postgres -p lore-postgres-server -- --check
cargo test -p lore-postgres-server --lib
cargo clippy -p lore-postgres-server --lib -- -D warnings
LORE_POSTGRES_TEST_URL='host=127.0.0.1 port=15439 user=lore_test password=<test-password> dbname=lore_test sslmode=disable' \
  cargo test -p lore-postgres --features integration-tests --test catalog_contract -- --nocapture
```

The ARM64 build on `enfis1` used an isolated source tree and Cargo cache:

```sh
docker run \
  --name lore-build4-pg-r2-consul-20260816t141340z \
  -e CARGO_HOME=/cargo-home \
  -v /var/tmp/lore-pg-r2-consul-20260816T141340Z/src:/workspace \
  -v /var/tmp/lore-pg-r2-consul-20260816T141340Z/cargo-home:/cargo-home \
  -w /workspace/contrib/postgres \
  rust:1.94.1-bookworm \
  cargo build --release -p lore-postgres-server
```

Rust 1.90 was also tried and correctly rejected by Cargo because the resolved AWS SDK requires
Rust 1.94.1. All build-container logs are retained. The successful clean release build took
1 minute 56 seconds; the exact final incremental ARM64 build took 6.92 seconds wall-clock
(5.59 seconds reported by Cargo).

## Live checks and timings

The final composite configuration is equivalent to
[`config-r2-consul.toml`](config-r2-consul.toml), with isolated paths, schema, bucket, and ports.
It includes `immutable_store.composite.replica_factory`; without that section Consul still polls,
but Lore cannot materialize discovered peers. The proof deliberately caught this configuration
error, corrected it, and verified the replacement server log has zero errors.

Key timings:

- corrected composite server startup: approximately 0.55 s;
- direct repository create: 1.10 s;
- stage: 0.02 s;
- commit: 1.71 s;
- direct R2 push: 1.89 s;
- cold direct clone: 1.66 s;
- composite push: 0.67 s;
- composite cached clone: 0.08 s;
- remote recursive obliteration: 0.52 s, first attempt successful.

Catalog and object checks:

```text
pre-obliteration metadata=19 associations=19 payload_bytes=525598
pre-obliteration exact R2 HEAD success=19/19
post-obliteration terminal rows=9 remaining associations=10
post-obliteration terminal R2 objects absent=9/9
migration checksum=31cdf92ab669b9568bd81c600e676cc358e7a35110500e293c08418bbbc36c49
```

## Checksums and logs

The live binary checksum below records the end-to-end R2 proof build. A subsequent source-only
refactor moved the unchanged catalog interface and contract from `lore-aws` to `lore-storage` and
made the Dynamo adapter private. After that move, 165 `lore-storage`, 72 `lore-aws`, 777
`lore-server`, and all 3 live PostgreSQL contract tests passed. No additional R2 data operation was
needed or performed.

```text
ARM64 loreserver-postgres:
ab0045965f0accb4bd4e69bb91c8ec35d5b5cd98132773630a9508b29303256b

Final live configuration:
00c3551f39614f257931ca0e774c01e0a8eed23cec50cd05c1c52d52112cc1b2

56-file exact-final evidence manifest:
d9554fe63b028b907d39162e67728e5b697208d9c32588d6f324cf21437be779
```

The exact-final binary remained running after a read-only repository-list check returned
`lore-pg-r2-consul-composite-proof`. Its runtime error evidence is empty. The first exact-final
restart intentionally remains in `exact-final-server-missing-env.log`: it omitted the retained R2
credential environment and exited before serving or performing a data operation. The corrected
restart used the same protected, bucket-scoped test credential file as the earlier proof.

All live logs, timings, catalog snapshots, hash lists, R2 check summaries, build metadata, and the
SHA-256 manifest are retained under:

```text
/var/tmp/lore-pg-r2-consul-20260816T141340Z/evidence
```

## Upstream rebase verification

On 2026-08-17 the branch was rebased from the v0.8.6-era base onto upstream commit `935fccb`.
The rewritten branch was `0` commits behind and `2` commits ahead. Upstream's newer immutable-store
model keeps fragment representation metadata on the S3/R2 object, so the PostgreSQL schema was
ported to store lifecycle state and associations only; the obsolete 697-line DynamoDB adapter was
removed rather than duplicating upstream's existing implementation.

Local verification after the port:

```text
lore-aws unit tests:             133 passed, 0 failed
lore-storage unit tests:         233 passed, 0 failed
lore-postgres unit tests:          1 passed, 0 failed
lore-postgres-server unit tests:   3 passed, 0 failed
core and PostgreSQL-server cargo check: PASS
git diff --check: PASS
```

A fresh retained clone of code commit `9331ed8d8f7b712254fa6ad856500aadd7d02331` then ran the
three live PostgreSQL catalog tests on `enfis1` in 50.42 seconds. All passed. The run used a new
schema generated by the test harness and did not prune or modify earlier proof resources.

```text
run root: /var/tmp/lore-rebase-verify-20260817t002000z
catalog-contract.log SHA-256: 3f8f0df1bb03dae95c5a8454d586b9eeeeb28ff84286496c790a032e76d164c0
clone.log SHA-256:            c64cc8ca0307e98c8f927417e7badaee7a71548e6c5aca07e30e06cf28a3933b
commit.txt SHA-256:           f39d3643e9c6e69955ff9d5255e6f6454d46ee4d20caeac65bebec08295fcb7f
```

## Quality revalidation

On 2026-08-17, code commit `27caeb0ba52a45a11c6f3e93a2a868ec6b22a1a4` was cloned from
the fork into a new retained directory on `enfis1`:

```text
run root: /var/tmp/lore-quality-20260817t023940z
clone log SHA-256:            56db1ee6d068fbca120df2a812c61bccb5e1c752dbc797bc85d7dc743747832b
commit file SHA-256:          eadb2a583a3190758dc8af34635316a601ef9535fe607060010a9d9197da2175
catalog contract SHA-256:     77fa97bfd0e0e6de7be781d4fa72bc6cb1506a2b480c5facd3b9a4da0f94f4e5
server binary SHA-256:        82a0b762ccf9b4c875831a29479213c2babac24b3dd725d76b707b906629b862
server log SHA-256:           51fc0a085d77271c53e078d6bc1147d15e474c9ab496159d9f28796664b15c8b
lore-server AWS tests:        ff3f9e1be04bfa8c851882b9cdab0caed400d3a97087a0aef71202ac296d086d
lore-server Clippy:           0360c2e57dd22f9f0a0028da52edda8170f6535802f98116c7ad0002801010f5
```

The live PostgreSQL contract passed 3/3 in 39.94 seconds using fresh schemas. The exact server then
created, staged, committed, pushed, and cold-cloned a unique 4 MiB payload through PostgreSQL and
R2. Timings were 0.65 s, 0.01 s, 1.65 s, 0.40 s, and 0.16 s respectively. Before obliteration,
all 61 catalog hashes returned successful R2 HEADs. The server log contained zero error, panic,
HTTP 501, or `ListObjectVersions` lines and six Consul/topology lines.

The retained bucket was shared with earlier isolated tests, so the safety check compared every new
hash before deletion. One hash overlapped the prior PostgreSQL catalog and was excluded. The live
test selected one of 59 unfragmented, single-association hashes with no overlap in either prior
catalog, then obliterated only that exact address. Evidence checksums:

```text
non-overlapping target selection: 207570ccae68b82166ffab47a65efff954f73013391e7b3da58eede014e6d83d
obliteration log:                 88135059b46dd894c650088cd221c0ecfe7cf4964ecf4b8308f5df4fcf7e7a34
post-catalog state:               4f3bcd001d9fa430c93c8f1b3e204706c5f790a89974cff5faede5972f408dbf
post-R2 HEAD:                     d3d3234bceebad37b540e0143432c82208132d72a96a3a53f75d3349029071ca
```

Local validation at the same code state passed 134 `lore-aws` tests, 233 `lore-storage` tests,
three `lore-postgres-server` tests, one `lore-postgres` unit test, and strict Clippy for all changed
crates. On `enfis1`, 13 built-in AWS plugin tests also passed in 114.11 seconds and strict
`lore-server` Clippy passed in 49.30 seconds. The review removed the 73-line Consul configuration
expansion, replaced the invalid
dual-`Option` backend state with one enum, removed the production panic and panic-capable
PostgreSQL row reads, made PostgreSQL publish/revive one transaction, and added explicit
unversioned-deletion coverage.
