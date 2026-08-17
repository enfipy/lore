<!-- Copyright 2026 David; SPDX-License-Identifier: MIT -->

# PostgreSQL + R2 + Consul storage proof

**Result: PASS**

The full live matrix ran on `enfis1` with Cloudflare R2 as the durable immutable payload store,
PostgreSQL as the fragment catalog, and Consul as topology discovery. The accepted current-tree
smoke used a newly created, initially empty bucket plus a fresh PostgreSQL schema, repository,
cache, mutable store, Consul service, ports, and 24-hour bucket-scoped credential. All accepted-run
resources and evidence were retained.

Full live run:

- root: `/var/tmp/lore-final-20260817t053000z`;
- live code: `bc599c97ab8c6c90a252ac1ae3aef0c61c418980`;
- final executable code: `f6bd661e10d7dcc28ef49d3012b6d004e97e10e7`;
- final executable tree: `c7a10de8ed73aa2d5d318cd488dc0922048c53ee`;
- exact-tree build/test validation: `/var/tmp/lore-finaltree4-20260822t020000z`;
- accepted exact-tree R2 smoke: `/var/tmp/lore-finaltree8-20260821t203000z`.

The changes after the full live run close the missing-payload/obliteration catalog race, preserve
current upstream's partition-wide copy semantics, and simplify the backend seam. The exact final
source was rebuilt and tested on ARM64, then its release server and client repeated an isolated R2
publish, empty-cache clone, and exact-delete obliteration smoke against an empty bucket.

## Acceptance report

| Acceptance criterion | Result | Evidence |
| --- | --- | --- |
| Recovered source and image are byte-identical | **PASS** | Final clone recovered all 3 files and 618,370,560-byte Ubuntu image. Source hashes `12e7a8…` and `ec1177…`; image hash `4a281a…` matched exactly. |
| No acknowledged push references a missing fragment | **PASS** | Full-run catalog: 8,523 stored states, 8,525 associations, zero invalid associations, and HEAD 200 for 8,523/8,523 stored hashes. The accepted exact-tree smoke had 11 stored states after push, zero invalid associations, and HEAD 200 for all 10 surviving stored hashes after obliteration. Outage and interruption attempts were nonzero and unacknowledged. |
| Complete recovery works with an empty server and cache | **PASS** | Mutable-state archive hashes matched (`4f7d5f…`), the full run restarted against a never-used cache path and recovered all 3/3 files, and the accepted exact-tree smoke repeated a never-used-cache clone with an exact payload checksum. |
| Interrupted operations are resumable and leave no partial revision | **PASS** | Server was killed only after a new cache file proved an upload was active. Client exit was 143; pre-resume clone exposed no file. Retry commit/push succeeded and the 128 MiB payload hash `b34430…` matched the clone. |
| Cache loss causes no durable data loss | **PASS** | A new empty cache was substituted rather than deleting any directory. The genuine cold clone recovered 589.89 MiB in 91.04 s; every stored catalog hash remained present in R2. |
| Obliteration passes and is not bypassed | **PASS** | Full live obliteration completed in 0.32 s. The accepted exact-tree rerun completed in 0.36 s: one tombstone, R2 HEAD 404, exactly one proxy-observed DELETE, and zero `ListObjectVersions` calls. |

Accepted-run failed invariants: **none**.

### Discarded credential-recovery attempts

The accepted PASS does not conceal earlier discarded attempts. Run 5 stopped before Lore started
because the retained proxy rejected a different proof-bucket name. Run 6 completed its Lore path
but its retained verifier rejected that name. Run 7 deliberately added a before/after inventory and
exposed why a shared content-addressed bucket is not a sufficiently isolated resource: two existing
test-owned keys were PUT again. Their ETags and sizes were identical, but their `Last-Modified`
timestamps changed. No existing object was deleted and no existing payload bytes changed. Testing
then moved to the newly created empty bucket; Run 8's inventory was 0 before, 10 after, with exactly
10 retained test objects and one newly created-and-obliterated test object.

## R2 operation matrix

`r2-matrix-bc599c9.json` records every result and its duration. Overall time was 171.32 s.

| Operation | Result | Recorded detail |
| --- | --- | --- |
| PutObject | **PASS** | 5,396 bytes, HTTP 200 |
| HeadObject + metadata | **PASS** | size and both metadata keys matched |
| GetObject | **PASS** | SHA-256 matched |
| ranged GetObject | **PASS** | bytes 7-117, 111 bytes, correct Content-Range |
| multipart upload/get | **PASS** | 618,370,560 bytes, 37 parts, SHA-256 matched |
| ListObjectsV2 pagination | **PASS** | 1,005 objects across 9 pages (`max_keys=113`) |
| retry | **PASS** | injected first failure; second SDK attempt reached R2 |
| concurrent duplicate puts | **PASS** | 32 writers, identical 131,072-byte result |
| exact-key delete | **PASS** | DELETE 204 followed by HEAD 404 |
| ListObjectVersions capability | **PASS** | R2 returned expected 501; Lore's explicit unversioned path never called it |

The final direct catalog/R2 sweep checked 737,093,931 payload bytes. It found 8,523 HEAD 200,
zero HEAD 404, and no unexpected object result.

## Other live results

- Representative warm clone: 1.14 s.
- Empty-cache clone: 91.04 s; the later cached clone took 1.40 s.
- Interrupted commit before kill: 22.874 s; client exit 143.
- Resumed commit: 10.66 s; resumed push: 0.33 s.
- R2 outage push: timed out after 180.00 s and was not acknowledged.
- Identical push after proxy recovery: 0.82 s.
- Live obliteration: 0.32 s.
- Final catalog/R2 sweep: 15.08 s.
- Consul 1.21.5 remained active and served Lore health-service discovery requests; it was not used
  as a database.
- Credential scan: `access_key_evidence_hits=0`, `secret_key_evidence_hits=0`.

## Reproducible commands

Run the commands below in one Bash shell on `enfis1`. Secret values are deliberately omitted. The
Cloudflare credential is limited to Object Read & Write on the new bucket and expires after 24
hours. Cloudflare's R2 token page supplies the one-time access key and secret; both are
operator-supplied below and must not be written to evidence.

### 1. Isolated resources

```sh
set -euo pipefail
RUN_ID="$(date -u +%Y%m%dt%H%M%Sz)"
RUN_ROOT="/var/tmp/lore-final-$RUN_ID"
BUCKET="lore-final-r2-$RUN_ID"
PG_CONTAINER="lore-final-postgres-$RUN_ID"
CONSUL_CONTAINER="lore-final-consul-$RUN_ID"
PG_SCHEMA="lore_final_$RUN_ID"
PG_PASSWORD="$(openssl rand -hex 16)"
: "${ACCOUNT_ID:?set the Cloudflare account ID}"
: "${R2_ACCESS_KEY_ID:?set the new bucket-scoped R2 access key ID}"
: "${R2_SECRET_ACCESS_KEY:?set the new bucket-scoped R2 secret access key}"

# Ask the kernel for five distinct ports and verify they are still free before binding.
read -r PG_PORT CONSUL_PORT LORE_PORT HTTP_PORT PROXY_PORT <<EOF
$(python3 - <<'PY'
import socket
sockets = []
for _ in range(5):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sockets.append(sock)
print(*(sock.getsockname()[1] for sock in sockets))
PY
)
EOF
test "$(printf '%s\n' "$PG_PORT" "$CONSUL_PORT" "$LORE_PORT" "$HTTP_PORT" "$PROXY_PORT" | sort -u | wc -l)" -eq 5
for port in "$PG_PORT" "$CONSUL_PORT" "$LORE_PORT" "$HTTP_PORT" "$PROXY_PORT"; do
  test -z "$(ss -ltnH "sport = :$port")"
done

install -d -m 0700 "$RUN_ROOT/secrets"
install -d "$RUN_ROOT"/{certs,config,evidence,repository,runtime,recovery,faults}

cf r2 buckets create --dry-run --name "$BUCKET" \
  --location-hint apac --storage-class Standard
cf r2 buckets create --name "$BUCKET" --location-hint apac --storage-class Standard
cf r2 buckets get "$BUCKET"
jq -n --arg accessKeyId "$R2_ACCESS_KEY_ID" \
  --arg secretAccessKey "$R2_SECRET_ACCESS_KEY" \
  '{accessKeyId: $accessKeyId, secretAccessKey: $secretAccessKey}' \
  >"$RUN_ROOT/secrets/r2-s3.json"
chmod 0600 "$RUN_ROOT/secrets/r2-s3.json"
unset R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY

test ! -e "$RUN_ROOT/postgres"
install -d "$RUN_ROOT/postgres"
docker run -d --name "$PG_CONTAINER" \
  -e POSTGRES_USER=lore_test \
  -e POSTGRES_PASSWORD="$PG_PASSWORD" \
  -e POSTGRES_DB=lore_test \
  -v "$RUN_ROOT/postgres:/var/lib/postgresql/data" \
  -p "127.0.0.1:$PG_PORT:5432" postgres:17-alpine
until docker exec "$PG_CONTAINER" pg_isready -U lore_test -d lore_test; do sleep 1; done

docker run -d --name "$CONSUL_CONTAINER" \
  -p "127.0.0.1:$CONSUL_PORT:8500" \
  hashicorp/consul:1.21 agent -dev -client=0.0.0.0
until curl --fail --silent "http://127.0.0.1:$CONSUL_PORT/v1/status/leader"; do sleep 1; done
```

### 2. Source, helpers, and configuration

The helper programs are retained in the original evidence and covered by its 146-file manifest.
Verify before copying them. Configuration starts from the repository's non-secret example; every
run-specific path, port, endpoint, name, and credential is supplied by environment override.

```sh
RETAINED=/var/tmp/lore-final-20260817t053000z
(cd "$RETAINED" && sha256sum --check evidence/sha256-manifest.txt)
install -m 0755 "$RETAINED/evidence/r2_matrix_bc599c9.py" \
  "$RUN_ROOT/evidence/r2_matrix.py"
install -m 0755 "$RETAINED/evidence/verify_catalog_r2_bc599c9.py" \
  "$RUN_ROOT/evidence/verify_catalog_r2.py"
install -m 0755 "$RETAINED/evidence/r2_reverse_proxy_bc599c9.py" \
  "$RUN_ROOT/evidence/r2_reverse_proxy.py"

git clone https://github.com/enfipy/lore.git "$RUN_ROOT/src"
git -C "$RUN_ROOT/src" switch --detach f6bd661e10d7dcc28ef49d3012b6d004e97e10e7

openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
  -subj '/CN=127.0.0.1' \
  -keyout "$RUN_ROOT/certs/server.key" \
  -out "$RUN_ROOT/certs/server.crt"
install -d "$RUN_ROOT/runtime/composite-cache" "$RUN_ROOT/runtime/mutable-state"
install -m 0600 "$RUN_ROOT/src/contrib/postgres/config-r2-consul.toml" \
  "$RUN_ROOT/config/local.toml"

export AWS_ACCESS_KEY_ID="$(jq -er .accessKeyId "$RUN_ROOT/secrets/r2-s3.json")"
export AWS_SECRET_ACCESS_KEY="$(jq -er .secretAccessKey "$RUN_ROOT/secrets/r2-s3.json")"
export AWS_REGION=auto
export CONSUL_HTTP_ADDR="http://127.0.0.1:$CONSUL_PORT"
export LORE_URI="lore://127.0.0.1:$LORE_PORT"
export PYTHON_BIN="$(readlink -f "$(command -v python3)")"
```

### 3. Build, tests, and exact process control

```sh
cd "$RUN_ROOT/src/contrib/postgres"
CARGO_TARGET_DIR="$RUN_ROOT/target" cargo build --release \
  -p lore-postgres-server --bin loreserver-postgres
cd "$RUN_ROOT/src"
CARGO_TARGET_DIR="$RUN_ROOT/target" cargo build --release -p lore-client --bin lore
export PATH="$RUN_ROOT/target/release:$PATH"

start_server() {
  local cache=$1 mutable=$2 endpoint=$3 log=$4
  nohup env \
    LORE__SERVER__QUIC__HOST=127.0.0.1 \
    LORE__SERVER__QUIC__PORT="$LORE_PORT" \
    LORE__SERVER__QUIC__CERTIFICATE__CERT_FILE="$RUN_ROOT/certs/server.crt" \
    LORE__SERVER__QUIC__CERTIFICATE__PKEY_FILE="$RUN_ROOT/certs/server.key" \
    LORE__SERVER__GRPC__HOST=127.0.0.1 \
    LORE__SERVER__GRPC__PORT="$LORE_PORT" \
    LORE__SERVER__HTTP__HOST=127.0.0.1 \
    LORE__SERVER__HTTP__PORT="$HTTP_PORT" \
    LORE__IMMUTABLE_STORE__COMPOSITE__LOCAL__LOCAL__PATH="$cache" \
    LORE__MUTABLE_STORE__LOCAL__PATH="$mutable" \
    LORE__PLUGINS__POSTGRES_S3__IMMUTABLE_STORE__S3_BUCKET="$BUCKET" \
    LORE__PLUGINS__POSTGRES_S3__IMMUTABLE_STORE__S3_ENDPOINT_URL="$endpoint" \
    LORE__PLUGINS__POSTGRES_S3__IMMUTABLE_STORE__POSTGRES__CONNECTION_STRING="host=127.0.0.1 port=$PG_PORT dbname=lore_test user=lore_test password=$PG_PASSWORD sslmode=disable" \
    LORE__PLUGINS__POSTGRES_S3__IMMUTABLE_STORE__POSTGRES__SCHEMA="$PG_SCHEMA" \
    LORE__PLUGINS__CONSUL__SERVICE_NAME="lore-final-$RUN_ID" \
    "$RUN_ROOT/target/release/loreserver-postgres" \
    --config "$RUN_ROOT/config" >"$log" 2>&1 </dev/null &
  SERVER_PID=$!
  printf '%s\n' "$SERVER_PID" >"$RUN_ROOT/evidence/server.pid"
  until test "$(readlink -f "/proc/$SERVER_PID/exe")" = \
    "$(readlink -f "$RUN_ROOT/target/release/loreserver-postgres")"; do
    kill -0 "$SERVER_PID"
    sleep .05
  done
  until ss -ltnpH "sport = :$LORE_PORT" | grep "pid=$SERVER_PID," >/dev/null; do
    kill -0 "$SERVER_PID"
    sleep .25
  done
}
stop_server() {
  SERVER_PID="$(cat "$RUN_ROOT/evidence/server.pid")"
  test "$(readlink -f "/proc/$SERVER_PID/exe")" = \
    "$(readlink -f "$RUN_ROOT/target/release/loreserver-postgres")"
  kill -TERM "$SERVER_PID"
  while kill -0 "$SERVER_PID" 2>/dev/null; do sleep .1; done
}

start_server "$RUN_ROOT/runtime/composite-cache" "$RUN_ROOT/runtime/mutable-state" \
  "https://$ACCOUNT_ID.r2.cloudflarestorage.com" "$RUN_ROOT/evidence/server.log"

python3 "$RUN_ROOT/evidence/r2_matrix.py" \
  --bucket "$BUCKET" \
  --endpoint "https://$ACCOUNT_ID.r2.cloudflarestorage.com" \
  --prefix "lore-final-matrix-$RUN_ID" \
  --image "$RETAINED/repository/artifacts/noble-server-cloudimg-arm64.img"
```

### 4. Representative repository and cold recovery

```sh
install -d "$RUN_ROOT/repository/src" "$RUN_ROOT/repository/artifacts"
cp "$RUN_ROOT/src/lore-storage/src/fragment_catalog.rs" "$RUN_ROOT/repository/src/"
cp "$RUN_ROOT/src/lore-aws/src/store/immutable_store.rs" "$RUN_ROOT/repository/src/"
curl --fail --location --output \
  "$RUN_ROOT/repository/artifacts/noble-server-cloudimg-arm64.img" \
  https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-arm64.img
(cd "$RUN_ROOT/repository" && \
  sha256sum src/fragment_catalog.rs src/immutable_store.rs \
    artifacts/noble-server-cloudimg-arm64.img) \
  >"$RUN_ROOT/evidence/original-checksums.sha256"

CREATE_OUTPUT="$(lore repository create --repository "$RUN_ROOT/repository" \
  "$LORE_URI/lore-final-proof")"
PRIMARY_REPOSITORY_ID="${CREATE_OUTPUT##* }"
lore file stage --repository "$RUN_ROOT/repository" --scan .
lore --force --non-interactive commit --repository "$RUN_ROOT/repository" \
  'source and Ubuntu image'
lore push --repository "$RUN_ROOT/repository"
lore repository clone "$LORE_URI/$PRIMARY_REPOSITORY_ID" \
  "$RUN_ROOT/recovery/warm"
(cd "$RUN_ROOT/recovery/warm" && \
  sha256sum --check "$RUN_ROOT/evidence/original-checksums.sha256")

tar --sort=name --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner \
  -C "$RUN_ROOT/runtime/mutable-state" -cf - . | sha256sum \
  >"$RUN_ROOT/evidence/mutable-source.sha256"
install -d "$RUN_ROOT/recovery/mutable-state"
cp -a "$RUN_ROOT/runtime/mutable-state/." "$RUN_ROOT/recovery/mutable-state/"
tar --sort=name --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner \
  -C "$RUN_ROOT/recovery/mutable-state" -cf - . | sha256sum \
  >"$RUN_ROOT/evidence/mutable-restored.sha256"
cmp "$RUN_ROOT/evidence/mutable-source.sha256" "$RUN_ROOT/evidence/mutable-restored.sha256"

stop_server
test ! -e "$RUN_ROOT/recovery/empty-cache"
install -d "$RUN_ROOT/recovery/empty-cache"
start_server "$RUN_ROOT/recovery/empty-cache" "$RUN_ROOT/recovery/mutable-state" \
  "https://$ACCOUNT_ID.r2.cloudflarestorage.com" "$RUN_ROOT/evidence/server-cold.log"
lore repository clone "$LORE_URI/$PRIMARY_REPOSITORY_ID" \
  "$RUN_ROOT/recovery/cold"
(cd "$RUN_ROOT/recovery/cold" && \
  sha256sum --check "$RUN_ROOT/evidence/original-checksums.sha256")
```

### 5. Failure, deletion, and obliteration checks

Create each fault repository with `lore repository create`, retain the returned ID, and use a
unique path below `"$RUN_ROOT/faults"`. The interruption waits for a new cache file, validates the
exact server executable, sends `KILL` only to that PID, and requires a nonzero client exit:

```sh
install -d "$RUN_ROOT/faults/interrupted-repository/data"
CREATE_OUTPUT="$(lore repository create \
  --repository "$RUN_ROOT/faults/interrupted-repository" \
  "$LORE_URI/lore-final-interrupted")"
INTERRUPTED_REPOSITORY_ID="${CREATE_OUTPUT##* }"
dd if=/dev/zero of="$RUN_ROOT/faults/interrupted-repository/data/payload.bin" \
  bs=1M count=512 status=none
lore file stage --repository "$RUN_ROOT/faults/interrupted-repository" --scan .
BASELINE="$(find "$RUN_ROOT/recovery/empty-cache" -type f -printf . | wc -c)"
lore --force --non-interactive commit \
  --repository "$RUN_ROOT/faults/interrupted-repository" interrupted >"$RUN_ROOT/evidence/interrupted.log" 2>&1 &
CLIENT_PID=$!
until test "$(find "$RUN_ROOT/recovery/empty-cache" -type f -printf . | wc -c)" -gt "$BASELINE"; do
  kill -0 "$CLIENT_PID"; sleep .1
done
SERVER_PID="$(cat "$RUN_ROOT/evidence/server.pid")"
test "$(readlink -f "/proc/$SERVER_PID/exe")" = \
  "$(readlink -f "$RUN_ROOT/target/release/loreserver-postgres")"
kill -KILL "$SERVER_PID"
set +e; wait "$CLIENT_PID"; INTERRUPTED_STATUS=$?; set -e
test "$INTERRUPTED_STATUS" -ne 0

start_server "$RUN_ROOT/recovery/empty-cache" "$RUN_ROOT/recovery/mutable-state" \
  "https://$ACCOUNT_ID.r2.cloudflarestorage.com" "$RUN_ROOT/evidence/server-resume.log"
lore repository clone "$LORE_URI/$INTERRUPTED_REPOSITORY_ID" \
  "$RUN_ROOT/faults/interrupted-pre-resume"
test "$(find "$RUN_ROOT/faults/interrupted-pre-resume" -type f ! -path '*/.lore/*' | wc -l)" -eq 0
lore --force --non-interactive commit \
  --repository "$RUN_ROOT/faults/interrupted-repository" interrupted
lore push --repository "$RUN_ROOT/faults/interrupted-repository"
lore repository clone "$LORE_URI/$INTERRUPTED_REPOSITORY_ID" \
  "$RUN_ROOT/faults/interrupted-resumed"
test "$(sha256sum "$RUN_ROOT/faults/interrupted-repository/data/payload.bin" | cut -d' ' -f1)" = \
  "$(sha256sum "$RUN_ROOT/faults/interrupted-resumed/data/payload.bin" | cut -d' ' -f1)"
```

Create and populate the outage, ordinary-deletion, and obliteration repositories while R2 is
healthy. This makes every later path and repository ID concrete:

```sh
install -d "$RUN_ROOT/faults/outage-repository/src"
CREATE_OUTPUT="$(lore repository create \
  --repository "$RUN_ROOT/faults/outage-repository" \
  "$LORE_URI/lore-final-outage")"
OUTAGE_REPOSITORY_ID="${CREATE_OUTPUT##* }"
printf '%s\n' "outage $RUN_ID" >"$RUN_ROOT/faults/outage-repository/src/outage.txt"
lore file stage --repository "$RUN_ROOT/faults/outage-repository" --scan .

install -d "$RUN_ROOT/faults/deletion-repository/src"
CREATE_OUTPUT="$(lore repository create \
  --repository "$RUN_ROOT/faults/deletion-repository" \
  "$LORE_URI/lore-final-deletion")"
DELETION_REPOSITORY_ID="${CREATE_OUTPUT##* }"
printf '%s\n' "deletion $RUN_ID" >"$RUN_ROOT/faults/deletion-repository/src/target.txt"
lore file stage --repository "$RUN_ROOT/faults/deletion-repository" --scan .
lore --force --non-interactive commit \
  --repository "$RUN_ROOT/faults/deletion-repository" 'deletion baseline'
lore push --repository "$RUN_ROOT/faults/deletion-repository"

install -d "$RUN_ROOT/faults/obliterate-repository/src"
CREATE_OUTPUT="$(lore repository create \
  --repository "$RUN_ROOT/faults/obliterate-repository" \
  "$LORE_URI/lore-final-obliterate")"
OBLITERATE_REPOSITORY_ID="${CREATE_OUTPUT##* }"
printf '%s\n' "obliterate $RUN_ID" >"$RUN_ROOT/faults/obliterate-repository/src/target.txt"
lore file stage --repository "$RUN_ROOT/faults/obliterate-repository" --scan .
lore --force --non-interactive commit \
  --repository "$RUN_ROOT/faults/obliterate-repository" 'obliteration baseline'
lore push --repository "$RUN_ROOT/faults/obliterate-repository"

test -n "$OUTAGE_REPOSITORY_ID"
test -n "$DELETION_REPOSITORY_ID"
test -n "$OBLITERATE_REPOSITORY_ID"
```

The bucket-locked proxy binds only to loopback. Validate the exact interpreter before signaling
it. With the proxy stopped, cached reads and the local commit pass, the push times out, and the
remote repository remains empty. Restarting the identical proxy lets the identical push succeed:

```sh
start_proxy() {
  local suffix=$1
  "$PYTHON_BIN" "$RUN_ROOT/evidence/r2_reverse_proxy.py" \
    --port "$PROXY_PORT" --bucket "$BUCKET" --account-id "$ACCOUNT_ID" \
    --credentials "$RUN_ROOT/secrets/r2-s3.json" \
    --log "$RUN_ROOT/evidence/r2-proxy.jsonl" \
    >"$RUN_ROOT/evidence/r2-proxy-$suffix.stdout" \
    2>"$RUN_ROOT/evidence/r2-proxy-$suffix.stderr" &
  PROXY_PID=$!
  printf '%s\n' "$PROXY_PID" >"$RUN_ROOT/evidence/r2-proxy.pid"
  until test "$(readlink -f "/proc/$PROXY_PID/exe")" = "$PYTHON_BIN"; do
    kill -0 "$PROXY_PID"
    sleep .05
  done
  until ss -ltnpH "sport = :$PROXY_PORT" | grep "pid=$PROXY_PID," >/dev/null; do
    kill -0 "$PROXY_PID"
    sleep .1
  done
}
stop_proxy() {
  PROXY_PID="$(cat "$RUN_ROOT/evidence/r2-proxy.pid")"
  test "$(readlink -f "/proc/$PROXY_PID/exe")" = "$PYTHON_BIN"
  kill -TERM "$PROXY_PID"
  while kill -0 "$PROXY_PID" 2>/dev/null; do sleep .1; done
}

start_proxy initial
stop_server
start_server "$RUN_ROOT/recovery/empty-cache" "$RUN_ROOT/recovery/mutable-state" \
  "http://127.0.0.1:$PROXY_PORT" "$RUN_ROOT/evidence/server-outage.log"
stop_proxy

lore repository clone "$LORE_URI/$PRIMARY_REPOSITORY_ID" \
  "$RUN_ROOT/faults/outage-cached-clone"
lore --force --non-interactive commit \
  --repository "$RUN_ROOT/faults/outage-repository" 'offline source change'
set +e
timeout 180 lore push --repository "$RUN_ROOT/faults/outage-repository"
OUTAGE_STATUS=$?
set -e
test "$OUTAGE_STATUS" -ne 0
lore repository clone "$LORE_URI/$OUTAGE_REPOSITORY_ID" \
  "$RUN_ROOT/faults/outage-pre-recovery"
test "$(find "$RUN_ROOT/faults/outage-pre-recovery" -type f ! -path '*/.lore/*' | wc -l)" -eq 0

start_proxy recovered
timeout 180 lore push --repository "$RUN_ROOT/faults/outage-repository"
lore repository clone "$LORE_URI/$OUTAGE_REPOSITORY_ID" \
  "$RUN_ROOT/faults/outage-recovered"
test "$(sha256sum "$RUN_ROOT/faults/outage-repository/src/outage.txt" | cut -d' ' -f1)" = \
  "$(sha256sum "$RUN_ROOT/faults/outage-recovered/src/outage.txt" | cut -d' ' -f1)"
```

Ordinary deletion retains the bytes in another test-owned directory. Remote obliteration then
removes a different file and is verified from PostgreSQL, R2, the proxy, and the Lore log:

```sh
install -d "$RUN_ROOT/faults/deletion-retained"
mv "$RUN_ROOT/faults/deletion-repository/src/target.txt" \
  "$RUN_ROOT/faults/deletion-retained/target.txt"
sha256sum "$RUN_ROOT/faults/deletion-retained/target.txt" \
  >"$RUN_ROOT/evidence/deletion-retained.sha256"
lore file stage --repository "$RUN_ROOT/faults/deletion-repository" --scan .
lore --force --non-interactive commit \
  --repository "$RUN_ROOT/faults/deletion-repository" 'ordinary deletion'
lore push --repository "$RUN_ROOT/faults/deletion-repository"
lore repository clone "$LORE_URI/$DELETION_REPOSITORY_ID" \
  "$RUN_ROOT/faults/deletion-clone"
test "$(find "$RUN_ROOT/faults/deletion-clone" -type f ! -path '*/.lore/*' | wc -l)" -eq 0
sha256sum --check "$RUN_ROOT/evidence/deletion-retained.sha256"

lore file obliterate --repository "$RUN_ROOT/faults/obliterate-repository" \
  --remote --force --non-interactive \
  --path "$RUN_ROOT/faults/obliterate-repository/src/target.txt"

docker exec -e PGPASSWORD="$PG_PASSWORD" "$PG_CONTAINER" \
  psql -U lore_test -d lore_test -At -v ON_ERROR_STOP=1 -c \
  "SELECT encode(hash, 'hex') FROM \"$PG_SCHEMA\".fragment_state
   WHERE state=2 ORDER BY hash" >"$RUN_ROOT/evidence/obliterate-target-hash.txt"
test "$(wc -l <"$RUN_ROOT/evidence/obliterate-target-hash.txt")" -eq 1
TARGET_HASH="$(cat "$RUN_ROOT/evidence/obliterate-target-hash.txt")"

docker exec -e PGPASSWORD="$PG_PASSWORD" "$PG_CONTAINER" \
  psql -U lore_test -d lore_test -At -v ON_ERROR_STOP=1 -c \
  "SELECT count(*) FILTER (WHERE state=0),
          count(*) FILTER (WHERE state=1),
          count(*) FILTER (WHERE state=2)
   FROM \"$PG_SCHEMA\".fragment_state;
   SELECT count(*) FROM \"$PG_SCHEMA\".fragment_association a
   LEFT JOIN \"$PG_SCHEMA\".fragment_state s USING (hash)
   WHERE s.state IS DISTINCT FROM 0;"

docker exec -e PGPASSWORD="$PG_PASSWORD" "$PG_CONTAINER" \
  psql -U lore_test -d lore_test -At -v ON_ERROR_STOP=1 -c \
  "SELECT encode(hash, 'hex') FROM \"$PG_SCHEMA\".fragment_state
   WHERE state=0 ORDER BY hash" >"$RUN_ROOT/evidence/catalog-stored-final.txt"
"$PYTHON_BIN" "$RUN_ROOT/evidence/verify_catalog_r2.py" \
  --bucket "$BUCKET" \
  --endpoint "https://$ACCOUNT_ID.r2.cloudflarestorage.com" \
  --hashes "$RUN_ROOT/evidence/catalog-stored-final.txt" --expect present
"$PYTHON_BIN" "$RUN_ROOT/evidence/verify_catalog_r2.py" \
  --bucket "$BUCKET" \
  --endpoint "https://$ACCOUNT_ID.r2.cloudflarestorage.com" \
  --hashes "$RUN_ROOT/evidence/obliterate-target-hash.txt" --expect missing
test "$(grep -c ListObjectVersions "$RUN_ROOT/evidence/server-outage.log")" -eq 0
test "$(jq -r 'select(.method == "DELETE") | .method' \
  "$RUN_ROOT/evidence/r2-proxy.jsonl" | wc -l)" -eq 1
docker logs "$CONSUL_CONTAINER" >"$RUN_ROOT/evidence/consul.log" 2>&1
grep -c "/v1/health/service/lore-final-$RUN_ID" \
  "$RUN_ROOT/evidence/consul.log" >"$RUN_ROOT/evidence/consul-health-request-count.txt"
test "$(cat "$RUN_ROOT/evidence/consul-health-request-count.txt")" -ge 1
```

### 6. Exact final source validation

```sh
cd "$RUN_ROOT/src"
RUSTC_WRAPPER= cargo test -p lore-storage --lib
RUSTC_WRAPPER= cargo test -p lore-aws --lib
RUSTC_WRAPPER= cargo test -p lore-server plugins::aws::tests --lib
RUSTC_WRAPPER= cargo test -p lore-revision --lib --tests
RUSTC_WRAPPER= cargo clippy \
  -p lore-storage -p lore-revision -p lore-aws -p lore-server \
  --all-targets -- -D warnings

cd "$RUN_ROOT/src/contrib/postgres"
RUSTC_WRAPPER= cargo test -p lore-postgres-server --lib
RUSTC_WRAPPER= cargo clippy \
  -p lore-postgres -p lore-postgres-server --all-targets -- -D warnings
LORE_POSTGRES_TEST_URL="host=127.0.0.1 port=$PG_PORT user=lore_test password=$PG_PASSWORD dbname=lore_test sslmode=disable" \
  RUSTC_WRAPPER= cargo test -p lore-postgres \
  --features integration-tests --test catalog_contract -- --nocapture
RUSTC_WRAPPER= cargo build --release \
  -p lore-postgres-server --bin loreserver-postgres
```

Exact-tree results on `enfis1`: storage 250/250 (38.28 s), AWS 144/144 (71.01 s), AWS plugin
13/13 (106.78 s), every `lore-revision` test target passed (97.83 s), PostgreSQL server 3/3
(102.22 s), live catalog 5/5 (25.17 s), and both strict Clippy commands passed (62.12 s root,
55.11 s PostgreSQL). Both format checks passed. The release server built in 111.72 s with SHA-256
`caaf230d485c5e760077b8774488e8842c39fa8070d5292e080033a647486a4c`; the release client built
in 272.49 s with SHA-256
`3c0aee149b026aa23dc117152f177d0246be961daf5174e258b52e6594bbadfe`.

The exact release binaries then committed in 2.41 s and pushed in 0.42 s through the exact-bucket
proxy. PostgreSQL held 11 stored states and zero invalid associations. Restarting against a
never-used cache cloned the file in 2.52 s with its SHA-256 intact. Remote obliteration took 0.36 s;
PostgreSQL held 10 stored states and one tombstone, R2 returned HEAD 404, the proxy recorded exactly
one DELETE, and Lore logged zero `ListObjectVersions` calls. A final direct sweep returned HEAD 200
for all 10 remaining stored hashes. The isolated Consul log recorded six Lore health-service
discovery requests. Both server and proxy PIDs were stopped by exact executable identity checks.

## Retained evidence

- Full live evidence: `/var/tmp/lore-final-20260817t053000z/evidence`.
- Full 146-file manifest SHA-256:
  `15da2a9d7877e2e3127922de1aa7da42d955a60062b6af6497a7218bf1e09264`.
- Exact final build/test evidence: `/var/tmp/lore-finaltree4-20260822t020000z/evidence`.
- Exact final build/test 41-file manifest SHA-256:
  `7539c0bfac0ffd00239af5a0b3b07755b50c45a85eb55c95ab9c9ee42b6a1603`.
- Accepted empty-bucket R2 smoke evidence:
  `/var/tmp/lore-finaltree8-20260821t203000z/evidence`.
- Accepted R2 smoke 40-file manifest SHA-256:
  `0fd8cb3aff9c6357d3c52fd8521e6942e4f138849d14adce3ae738b9c92ca030`.
- R2 bucket: `lore-final-r2-20260817t053000z`.
- PostgreSQL container/schema: `lore-final-postgres-20260817t053000z` /
  `lore_final_20260817t053000z`.
- Consul container: `lore-final-consul-20260817t053000z`.
- Exact-tree PostgreSQL/Consul containers: `lore-finaltree-postgres-20260817t173000z` and
  `lore-finaltree-consul-20260817t173000z`.
- Accepted R2 bucket: `lore-finaltree-r2-20260822t020000z` (10 retained objects after the one
  test-owned obliteration target was deleted).
- Accepted PostgreSQL schema and Consul service:
  `lore_finaltree8_20260821t203000z` / `lore-finaltree8-20260821t203000z`.
- The additional empty bucket `lore-finaltree-r2-20260817t173000z` was retained after Cloudflare
  rejected minting its scoped credential; no object was written to it. The discarded Run 5 empty
  bucket `lore-finaltree-r2-20260822t020000z` became the accepted Run 8 bucket after a fresh scoped
  credential was created; Run 5 itself wrote no object.

All resources and evidence remain in place. No pull request was opened.
