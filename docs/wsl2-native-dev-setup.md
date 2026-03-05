# WSL2 Native Dev Setup — OpenMetadata 1.12.0-SNAPSHOT

**Date:** 2026-03-01
**Goal:** Run OpenMetadata natively in WSL2 (no Docker for OM) to develop three-dimensional lineage ingesters
**Infra repo:** `E:\A\infra-devops`
**OM repo:** `E:\A\OpenMetadata`

---

## Architecture

```
Docker Desktop (WSL2 backend)
├── OpenSearch  :9200   ← OM search index
└── Airflow     :8080   ← runs openmetadata_lineage_sync DAG

Native WSL2
├── PostgreSQL  :5432   ← OM metadata DB + Airflow metadata DB
├── OM Server   :8585   ← REST API target for lineage POST calls
└── Python venv         ← develop azure_ingester, ado_ingester, stitch_lineage
```

---

## What Was Done

### Step 1 — Fix Yarn

System yarn (0.32) was too old. Replaced with yarn classic via npm:

```bash
npm install -g yarn
# Result: yarn 1.22.22
```

---

### Step 2 — Install PostgreSQL natively in WSL2

```bash
sudo apt install -y postgresql postgresql-contrib
sudo service postgresql start
```

Created databases using the existing OM script:

```bash
sudo -u postgres psql -f /mnt/e/A/OpenMetadata/docker/postgresql/postgres-script.sql
```

This created:
- `openmetadata_db` owned by `openmetadata_user` / `openmetadata_password`
- `airflow_db` owned by `airflow_user` / `airflow_pass`

---

### Step 3 — Docker infrastructure (OpenSearch + Airflow)

Created `/mnt/e/A/OpenMetadata/docker/development/docker-compose-infra.yml` — a minimal compose file with only OpenSearch and Airflow (no OM server, no PostgreSQL in Docker).

**Key design decisions:**
- Airflow uses `host.docker.internal:host-gateway` to reach native WSL2 PostgreSQL
- DAGs volume-mounted from `E:\A\infra-devops\openmetadata\airflow\dags\`
- Airflow image built from `ingestion/Dockerfile.ci` with `INGESTION_DEPENDENCY=all`

**CRLF fix:** `ingestion/ingestion_dependency.sh` has Windows CRLF line endings on the NTFS mount. Fixed via docker-compose command:

```yaml
entrypoint: /bin/bash
command:
  - "-c"
  - "sed -i 's/\r//' /opt/airflow/ingestion_dependency.sh && bash /opt/airflow/ingestion_dependency.sh"
```

Start command:

```bash
cd /mnt/e/A/OpenMetadata/docker/development
docker compose -f docker-compose-infra.yml up -d
```

> **First time:** requires `--build` flag (~10–15 min to build Airflow image from source).
> Docker Desktop WSL2 integration must be enabled: Docker Desktop → Settings → Resources → WSL Integration → enable your distro.

---

### Step 4 — Build OM backend from source

#### Problem: OOM in UI build

Running `mvn clean package -DskipTests -pl !openmetadata-ui` succeeded for all Java modules (1-9) but failed at module 10 (`openmetadata-ui-core-components`) with Node.js heap out-of-memory during Vite build. The distribution (module 11) was skipped.

#### Fix: build with `only-backend` profile

The `openmetadata-dist/pom.xml` has an `only-backend` Maven profile (activated by `-DonlyBackend`) that excludes the `openmetadata-ui` dependency. This lets the distribution assemble without the React frontend.

```bash
cd /mnt/e/A/OpenMetadata
mvn install -DskipTests -DonlyBackend -pl openmetadata-dist -am
```

The `-am` flag (also-make) builds the full dependency chain:
`common` → `openmetadata-spec` → `openmetadata-sdk` → `shaded-deps` → `openmetadata-service` → `openmetadata-mcp` → `openmetadata-dist`

**Result:** 557 JARs assembled in `openmetadata-dist/target/libs/`

#### Create libs symlink

The server start script reads from `$project_root/libs/*.jar`. Since the dist module outputs to `openmetadata-dist/target/libs/`, create a symlink:

```bash
cd /mnt/e/A/OpenMetadata
ln -s openmetadata-dist/target/libs libs
```

> **Note:** This symlink must be recreated after a `mvn clean`.

---

### Step 5 — Environment variables

Created `/mnt/e/A/OpenMetadata/conf/local-dev.env`:

```bash
# Key settings:
export DB_DRIVER_CLASS=org.postgresql.Driver
export DB_SCHEME=postgresql
export DB_USER=openmetadata_user
export DB_USER_PASSWORD=openmetadata_password
export DB_HOST=localhost
export DB_PORT=5432
export OM_DATABASE=openmetadata_db
export ELASTICSEARCH_HOST=localhost
export ELASTICSEARCH_PORT=9200
export SEARCH_TYPE=opensearch
export PIPELINE_SERVICE_CLIENT_ENDPOINT=http://localhost:8080
export AUTHENTICATION_PROVIDER=basic
export SERVER_PORT=8585
export SERVER_ADMIN_PORT=8586
export OPENMETADATA_HEAP_OPTS="-Xmx2G -Xms1G"
```

---

### Step 6 — CRLF fix for native scripts

All shell scripts on the NTFS mount (`/mnt/e/`) have Windows CRLF line endings. Strip before running:

```bash
sed -i 's/\r//' \
  bootstrap/openmetadata-ops.sh \
  bin/openmetadata-server-start.sh \
  conf/openmetadata-env.sh
```

> Only needed once per checkout. Do this again after any `git checkout` that touches these files.

---

### Step 7 — Database migrations

```bash
cd /mnt/e/A/OpenMetadata
source conf/local-dev.env
./bootstrap/openmetadata-ops.sh migrate --force
```

Applied all schema versions from 1.1.0 through 1.12.0 into `openmetadata_db`.

---

### Step 8 — Build and deploy the UI JAR

The UI must be built on the **WSL2 native filesystem** (`/root/`) not on the NTFS mount (`/mnt/e/`). Two issues affect NTFS builds:

1. **Less CSS timeout** — Vite's CSS preprocessor runs in worker threads; slow NTFS I/O causes workers to time out mid-build. Fix: add `preprocessorMaxWorkers: 0` to `vite.config.ts` (already committed to the repo).
2. **General slowness** — Even with the worker fix, NTFS I/O makes the full build ~45 min vs ~4 min on native ext4.

#### One-time vite.config.ts fix (already applied)

`openmetadata-ui/src/main/resources/ui/vite.config.ts` — added `preprocessorMaxWorkers: 0` inside the `css:` block:

```ts
css: {
  preprocessorMaxWorkers: 0,   // ← disables worker threads, avoids Less timeout on slow filesystems
  preprocessorOptions: {
    less: { ... }
  }
}
```

#### Build procedure

```bash
# 1. Rsync source to WSL native filesystem (fast ext4)
rsync -a --exclude='node_modules' --exclude='target' --exclude='.git' --exclude='dist' \
  /mnt/e/A/OpenMetadata/ /root/OpenMetadata/

# 2. Stop everything to free RAM for the build
kill $(pgrep -f OpenMetadataApplication) 2>/dev/null
cd /mnt/e/A/OpenMetadata/docker/development
docker compose -f docker-compose-infra.yml stop
sleep 2

# 3. Build the UI JAR on native filesystem (~4 min)
cd /root/OpenMetadata
export NODE_OPTIONS=--max-old-space-size=4096
mvn package -DskipTests -pl openmetadata-ui -am

# 4. Copy the built JAR into the project's libs/
cp /root/OpenMetadata/openmetadata-ui/target/openmetadata-ui-1.12.0-SNAPSHOT.jar \
   /mnt/e/A/OpenMetadata/libs/

# 5. Bring everything back up
cd /mnt/e/A/OpenMetadata/docker/development
docker compose -f docker-compose-infra.yml start
cd /mnt/e/A/OpenMetadata
source conf/local-dev.env
./bin/openmetadata-server-start.sh -daemon conf/openmetadata.yaml
```

> Re-run from step 1 any time you change frontend source files.

---

### Step 9 — Start OM server

```bash
cd /mnt/e/A/OpenMetadata
source conf/local-dev.env
./bin/openmetadata-server-start.sh -daemon conf/openmetadata.yaml
```

Verify:

```bash
curl http://localhost:8586/healthcheck
# {"OpenMetadataServerHealthCheck":{"healthy":true},"database":{"healthy":true},"deadlocks":{"healthy":true}}

curl http://localhost:8585/api/v1/system/config/jwks
# {"keys":[{"kty":"RSA",...}]}

curl -o /dev/null -w "%{http_code}" http://localhost:8585/
# 200
```

---

## Current State

| Service | URL | Status | Credentials |
|---------|-----|--------|-------------|
| OM Server API | http://localhost:8585/api/v1/ | ✅ Running | admin / admin |
| OM Admin | http://localhost:8586/healthcheck | ✅ Healthy | — |
| OpenSearch | http://localhost:9200 | ✅ Running | none |
| Airflow | http://localhost:8080 | ✅ Running | admin / admin |
| PostgreSQL | localhost:5432 | ✅ Running | openmetadata_user / openmetadata_password |
| OM Browser UI | http://localhost:8585 | ✅ Running | admin / admin |

> Airflow `WARN` messages in the OM server log are expected — Airflow is running in Docker and the OM server polls it periodically. These do not affect UI or API functionality.

**Memory at full load (15.9 GB machine, ~11.7 GB WSL2):**

| Process | RAM |
|---------|-----|
| OM Server (JVM -Xmx2G) | ~2.4 GB |
| OpenSearch (Docker) | ~1.6 GB |
| Airflow (Docker) | ~0.75 GB |
| WSL2 OS + buffers | ~1.5 GB |
| **Total** | **~97% of WSL2 allocation** |

**This leaves no headroom for a UI build (Node.js needs ~4 GB).** Stop everything before building — see "Rebuild UI After Frontend Changes" below.

---

## Daily Workflow

```bash
# Terminal 1 — Infrastructure
sudo service postgresql start
cd /mnt/e/A/OpenMetadata/docker/development
docker compose -f docker-compose-infra.yml up -d

# Terminal 2 — OM Server
cd /mnt/e/A/OpenMetadata
source conf/local-dev.env
./bin/openmetadata-server-start.sh -daemon conf/openmetadata.yaml
# Logs: tail -f logs/openmetadata.log

# Terminal 3 — Ingester development
cd /mnt/e/A/infra-devops/openmetadata
source .venv/bin/activate
python connectors/azure_ingester.py
```

---

## Rebuild After Java Changes

```bash
cd /mnt/e/A/OpenMetadata
mvn spotless:apply                              # format first
mvn package -DskipTests -pl openmetadata-service,openmetadata-mcp -am
# Restart server (Ctrl+C the running process or kill the daemon)
source conf/local-dev.env
./bin/openmetadata-server-start.sh -daemon conf/openmetadata.yaml
```

> The `libs/` symlink does not need to be recreated after a partial rebuild (only after `mvn clean`).

---

## Rebuild UI After Frontend Changes

```bash
# Rsync changed source to WSL native fs
rsync -a --exclude='node_modules' --exclude='target' --exclude='.git' --exclude='dist' \
  /mnt/e/A/OpenMetadata/ /root/OpenMetadata/

# Stop everything to free RAM for the build
kill $(pgrep -f OpenMetadataApplication) 2>/dev/null
cd /mnt/e/A/OpenMetadata/docker/development
docker compose -f docker-compose-infra.yml stop
sleep 2

# Build
cd /root/OpenMetadata
export NODE_OPTIONS=--max-old-space-size=4096
mvn package -DskipTests -pl openmetadata-ui -am

# Deploy
cp /root/OpenMetadata/openmetadata-ui/target/openmetadata-ui-1.12.0-SNAPSHOT.jar \
   /mnt/e/A/OpenMetadata/libs/

# Bring everything back up
cd /mnt/e/A/OpenMetadata/docker/development
docker compose -f docker-compose-infra.yml start
cd /mnt/e/A/OpenMetadata
source conf/local-dev.env
./bin/openmetadata-server-start.sh -daemon conf/openmetadata.yaml
```

---

## Key Files Created / Modified

| File | Purpose |
|------|---------|
| `docker/development/docker-compose-infra.yml` | Minimal Docker compose (OpenSearch + Airflow only) |
| `conf/local-dev.env` | Environment variables for native OM server |
| `libs` (symlink) | Points to `openmetadata-dist/target/libs/` |
| `openmetadata-ui/src/main/resources/ui/vite.config.ts` | Added `preprocessorMaxWorkers: 0` to fix Less CSS timeout |

---

## Known Issues & Fixes

| Issue | Cause | Fix |
|-------|-------|-----|
| `bash\r: No such file or directory` | CRLF line endings on NTFS `/mnt/e/` | `sed -i 's/\r//' <script>` |
| `docker: command not found` | Docker Desktop WSL integration disabled | Docker Desktop → Settings → Resources → WSL Integration → enable distro → Apply & Restart |
| UI build OOM / hangs at 97% RAM | Full stack leaves no room for Node.js (~4 GB needed) | Stop OM server + Docker infra before building (see "Rebuild UI" section) |
| Node.js heap OOM during UI build | Vite needs more than default heap | `NODE_OPTIONS=--max-old-space-size=4096` |
| `[vite:css] [less] timed-out` during UI build | Vite CSS worker threads time out on slow NTFS I/O | Build on WSL native fs (`/root/`) + `preprocessorMaxWorkers: 0` in `vite.config.ts` (already committed) |
| UI build takes 45+ min on `/mnt/e/` | NTFS filesystem I/O is slow from WSL2 | Rsync source to `/root/OpenMetadata/` and build there (~4 min) |
| `libs/` missing after `mvn clean` | Symlink target wiped by clean | Re-run `mvn install -DskipTests -DonlyBackend -pl openmetadata-dist -am` then `ln -s openmetadata-dist/target/libs libs` |
| Airflow `WARN` in OM server logs | OM polls Airflow API every 5 min; Airflow in Docker | Expected — does not affect UI or API functionality |
| WSL2 memory limit too low | Default is min(RAM/2, 8 GB) | Create `C:\Users\<you>\.wslconfig` with `[wsl2]` / `memory=12GB` then `wsl --shutdown` |
