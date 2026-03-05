# Windows Native Dev Setup — OpenMetadata 1.12.0-SNAPSHOT

**Date:** 2026-03-01
**Goal:** Run OpenMetadata natively on Windows to develop lineage ingesters, using less RAM than the WSL2 approach.
**OM repo:** `E:\A\OpenMetadata`

---

## Why Windows Native

On a 15.9 GB machine, the WSL2 approach hits ~97% RAM when all services are running.
Running natively on Windows eliminates the WSL2 distro overhead (~2–4 GB) and frees that memory for the Node.js UI build.

**Memory comparison:**

| Approach | Peak RAM |
|----------|----------|
| WSL2 (previous) | ~97% (15.4 GB) |
| Windows native | ~63% (10 GB) |

---

## Architecture

```
Windows (native)
├── Claude Code CLI      ← run from Git Bash terminal
├── Java 21 + Maven      ← build + run OM server
├── Node.js + Yarn       ← UI build
├── PostgreSQL           ← OM metadata DB + Airflow metadata DB
└── OM Server   :8585    ← REST API target for lineage POST calls

Docker Desktop (Windows)
├── OpenSearch  :9200    ← OM search index
└── Airflow     :8080    ← runs openmetadata_lineage_sync DAG
```

---

## Prerequisites

Verify these are installed before starting. Run in **Git Bash**:

```bash
java -version        # must be 21.x
mvn -version         # any recent 3.x
node -version        # 18+ recommended
yarn -version        # 1.22+
git --version        # any recent
docker version       # Docker Desktop must be running
```

Install missing tools:
```powershell
# PowerShell (as Administrator) — using winget
winget install Microsoft.OpenJDK.21
winget install Apache.Maven
winget install OpenJS.NodeJS.LTS
winget install Yarn.Yarn
# Docker Desktop: https://www.docker.com/products/docker-desktop/
```

After installing Java/Maven, set `JAVA_HOME` in System Environment Variables:
- Variable: `JAVA_HOME`
- Value: `C:\Program Files\Microsoft\jdk-21.0.x.x-hotspot` (adjust to actual path)
- Add `%JAVA_HOME%\bin` and Maven's `bin\` to `PATH`

> **Claude Code**: always launch from a **Git Bash** terminal so all shell commands in this guide work without modification.

---

## Step 1 — Install PostgreSQL for Windows

```powershell
# PowerShell (as Administrator)
winget install PostgreSQL.PostgreSQL.16
```

During installation:
- Set superuser (`postgres`) password — remember it
- Keep default port **5432**
- Keep default locale

Add PostgreSQL `bin\` to your PATH (e.g. `C:\Program Files\PostgreSQL\16\bin`) so `psql` is available in Git Bash.

Verify in Git Bash:
```bash
psql --version
# psql (PostgreSQL) 16.x
```

---

## Step 2 — Create databases

The PostgreSQL service starts automatically on Windows. Run the OM init script in Git Bash:

```bash
cd /e/A/OpenMetadata
psql -U postgres -f docker/postgresql/postgres-script.sql
```

Enter the `postgres` superuser password when prompted.

This creates:
- `openmetadata_db` owned by `openmetadata_user` / `openmetadata_password`
- `airflow_db` owned by `airflow_user` / `airflow_pass`

Verify:
```bash
psql -U postgres -c "\l" | grep -E "openmetadata|airflow"
```

---

## Step 3 — Docker infrastructure (OpenSearch + Airflow)

Use the Windows-adapted compose file. DAGs are served from `ingestion/examples/airflow/dags/` in this repo via a relative path — no path editing needed.

Start infra (first time requires `--build` to build the Airflow image, ~10–15 min):

```bash
cd /e/A/OpenMetadata/docker/development
docker compose -f docker-compose-infra-windows.yml up -d --build
```

Subsequent starts (no rebuild needed):
```bash
docker compose -f docker-compose-infra-windows.yml up -d
```

Verify:
```bash
docker ps
# om_opensearch   Up ... (healthy)
# om_airflow      Up ...

curl http://localhost:9200/_cluster/health
# {"status":"green",...}
```

> `host.docker.internal` is automatically resolved to the Windows host by Docker Desktop — no extra config needed.

---

## Step 4 — Build OM backend

```bash
cd /e/A/OpenMetadata
mvn install -DskipTests -DonlyBackend -pl openmetadata-dist -am
```

This builds the full Java dependency chain and assembles 557 JARs in `openmetadata-dist/target/libs/`.

Create the `libs/` symlink (Git Bash supports `ln -s`):

```bash
cd /e/A/OpenMetadata
ln -s openmetadata-dist/target/libs libs
```

> Recreate this symlink after `mvn clean`.

---

## Step 5 — Environment variables

The existing `conf/local-dev.env` works unchanged in Git Bash — it uses `export` syntax which Git Bash handles natively.

Review it to confirm the PostgreSQL settings point to localhost:

```bash
grep DB_ conf/local-dev.env
# DB_HOST=localhost
# DB_PORT=5432
# DB_USER=openmetadata_user
```

No changes needed — PostgreSQL is on `localhost:5432` on Windows, same as WSL2.

---

## Step 6 — CRLF fix for shell scripts

Shell scripts on the Windows filesystem have CRLF line endings. Fix them once in Git Bash:

```bash
cd /e/A/OpenMetadata
sed -i 's/\r//' \
  bootstrap/openmetadata-ops.sh \
  bin/openmetadata-server-start.sh \
  conf/openmetadata-env.sh
```

> Redo this after any `git checkout` that touches these files.

---

## Step 7 — Database migrations

```bash
cd /e/A/OpenMetadata
source conf/local-dev.env
./bootstrap/openmetadata-ops.sh migrate --force
```

Applies all schema versions from 1.1.0 through 1.12.0 into `openmetadata_db`.

---

## Step 8 — Build the UI JAR

On Windows native filesystem (no rsync needed — NTFS is the native FS here):

```bash
# Stop Docker infra to free RAM for the Node.js build (~4 GB needed)
cd /e/A/OpenMetadata/docker/development
docker compose -f docker-compose-infra-windows.yml stop

# Build (~4–6 min on Windows native NTFS)
cd /e/A/OpenMetadata
export NODE_OPTIONS=--max-old-space-size=4096
mvn package -DskipTests -pl openmetadata-ui -am

# Bring Docker infra back up
cd /e/A/OpenMetadata/docker/development
docker compose -f docker-compose-infra-windows.yml start
```

> `preprocessorMaxWorkers: 0` is already set in `vite.config.ts` — this prevents the Less CSS worker thread timeout on NTFS.
> No rsync to WSL needed; you build directly on `E:\A\OpenMetadata`.

---

## Step 9 — Start OM server

```bash
cd /e/A/OpenMetadata
source conf/local-dev.env
./bin/openmetadata-server-start.sh -daemon conf/openmetadata.yaml
```

Verify (server takes ~15–20 s to start):

```bash
curl http://localhost:8586/healthcheck
# {"OpenMetadataServerHealthCheck":{"healthy":true},"database":{"healthy":true},"deadlocks":{"healthy":true}}

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

**Memory at full load (~15.9 GB machine):**

| Process | RAM |
|---------|-----|
| Windows OS | ~4 GB |
| Docker Desktop (internal WSL2) | ~1 GB |
| OpenSearch (container) | ~1.6 GB |
| Airflow (container) | ~0.75 GB |
| OM Server (JVM -Xmx2G) | ~2.4 GB |
| PostgreSQL (Windows) | ~0.25 GB |
| **Total** | **~10 GB (~63%)** |

---

## Daily Workflow

Open **Git Bash** and run:

```bash
# Infrastructure (Docker)
cd /e/A/OpenMetadata/docker/development
docker compose -f docker-compose-infra-windows.yml up -d

# OM Server
cd /e/A/OpenMetadata
source conf/local-dev.env
./bin/openmetadata-server-start.sh -daemon conf/openmetadata.yaml
# Logs: tail -f logs/openmetadata.log

# Ingester development
cd /e/A/infra-devops/openmetadata
source .venv/bin/activate
python connectors/azure_ingester.py
```

> PostgreSQL starts automatically with Windows — no manual start needed.

---

## Rebuild After Java Changes

```bash
cd /e/A/OpenMetadata
mvn spotless:apply
mvn package -DskipTests -pl openmetadata-service,openmetadata-mcp -am

# Restart server
kill $(pgrep -f OpenMetadataApplication) 2>/dev/null; sleep 2
source conf/local-dev.env
./bin/openmetadata-server-start.sh -daemon conf/openmetadata.yaml
```

---

## Rebuild UI After Frontend Changes

```bash
# Stop everything to free RAM for the Node.js build
kill $(pgrep -f OpenMetadataApplication) 2>/dev/null
cd /e/A/OpenMetadata/docker/development
docker compose -f docker-compose-infra-windows.yml stop
sleep 2

# Build directly on Windows NTFS (no rsync needed)
cd /e/A/OpenMetadata
export NODE_OPTIONS=--max-old-space-size=4096
mvn package -DskipTests -pl openmetadata-ui -am

# Bring everything back up
cd /e/A/OpenMetadata/docker/development
docker compose -f docker-compose-infra-windows.yml start
cd /e/A/OpenMetadata
source conf/local-dev.env
./bin/openmetadata-server-start.sh -daemon conf/openmetadata.yaml
```

---

## Key Files

| File | Purpose |
|------|---------|
| `docker/development/docker-compose-infra-windows.yml` | Windows Docker compose (OpenSearch + Airflow) |
| `conf/local-dev.env` | Environment variables (same as WSL2, works in Git Bash) |
| `libs` (symlink) | Points to `openmetadata-dist/target/libs/` |
| `openmetadata-ui/src/main/resources/ui/vite.config.ts` | `preprocessorMaxWorkers: 0` — fixes Less CSS timeout |

---

## Known Issues & Fixes

| Issue | Cause | Fix |
|-------|-------|-----|
| `bash\r: No such file or directory` | CRLF line endings on Windows NTFS | `sed -i 's/\r//' <script>` in Git Bash |
| `JAVA_HOME` not found | Java installed but env var not set | Set `JAVA_HOME` in System Environment Variables |
| `psql: command not found` | PostgreSQL bin not in PATH | Add `C:\Program Files\PostgreSQL\16\bin` to PATH |
| `[vite:css] [less] timed-out` | Vite CSS worker threads timeout | Already fixed: `preprocessorMaxWorkers: 0` in `vite.config.ts` |
| UI build OOM | Full stack leaves no room for Node.js | Stop OM server + Docker before building (see "Rebuild UI" section) |
| `libs/` missing after `mvn clean` | Symlink wiped by clean | Re-run `mvn install -DskipTests -DonlyBackend -pl openmetadata-dist -am` then `ln -s openmetadata-dist/target/libs libs` |
| Docker `host.docker.internal` not resolving | Docker Desktop not running | Start Docker Desktop before running compose |
| Airflow `WARN` in OM server logs | OM polls Airflow every 5 min | Expected — does not affect functionality |
