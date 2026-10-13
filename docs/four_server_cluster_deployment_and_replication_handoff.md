# LinkX 4-Server Cluster Deployment & Replication Handoff Guide

**Target Scope**: Turnkey replication and deployment of the LinkX distributed system onto fresh 4-server clusters for new clients or environments.  
**Version**: 1.0 (Production Verified)  
**Last Updated**: October 2026  

---

## 1. System Mission & High-Level Architecture

LinkX is a high-throughput, distributed financial graph analytics, Anti-Money Laundering (AML) typologies detection (smurfing, circular flow, pass-through hubs, rapid fan-out, mule accounts), and automated compliance reporting platform. It features **xVigilance**, an autonomous detective engine running temporal transaction window analysis.

The architecture physically decouples API ingestion, database coordination, intensive graph/rule computations, and graph database storage across 4 dedicated servers:

```
                          [ React / Vite Frontend ]
                                      │
                                      │ HTTP / WebSocket (8000 / 443)
                                      ▼
┌────────────────────────────────────────────────────────────────────────────┐
│ SERVER 1: API Gateway (Flask + Socket.IO + RBAC)                           │
│ - Hostname: node19                                                         │
│ - Primary Role: Ingress Gateway, Auth/RBAC, Session Orchestration          │
│ - Storage Role: NFS Server exporting /mnt/linkx-artifacts                  │
│ - Unit: linkx-api.service behind Nginx reverse proxy                       │
└─────────────────────┬───────────────────────────────┬──────────────────────┘
                      │                               │
           Postgres/Redis (5432, 6379)     NFS Mount (/mnt/linkx-artifacts)
                      │                               │
                      ▼                               ▼
┌─────────────────────────────────────────┐  ┌───────────────────────────────┐
│ SERVER 2: Control Data Plane            │  │ SERVER 3: Worker Execution    │
│ - Hostname: node20                      │  │ - Hostname: node21            │
│ - Primary Role: Shared Coordination DB  │  │ - Primary Role: Rules & AML   │
│ - Services: PostgreSQL 16 & Redis 7     │  │ - Units: linkx-worker.service │
│ - Runtime: Docker Compose via systemd   │  │   xvigilance-consumer.service │
└─────────────────────▲───────────────────┘  └───────────────┬───────────────┘
                      │                                      │ Cypher (7687)
                      │                                      │
┌─────────────────────┴──────────────────────────────────────▼───────────────┐
│ SERVER 4: Graph Persistence & Maintenance                                  │
│ - Hostname: node22                                                         │
│ - Primary Role: Graph Database (Neo4j) & Autonomous Maintenance            │
│ - Services: Neo4j 5 Enterprise/Community, xCleanup Worker/Scheduler        │
│ - Units: Docker Neo4j, linkx-xcleanup-worker, linkx-xvigilance             │
│ - Storage: NFS Client mounting /mnt/linkx-artifacts                        │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Server Roles & Resource Specifications

| Node | Role Name | Service Package | Minimum CPU | Minimum RAM | Swap Target | Key Storage / Mounts |
| :--- | :--- | :--- | :---: | :---: | :---: | :--- |
| **Server 1** | `api` | `service_factory/services/linkx-api` | 4 Cores | 8 GiB | 4 GiB | Local SSD; exports `/mnt/linkx-artifacts` (NFS) |
| **Server 2** | `control-data` | `service_factory/services/linkx-control-data` | 4 Cores | 8 GiB | 4 GiB | Fast SSD for PostgreSQL 16 & Redis 7 Docker volumes |
| **Server 3** | `worker` | `service_factory/services/linkx-worker` | 4 Cores | 10 GiB | 4 GiB | NFS client mount at `/mnt/linkx-artifacts` |
| **Server 4** | `graph-maintenance` | `service_factory/services/Linkx_xmaintenance` | 4 Cores | 10 GiB | 4 GiB | SSD for Neo4j data + NFS mount at `/mnt/linkx-artifacts` |

> [!IMPORTANT]
> **Server 4 Memory Architecture**: Neo4j requires `4G` initial/max heap and `3G` pagecache (`NEO4J_server_memory_pagecache_size: 3G`), consuming 7 GiB of physical RAM. Server 4 **must** have at least 10 GiB RAM plus a 4 GiB swapfile (`vm.swappiness = 10`) to eliminate Linux Out-Of-Memory (OOM) killer terminations during algorithmic queries.

---

## 3. Network Communication Matrix & Port Security

All 4 servers must share an isolated internal subnet (e.g., `172.21.13.0/24` or similar `/24` private network).

| Source | Destination | Port | Protocol | Purpose | UFW / Firewall Rule |
| :--- | :--- | :---: | :---: | :--- | :--- |
| Client / Frontend | Server 1 | `80` / `443` | TCP | Web UI & API traffic | Public / Customer Corporate Network |
| Frontend | Server 1 | `8000` | TCP | Direct API / Socket.IO | Restricted to frontend host or localhost |
| Server 1, 3, 4 | Server 2 | `5432` | TCP | PostgreSQL control connections | Allow strictly from Server 1, 3, 4 |
| Server 1, 3, 4 | Server 2 | `6379` | TCP | Redis queues & coordination | Allow strictly from Server 1, 3, 4 (Require Password) |
| Server 1, 3 | Server 4 | `7687` | TCP | Neo4j Bolt graph queries | Allow strictly from Server 1, 3 |
| Admin Host | Server 4 | `7474` | TCP | Neo4j Browser admin console | Restricted to Admin Workstation IP |
| Server 3, 4 | Server 1 | `2049` | TCP | NFS mount (`/mnt/linkx-artifacts`) | Allow strictly from Server 3, 4 |

---

## 4. Pre-Deployment Discovery Command

Before starting a deployment on any new cluster, run this single-line diagnostic on all 4 candidate machines:

```bash
echo "=== HOST & OS ==="; hostname; cat /etc/os-release | grep PRETTY; echo "=== CPU & RAM ==="; echo "Cores: $(nproc)"; free -h; echo "=== DISK ==="; df -h /; echo "=== NET ==="; ip -br a; echo "=== PORTS ==="; ss -tlpn
```

**Pass Criteria**:
1. OS is Ubuntu 22.04 LTS or 24.04 LTS.
2. Server 1 & 2 have ≥ 8 GB RAM; Server 3 & 4 have ≥ 10 GB RAM.
3. Ports `8000`, `5432`, `6379`, `7687`, `7474`, `2049` are completely free.
4. All nodes can ping each other across the private network with < 5ms latency.

---

## 5. End-to-End Step-by-Step Deployment Blueprint

### Step 0: Base System Packages

Run on each server according to its assigned role:

#### On Server 2 & Server 4 (Docker Hosts):
```bash
sudo apt update && sudo apt install -y docker.io docker-compose-v2 git
sudo systemctl enable --now docker
sudo usermod -aG docker $USER
```

#### On Server 1 (API & NFS Server):
```bash
sudo apt update && sudo apt install -y nfs-kernel-server nginx python3-venv python3-pip git
```

#### On Server 3 & Server 4 (NFS Clients & Python Runtimes):
```bash
sudo apt update && sudo apt install -y nfs-common python3-venv python3-pip git
```

---

### Step 1: Master Encryption Key Generation

The API and Workers share an identical Python Fernet key to encrypt and decrypt database credentials in `managed_secrets`.

Run once on your admin workstation:
```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```
*Save this key securely. It must be placed as `LINKX_SECRET_ENCRYPTION_KEY` in the `.env` files of Server 1, Server 3, and Server 4.*

---

### Step 2: Shared Artifact Storage (NFS)

#### On Server 1 (`node19` - NFS Server):
```bash
# 1. Create directory structure
sudo mkdir -p /mnt/linkx-artifacts/{uploads,dfparts,logs,rules,graphs,reports,configs}
sudo chown -R nobody:nogroup /mnt/linkx-artifacts
sudo chmod -R 775 /mnt/linkx-artifacts

# 2. Export to Server 3 and Server 4 (Replace with actual target IPs)
sudo mkdir -p /etc/exports.d
echo "/mnt/linkx-artifacts <SERVER_3_IP>(rw,sync,no_subtree_check,no_root_squash) <SERVER_4_IP>(rw,sync,no_subtree_check,no_root_squash)" | sudo tee /etc/exports.d/linkx-artifacts.exports

# 3. Apply exports
sudo exportfs -ra
sudo systemctl restart nfs-kernel-server
```

#### On Server 3 & Server 4 (NFS Clients):
```bash
sudo mkdir -p /mnt/linkx-artifacts
sudo mount -t nfs <SERVER_1_IP>:/mnt/linkx-artifacts /mnt/linkx-artifacts

# Add to /etc/fstab for boot persistence
grep -q '<SERVER_1_IP>:/mnt/linkx-artifacts' /etc/fstab || \
echo "<SERVER_1_IP>:/mnt/linkx-artifacts /mnt/linkx-artifacts nfs defaults,_netdev,nofail 0 0" | sudo tee -a /etc/fstab
```

---

### Step 3: Deploy Server 2 (PostgreSQL 16 & Redis 7)

#### 1. Setup Directory & Compose File:
```bash
sudo mkdir -p /opt/linkx-control-data
sudo chown -R $USER:$USER /opt/linkx-control-data
```
Copy `service_factory/services/linkx-control-data/` to `/opt/linkx-control-data/`.

#### 2. Create `/opt/linkx-control-data/.env`:
```ini
POSTGRES_USER=linkx
POSTGRES_PASSWORD=<STRONG_POSTGRES_PASSWORD>
POSTGRES_DB=linkx
REDIS_PASSWORD=<STRONG_REDIS_PASSWORD>
REDIS_BIND_ADDR=0.0.0.0
REDIS_PORT=6379
```

#### 3. Start Containers:
```bash
cd /opt/linkx-control-data
docker compose up -d
docker compose ps
```

#### 4. Execute Full Schema Migrations:
```bash
# Run SQL migrations 001 through 006 in order:
docker exec -i linkx-postgres psql -U linkx -d linkx < /opt/linkx-control-data/postgres/initdb/001_core_schema.sql
docker exec -i linkx-postgres psql -U linkx -d linkx < /opt/linkx-control-data/postgres/initdb/002_seed_rbac.sql
docker exec -i linkx-postgres psql -U linkx -d linkx < /opt/linkx-control-data/postgres/migrations/003_cancellation_contract.sql
docker exec -i linkx-postgres psql -U linkx -d linkx < /opt/linkx-control-data/postgres/migrations/004_session_config_store.sql
docker exec -i linkx-postgres psql -U linkx -d linkx < /opt/linkx-control-data/postgres/migrations/005_report_pipeline.sql
docker exec -i linkx-postgres psql -U linkx -d linkx < /opt/linkx-control-data/postgres/migrations/006_cluster_bootstrap.sql
```
*(006_cluster_bootstrap.sql automatically seeds global_rule_thresholds, global_entity_classification, global_score_lineage, managed_secrets, security_audit_events, link_analysis_evidence, and xvigilance checkpoints without requiring host Python libraries).*

---

### Step 4: Deploy Server 4 (Neo4j Graph Database & Maintenance)

#### 1. Deploy Neo4j Docker Compose:
```bash
sudo mkdir -p /opt/linkx-neo4j
sudo chown -R $USER:$USER /opt/linkx-neo4j
```
Create `/opt/linkx-neo4j/.env`:
```ini
NEO4J_AUTH=neo4j/<STRONG_NEO4J_PASSWORD>
NEO4J_server_memory_heap_initial__size=4G
NEO4J_server_memory_heap_max__size=4G
NEO4J_server_memory_pagecache_size=3G
```
Start Neo4j:
```bash
cd /opt/linkx-neo4j
docker compose up -d
```

#### 2. Deploy Maintenance Services:
```bash
sudo mkdir -p /opt/Linkx_xmaintenance
sudo chown -R $USER:$USER /opt/Linkx_xmaintenance
```
Copy `service_factory/services/Linkx_xmaintenance/` to `/opt/Linkx_xmaintenance/`.

Create Virtual Environment & Install Dependencies:
```bash
python3 -m venv /opt/Linkx_xmaintenance/.venv
/opt/Linkx_xmaintenance/.venv/bin/pip install --upgrade pip
/opt/Linkx_xmaintenance/.venv/bin/pip install -r /opt/Linkx_xmaintenance/src/requirements.txt
```

Create `/opt/Linkx_xmaintenance/.env`:
```ini
PYTHONUNBUFFERED=1
DATABASE_URL=postgresql://linkx:<STRONG_POSTGRES_PASSWORD>@<SERVER_2_IP>:5432/linkx
LINKX_POSTGRES_DSN=postgresql://linkx:<STRONG_POSTGRES_PASSWORD>@<SERVER_2_IP>:5432/linkx
LINKX_REDIS_URL=redis://:<STRONG_REDIS_PASSWORD>@<SERVER_2_IP>:6379/0
LINKX_ARTIFACT_ROOT=/mnt/linkx-artifacts

LINKX_NEO4J_URL=bolt://127.0.0.1:7687
LINKX_NEO4J_USERNAME=neo4j
LINKX_NEO4J_PASSWORD=<STRONG_NEO4J_PASSWORD>
LINKX_SECRET_ENCRYPTION_KEY=<SHARED_FERNET_KEY>

CLEANUP_WORKER_NAME=linkx-xcleanup-1
CLEANUP_POLL_INTERVAL=5
CLEANUP_SCHEDULE_INTERVAL_SECONDS=3600
```

Install & Start Systemd Services:
```bash
sudo cp /opt/Linkx_xmaintenance/deploy/systemd/linkx-xcleanup-worker.service /etc/systemd/system/
sudo cp /opt/Linkx_xmaintenance/deploy/systemd/linkx-xcleanup-scheduler.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now linkx-xcleanup-worker linkx-xcleanup-scheduler
```

---

### Step 5: Deploy Server 3 (Analysis & Ingestion Worker)

#### 1. Setup Directory:
```bash
sudo mkdir -p /opt/linkx-worker
sudo chown -R $USER:$USER /opt/linkx-worker
```
Copy `service_factory/services/linkx-worker/` to `/opt/linkx-worker/`.

#### 2. Create Virtual Environment:
```bash
python3 -m venv /opt/linkx-worker/.venv
/opt/linkx-worker/.venv/bin/pip install --upgrade pip
/opt/linkx-worker/.venv/bin/pip install -r /opt/linkx-worker/src/requirements.txt
```

#### 3. Create `/opt/linkx-worker/.env`:
```ini
PYTHONUNBUFFERED=1
LINKX_SECRET_ENCRYPTION_KEY=<SHARED_FERNET_KEY>
LINKX_POSTGRES_DSN=postgresql://linkx:<STRONG_POSTGRES_PASSWORD>@<SERVER_2_IP>:5432/linkx
LINKX_REDIS_URL=redis://:<STRONG_REDIS_PASSWORD>@<SERVER_2_IP>:6379/0
LINKX_ARTIFACT_ROOT=/mnt/linkx-artifacts

# Neo4j Graph Target on Server 4
LINKX_NEO4J_URL=bolt://<SERVER_4_IP>:7687
LINKX_ACTIVE_TOOL_PROTOCOL=bolt://<SERVER_4_IP>:7687
LINKX_NEO4J_USERNAME=neo4j
LINKX_NEO4J_PASSWORD=<STRONG_NEO4J_PASSWORD>

# Worker Timeouts & Limits
WORKER_JOB_TIMEOUT_SECONDS_SEARCH=300
WORKER_JOB_TIMEOUT_SECONDS_GRAPH=300
WORKER_JOB_TIMEOUT_SECONDS_DATAFRAME=3600
WORKER_JOB_TIMEOUT_SECONDS_ANALYSIS=7200
```

#### 4. Install & Start Systemd Service:
```bash
sudo cp /opt/linkx-worker/deploy/systemd/linkx-worker.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now linkx-worker
```

---

### Step 6: Deploy Server 1 (API Gateway & Nginx)

#### 1. Setup Directory:
```bash
sudo mkdir -p /opt/linkx-backend-api
sudo chown -R $USER:$USER /opt/linkx-backend-api
```
Copy `service_factory/services/linkx-api/` to `/opt/linkx-backend-api/`.

#### 2. Create Virtual Environment:
```bash
python3 -m venv /opt/linkx-backend-api/.venv
/opt/linkx-backend-api/.venv/bin/pip install --upgrade pip
/opt/linkx-backend-api/.venv/bin/pip install -r /opt/linkx-backend-api/src/requirements.txt
```

#### 3. Create `/opt/linkx-backend-api/.env`:
```ini
PORT=8000
FLASK_ENV=production
PYTHONUNBUFFERED=1
LINKX_SECRET_ENCRYPTION_KEY=<SHARED_FERNET_KEY>
LINKX_FLASK_SECRET_KEY=<RANDOM_HEX_64_CHARS>

# Data Plane Connections
DATABASE_URL=postgresql://linkx:<STRONG_POSTGRES_PASSWORD>@<SERVER_2_IP>:5432/linkx
LINKX_POSTGRES_DSN=postgresql://linkx:<STRONG_POSTGRES_PASSWORD>@<SERVER_2_IP>:5432/linkx
LINKX_REDIS_URL=redis://:<STRONG_REDIS_PASSWORD>@<SERVER_2_IP>:6379/0
LINKX_ARTIFACT_ROOT=/mnt/linkx-artifacts

# Neo4j Target on Server 4
LINKX_NEO4J_URL=bolt://<SERVER_4_IP>:7687
LINKX_ACTIVE_TOOL_PROTOCOL=bolt://<SERVER_4_IP>:7687

# Authentication Mode (Set true for standalone admin auto-login, or false for strict tokens)
LINKX_AUTO_LOGIN_ADMIN=true
LINKX_CORS_ORIGINS=*
LINKX_MAX_JSON_BYTES=2097152
LINKX_ASYNC_WORKER_JOBS=true
```

#### 4. Configure Systemd Service:
```bash
sudo cp /opt/linkx-backend-api/deploy/systemd/linkx-api.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now linkx-api
```

#### 5. Configure Nginx Reverse Proxy:
Copy `/opt/linkx-backend-api/deploy/nginx/linkx-api-gateway.conf` to `/etc/nginx/sites-available/linkx-api-gateway.conf`.  
Enable the site and reload Nginx:
```bash
sudo ln -sf /etc/nginx/sites-available/linkx-api-gateway.conf /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx
```

---

## 6. Post-Deployment Verification & Operational Drift Checks

LinkX includes an operational verification script (`verify-linkx-server.py`) that audits live systemd units, listening ports, Redis auth, and HTTP endpoints.

Run the script on each server specifying its active role:

#### On Server 1:
```bash
python3 /opt/linkx-backend-api/service_factory/deploy/security/verify-linkx-server.py --role api
# Expected: PASS on health endpoints, Redis auth check, and systemd units
```

#### On Server 2:
```bash
python3 /opt/linkx-control-data/deploy/security/verify-linkx-server.py --role control-data
# Expected: PASS on Redis authentication requirement and Docker Compose containers
```

#### On Server 3:
```bash
python3 /opt/linkx-worker/deploy/security/verify-linkx-server.py --role worker
# Expected: PASS on worker systemd unit and Redis connectivity
```

#### On Server 4:
```bash
python3 /opt/Linkx_xmaintenance/deploy/security/verify-linkx-server.py --role graph-maintenance
# Expected: PASS on cleanup units and Redis connectivity
```

---

## 7. Current Deployment Reference (October 2026 Cluster)

For the active deployment:
* **Server 1 (`node19`)**: `172.21.13.70` (API Gateway & NFS)
* **Server 2 (`node20`)**: `172.21.13.68` (PostgreSQL 16 & Redis 7)
* **Server 3 (`node21`)**: `172.21.13.66` (Worker Node)
* **Server 4 (`node22`)**: `172.21.13.67` (Neo4j & Maintenance)

All 4 nodes run Ubuntu 24.04.5 LTS on subnet `172.21.13.0/24`.

---

## 8. Live Cluster Execution Tracker (Active Rollout)

### Cluster Cryptographic Secrets & Passwords (Generated for this Deployment)
* **Master Encryption Key (`LINKX_SECRET_ENCRYPTION_KEY`)**:
  ```text
  VrJVoT3RQ3Qvf1wgg0_Y_PX1sMGOufoMLeS8ZLS0Ebg=
  ```
  *(Used across Server 1, Server 3, and Server 4 to encrypt/decrypt managed credentials in PostgreSQL).*
* **Flask Secret Key (`LINKX_FLASK_SECRET_KEY`)**:
  ```text
  a6b6e536f073f984e1f1889312bb7de3f8faf5aab0bf18033c1a2424a0721160
  ```
  *(Used on Server 1 for signing sessions).*
* **PostgreSQL Password (`POSTGRES_PASSWORD`)**:
  ```text
  tqvyTlaM_Dhl0mB6vrme9g
  ```
* **Redis Password (`REDIS_PASSWORD`)**:
  ```text
  Yp8R5KULTolW7wkllybkag
  ```
* **Neo4j Password (`NEO4J_PASSWORD`)**:
  ```text
  kMea_0CMcoWRIJEWKh-eyQ
  ```

### Live Progress Checklist
* [x] **Pre-flight Audit**: Completed across `node19`, `node20`, `node21`, `node22`. Subnet `172.21.13.0/24` verified.
* [x] **Git Repository Synced**: Pushed latest code & handoff documentation to `https://github.com/atenatiwosgetachew1-ET/Linkx-Backend_Beta_v1.git`.
* [x] **Base Packages (`node19`)**: `nfs-kernel-server`, `nginx`, `python3-venv`, `python3-pip`, `git` installed.
* [x] **Base Packages (`node20`)**: `docker.io`, `docker-compose-v2`, `git` installed.
* [x] **Base Packages (`node21`)**: `nfs-common`, `python3-venv`, `python3-pip`, `git` installed.
* [x] **Base Packages (`node22`)**: `docker.io`, `docker-compose-v2`, `nfs-common`, `python3-venv`, `python3-pip` installed.
* [x] **Git Clones**: Cloned `/opt/linkx-backend-update` successfully on `node19`, `node20`, `node21`, `node22`.
* [x] **NFS Storage Export (`node19`)**: `/mnt/linkx-artifacts` created and exported to `172.21.13.66` and `172.21.13.67`.
* [x] **NFS Storage Mount (`node22`)**: Mounted at `/mnt/linkx-artifacts` and added to `/etc/fstab`.
* [ ] **NFS Storage Mount (`node21`)**: Run mount command on `node21`.
* [x] **Server 2 Setup**: Postgres 16 & Redis 7 Docker launched, all 25 tables created, indexed & seeded.
* [ ] **Server 4 Setup**: Neo4j Docker launch + maintenance services systemd launch.
* [ ] **Server 3 Setup**: Worker service systemd launch.
* [ ] **Server 1 Setup**: API service systemd launch + Nginx reverse proxy.
* [ ] **Drift & Health Audits**: `verify-linkx-server.py` green on all 4 nodes.


