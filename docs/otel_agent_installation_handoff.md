# CTMS OpenTelemetry Agent — Installation Handoff

**Last updated**: 2026-09-01
**Based on**: Successful rollout across LinkX servers node-19 through node-22

---

## 1. Purpose

This document describes how to install the **CTMS standard OpenTelemetry collector agent** on a server so it reports host health metrics to the central monitoring server and appears on the **NBE dashboard**.

### What it does
- Collects **host-level metrics**: CPU, memory, disk, network, load, paging, processes, and filesystem usage
- Exposes metrics in Prometheus format for the monitoring server to scrape
- Forwards OTLP traces to the central trace collector
- Runs as a lightweight systemd service (`otelcol-contrib`)

### What it does NOT do
- Does not read application data, logs, or files
- Does not open anything to the internet
- Only the monitoring server at `172.27.23.36` is allowed to read metrics

---

## 2. Prerequisites

| Requirement | Detail |
|:---|:---|
| OS | Linux (Debian/Ubuntu or RHEL/CentOS) |
| Access | `sudo` / root |
| Internet | Required to download the `.deb` package from GitHub (or provide the package manually) |
| Firewall | `ufw` (or `iptables` for Docker hosts) |

---

## 3. Port Convention

| Port | Purpose | Bound to | Who accesses it |
|:---|:---|:---|:---|
| **8889** | Default CTMS metrics port (use when no app occupies it) | `0.0.0.0` | Monitoring server `172.27.23.36` |
| **8890** | Alternate metrics port (use when the app already uses 8889) | `0.0.0.0` | Monitoring server `172.27.23.36` |
| **13133** | OTel health check | `0.0.0.0` | Monitoring server `172.27.23.36` only |
| **4317** | OTLP gRPC receiver | `127.0.0.1` | Localhost only (app can send traces here) |
| **4318** | OTLP HTTP receiver | `127.0.0.1` | Localhost only (app can send traces here) |

**When to use port 8890**: If the server already has an application exposing Prometheus metrics on port 8889 (e.g., a Python `prometheus_client` server, a Node.js metrics endpoint, or any existing `/metrics` handler), use port **8890** for the CTMS agent to avoid collision.

---

## 4. Installation Steps

### Step 1 — Get the install script onto the server

The install script is located at:
```
service_factory/deploy/security/install-otel-agent.sh
```

If the server has a repo clone, pull the latest:
```bash
cd /opt/<your-repo-update-path> && sudo git pull
```

Otherwise, copy the script to the server manually (e.g., via `scp`).

### Step 2 — Review and run the installer

```bash
# Review the script (optional but recommended)
less install-otel-agent.sh

# Run it
sudo bash install-otel-agent.sh <server-name> <role> <port>
```

**Parameters:**

| Param | Required | Description | Example |
|:---|:---|:---|:---|
| `server-name` | Yes | Must match the node's label in the monitoring target list | `node-50` |
| `role` | Optional | Describes the server's function | `api`, `worker`, `control-data`, `web` |
| `port` | Optional | Metrics export port (default: `8889`) | `8890` |

**Examples:**
```bash
# Standard server (port 8889 is free)
sudo bash install-otel-agent.sh node-50 web

# Server where an app already uses port 8889
sudo bash install-otel-agent.sh node-50 web 8890
```

### Step 3 — Configure the firewall

**For servers using UFW (no Docker):**
```bash
sudo ufw allow from 172.27.23.36 to any port <port> proto tcp comment "CTMS OTel host metrics"
sudo ufw allow from 172.27.23.36 to any port 13133 proto tcp comment "OTel health check"
sudo ufw deny 13133/tcp comment "Block OTel health check from others"
```

**For servers running Docker (additional rule needed):**
```bash
# UFW rules (same as above)
sudo ufw allow from 172.27.23.36 to any port <port> proto tcp comment "CTMS OTel host metrics"
sudo ufw allow from 172.27.23.36 to any port 13133 proto tcp comment "OTel health check"
sudo ufw deny 13133/tcp comment "Block OTel health check from others"

# Docker-specific iptables rule
sudo iptables -I DOCKER-USER -p tcp --dport <port> -s 172.27.23.36 -j ACCEPT
```

Replace `<port>` with `8889` or `8890` depending on which port you used in Step 2.

---

## 5. Verification

### Immediate check (run right after install)
```bash
# Check the service is active
systemctl is-active otelcol-contrib

# Health check
curl -s http://127.0.0.1:13133/ && echo " health check OK"
```

### Metrics check (wait ~15 seconds after install for first batch)
```bash
# Should return system_cpu, system_memory, system_filesystem lines
curl -s http://127.0.0.1:<port>/metrics | grep -m5 -E '^system_(cpu|memory|filesystem)'
```

### Dashboard check
The server should appear on the NBE dashboard within **30 seconds** of the metrics port being reachable from `172.27.23.36`.

---

## 6. What Gets Installed

| Item | Location |
|:---|:---|
| Binary | `/usr/bin/otelcol-contrib` |
| Config | `/etc/otelcol-contrib/config.yaml` |
| Systemd unit | `/usr/lib/systemd/system/otelcol-contrib.service` |
| Version | `0.157.0` |

### Config overview

```yaml
receivers:
  hostmetrics:          # CPU, memory, disk, network, load, paging, processes, filesystem
    collection_interval: 15s
  otlp:                 # gRPC on 127.0.0.1:4317, HTTP on 127.0.0.1:4318

processors:
  memory_limiter:       # 80% limit, 25% spike
  resourcedetection:    # Auto-detects hostname, OS
  resource:             # Adds server=<name>, role=<role>, environment=production
  batch:                # 10s batch timeout

exporters:
  prometheus:           # Exposes metrics on 0.0.0.0:<port>
  otlp/traces:          # Forwards traces to 172.27.23.213:4317

extensions:
  health_check:         # Exposes health on 0.0.0.0:13133
```

---

## 7. Security Checklist

After installation, verify these security properties:

- [ ] Metrics port (`8889` or `8890`) — UFW allows **only** `172.27.23.36`
- [ ] Health check port (`13133`) — UFW allows `172.27.23.36`, denies all others
- [ ] OTLP receivers (`4317`, `4318`) — bound to `127.0.0.1` (localhost only, not exposed)
- [ ] Docker hosts — `iptables DOCKER-USER` rule added for metrics port
- [ ] No other IPs have access to OTel ports

---

## 8. Troubleshooting

### "FAIL: no system_* metrics on :\<port\>"
**Not a failure** — the install script only waits 5 seconds, but the batch processor has a 10s timeout and the collection interval is 15s. Wait 20 seconds and retry:
```bash
curl -s http://127.0.0.1:<port>/metrics | grep -m3 -E '^system_(cpu|memory|filesystem)'
```

### "It failed downloading the package"
The server has no internet access. Download the `.deb` manually and transfer it:
```bash
# From a machine with internet
curl -fsSLO https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v0.157.0/otelcol-contrib_0.157.0_linux_amd64.deb

# Transfer to target server, then install
sudo dpkg -i otelcol-contrib_0.157.0_linux_amd64.deb
```
Then re-run the install script — it skips the download if the package is already installed.

### Service is active but no metrics
Check the logs:
```bash
sudo journalctl -u otelcol-contrib --no-pager -n 30
```

### Port conflict
If the chosen port is already in use:
```bash
ss -tlnp | grep <port>
```
Switch to the alternate port (`8890` or `8889`) and re-run the installer.

### Server doesn't appear on the dashboard
Almost always a firewall issue:
```bash
# Check UFW is active
sudo ufw status

# Verify the rule exists
sudo ufw status numbered | grep <port>

# Docker hosts: verify iptables
sudo iptables -L DOCKER-USER -n | grep <port>
```

---

## 9. Service Management

```bash
# Check status
systemctl status otelcol-contrib

# Restart after config changes
sudo systemctl restart otelcol-contrib

# View logs
sudo journalctl -u otelcol-contrib -f

# Stop (not recommended in production)
sudo systemctl stop otelcol-contrib
```

---

## 10. Reference: Completed Rollout

| Server | Node | Role | Port | Status |
|:---|:---|:---|:---|:---|
| LinkX API | node-19 | `api` | 8890 | Live |
| LinkX Control Data | node-20 | `control-data` | 8890 | Live |
| LinkX Worker | node-21 | `worker` | 8890 | Live |
| LinkX Graph Maintenance | node-22 | `graph-maintenance` | 8890 | Live |

These servers use port **8890** because their applications already occupy port 8889 with Python `prometheus_client` metrics servers.

---

## 11. Common Pitfalls From the LinkX Rollout

1. **Do NOT use a Docker-based OTel collector for host metrics** — it can't see the host's real CPU/memory/disk from inside a container. Always use the systemd-based install.

2. **Never configure a Prometheus receiver to scrape the same port the Prometheus exporter serves on** — this creates a circular self-scrape loop that consumes unbounded CPU and RAM. (This was the root cause of the broken Server 2 collector during the LinkX rollout.)

3. **The install script's verification may show `FAIL` even when everything is fine** — the batch processor needs ~15 seconds to flush the first metrics. Always re-check manually after 20 seconds.

4. **On Docker hosts, UFW alone is not enough** — Docker bypasses UFW by inserting its own iptables chains. You must also add an `iptables -I DOCKER-USER` rule.
