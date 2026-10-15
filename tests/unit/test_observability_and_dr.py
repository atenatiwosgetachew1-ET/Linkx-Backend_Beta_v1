"""Unit tests for Phase 5: Observability and Disaster Recovery.

Verifies:
1. Syntax and readability of all backup and restore scripts.
2. Structure and multi-cluster targets in prometheus.yml.
3. Alert rules in alerts.yml (critical infrastructure and database alerts).
4. Completeness of disaster_recovery_playbook.md.
"""

import os
import subprocess
import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BACKUP_RESTORE_SCRIPTS = [
    "service_factory/services/linkx-control-data/scripts/backup-postgres.sh",
    "service_factory/services/linkx-control-data/scripts/restore-postgres.sh",
    "service_factory/services/Linkx_xmaintenance/scripts/backup-neo4j-offline.sh",
    "service_factory/services/Linkx_xmaintenance/scripts/restore-neo4j.sh",
    "service_factory/scripts/backup-artifacts.sh",
    "service_factory/scripts/restore-artifacts-to-dir.sh",
    "service_factory/scripts/prune-backups.sh",
    "service_factory/scripts/sync-backups-offhost.sh",
]


@pytest.mark.parametrize("rel_path", BACKUP_RESTORE_SCRIPTS)
def test_backup_restore_script_syntax(rel_path):
    """Verify each backup/restore script exists, is readable, and passes bash syntax check."""
    full_path = os.path.join(REPO_ROOT, rel_path)
    assert os.path.isfile(full_path), f"Script not found: {rel_path}"
    assert os.access(full_path, os.R_OK), f"Script not readable: {rel_path}"

    result = subprocess.run(["bash", "-n", full_path], capture_output=True, text=True)
    assert result.returncode == 0, f"Bash syntax error in {rel_path}: {result.stderr}"


def test_prometheus_scrape_config():
    """Verify prometheus.yml exists and defines scrape targets for both NBE and CDR clusters."""
    prom_path = os.path.join(REPO_ROOT, "service_factory", "deploy", "monitoring", "prometheus.yml")
    assert os.path.isfile(prom_path), "prometheus.yml not found"

    with open(prom_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Verify both cluster job names exist
    assert "nbe-hosts" in content
    assert "cdr-hosts" in content
    assert "alerts.yml" in content

    # Verify key node subnets are represented
    assert "172.27.23.95" in content   # NBE API
    assert "172.27.23.106" in content  # NBE Control-Data
    assert "172.21.13.70" in content   # CDR API
    assert "172.21.13.68" in content   # CDR Control-Data


def test_prometheus_alerting_rules():
    """Verify alerts.yml defines required operational and database alert rules."""
    alerts_path = os.path.join(REPO_ROOT, "service_factory", "deploy", "monitoring", "alerts.yml")
    assert os.path.isfile(alerts_path), "alerts.yml not found"

    with open(alerts_path, "r", encoding="utf-8") as f:
        content = f.read()

    expected_alerts = [
        "LinkXNodeDown",
        "LinkXHighCPUUsage",
        "LinkXLowDiskSpace",
        "LinkXCriticalDiskSpace",
        "LinkXNeo4jHeapCritical",
        "LinkXRedisMemoryHigh",
        "LinkXWorkerQueueBacklog",
    ]
    for alert in expected_alerts:
        assert f"alert: {alert}" in content, f"Missing alert definition: {alert}"


def test_disaster_recovery_playbook_contents():
    """Verify disaster_recovery_playbook.md contains procedures for all primary failure scenarios."""
    playbook_path = os.path.join(REPO_ROOT, "docs", "disaster_recovery_playbook.md")
    assert os.path.isfile(playbook_path), "docs/disaster_recovery_playbook.md not found"

    with open(playbook_path, "r", encoding="utf-8") as f:
        content = f.read()

    assert "Scenario A: Server 2 Failure" in content
    assert "Scenario B: Server 4 Failure" in content
    assert "Scenario C: Server 1 Failure" in content
    assert "Scenario D: Server 3 Failure" in content
    assert "Scenario E: Complete Cluster Rebuild" in content
    assert "restore-postgres.sh" in content
    assert "restore-neo4j.sh" in content
