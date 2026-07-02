#!/usr/bin/env python3
"""Verify LinkX backup automation and recovery-readiness signals.

This script checks the local automation state for each backup family. It does
not perform destructive restore drills; those remain explicit operator actions.
"""

from __future__ import annotations

import argparse
import glob
import subprocess
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path("/opt/linkx-backend-update")

BACKUP_ROLES = {
    "postgres": {
        "service": "linkx-postgres-backup.service",
        "timer": "linkx-postgres-backup.timer",
        "backup_root": Path("/opt/linkx-backups/postgres"),
        "patterns": ["*.dump"],
        "checksum_patterns": ["*.sha256"],
        "scripts": [
            "service_factory/services/linkx-control-data/scripts/backup-postgres.sh",
            "service_factory/services/linkx-control-data/scripts/restore-postgres.sh",
            "service_factory/scripts/prune-backups.sh",
            "service_factory/scripts/sync-backups-offhost.sh",
        ],
        "units": [
            "service_factory/services/linkx-control-data/deploy/systemd/linkx-postgres-backup.service",
            "service_factory/services/linkx-control-data/deploy/systemd/linkx-postgres-backup.timer",
        ],
        "restore_note": "restore drill target should be isolated DB linkx_restore_test",
    },
    "artifacts": {
        "service": "linkx-artifacts-backup.service",
        "timer": "linkx-artifacts-backup.timer",
        "backup_root": Path("/opt/linkx-backups/artifacts"),
        "patterns": ["*.tar.gz"],
        "checksum_patterns": ["*.sha256"],
        "scripts": [
            "service_factory/scripts/backup-artifacts.sh",
            "service_factory/scripts/restore-artifacts-to-dir.sh",
            "service_factory/scripts/prune-backups.sh",
            "service_factory/scripts/sync-backups-offhost.sh",
        ],
        "units": [
            "service_factory/deploy/systemd/linkx-artifacts-backup.service",
            "service_factory/deploy/systemd/linkx-artifacts-backup.timer",
        ],
        "restore_note": "restore drill target must be an isolated empty directory",
    },
    "neo4j": {
        "service": "linkx-neo4j-backup.service",
        "timer": "linkx-neo4j-backup.timer",
        "backup_root": Path("/opt/linkx-backups/neo4j"),
        "patterns": ["*/*.dump", "*.dump"],
        "checksum_patterns": ["*/SHA256SUMS", "SHA256SUMS"],
        "scripts": [
            "service_factory/services/linkx-graph-maintenance/scripts/backup-neo4j-offline.sh",
            "service_factory/scripts/sync-backups-offhost.sh",
        ],
        "units": [
            "service_factory/services/linkx-graph-maintenance/deploy/systemd/linkx-neo4j-backup.service",
            "service_factory/services/linkx-graph-maintenance/deploy/systemd/linkx-neo4j-backup.timer",
        ],
        "restore_note": "repeat isolated restore after a representative non-empty graph exists",
    },
}


class Reporter:
    def __init__(self) -> None:
        self.failures = 0
        self.warnings = 0

    def pass_(self, message: str) -> None:
        print(f"PASS: {message}")

    def warn(self, message: str) -> None:
        self.warnings += 1
        print(f"WARN: {message}")

    def fail(self, message: str) -> None:
        self.failures += 1
        print(f"FAIL: {message}")


def run(command: list[str], timeout: int = 15) -> subprocess.CompletedProcess[str] | None:
    try:
        return subprocess.run(command, check=False, capture_output=True, text=True, timeout=timeout)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None


def check_repo_files(reporter: Reporter, config: dict[str, object]) -> None:
    for relative in config["scripts"] + config["units"]:  # type: ignore[operator]
        path = REPO_ROOT / relative
        if path.exists():
            reporter.pass_(f"repo file exists: {path}")
        else:
            reporter.fail(f"repo file missing: {path}")


def check_bash_syntax(reporter: Reporter, config: dict[str, object]) -> None:
    for relative in config["scripts"]:  # type: ignore[index]
        path = REPO_ROOT / relative
        if not path.exists():
            continue
        result = run(["bash", "-n", str(path)])
        if result and result.returncode == 0:
            reporter.pass_(f"bash syntax ok: {path}")
        else:
            detail = (result.stderr if result else "bash unavailable").strip()
            reporter.fail(f"bash syntax failed: {path} {detail}".strip())


def check_systemd_timer(reporter: Reporter, service: str, timer: str) -> None:
    for unit in (service, timer):
        path = Path("/etc/systemd/system") / unit
        if path.exists():
            reporter.pass_(f"systemd unit installed: {path}")
        else:
            reporter.fail(f"systemd unit not installed: {path}")

    enabled = run(["systemctl", "is-enabled", timer])
    if enabled and enabled.returncode == 0:
        reporter.pass_(f"timer enabled: {timer}")
    else:
        detail = (enabled.stdout + enabled.stderr if enabled else "").strip()
        reporter.fail(f"timer is not enabled: {timer} {detail}".strip())

    active = run(["systemctl", "is-active", timer])
    if active and active.returncode == 0 and active.stdout.strip() == "active":
        reporter.pass_(f"timer active: {timer}")
    else:
        detail = (active.stdout + active.stderr if active else "").strip()
        reporter.fail(f"timer is not active: {timer} {detail}".strip())

    listed = run(["systemctl", "list-timers", "--all", timer])
    if listed and timer in listed.stdout:
        reporter.pass_(f"timer appears in list-timers: {timer}")
    else:
        reporter.warn(f"timer not visible in list-timers output yet: {timer}")


def newest_matching(root: Path, patterns: list[str]) -> Path | None:
    matches: list[Path] = []
    for pattern in patterns:
        matches.extend(Path(p) for p in glob.glob(str(root / pattern)))
    if not matches:
        return None
    return max(matches, key=lambda path: path.stat().st_mtime)


def check_latest_backup(reporter: Reporter, config: dict[str, object], max_age_hours: int) -> None:
    root = config["backup_root"]  # type: ignore[assignment]
    assert isinstance(root, Path)
    if root.exists():
        reporter.pass_(f"backup root exists: {root}")
    else:
        reporter.fail(f"backup root missing: {root}")
        return

    latest = newest_matching(root, config["patterns"])  # type: ignore[arg-type]
    if not latest:
        reporter.fail(f"no local backup files found under {root}")
        return

    age_hours = (datetime.now(timezone.utc).timestamp() - latest.stat().st_mtime) / 3600
    if age_hours <= max_age_hours:
        reporter.pass_(f"latest backup is recent: {latest} age_hours={age_hours:.1f}")
    else:
        reporter.warn(f"latest backup is older than {max_age_hours}h: {latest} age_hours={age_hours:.1f}")

    if newest_matching(root, config["checksum_patterns"]):  # type: ignore[arg-type]
        reporter.pass_(f"checksum evidence exists under {root}")
    else:
        reporter.fail(f"checksum evidence missing under {root}")


def check_offhost_setting(reporter: Reporter, service: str) -> None:
    result = run(["systemctl", "show", service, "--property=Environment"])
    environment = result.stdout if result else ""
    if "LINKX_BACKUP_OFFHOST_TARGET=" in environment:
        reporter.pass_(f"off-host sync target configured for {service}")
    else:
        reporter.warn(f"LINKX_BACKUP_OFFHOST_TARGET is not configured for {service}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", choices=sorted(BACKUP_ROLES), required=True)
    parser.add_argument("--max-age-hours", type=int, default=36)
    args = parser.parse_args()

    config = BACKUP_ROLES[args.role]
    reporter = Reporter()
    print(f"LinkX backup automation verification role={args.role}")

    check_repo_files(reporter, config)
    check_bash_syntax(reporter, config)
    check_systemd_timer(reporter, config["service"], config["timer"])  # type: ignore[arg-type]
    check_latest_backup(reporter, config, args.max_age_hours)
    check_offhost_setting(reporter, config["service"])  # type: ignore[arg-type]
    reporter.warn(config["restore_note"])  # type: ignore[arg-type]

    print(f"summary: failures={reporter.failures} warnings={reporter.warnings}")
    return 1 if reporter.failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
