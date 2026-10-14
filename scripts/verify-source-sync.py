#!/usr/bin/env python3
"""Verify that all 4 service-package copies of batch_manager/ are identical.

Compares the canonical batch_manager/ tree in the repo root against
the copies inside each service_factory/services/*/src/batch_manager/.

Exit code 0 = all copies match.
Exit code 1 = drift detected (details printed).

Usage:
    python3 scripts/verify-source-sync.py
"""

import os
import sys
import hashlib
import argparse

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CANONICAL = os.path.join(REPO_ROOT, "batch_manager")

SERVICE_COPIES = [
    os.path.join(REPO_ROOT, "service_factory", "services", "linkx-api", "src", "batch_manager"),
    os.path.join(REPO_ROOT, "service_factory", "services", "linkx-worker", "src", "batch_manager"),
    os.path.join(REPO_ROOT, "service_factory", "services", "Linkx_xmaintenance", "src", "batch_manager"),
]

IGNORE_PATTERNS = {"__pycache__", ".pyc", ".pyo"}


def _should_ignore(path):
    for pat in IGNORE_PATTERNS:
        if pat in path:
            return True
    return False


def _file_hash(filepath):
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _collect_files(root):
    """Return {relative_path: sha256_hash} for all files under root."""
    result = {}
    for dirpath, dirnames, filenames in os.walk(root):
        # Filter out ignored dirs in-place
        dirnames[:] = [d for d in dirnames if not _should_ignore(d)]
        for fname in filenames:
            if _should_ignore(fname):
                continue
            full = os.path.join(dirpath, fname)
            rel = os.path.relpath(full, root)
            result[rel] = _file_hash(full)
    return result


def main():
    parser = argparse.ArgumentParser(description="Verify batch_manager source sync across service packages")
    parser.add_argument("--fix", action="store_true", help="Copy canonical files to out-of-sync service packages")
    args = parser.parse_args()

    if not os.path.isdir(CANONICAL):
        print(f"FAIL: Canonical batch_manager/ not found at {CANONICAL}")
        sys.exit(1)

    canonical_files = _collect_files(CANONICAL)
    print(f"Canonical batch_manager/: {len(canonical_files)} files")

    total_drift = 0
    drift_details = []

    for copy_path in SERVICE_COPIES:
        service_name = copy_path.split("services/")[1].split("/")[0] if "services/" in copy_path else copy_path
        if not os.path.isdir(copy_path):
            print(f"  SKIP: {service_name} — batch_manager/ not found at {copy_path}")
            continue

        copy_files = _collect_files(copy_path)

        # Files in canonical but missing in copy
        missing = set(canonical_files.keys()) - set(copy_files.keys())
        # Files in copy but not in canonical
        extra = set(copy_files.keys()) - set(canonical_files.keys())
        # Files present in both but with different content
        changed = {
            f for f in (set(canonical_files.keys()) & set(copy_files.keys()))
            if canonical_files[f] != copy_files[f]
        }

        if not missing and not extra and not changed:
            print(f"  PASS: {service_name} — all {len(copy_files)} files match")
        else:
            count = len(missing) + len(extra) + len(changed)
            total_drift += count
            print(f"  DRIFT: {service_name} — {count} differences")
            for f in sorted(missing):
                print(f"    MISSING: {f}")
                drift_details.append((copy_path, "missing", f))
            for f in sorted(extra):
                print(f"    EXTRA:   {f}")
                drift_details.append((copy_path, "extra", f))
            for f in sorted(changed):
                print(f"    CHANGED: {f}")
                drift_details.append((copy_path, "changed", f))

    if total_drift == 0:
        print(f"\nsummary: all {len(SERVICE_COPIES)} service copies are in sync")
        sys.exit(0)
    else:
        print(f"\nsummary: {total_drift} file(s) out of sync across {len(SERVICE_COPIES)} service packages")
        if args.fix:
            import shutil
            fixed = 0
            for copy_path, kind, rel_file in drift_details:
                src = os.path.join(CANONICAL, rel_file)
                dst = os.path.join(copy_path, rel_file)
                if kind == "missing" or kind == "changed":
                    os.makedirs(os.path.dirname(dst), exist_ok=True)
                    shutil.copy2(src, dst)
                    print(f"  FIXED: {dst}")
                    fixed += 1
                elif kind == "extra":
                    print(f"  SKIPPED (extra file): {dst}")
            print(f"\nFixed {fixed} file(s). Re-run without --fix to verify.")
        else:
            print("Run with --fix to copy canonical files to drifted packages.")
        sys.exit(1)


if __name__ == "__main__":
    main()
