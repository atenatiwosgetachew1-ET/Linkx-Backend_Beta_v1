#!/usr/bin/env python3
"""Validate LinkX transport-security-related environment settings.

Default mode fails only for unsafe external Parent/JWKS HTTP configuration.
Use --strict-east-west to also fail plaintext Postgres, Redis, and remote Neo4j URLs.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse

TRUTHY = {"1", "true", "yes", "on"}
PARENT_URL_KEYS = {
    "LINKX_PARENT_AUTH_BASE_URL",
    "LINKX_PARENT_JWKS_URL",
    "LINKX_PARENT_JWT_JWKS_URL",
    "LINKX_PARENT_SSO_TOKEN_URL",
    "LINKX_PARENT_SSO_USERINFO_URL",
    "LINKX_PARENT_SSO_REVOKE_URL",
}
POSTGRES_KEYS = {"DATABASE_URL", "LINKX_POSTGRES_DSN"}
REDIS_KEYS = {"LINKX_REDIS_URL"}
NEO4J_KEYS = {"LINKX_NEO4J_URL", "LINKX_CLEANUP_NEO4J_URL"}


def parse_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def is_loopback_host(hostname: str | None) -> bool:
    return (hostname or "").lower() in {"localhost", "127.0.0.1", "::1"}


def postgres_uses_tls(url: str) -> bool:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    sslmode = (query.get("sslmode") or [""])[0].lower()
    return sslmode in {"require", "verify-ca", "verify-full"}


def validate(path: Path, strict_east_west: bool) -> tuple[list[str], list[str]]:
    values = parse_env(path)
    errors: list[str] = []
    warnings: list[str] = []
    allow_parent_http = values.get("LINKX_PARENT_AUTH_ALLOW_HTTP", "").lower() in TRUTHY

    for key in sorted(PARENT_URL_KEYS):
        value = values.get(key)
        if not value:
            continue
        parsed = urlparse(value)
        if parsed.scheme != "https" and not allow_parent_http:
            errors.append(f"{path}: {key} must use https:// unless LINKX_PARENT_AUTH_ALLOW_HTTP=true")
        elif parsed.scheme == "http" and allow_parent_http:
            warnings.append(f"{path}: {key} uses http:// by explicit exception LINKX_PARENT_AUTH_ALLOW_HTTP=true")

    for key in sorted(POSTGRES_KEYS):
        value = values.get(key)
        if value and value.startswith("postgresql://") and not postgres_uses_tls(value):
            message = f"{path}: {key} does not specify sslmode=require/verify-ca/verify-full"
            (errors if strict_east_west else warnings).append(message)

    for key in sorted(REDIS_KEYS):
        value = values.get(key)
        if value and value.startswith("redis://"):
            message = f"{path}: {key} uses redis://; use rediss:// if Redis TLS is available"
            (errors if strict_east_west else warnings).append(message)

    for key in sorted(NEO4J_KEYS):
        value = values.get(key)
        if not value:
            continue
        parsed = urlparse(value)
        if parsed.scheme == "neo4j://" and not is_loopback_host(parsed.hostname):
            message = f"{path}: {key} uses remote neo4j://; use neo4j+s:// or neo4j+ssc:// if Neo4j TLS is available"
            (errors if strict_east_west else warnings).append(message)
        elif parsed.scheme == "bolt://" and not is_loopback_host(parsed.hostname):
            message = f"{path}: {key} uses remote bolt://; use bolt+s:// or bolt+ssc:// if Neo4j TLS is available"
            (errors if strict_east_west else warnings).append(message)

    return errors, warnings


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strict-east-west", action="store_true", help="fail plaintext internal database/cache/graph URLs")
    parser.add_argument("env_files", nargs="+", type=Path)
    args = parser.parse_args()

    all_errors: list[str] = []
    all_warnings: list[str] = []
    for path in args.env_files:
        errors, warnings = validate(path, args.strict_east_west)
        all_errors.extend(errors)
        all_warnings.extend(warnings)

    for warning in all_warnings:
        print(f"WARN: {warning}", file=sys.stderr)
    for error in all_errors:
        print(f"ERROR: {error}", file=sys.stderr)

    if all_errors:
        return 1
    print("transport security validation passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
