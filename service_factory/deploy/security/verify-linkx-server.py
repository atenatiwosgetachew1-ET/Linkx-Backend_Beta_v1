#!/usr/bin/env python3
"""Verify deployed LinkX server security posture for drift.

The checks are intentionally operational: they validate the live files,
services, ports, and security markers that can drift after manual deploys.
"""

from __future__ import annotations

import argparse
import socket
import subprocess
from pathlib import Path
from urllib.parse import unquote, urlparse
from urllib.request import Request, urlopen


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


def read_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw in path.read_text(errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def run_command(
    command: list[str],
    timeout: int = 10,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess[str] | None:
    try:
        return subprocess.run(command, check=False, capture_output=True, text=True, timeout=timeout, cwd=cwd)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None


def check_path(reporter: Reporter, path: Path, description: str, *, directory: bool = False) -> None:
    exists = path.is_dir() if directory else path.exists()
    if exists:
        reporter.pass_(f"{description} exists: {path}")
    else:
        reporter.fail(f"{description} missing: {path}")


def check_systemd_active(reporter: Reporter, unit: str) -> None:
    result = run_command(["systemctl", "is-active", unit])
    if result and result.returncode == 0 and result.stdout.strip() == "active":
        reporter.pass_(f"systemd unit active: {unit}")
    else:
        detail = ""
        if result:
            detail = (result.stdout + result.stderr).strip()
        reporter.fail(f"systemd unit is not active: {unit} {detail}".strip())


def check_http_status(reporter: Reporter, url: str, expected: set[int], *, host: str | None = None) -> None:
    headers = {"Host": host} if host else {}
    request = Request(url, headers=headers)
    try:
        with urlopen(request, timeout=5) as response:
            status = response.status
    except Exception as exc:  # noqa: BLE001 - reports operational drift clearly.
        status = getattr(exc, "code", None)
        if status is None:
            reporter.fail(f"HTTP check failed for {url}: {exc}")
            return
    if status in expected:
        host_suffix = f" host={host}" if host else ""
        reporter.pass_(f"HTTP check {url}{host_suffix} returned {status}")
    else:
        reporter.fail(f"HTTP check {url} returned {status}, expected one of {sorted(expected)}")


def redis_request(host: str, port: int, payload: bytes) -> str:
    with socket.create_connection((host, port), timeout=5) as sock:
        sock.sendall(payload)
        return sock.recv(4096).decode("utf-8", errors="replace").strip()


def redis_auth_then_ping(host: str, port: int, password: str) -> tuple[str, str]:
    with socket.create_connection((host, port), timeout=5) as sock:
        sock.sendall(redis_auth_payload(password))
        auth = sock.recv(4096).decode("utf-8", errors="replace").strip()
        sock.sendall(b"*1\r\n$4\r\nPING\r\n")
        pong = sock.recv(4096).decode("utf-8", errors="replace").strip()
        return auth, pong


def redis_auth_payload(password: str) -> bytes:
    encoded = password.encode()
    return b"*2\r\n$4\r\nAUTH\r\n$" + str(len(encoded)).encode() + b"\r\n" + encoded + b"\r\n"


def check_redis_auth(reporter: Reporter, host: str, port: int, password: str) -> None:
    try:
        unauthenticated = redis_request(host, port, b"*1\r\n$4\r\nPING\r\n")
    except OSError as exc:
        reporter.fail(f"Redis connection failed {host}:{port}: {exc}")
        return
    if unauthenticated.startswith("-NOAUTH"):
        reporter.pass_("Redis rejects unauthenticated PING")
    else:
        reporter.fail(f"Redis unauthenticated PING did not return NOAUTH: {unauthenticated}")

    try:
        auth, pong = redis_auth_then_ping(host, port, password)
    except OSError as exc:
        reporter.fail(f"Redis authenticated check failed {host}:{port}: {exc}")
        return

    if auth.startswith("+OK"):
        reporter.pass_("Redis accepts configured password")
    else:
        reporter.fail(f"Redis AUTH failed with configured password: {auth}")
    if pong == "+PONG":
        reporter.pass_("Redis authenticated PING returns PONG")
    else:
        reporter.fail(f"Redis authenticated PING did not return PONG: {pong}")


def redis_from_url(url: str) -> tuple[str, int, str] | None:
    parsed = urlparse(url)
    if parsed.scheme not in {"redis", "rediss"}:
        return None
    password = unquote(parsed.password or "")
    return parsed.hostname or "127.0.0.1", parsed.port or 6379, password


def check_env_redis_url(reporter: Reporter, env: dict[str, str], env_path: Path) -> None:
    redis_url = env.get("LINKX_REDIS_URL", "")
    parsed = redis_from_url(redis_url)
    if not parsed:
        reporter.fail(f"{env_path}: LINKX_REDIS_URL is missing or invalid")
        return
    host, port, password = parsed
    if password:
        reporter.pass_(f"{env_path}: LINKX_REDIS_URL includes a password")
    else:
        reporter.fail(f"{env_path}: LINKX_REDIS_URL does not include a password")
        return
    check_redis_auth(reporter, host, port, password)


def check_api_code_markers(reporter: Reporter, src: Path) -> None:
    expected = {
        "auth/tokens.py": ["jti", "is_token_jti_revoked"],
        "auth/routes.py": ["token_invalidated", "_revoke_current_bearer_token"],
        "api/ai_service.py": ["ai:session:read", "ai:artifact:read", "ai:cleanup:read", "ai:graph:metadata:read"],
        "api/STR_link_analysis.py": ["redact_value"],
    }
    for relative, markers in expected.items():
        path = src / relative
        if not path.exists():
            reporter.fail(f"API source file missing: {path}")
            continue
        content = path.read_text(errors="replace")
        missing = [marker for marker in markers if marker not in content]
        if missing:
            reporter.fail(f"{path} missing security markers: {', '.join(missing)}")
        else:
            reporter.pass_(f"{path} contains expected security markers")


def check_nginx_gateway(reporter: Reporter, expected_auth_status: set[int] | None = None) -> None:
    result = run_command(["nginx", "-t"])
    if result and result.returncode == 0:
        reporter.pass_("nginx configuration test passes")
    else:
        reporter.fail("nginx configuration test failed or nginx is unavailable")

    enabled = Path("/etc/nginx/sites-enabled/linkx-api-gateway.conf")
    available = Path("/etc/nginx/sites-available/linkx-api-gateway.conf")
    if enabled.exists() or available.exists():
        reporter.pass_("linkx API gateway nginx config is installed")
    else:
        reporter.warn("linkx API gateway nginx config was not found in sites-enabled/sites-available")

    expected_auth = expected_auth_status or {401}
    check_http_status(reporter, "http://127.0.0.1/db/health", {200}, host="linkx-api.local")
    check_http_status(reporter, "http://127.0.0.1/auth/me", expected_auth, host="linkx-api.local")


def check_otel_metrics_port(reporter: Reporter, port: int = 8889) -> None:
    success = False
    for host in ("127.0.0.1", "0.0.0.0"):
        try:
            with socket.create_connection((host, port), timeout=2):
                success = True
                break
        except OSError:
            continue

    if success:
        reporter.pass_(f"OpenTelemetry metrics port {port} is active and listening")
    else:
        reporter.warn(f"OpenTelemetry metrics port {port} is not listening locally yet (Phase 2 instrumentation pending)")


def verify_api(reporter: Reporter) -> None:
    root = Path("/opt/linkx-backend-api")
    env_path = root / ".env"
    src = root / "src"
    check_path(reporter, root, "API deploy root", directory=True)
    check_path(reporter, env_path, "API env file")
    check_path(reporter, src, "API source root", directory=True)
    check_systemd_active(reporter, "linkx-api")

    env = read_env(env_path)
    if env.get("LINKX_AI_ALLOW_GLOBAL_READ", "").lower() == "false":
        reporter.pass_("AI global read is disabled")
    else:
        reporter.fail("LINKX_AI_ALLOW_GLOBAL_READ must be false on API server")
    check_env_redis_url(reporter, env, env_path)
    auto_admin = env.get("LINKX_AUTO_LOGIN_ADMIN", "true").lower() in ("1", "true", "yes")
    expected_auth_status = {200} if auto_admin else {401}
    check_http_status(reporter, "http://127.0.0.1:8000/db/health", {200})
    check_http_status(reporter, "http://127.0.0.1:8000/auth/me", expected_auth_status)
    check_nginx_gateway(reporter, expected_auth_status=expected_auth_status)
    check_otel_metrics_port(reporter)


def verify_control_data(reporter: Reporter) -> None:
    root = Path("/opt/linkx-control-data")
    env_path = root / ".env"
    compose = root / "docker-compose.yml"
    check_path(reporter, root, "Control-data deploy root", directory=True)
    check_path(reporter, env_path, "Control-data env file")
    check_path(reporter, compose, "Control-data compose file")

    env = read_env(env_path)
    host = env.get("REDIS_BIND_ADDR", "127.0.0.1")
    port = int(env.get("REDIS_PORT", "6379"))
    password = env.get("REDIS_PASSWORD", "")
    if not password:
        reporter.fail(f"{env_path}: REDIS_PASSWORD is missing")
    else:
        reporter.pass_(f"{env_path}: REDIS_PASSWORD is configured")
        check_redis_auth(reporter, host, port, password)

    compose_text = compose.read_text(errors="replace") if compose.exists() else ""
    for marker in ["--requirepass", "REDIS_BIND_ADDR", "redis-cli -a"]:
        if marker in compose_text:
            reporter.pass_(f"docker-compose.yml contains Redis marker: {marker}")
        else:
            reporter.fail(f"docker-compose.yml missing Redis marker: {marker}")

    docker_ps = run_command(["docker", "compose", "ps"], timeout=15, cwd=root)
    if docker_ps and docker_ps.returncode == 0 and "linkx-redis" in docker_ps.stdout:
        reporter.pass_("docker compose ps shows linkx-redis")
    else:
        reporter.warn("docker compose ps did not confirm linkx-redis; check Docker manually")
    check_otel_metrics_port(reporter)


def verify_worker(reporter: Reporter) -> None:
    root = Path("/opt/linkx-worker")
    env_path = root / ".env"
    check_path(reporter, root, "Worker deploy root", directory=True)
    check_path(reporter, env_path, "Worker env file")
    check_systemd_active(reporter, "linkx-worker")
    check_env_redis_url(reporter, read_env(env_path), env_path)
    check_otel_metrics_port(reporter)


def verify_graph_maintenance(reporter: Reporter) -> None:
    root = Path("/opt/Linkx_xmaintenance")
    env_path = root / ".env"
    tasks = root / "src/linkx_xcleanup/tasks.py"
    check_path(reporter, root, "Graph maintenance deploy root", directory=True)
    check_path(reporter, env_path, "Graph maintenance env file")
    check_path(reporter, tasks, "Cleanup tasks source file")
    check_systemd_active(reporter, "linkx-xcleanup-worker")
    check_systemd_active(reporter, "linkx-xcleanup-scheduler")
    check_env_redis_url(reporter, read_env(env_path), env_path)

    if tasks.exists():
        content = tasks.read_text(errors="replace")
        if "Neo4j credential source" in content and "creds=" not in content:
            reporter.pass_("Cleanup Neo4j credential-source logging is metadata-only")
        else:
            reporter.fail("Cleanup Neo4j credential-source logging may expose credential-shaped payloads")
    check_otel_metrics_port(reporter)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", choices=["api", "control-data", "worker", "graph-maintenance"], required=True)
    args = parser.parse_args()

    reporter = Reporter()
    print(f"LinkX security drift verification role={args.role} host={socket.gethostname()}")
    if args.role == "api":
        verify_api(reporter)
    elif args.role == "control-data":
        verify_control_data(reporter)
    elif args.role == "worker":
        verify_worker(reporter)
    elif args.role == "graph-maintenance":
        verify_graph_maintenance(reporter)

    print(f"summary: failures={reporter.failures} warnings={reporter.warnings}")
    return 1 if reporter.failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
