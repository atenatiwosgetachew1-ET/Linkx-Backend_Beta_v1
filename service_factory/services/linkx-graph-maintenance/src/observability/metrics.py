import os
import time

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest


CLEANUP_TASK_COUNT = Counter(
    "linkx_cleanup_tasks_total",
    "Total cleanup tasks executed.",
    ("task_type", "status"),
)

_metrics_server_started = False


def start_otel_metrics_server(default_port=8889):
    global _metrics_server_started
    if _metrics_server_started:
        return True

    port_str = os.getenv("LINKX_OTEL_METRICS_PORT", os.getenv("LINKX_METRICS_PORT", str(default_port)))
    host = os.getenv("LINKX_OTEL_METRICS_HOST", os.getenv("LINKX_METRICS_HOST", "0.0.0.0"))
    try:
        port = int(port_str)
    except (ValueError, TypeError):
        port = default_port

    try:
        from prometheus_client import start_http_server

        start_http_server(port, addr=host)
        _metrics_server_started = True
        print(f"OpenTelemetry cleanup metrics server listening on {host}:{port}", flush=True)
        return True
    except Exception as exc:
        print(f"Failed to start OpenTelemetry cleanup metrics server on {host}:{port}: {exc}", flush=True)
        return False
