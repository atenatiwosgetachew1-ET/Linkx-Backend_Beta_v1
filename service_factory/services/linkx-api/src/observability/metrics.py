import os
import time

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest


REQUEST_COUNT = Counter(
    "linkx_api_requests_total",
    "Total HTTP requests handled by the LinkX API.",
    ("method", "route", "status_code"),
)

REQUEST_LATENCY = Histogram(
    "linkx_api_request_duration_seconds",
    "HTTP request latency for the LinkX API.",
    ("method", "route"),
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
)

REQUESTS_IN_PROGRESS = Gauge(
    "linkx_api_requests_in_progress",
    "Current in-flight HTTP requests handled by the LinkX API.",
    ("method", "route"),
)

JOB_ENQUEUE_COUNT = Counter(
    "linkx_api_jobs_enqueued_total",
    "Total worker jobs enqueued by the LinkX API.",
    ("queue", "job_type"),
)


def metrics_enabled():
    return str(os.getenv("LINKX_ENABLE_METRICS", "false")).lower() in {"1", "true", "yes", "on"}


def metrics_token():
    return str(os.getenv("LINKX_METRICS_TOKEN", "")).strip()


def normalize_route(request):
    rule = getattr(request, "url_rule", None)
    if rule and getattr(rule, "rule", None):
        return rule.rule
    path = str(getattr(request, "path", "") or "").strip()
    return path or "unknown"


def should_track_request(request):
    return metrics_enabled() and normalize_route(request) != "/metrics"


def request_started():
    return time.perf_counter()


def observe_request(method, route, status_code, started_at):
    elapsed = max(0.0, time.perf_counter() - float(started_at or 0.0))
    REQUEST_COUNT.labels(method=method, route=route, status_code=str(status_code)).inc()
    REQUEST_LATENCY.labels(method=method, route=route).observe(elapsed)


def request_in_progress_inc(method, route):
    REQUESTS_IN_PROGRESS.labels(method=method, route=route).inc()


def request_in_progress_dec(method, route):
    REQUESTS_IN_PROGRESS.labels(method=method, route=route).dec()


def record_job_enqueue(queue_name, job_type):
    JOB_ENQUEUE_COUNT.labels(queue=str(queue_name), job_type=str(job_type)).inc()


def metrics_response():
    payload = generate_latest()
    return payload, 200, {"Content-Type": CONTENT_TYPE_LATEST}


_metrics_server_started = False


def start_otel_metrics_server(default_port=8889):
    global _metrics_server_started
    if _metrics_server_started:
        return True

    port_str = os.getenv("LINKX_OTEL_METRICS_PORT", os.getenv("LINKX_METRICS_PORT", str(default_port)))
    try:
        port = int(port_str)
    except (ValueError, TypeError):
        port = default_port

    try:
        from prometheus_client import start_http_server

        start_http_server(port)
        _metrics_server_started = True
        print(f"OpenTelemetry metrics server listening on port {port}", flush=True)
        return True
    except Exception as exc:
        print(f"Failed to start OpenTelemetry metrics server on port {port}: {exc}", flush=True)
        return False

