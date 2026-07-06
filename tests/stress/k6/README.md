# LinkX K6 Stress Starter

This folder contains a minimal concurrent-load simulation for Server 1.

Current script:

- `concurrent_api_mix.js`

It exercises:

- `GET /db/health`
- `POST /auth/login`
- `POST /init`

The script is intentionally narrow so you can validate concurrency effects on the API/control plane before adding heavier worker-backed routes.

## Required Inputs

Set these environment variables before running:

- `BASE_URL`
- `LINKX_USERNAME`
- `LINKX_PASSWORD`

Optional:

- `K6_VUS`
- `K6_DURATION`
- `THINK_TIME_MS`
- `ENABLE_INIT`

## Example Run

```bash
k6 run \
  -e BASE_URL=http://172.27.23.95:8000 \
  -e LINKX_USERNAME=<username> \
  -e LINKX_PASSWORD=<password> \
  -e K6_VUS=20 \
  -e K6_DURATION=5m \
  tests/stress/k6/concurrent_api_mix.js
```

## What To Watch

In Prometheus or Grafana, watch:

- `node_load1`
- `100 * (1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes))`
- `rate(node_network_receive_bytes_total[5m])`
- `rate(node_network_transmit_bytes_total[5m])`

Once Prometheus can scrape the LinkX API metrics directly, add:

- `rate(linkx_api_requests_total[5m])`
- `linkx_api_requests_in_progress`
- `histogram_quantile(0.95, sum(rate(linkx_api_request_duration_seconds_bucket[5m])) by (le, route))`

## Suggested Test Levels

Start small and step up:

1. `K6_VUS=5`, `K6_DURATION=1m`
2. `K6_VUS=20`, `K6_DURATION=5m`
3. `K6_VUS=50`, `K6_DURATION=10m`

If login rate limiting becomes the dominant signal, either:

- lower VUs, or
- use a longer think time, or
- split health-only and authenticated tests into separate runs.
