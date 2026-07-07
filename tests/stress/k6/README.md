# LinkX K6 Stress Suite

These scripts are intentionally split by behavior so we can tell auth noise, control-plane cost, and heavier search/dataframe work apart.

## Scripts

- `concurrent_api_mix.js`
  - Baseline auth + init loop
  - Good for a quick mixed smoke check
  - Not ideal once login throttling becomes the main signal

- `server1_session_control_plane.js`
  - Logs in once in `setup()` and reuses the token
  - Exercises `/auth/me`, `/auth/verify`, `/workspace/layout`, and `/auth/preferences`
  - Optional write mode can update workspace layout and preferences

- `server1_graph_routes.js`
  - Logs in once in `setup()` and reuses the token and session id
  - Connects Neo4j into the fresh session with `/connect_to_tool` before graph calls
  - Exercises `/graph_link` and `/get_graph`
  - Optional unlink mode can toggle the graph link path

- `server1_str_analysis.js`
  - Logs in once in `setup()` and reuses the token and session id
  - Exercises `/api/STR_link_analysis`
  - Measures the configured search/dataframe/Neo4j path as the server is actually deployed
  - If the search value does not match live data, you may see `Not found!`; that still tells you the route is healthy, but it does not measure the full downstream pipeline

## Required Inputs

Set these for all scripts:

- `BASE_URL`
- `LINKX_USERNAME`
- `LINKX_PASSWORD`

Optional common inputs:

- `K6_VUS`
- `K6_DURATION`
- `THINK_TIME_MS`

Route-specific optional inputs:

- `ENABLE_WRITE` for `server1_session_control_plane.js`
- `ENABLE_UNLINK` for `server1_graph_routes.js`
- `GRAPH_RELATIONSHIP` for `server1_graph_routes.js`
- `NEO4J_URL`, `NEO4J_USERNAME`, `NEO4J_PASSWORD`, `NEO4J_DATABASE` for `server1_graph_routes.js`
- `STR_VALUE`, `STR_DATE`, `STR_PUBLIC_API_KEY`, `ACCEPT_NOT_FOUND` for `server1_str_analysis.js`

## Example Runs

Baseline mixed smoke test:

```bash
k6 run \
  -e BASE_URL=http://172.27.23.95:8000 \
  -e LINKX_USERNAME=<username> \
  -e LINKX_PASSWORD=<password> \
  -e K6_VUS=20 \
  -e K6_DURATION=5m \
  tests/stress/k6/concurrent_api_mix.js
```

Control-plane test:

```bash
k6 run \
  -e BASE_URL=http://172.27.23.95:8000 \
  -e LINKX_USERNAME=<username> \
  -e LINKX_PASSWORD=<password> \
  -e K6_VUS=20 \
  -e K6_DURATION=5m \
  tests/stress/k6/server1_session_control_plane.js
```

Graph route test:

```bash
k6 run \
  -e BASE_URL=http://172.27.23.95:8000 \
  -e LINKX_USERNAME=<username> \
  -e LINKX_PASSWORD=<password> \
  -e NEO4J_URL=bolt://172.27.23.85:7687 \
  -e NEO4J_USERNAME=neo4j \
  -e NEO4J_PASSWORD=<neo4j-password> \
  -e K6_VUS=20 \
  -e K6_DURATION=5m \
  tests/stress/k6/server1_graph_routes.js
```

STR search/dataframe test:

```bash
k6 run \
  -e BASE_URL=http://172.27.23.95:8000 \
  -e LINKX_USERNAME=<username> \
  -e LINKX_PASSWORD=<password> \
  -e STR_VALUE=<known-good-account-number> \
  -e STR_PUBLIC_API_KEY=<if-required> \
  -e K6_VUS=10 \
  -e K6_DURATION=5m \
  tests/stress/k6/server1_str_analysis.js
```

## What To Watch

Watch both response quality and server pressure:

- `http_req_failed`
- `http_req_duration`
- `node_load1`
- CPU busy percentage
- memory used percentage
- network receive/transmit bytes

## Honest Notes

- Do not use `/auth/login` in a hot loop if login throttling is the thing you want to measure separately.
- For the STR route, a `Not found!` response is not the same as a broken endpoint; it only means the search term did not match live data.
- If you want to measure the full dataframe/Neo4j path, use a known-good value from the real dataset and the deployment settings currently live on Server 1.
