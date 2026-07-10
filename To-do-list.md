# LinkX Worker Split To-Do

## Goal

Move all heavy search and dataframe work to the worker server, keep the API thin, and preserve parquet as the shared dataframe artifact format.

## Implementation Checklist

### 1. Make the worker Spark-ready - Done

- Worker venv provides Spark/PySpark `3.5.5`.
- Set `JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64`.
- Set `PYSPARK_PYTHON=/opt/linkx-worker/.venv/bin/python`.
- Set `PYSPARK_DRIVER_PYTHON=/opt/linkx-worker/.venv/bin/python`.
- Add `/opt/linkx-worker/.venv/bin` and Java `bin` to worker `PATH`.
- Verified `/opt/linkx-worker/.venv/bin/spark-submit --version` works.
- Verified `/opt/linkx-worker/.venv/bin/pyspark --version` works.
- Verified worker-side Spark smoke test returns Spark `3.5.5` and `s.range(1).count() == 1`.
- Verified worker Spark can bind `fs.defaultFS` to `hdfs://172.27.23.43:9000`.
- Verified worker Spark can bind Hive Metastore to `thrift://172.27.23.43:9083`.
- Removed duplicate blank `LINKX_ACTIVE_STORAGE_ADDRESS=` from `/opt/linkx-worker/.env` on `node-21`.

### 2. Keep the API as intake only

- Keep upload acceptance and validation on the API.
- Keep session creation and request routing on the API.
- Remove heavy raw file parsing, search execution, and dataframe creation from the API path.

### 3. Move `/live_batch_files` heavy work to the worker

- Replace direct API execution with worker job enqueueing.
- Return `202 Accepted` with `job_id` and `poll_url`.
- Use the same worker-job pattern already used by `stream` and graph flows.

### 4. Route work by queue

- Use the `search` queue for raw search, strict search, and fuzzy search jobs.
- Use the `dataframe` queue for `load_sourceData` and `create_DF`.
- Keep `analysis` for STR or related analysis work.

### 5. Move raw file handling to the worker

- Move raw file search/listing off the API.
- Move raw uploaded file loading off the API.
- Keep the API from touching file contents directly.

### 6. Keep pandas for Excel

- Keep `.xlsx` and `.xls` on pandas first.
- Do not force Excel into Spark unless scaling requires it later.
- Prefer pandas for small or messy upload files.

### 7. Keep Spark for heavy jobs

- Use Spark for HDFS.
- Use Spark for Hive.
- Use Spark for large Elastic fallback flows.
- Use Spark for parquet reads and mixed-source merges when needed.

### 8. Standardize dataframe output

- Save every final dataframe artifact as parquet.
- Return the dataframe path, row count, columns, and source manifest.
- Preserve the `use_spark` flag in result metadata.

### 9. Make mixed merges explicit

- Keep all-pandas merges in pandas.
- Keep all-Spark merges in Spark.
- Convert pandas inputs to Spark only when a mixed merge needs it.

### 10. Add guardrail tests

- Add API tests to prove heavy work is enqueued, not run locally.
- Add worker tests for Excel fallback, parquet reads, mixed merges, and raw search.
- Add tests for strict and fuzzy search column resolution.

### 11. Roll out in order

- First: worker Spark installation and environment wiring.
- Second: raw file processing on the worker.
- Third: strict and fuzzy search on the worker.
- Fourth: dataframe creation on the worker.
- Fifth: API cleanup to remove remaining heavy execution paths.

## Operational Rule

- API receives and routes.
- Worker computes and writes artifacts.
- Excel stays pandas-friendly.
- Heavy data stays on the worker.
- Final dataframe artifacts stay parquet.
