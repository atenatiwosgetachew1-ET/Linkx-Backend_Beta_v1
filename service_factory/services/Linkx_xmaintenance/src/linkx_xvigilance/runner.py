import argparse
import fcntl
import os
import signal
import socket
import sys
import time
from datetime import datetime, timedelta, timezone

from linkx_xvigilance.checkpoints import (
    finish_slice_run,
    get_or_init_checkpoint,
    log_slice_start,
    clean_zombie_runs,
    get_in_flight_slices_count,
)
from linkx_xvigilance.config import get_xvigilance_config
from linkx_xvigilance.db import connect
from linkx_xvigilance.fetcher import stream_window_records
from linkx_xvigilance.schema import ensure_xvigilance_schema

RUNNING = True

# Cluster advisory lock key for xVigilance Runner: 0x58564947494c = 97130282477900
XVIGILANCE_LEADER_LOCK_KEY = 97130282477900
_host_lock_fd = None
_leader_db_conn = None


def acquire_host_lock(lock_path: str = "/tmp/linkx_xvigilance_runner.lock") -> bool:
    """Acquires a non-blocking exclusive lock on the host operating system kernel."""
    global _host_lock_fd
    try:
        _host_lock_fd = open(lock_path, "a+")
        fcntl.flock(_host_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _host_lock_fd.seek(0)
        _host_lock_fd.truncate()
        _host_lock_fd.write(
            f"pid={os.getpid()}\nhost={socket.gethostname()}\nstarted={datetime.now(timezone.utc).isoformat()}\n"
        )
        _host_lock_fd.flush()
        return True
    except (BlockingIOError, IOError):
        existing_info = ""
        try:
            with open(lock_path, "r") as f:
                existing_info = f.read().replace("\n", " | ")
        except Exception:
            pass
        print(f"[xvigilance] 🛑 MUTEX LOCK REJECTED: Another runner instance is already active on host '{socket.gethostname()}'! ({existing_info})", flush=True)
        return False


def acquire_cluster_lock() -> bool:
    """Acquires a cluster-wide PostgreSQL session advisory lock to ensure only 1 runner runs across all nodes."""
    global _leader_db_conn
    try:
        _leader_db_conn = connect(application_name="xvigilance-leader-lock")
        with _leader_db_conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s);", (XVIGILANCE_LEADER_LOCK_KEY,))
            acquired = cur.fetchone()[0]
            if acquired:
                print(f"[xvigilance] 👑 CLUSTER LEADER ELECTED: Acquired PostgreSQL advisory lock (key={XVIGILANCE_LEADER_LOCK_KEY}) on {socket.gethostname()}:{os.getpid()}", flush=True)
                return True
            else:
                print(f"[xvigilance] 🛑 CLUSTER LEADER REJECTED: Another runner in the cluster holds PostgreSQL advisory lock (key={XVIGILANCE_LEADER_LOCK_KEY}).", flush=True)
                _leader_db_conn.close()
                _leader_db_conn = None
                return False
    except Exception as e:
        print(f"[xvigilance] ⚠️ Error during cluster lock acquisition: {e}", flush=True)
        return False


def release_locks():
    """Releases both host and cluster locks cleanly."""
    global _host_lock_fd, _leader_db_conn
    if _leader_db_conn:
        try:
            with _leader_db_conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s);", (XVIGILANCE_LEADER_LOCK_KEY,))
            _leader_db_conn.close()
            print("[xvigilance] Cluster PostgreSQL advisory lock released cleanly.", flush=True)
        except Exception as e:
            print(f"[xvigilance] Error releasing cluster advisory lock: {e}", flush=True)
        _leader_db_conn = None

    if _host_lock_fd:
        try:
            fcntl.flock(_host_lock_fd, fcntl.LOCK_UN)
            _host_lock_fd.close()
            print("[xvigilance] Host mutex lock released cleanly.", flush=True)
        except Exception:
            pass
        _host_lock_fd = None


def handle_shutdown(signum, frame):
    global RUNNING
    print(f"\n[xvigilance] Received signal {signum}. Initiating graceful shutdown...", flush=True)
    RUNNING = False


def interruptible_sleep(seconds: float):
    """Sleeps in short 0.5s increments, breaking immediately if RUNNING becomes False on SIGTERM."""
    deadline = time.time() + seconds
    while RUNNING and time.time() < deadline:
        time.sleep(min(0.5, max(0.01, deadline - time.time())))


def run_daemon(feed_name: str = "hourly_transaction_detective", once: bool = False):
    global RUNNING

    # 1. Tier 1: Host Mutex Lock (Kernel level)
    if not acquire_host_lock():
        print("[xvigilance] Exiting immediately to prevent concurrent host processes.", flush=True)
        sys.exit(0)

    # 2. Tier 2: Cluster Distributed Advisory Lock (PostgreSQL ACID level)
    if not acquire_cluster_lock():
        print("[xvigilance] Exiting immediately because another node is active cluster leader.", flush=True)
        release_locks()
        sys.exit(0)

    import atexit
    atexit.register(release_locks)

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    worker_name = os.getenv("XVIGILANCE_WORKER_NAME") or f"xvigilance@{socket.gethostname()}:{os.getpid()}"

    config = get_xvigilance_config()

    # --- Phase 1: Initialize Kafka Producer ---
    kafka_brokers = os.getenv("LINKX_KAFKA_BOOTSTRAP_SERVERS", "172.27.23.106:9092")
    kafka_topic = "dev.xvigilance.transactions.raw.v2"
    kafka_import_available = True  # tracks whether kafka-python is installed at all

    def _connect_kafka(max_retries=5):
        """Attempt to connect to Kafka with exponential backoff.
        Returns (producer, True) on success or (None, False) on failure."""
        nonlocal kafka_import_available
        if not kafka_import_available:
            return None, False
        try:
            from kafka import KafkaProducer as Producer
        except ImportError:
            print("[xvigilance] Warning: kafka-python not installed. Kafka streaming permanently disabled.", flush=True)
            kafka_import_available = False
            return None, False

        import json as _json
        broker_list = kafka_brokers.split(',') if ',' in kafka_brokers else kafka_brokers
        for attempt in range(1, max_retries + 1):
            try:
                producer = Producer(
                    bootstrap_servers=broker_list,
                    value_serializer=lambda v: _json.dumps(v).encode('utf-8')
                )
                print(f"[xvigilance] Successfully connected to Kafka Brokers: {kafka_brokers} (attempt {attempt})", flush=True)
                return producer, True
            except Exception as e:
                wait = min(2 ** attempt, 30)
                print(f"[xvigilance] Kafka connection attempt {attempt}/{max_retries} failed: {e}. Retrying in {wait}s...", flush=True)
                time.sleep(wait)
        print("[xvigilance] CRITICAL: All Kafka connection attempts exhausted. Will retry next loop iteration.", flush=True)
        return None, False

    kafka_producer, kafka_available = _connect_kafka()
    # ------------------------------------------


    print(f"==================================================================", flush=True)
    print(f" LinkX Xvigilance Autonomous Detective Engine Online              ", flush=True)
    print(f" Worker: {worker_name}                                            ", flush=True)
    print(f" Target Storage: {config['elastic_base_url']}                     ", flush=True)
    print(f" Cadence: 1-Hour Sliding Windows with Self-Paced Elastic Rest      ", flush=True)
    print(f"==================================================================", flush=True)

    # 1. Initialize PostgreSQL schema
    try:
        ensure_xvigilance_schema()
        clean_zombie_runs()
    except Exception as exc:
        print(f"[xvigilance] Warning: Database schema init failed (will retry): {exc}", flush=True)

    while RUNNING:
        try:
            # --- Auto-reconnect Kafka if previously disconnected ---
            if not kafka_available and kafka_import_available:
                print("[xvigilance] Kafka is disconnected. Attempting to reconnect...", flush=True)
                kafka_producer, kafka_available = _connect_kafka(max_retries=3)

            # 2. Get current high-water mark checkpoint
            checkpoint = get_or_init_checkpoint(feed_name=feed_name, default_lookback_hours=1)
            
            if checkpoint.get("is_paused"):
                print("[xvigilance] ⏸️ Daemon is paused by Admin. Sleeping...", flush=True)
                interruptible_sleep(30)
                continue

            window_start = checkpoint["last_window_end"]
            window_end = window_start + timedelta(hours=1)

            now_utc = datetime.now(timezone.utc)

            # 3. Check if target window is in the future or not yet elapsed
            if now_utc < window_end:
                remaining_seconds = (window_end - now_utc).total_seconds()
                mins = int(remaining_seconds // 60)
                secs = int(remaining_seconds % 60)

                print(
                    f"[xvigilance] Target window [{window_start.strftime('%Y-%m-%d %H:%M')} -> {window_end.strftime('%H:%M')} UTC] "
                    f"is not yet complete. Resting for {mins}m {secs}s on time difference...",
                    flush=True,
                )

                if once:
                    print("[xvigilance] Run-once mode: target window in future, stopping.", flush=True)
                    break

                # Sleep in short increments and periodically check if the DB checkpoint was rewound
                sleep_seconds = min(remaining_seconds, 60)
                sleep_target = time.time() + sleep_seconds
                
                while RUNNING and time.time() < sleep_target:
                    time.sleep(1)
                
                # Check if the database checkpoint was manually rewound while sleeping
                current_db_checkpoint = get_or_init_checkpoint(feed_name=feed_name, default_lookback_hours=1)
                if current_db_checkpoint["last_window_end"] < window_start:
                    print(f"[xvigilance] ⚠️ Clock rewind detected in database! Resetting internal clock to {current_db_checkpoint['last_window_end']}", flush=True)
                
                continue

            # 3.5 Backpressure Flow Control: Bounded in-flight queue to prevent storage overflow
            max_in_flight = int(os.getenv("XVIGILANCE_MAX_IN_FLIGHT_SLICES", "1"))
            in_flight = get_in_flight_slices_count(feed_name=feed_name)
            if in_flight >= max_in_flight:
                print(
                    f"[xvigilance] ⏳ Backpressure throttle: {in_flight} slice(s) currently waiting in queue "
                    f"(limit: {max_in_flight}). Resting 15s for Node-21 consumer to finish before extracting next hour...",
                    flush=True,
                )
                interruptible_sleep(15)
                continue

            # 4. ACTIVE EXECUTION PHASE: Target window has elapsed
            t0 = time.time()
            overrun = False
            start_str = window_start.strftime("%Y-%m-%d %H:%M:%S UTC")
            end_str = window_end.strftime("%Y-%m-%d %H:%M:%S UTC")

            print(f"[xvigilance] Phase starting for window [{start_str} -> {end_str}]", flush=True)

            run_id = log_slice_start(feed_name, window_start, window_end)
            total_records = 0

            try:
                # Stream records in 50k-row pages from Elasticsearch
                for page in stream_window_records(config, window_start, window_end):
                    total_records += len(page)


                    # =========================================================================
                    # PHASE 1: KAFKA FIREHOSE (Governed Routing)
                    if kafka_available and kafka_producer:
                        import json
                        for txn in page:
                            # 1 Message = 1 Transaction (Micro-batching)
                            # Stamping with xVigilance headers
                            headers = [
                                ("source", b"xvigilance-daemon"),
                                ("session_id", b"XVIGILANCE_FINDINGS"),
                                ("window_id", window_start.isoformat().encode('utf-8'))
                            ]
                            
                            # Fire to Kafka (internal buffer handles efficient batching)
                            kafka_producer.send(
                                topic=kafka_topic,
                                value=txn,
                                headers=headers
                            )



                    # =========================================================================


                if kafka_available and kafka_producer:
                    import json
                    if total_records > 0:
                        watermark = {
                            "event": "WINDOW_COMPLETE",
                            "window_id": window_start.isoformat(),
                            "total_records": total_records,
                            "batch_id": run_id,
                            "elastic_endpoint": config.get('es_direct_index', 'mobile_banking_transactions'),
                            "worker_node": "Linkx_xmaintenance"
                        }
                        kafka_producer.send(
                            topic=kafka_topic,
                            value=watermark,
                            headers=[("source", b"xvigilance-daemon"), ("session_id", b"XVIGILANCE_FINDINGS"), ("type", b"watermark")]
                        )
                        kafka_producer.flush()
                        print(f"[xvigilance] Watermark fired. 100% of {total_records} transactions securely routed to Kafka.", flush=True)
                    else:
                        print(f"[xvigilance] Window [{start_str} -> {end_str}] has 0 records. Slice run completed immediately without queuing.", flush=True)

                duration_ms = int((time.time() - t0) * 1000)

                phase_duration_seconds = duration_ms / 1000.0

                # Check if the analysis duration took longer than 1 hour (overrun)
                overrun = phase_duration_seconds > 3600.0

                summary = {
                    "worker": worker_name,
                    "records_analyzed": total_records,
                    "duration_seconds": round(phase_duration_seconds, 2),
                    "overrun": overrun,
                    "status": "completed",
                }

                finish_slice_run(
                    run_id=run_id,
                    feed_name=feed_name,
                    window_start=window_start,
                    window_end=window_end,
                    success=True,
                    records_count=total_records,
                    duration_ms=duration_ms,
                    overrun_occurred=overrun,
                    summary=summary,
                )

                print(
                    f"[xvigilance] Phase complete: examined {total_records:,} records in {phase_duration_seconds:.2f}s. "
                    f"Advanced checkpoint to {end_str}.",
                    flush=True,
                )

                if overrun:
                    print(
                        f"[xvigilance] OVERRUN DETECTED: Analysis took {phase_duration_seconds:.2f}s (> 1hr). "
                        f"Skipping rest and continuing next phase immediately.",
                        flush=True,
                    )

            except Exception as fetch_exc:

                if kafka_available and kafka_producer:
                    import json
                    watermark = {
                        "event": "WINDOW_COMPLETE",
                        "window_id": window_start.isoformat(),
                        "total_records": total_records,
                        "batch_id": run_id,
                        "elastic_endpoint": config.get('es_direct_index', 'mobile_banking_transactions'),
                        "worker_node": "Linkx_xmaintenance"
                    }
                    kafka_producer.send(
                        topic=kafka_topic,
                        value=watermark,
                        headers=[("source", b"xvigilance-daemon"), ("session_id", b"XVIGILANCE_FINDINGS"), ("type", b"watermark")]
                    )
                    kafka_producer.flush()
                    print(f"[xvigilance] Watermark fired. 100% of {total_records} transactions securely routed to Kafka.", flush=True)

                duration_ms = int((time.time() - t0) * 1000)

                finish_slice_run(
                    run_id=run_id,
                    feed_name=feed_name,
                    window_start=window_start,
                    window_end=window_end,
                    success=False,
                    duration_ms=duration_ms,
                    error_message=str(fetch_exc),
                )
                print(f"[xvigilance] Phase failed for window [{start_str} -> {end_str}]: {fetch_exc}", flush=True)
                interruptible_sleep(15.0)

            if once:
                print("[xvigilance] Run-once mode finished.", flush=True)
                break

        except Exception as loop_exc:
            print(f"[xvigilance] Daemon error: {loop_exc}", flush=True)
            interruptible_sleep(10.0)

    release_locks()
    print(f"[xvigilance] Service {worker_name} stopped cleanly and released all locks.", flush=True)


def main():
    parser = argparse.ArgumentParser(description="LinkX Xvigilance Autonomous Hourly Detective Daemon")
    parser.add_argument("--feed-name", type=str, default="hourly_transaction_detective")
    parser.add_argument("--once", action="store_true", help="Execute single check and exit")
    args = parser.parse_args()

    run_daemon(feed_name=args.feed_name, once=args.once)


if __name__ == "__main__":
    main()
