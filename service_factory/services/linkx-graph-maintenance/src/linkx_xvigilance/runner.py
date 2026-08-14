import argparse
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
)
from linkx_xvigilance.config import get_xvigilance_config
from linkx_xvigilance.fetcher import stream_window_records
from linkx_xvigilance.schema import ensure_xvigilance_schema

RUNNING = True


def handle_shutdown(signum, frame):
    global RUNNING
    print(f"\n[xvigilance] Received signal {signum}. Initiating graceful shutdown...", flush=True)
    RUNNING = False


def run_daemon(feed_name: str = "hourly_transaction_detective", once: bool = False):
    global RUNNING
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    worker_name = os.getenv("XVIGILANCE_WORKER_NAME") or f"xvigilance@{socket.gethostname()}:{os.getpid()}"
    config = get_xvigilance_config()

    print(f"==================================================================", flush=True)
    print(f" LinkX Xvigilance Autonomous Detective Engine Online              ", flush=True)
    print(f" Worker: {worker_name}                                            ", flush=True)
    print(f" Target Storage: {config['elastic_base_url']}                     ", flush=True)
    print(f" Cadence: 1-Hour Sliding Windows with Self-Paced Elastic Rest      ", flush=True)
    print(f"==================================================================", flush=True)

    # 1. Initialize PostgreSQL schema
    try:
        ensure_xvigilance_schema()
    except Exception as exc:
        print(f"[xvigilance] Warning: Database schema init failed (will retry): {exc}", flush=True)

    while RUNNING:
        try:
            # 2. Get current high-water mark checkpoint
            checkpoint = get_or_init_checkpoint(feed_name=feed_name, default_lookback_hours=1)
            window_start = checkpoint["last_window_end"]
            window_end = window_start + timedelta(hours=1)

            now_utc = datetime.now(timezone.utc)

            # 3. Check if target window is in the future or not yet elapsed
            if now_utc < window_end:
                remaining_seconds = (window_end - now_utc).total_seconds()
                mins = int(remaining_seconds // 60)
                secs = int(remaining_seconds % 60)

                print(
                    f"[xvigilance] Target window [{window_start.strftime('%Y-%m-%d %H:%M')} ──► {window_end.strftime('%H:%M')} UTC] "
                    f"is not yet complete. 💤 Resting for {mins}m {secs}s on time difference...",
                    flush=True,
                )

                if once:
                    print("[xvigilance] Run-once mode: target window in future, stopping.", flush=True)
                    break

                # Sleep in short increments to remain responsive to signals
                sleep_target = time.time() + remaining_seconds
                while RUNNING and time.time() < sleep_target:
                    time.sleep(min(5.0, sleep_target - time.time()))
                continue

            # 4. ACTIVE EXECUTION PHASE: Target window has elapsed
            t0 = time.time()
            overrun = False
            start_str = window_start.strftime("%Y-%m-%d %H:%M:%S UTC")
            end_str = window_end.strftime("%Y-%m-%d %H:%M:%S UTC")

            print(f"[xvigilance] ⏰ Waking up! Phase starting for window [{start_str} ──► {end_str}]", flush=True)

            run_id = log_slice_start(feed_name, window_start, window_end)
            total_records = 0

            try:
                # Stream records in 50k-row pages from Elasticsearch
                for page in stream_window_records(config, window_start, window_end):
                    total_records += len(page)

                    # =========================================================================
                    # DETECTIVE ANALYSIS HOOK: (Placeholder for anomaly/fraud heuristics)
                    # e.g., analyze_window_anomalies(page, window_start, window_end)
                    # =========================================================================

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
                    window_end=window_end,
                    success=True,
                    records_count=total_records,
                    duration_ms=duration_ms,
                    overrun_occurred=overrun,
                    summary=summary,
                )

                print(
                    f"[xvigilance]  Phase complete! Examined {total_records:,} records in {phase_duration_seconds:.2f}s. "
                    f"Advanced checkpoint to {end_str}.",
                    flush=True,
                )

                if overrun:
                    print(
                        f"[xvigilance] ⚡ OVERRUN DETECTED: Analysis took {phase_duration_seconds:.2f}s (> 1hr). "
                        f"Skipping rest and immediately continuing next phase!",
                        flush=True,
                    )

            except Exception as fetch_exc:
                duration_ms = int((time.time() - t0) * 1000)
                finish_slice_run(
                    run_id=run_id,
                    feed_name=feed_name,
                    window_end=window_end,
                    success=False,
                    duration_ms=duration_ms,
                    error_message=str(fetch_exc),
                )
                print(f"[xvigilance] Phase failed for window [{start_str} -> {end_str}]: {fetch_exc}", flush=True)
                time.sleep(15.0)

            if once:
                print("[xvigilance] Run-once mode finished.", flush=True)
                break

        except Exception as loop_exc:
            print(f"[xvigilance] Daemon error: {loop_exc}", flush=True)
            time.sleep(10.0)

    print(f"[xvigilance] Service {worker_name} stopped cleanly.", flush=True)


def main():
    parser = argparse.ArgumentParser(description="LinkX Xvigilance Autonomous Hourly Detective Daemon")
    parser.add_argument("--feed-name", type=str, default="hourly_transaction_detective")
    parser.add_argument("--once", action="store_true", help="Execute single check and exit")
    args = parser.parse_args()

    run_daemon(feed_name=args.feed_name, once=args.once)


if __name__ == "__main__":
    main()
