import json
from datetime import datetime, timedelta, timezone
from linkx_xvigilance.db import connect


def get_or_init_checkpoint(feed_name: str = "hourly_transaction_detective", default_lookback_hours: int = 1) -> dict:
    """
    Retrieves the current high-water mark. If not found, initializes it
    to the top of the hour from `default_lookback_hours` ago.
    """
    with connect(application_name="xvigilance-checkpoint") as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT feed_name, last_window_end, total_records_analyzed, total_slices_completed, status
                FROM xvigilance_checkpoints
                WHERE feed_name = %s
                """,
                (feed_name,),
            )
            row = cur.fetchone()
            if row:
                return {
                    "feed_name": row[0],
                    "last_window_end": row[1],
                    "total_records_analyzed": row[2],
                    "total_slices_completed": row[3],
                    "status": row[4],
                }

            # Initialize to top of the hour (e.g. 1 hour ago)
            now_utc = datetime.now(timezone.utc)
            initial_time = (now_utc - timedelta(hours=default_lookback_hours)).replace(minute=0, second=0, microsecond=0)

            cur.execute(
                """
                INSERT INTO xvigilance_checkpoints (feed_name, last_window_end)
                VALUES (%s, %s)
                RETURNING feed_name, last_window_end, total_records_analyzed, total_slices_completed, status
                """,
                (feed_name, initial_time),
            )
            created = cur.fetchone()
            conn.commit()
            return {
                "feed_name": created[0],
                "last_window_end": created[1],
                "total_records_analyzed": created[2],
                "total_slices_completed": created[3],
                "status": created[4],
            }


def log_slice_start(feed_name: str, window_start: datetime, window_end: datetime) -> int:
    with connect(application_name="xvigilance-slice-log") as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO xvigilance_slice_runs (feed_name, window_start, window_end, status)
                VALUES (%s, %s, %s, 'running')
                RETURNING id
                """,
                (feed_name, window_start, window_end),
            )
            run_id = cur.fetchone()[0]
        conn.commit()
    return run_id


def finish_slice_run(
    run_id: int,
    feed_name: str,
    window_end: datetime,
    success: bool,
    records_count: int = 0,
    duration_ms: int = 0,
    overrun_occurred: bool = False,
    summary: dict = None,
    error_message: str = None,
):
    status_str = "succeeded" if success else "failed"
    summary_json = json.dumps(summary or {})

    with connect(application_name="xvigilance-slice-finish") as conn:
        with conn.cursor() as cur:
            # 1. Update slice run audit row
            cur.execute(
                """
                UPDATE xvigilance_slice_runs
                SET status = %s,
                    records_count = %s,
                    duration_ms = %s,
                    overrun_occurred = %s,
                    summary = %s::jsonb,
                    error_message = %s,
                    finished_at = NOW()
                WHERE id = %s
                """,
                (status_str, records_count, duration_ms, overrun_occurred, summary_json, error_message, run_id),
            )

            # 2. Advance high-water mark if successful
            if success:
                cur.execute(
                    """
                    UPDATE xvigilance_checkpoints
                    SET last_window_end = %s,
                        total_records_analyzed = total_records_analyzed + %s,
                        total_slices_completed = total_slices_completed + 1,
                        updated_at = NOW()
                    WHERE feed_name = %s
                    """,
                    (window_end, records_count, feed_name),
                )

        conn.commit()
