import argparse
import os
import sys
from datetime import datetime


from batch_manager.utils.neo4j_cleanup import clean_existing_session
from batch_manager.utils.neo4j_utils import create_neo4j_driver
from logger import log_writer


def main():
    parser = argparse.ArgumentParser(description="Clean Neo4j data for a stopped LinkX session.")
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--run-id")
    parser.add_argument("--log-file")
    parser.add_argument("--batch-size", type=int, default=10000)
    args = parser.parse_args()

    neo4j_url = os.environ.get("LINKX_CLEANUP_NEO4J_URL")
    neo4j_username = os.environ.get("LINKX_CLEANUP_NEO4J_USERNAME")
    neo4j_password = os.environ.get("LINKX_CLEANUP_NEO4J_PASSWORD")
    neo4j_database = os.environ.get("LINKX_CLEANUP_NEO4J_DATABASE") or os.environ.get("LINKX_NEO4J_DATABASE")
    if not neo4j_url or not neo4j_username or not neo4j_password:
        if args.log_file:
            log_writer(args.log_file, f"[{datetime.now()}] [Error] - Cleanup process missing Neo4j credentials")
        return 2

    driver = create_neo4j_driver({"url": neo4j_url, "username": neo4j_username, "password": neo4j_password, "database": neo4j_database})
    try:
        clean_existing_session(
            driver,
            args.session_id,
            log_file=args.log_file,
            batch_size=args.batch_size,
            run_id=args.run_id or None,
        )
    except Exception as exc:
        if args.log_file:
            log_writer(args.log_file, f"[{datetime.now()}] [Error] - Cleanup process failed: {exc}")
        return 1
    finally:
        driver.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
