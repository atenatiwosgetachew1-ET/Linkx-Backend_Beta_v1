import argparse
import json

from linkx_cleanup.scheduler import enqueue_cleanup


def main():
    parser = argparse.ArgumentParser(description="Enqueue a cleanup run.")
    parser.add_argument("cleanup_type")
    parser.add_argument("--payload", default="{}")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    cleanup_id = enqueue_cleanup(args.cleanup_type, json.loads(args.payload), dry_run=args.dry_run)
    print(cleanup_id)


if __name__ == "__main__":
    main()
