#!/usr/bin/env python3
import argparse
import secrets
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from auth.repository import DEFAULT_SERVICE_PERMISSIONS, create_or_update_service_account


def main():
    parser = argparse.ArgumentParser(description="Create or reset a Linkx service account.")
    parser.add_argument("client_id", help="Stable sibling-service client id, e.g. reporting_service")
    parser.add_argument("--secret", help="Client secret. If omitted, a new one is generated and printed once.")
    parser.add_argument(
        "--permission",
        action="append",
        dest="permissions",
        help="Permission to grant. Can be repeated. Defaults to known permissions for the client id if available.",
    )
    parser.add_argument("--display-name", help="Human-readable display name")
    args = parser.parse_args()

    generated = False
    secret = args.secret
    if not secret:
        secret = secrets.token_urlsafe(32)
        generated = True

    permissions = args.permissions or DEFAULT_SERVICE_PERMISSIONS.get(args.client_id, [])
    service = create_or_update_service_account(
        args.client_id,
        secret,
        permissions=permissions,
        display_name=args.display_name or args.client_id,
    )

    print(f"client_id={service['client_id']}")
    print(f"permissions={','.join(service.get('permissions') or [])}")
    if generated:
        print(f"client_secret={secret}")
        print("Store this secret in the calling service now; it is not recoverable later.")
    else:
        print("client_secret=<provided>")


if __name__ == "__main__":
    main()
