# -*- coding: utf-8 -*-
# Author : Gemini CLI
# Date: 2026-04-16

import os
import sys
import argparse

# Add the parent directory to sys.path to allow imports from app
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
sys.path.append(project_root)

from app.lib.db_watcher.mongoengine_tool import mongo_watcher  # noqa: E402
from app.utilities.auth_util import generate_service_token  # noqa: E402
from app.model.service_token import ServiceToken  # noqa: E402


def connect():
    mongo_watcher.connect_to_db()


def cmd_create(args):
    connect()
    scopes = args.scopes.split(",")
    plain_token, doc = generate_service_token(args.name, scopes, args.expires)
    print("Service Token Created Successfully!")
    print(f"Name: {doc.name}")
    print(f"Scopes: {doc.scopes}")
    print(f"Expires At: {doc.expires_at}")
    print("-" * 20)
    print(f"PLAIN TOKEN: {plain_token}")
    print("-" * 20)
    print("CRITICAL: Copy this token now. It will NOT be shown again.")


def cmd_list(args):
    connect()
    tokens = ServiceToken.objects.all()
    if not tokens:
        print("No service tokens found.")
        return

    print(f"{'Name':<20} | {'Status':<8} | {'Expires At':<20} | {'Scopes'}")
    print("-" * 80)
    for t in tokens:
        expires = str(t.expires_at) if t.expires_at else "Never"
        print(f"{t.name:<20} | {t.status:<8} | {expires:<20} | {','.join(t.scopes)}")


def cmd_revoke(args):
    connect()
    token = ServiceToken.objects(name=args.name).first()
    if not token:
        print(f"Error: Token with name '{args.name}' not found.")
        return

    token.status = "revoked"
    token.save()
    print(f"Token '{args.name}' has been revoked.")


def main():
    parser = argparse.ArgumentParser(description="Manage Caifubao Service Tokens")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Create command
    p_create = subparsers.add_parser("create", help="Create a new service token")
    p_create.add_argument(
        "--name", required=True, help="Name of the service (e.g. openclaw-mvp)"
    )
    p_create.add_argument(
        "--scopes", default="openclaw:data-read", help="Comma separated scopes"
    )
    p_create.add_argument(
        "--expires",
        type=int,
        default=365,
        help="Expires in days (default 365, 0 for never)",
    )

    # List command
    subparsers.add_parser("list", help="List all service tokens")

    # Revoke command
    p_revoke = subparsers.add_parser("revoke", help="Revoke a service token")
    p_revoke.add_argument("--name", required=True, help="Name of the token to revoke")

    args = parser.parse_args()

    if args.command == "create":
        cmd_create(args)
    elif args.command == "list":
        cmd_list(args)
    elif args.command == "revoke":
        cmd_revoke(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
