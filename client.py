from __future__ import annotations

import argparse
import json
import os
import socket
import sys
from typing import Any

from kvstore.common import env_or, load_config, rpc_call


def parse_value(raw: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def request_for(args: argparse.Namespace) -> dict[str, Any]:
    if args.command == "put":
        return {"op": "put", "key": args.key, "value": parse_value(args.value)}
    if args.command == "get":
        return {"op": "get", "key": args.key}
    if args.command == "delete":
        return {"op": "delete", "key": args.key}
    if args.command == "keys":
        return {"op": "keys"}
    if args.command == "ping":
        return {"op": "ping"}
    raise ValueError("unsupported command")


def call_node(node: dict[str, Any], request: dict[str, Any]) -> dict[str, Any]:
    return rpc_call(node["host"], int(node["port"]), request)


def main() -> None:
    parser = argparse.ArgumentParser(description="Client for the distributed key-value store")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--server", choices=["auto", "primary", "backup"], default="auto")

    sub = parser.add_subparsers(dest="command", required=True)
    put = sub.add_parser("put")
    put.add_argument("key")
    put.add_argument("value")
    get = sub.add_parser("get")
    get.add_argument("key")
    delete = sub.add_parser("delete")
    delete.add_argument("key")
    sub.add_parser("keys")
    sub.add_parser("ping")
    args = parser.parse_args()

    config = load_config(args.config)
    primary = {
        "host": env_or(config["primary"]["host"], "PRIMARY_HOST"),
        "port": env_or(config["primary"]["port"], "PRIMARY_PORT", int),
    }
    backup = {
        "host": env_or(config["backup"]["host"], "BACKUP_HOST"),
        "port": env_or(config["backup"]["port"], "BACKUP_PORT", int),
    }
    request = request_for(args)

    try:
        if args.server == "primary":
            response = call_node(primary, request)
        elif args.server == "backup":
            response = call_node(backup, request)
        else:
            try:
                response = call_node(primary, request)
            except (OSError, ValueError, ConnectionError, socket.timeout):
                if args.command not in {"get", "keys", "ping"}:
                    raise RuntimeError("primary unavailable; backup is read-only, so the write was not sent")
                response = call_node(backup, request)
    except Exception as exc:
        print(json.dumps({"ok": False, "error": "CONNECTION_FAILED", "message": str(exc)}, indent=2))
        sys.exit(1)

    print(json.dumps(response, indent=2, ensure_ascii=False))
    sys.exit(0 if response.get("ok") else 2)


if __name__ == "__main__":
    main()
