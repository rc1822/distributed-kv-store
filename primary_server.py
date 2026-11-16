from __future__ import annotations

import argparse
import socket
import uuid
from typing import Any

from kvstore.common import configure_logger, env_or, error, load_config, ok, rpc_call
from kvstore.locking import KeyLockManager
from kvstore.server import serve_tcp
from kvstore.storage import SQLiteStore


class PrimaryService:
    def __init__(self, db_path: str, backup_host: str, backup_port: int, logger) -> None:
        self.store = SQLiteStore(db_path)
        self.locks = KeyLockManager()
        self.backup_host = backup_host
        self.backup_port = backup_port
        self.logger = logger

    def handle(self, request: dict[str, Any], peer: str) -> dict[str, Any]:
        op = request.get("op")
        if not isinstance(op, str):
            return error("BAD_REQUEST", "field 'op' must be a string")

        if op == "put":
            response = self._put(request)
        elif op == "get":
            response = self._get(request)
        elif op == "delete":
            response = self._delete(request)
        elif op == "keys":
            response = ok(keys=self.store.keys(), served_by="primary")
        elif op == "ping":
            response = ok(role="primary", writable=True, key_count=self.store.count())
        else:
            response = error("UNSUPPORTED_OPERATION", f"unsupported operation: {op}")

        self.logger.info("peer=%s op=%s result=%s", peer, op, response)
        return response

    def _put(self, request: dict[str, Any]) -> dict[str, Any]:
        key = self._required_key(request)
        if isinstance(key, dict):
            return key
        if "value" not in request:
            return error("BAD_REQUEST", "put requires field 'value'")

        value = request["value"]
        with self.locks.acquire(key):
            existed, previous = self.store.put(key, value)
            replication = self._replicate("put", key, value)
            if not replication.get("ok"):
                self.store.restore(key, existed, previous)
                return error(
                    "REPLICATION_FAILED",
                    replication.get("message", "backup did not confirm the write"),
                )
        return ok(key=key, value=value, replicated=True)

    def _get(self, request: dict[str, Any]) -> dict[str, Any]:
        key = self._required_key(request)
        if isinstance(key, dict):
            return key
        found, value = self.store.get(key)
        if not found:
            return error("NOT_FOUND", f"key '{key}' does not exist")
        return ok(value=value, served_by="primary")

    def _delete(self, request: dict[str, Any]) -> dict[str, Any]:
        key = self._required_key(request)
        if isinstance(key, dict):
            return key

        with self.locks.acquire(key):
            existed, previous = self.store.delete(key)
            if not existed:
                return error("NOT_FOUND", f"key '{key}' does not exist")

            replication = self._replicate("delete", key)
            if not replication.get("ok"):
                self.store.restore(key, True, previous)
                return error(
                    "REPLICATION_FAILED",
                    replication.get("message", "backup did not confirm the delete"),
                )
        return ok(key=key, deleted=True, replicated=True)

    def _replicate(self, action: str, key: str, value: Any = None) -> dict[str, Any]:
        request: dict[str, Any] = {
            "op": "replicate",
            "op_id": str(uuid.uuid4()),
            "action": action,
            "key": key,
        }
        if action == "put":
            request["value"] = value

        try:
            response = rpc_call(self.backup_host, self.backup_port, request)
            if not response.get("ok"):
                return error("BACKUP_REJECTED", response.get("message", "backup rejected replication"))
            return response
        except (OSError, ValueError, ConnectionError, socket.timeout) as exc:
            self.logger.warning("replication_failed action=%s key=%s error=%s", action, key, exc)
            return error("BACKUP_UNAVAILABLE", str(exc))

    @staticmethod
    def _required_key(request: dict[str, Any]):
        key = request.get("key")
        if not isinstance(key, str) or not key.strip():
            return error("BAD_REQUEST", "field 'key' must be a non-empty string")
        return key


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the primary key-value server")
    parser.add_argument("--config", default="config.json")
    args = parser.parse_args()

    config = load_config(args.config)
    primary = config["primary"]
    backup = config["backup"]
    storage = config["storage"]
    logs = config["logs"]

    host = env_or(primary["host"], "PRIMARY_HOST")
    port = env_or(primary["port"], "PRIMARY_PORT", int)
    backup_host = env_or(backup["host"], "BACKUP_HOST")
    backup_port = env_or(backup["port"], "BACKUP_PORT", int)
    db_path = env_or(storage["primary"], "PRIMARY_DB")
    log_path = env_or(logs["primary"], "PRIMARY_LOG")

    logger = configure_logger("primary", log_path)
    service = PrimaryService(db_path, backup_host, backup_port, logger)
    serve_tcp(host, port, service.handle, logger, "primary")


if __name__ == "__main__":
    main()
