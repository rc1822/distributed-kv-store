from __future__ import annotations

import argparse
from typing import Any

from kvstore.common import configure_logger, env_or, error, load_config, ok
from kvstore.locking import KeyLockManager
from kvstore.server import serve_tcp
from kvstore.storage import SQLiteStore


class BackupService:
    def __init__(self, db_path: str, logger) -> None:
        self.store = SQLiteStore(db_path)
        self.locks = KeyLockManager()
        self.logger = logger

    def handle(self, request: dict[str, Any], peer: str) -> dict[str, Any]:
        op = request.get("op")
        if not isinstance(op, str):
            return error("BAD_REQUEST", "field 'op' must be a string")

        if op == "get":
            response = self._get(request)
        elif op == "keys":
            response = ok(keys=self.store.keys(), served_by="backup")
        elif op == "ping":
            response = ok(role="backup", writable=False, key_count=self.store.count())
        elif op == "replicate":
            response = self._replicate(request)
        elif op in {"put", "delete"}:
            response = error("READ_ONLY", "backup accepts reads only; writes must go to the primary")
        else:
            response = error("UNSUPPORTED_OPERATION", f"unsupported operation: {op}")

        self.logger.info("peer=%s op=%s result=%s", peer, op, response)
        return response

    def _get(self, request: dict[str, Any]) -> dict[str, Any]:
        key = self._required_key(request)
        if isinstance(key, dict):
            return key
        found, value = self.store.get(key)
        if not found:
            return error("NOT_FOUND", f"key '{key}' does not exist")
        return ok(value=value, served_by="backup")

    def _replicate(self, request: dict[str, Any]) -> dict[str, Any]:
        op_id = request.get("op_id")
        action = request.get("action")
        key = self._required_key(request)

        if not isinstance(op_id, str) or not op_id.strip():
            return error("BAD_REQUEST", "replication requires a non-empty 'op_id'")
        if action not in {"put", "delete"}:
            return error("BAD_REQUEST", "replication action must be 'put' or 'delete'")
        if isinstance(key, dict):
            return key
        if action == "put" and "value" not in request:
            return error("BAD_REQUEST", "replicated put requires field 'value'")

        with self.locks.acquire(key):
            applied = self.store.apply_replication(op_id, action, key, request.get("value"))
        return ok(replicated=True, duplicate=not applied, action=action)

    @staticmethod
    def _required_key(request: dict[str, Any]):
        key = request.get("key")
        if not isinstance(key, str) or not key.strip():
            return error("BAD_REQUEST", "field 'key' must be a non-empty string")
        return key


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the backup key-value server")
    parser.add_argument("--config", default="config.json")
    args = parser.parse_args()

    config = load_config(args.config)
    backup = config["backup"]
    storage = config["storage"]
    logs = config["logs"]

    host = env_or(backup["host"], "BACKUP_HOST")
    port = env_or(backup["port"], "BACKUP_PORT", int)
    db_path = env_or(storage["backup"], "BACKUP_DB")
    log_path = env_or(logs["backup"], "BACKUP_LOG")

    logger = configure_logger("backup", log_path)
    service = BackupService(db_path, logger)
    serve_tcp(host, port, service.handle, logger, "backup")


if __name__ == "__main__":
    main()
