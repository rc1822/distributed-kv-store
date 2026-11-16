from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any


class SQLiteStore:
    """Small durable key-value store backed by SQLite."""

    def __init__(self, path: str) -> None:
        self.path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._schema_lock = threading.Lock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=5.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def _initialize(self) -> None:
        with self._schema_lock, self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS kv (
                    key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS replication_ops (
                    op_id TEXT PRIMARY KEY,
                    applied_at REAL NOT NULL
                )
                """
            )

    def get(self, key: str) -> tuple[bool, Any]:
        with self._connect() as conn:
            row = conn.execute("SELECT value_json FROM kv WHERE key = ?", (key,)).fetchone()
        if row is None:
            return False, None
        return True, json.loads(row[0])

    def keys(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute("SELECT key FROM kv ORDER BY key").fetchall()
        return [row[0] for row in rows]

    def count(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) FROM kv").fetchone()
        return int(row[0])

    def put(self, key: str, value: Any) -> tuple[bool, Any]:
        existed, previous = self.get(key)
        encoded = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO kv(key, value_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value_json = excluded.value_json,
                    updated_at = excluded.updated_at
                """,
                (key, encoded, time.time()),
            )
        return existed, previous

    def delete(self, key: str) -> tuple[bool, Any]:
        existed, previous = self.get(key)
        if not existed:
            return False, None
        with self._connect() as conn:
            conn.execute("DELETE FROM kv WHERE key = ?", (key,))
        return True, previous

    def restore(self, key: str, existed: bool, previous: Any) -> None:
        if existed:
            self.put(key, previous)
        else:
            with self._connect() as conn:
                conn.execute("DELETE FROM kv WHERE key = ?", (key,))

    def has_replication_op(self, op_id: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM replication_ops WHERE op_id = ?",
                (op_id,),
            ).fetchone()
        return row is not None

    def apply_replication(self, op_id: str, action: str, key: str, value: Any = None) -> bool:
        """Apply a replicated operation exactly once. Returns False for a duplicate op."""
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            duplicate = conn.execute(
                "SELECT 1 FROM replication_ops WHERE op_id = ?",
                (op_id,),
            ).fetchone()
            if duplicate is not None:
                conn.rollback()
                return False

            if action == "put":
                encoded = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
                conn.execute(
                    """
                    INSERT INTO kv(key, value_json, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        value_json = excluded.value_json,
                        updated_at = excluded.updated_at
                    """,
                    (key, encoded, time.time()),
                )
            elif action == "delete":
                conn.execute("DELETE FROM kv WHERE key = ?", (key,))
            else:
                conn.rollback()
                raise ValueError("unsupported replication action")

            conn.execute(
                "INSERT INTO replication_ops(op_id, applied_at) VALUES (?, ?)",
                (op_id, time.time()),
            )
            conn.commit()
        return True
