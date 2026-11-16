from __future__ import annotations

import json
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from kvstore.common import rpc_call  # noqa: E402

PRIMARY_PORT = 55100
BACKUP_PORT = 55101


def wait_for_port(port: int, timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.15):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"server on port {port} did not start")


class DistributedKVIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        base = Path(self.temp.name)
        self.config_path = base / "config.json"
        self.config_path.write_text(
            json.dumps(
                {
                    "primary": {"host": "127.0.0.1", "port": PRIMARY_PORT},
                    "backup": {"host": "127.0.0.1", "port": BACKUP_PORT},
                    "storage": {
                        "primary": str(base / "primary.db"),
                        "backup": str(base / "backup.db"),
                    },
                    "logs": {
                        "primary": str(base / "primary.log"),
                        "backup": str(base / "backup.log"),
                    },
                }
            ),
            encoding="utf-8",
        )
        self.processes: list[subprocess.Popen] = []
        self.backup = self.start_server("backup_server.py", BACKUP_PORT)
        self.primary = self.start_server("primary_server.py", PRIMARY_PORT)

    def tearDown(self) -> None:
        for process in reversed(self.processes):
            if process.poll() is None:
                process.terminate()
        for process in reversed(self.processes):
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
        self.temp.cleanup()

    def start_server(self, filename: str, port: int) -> subprocess.Popen:
        process = subprocess.Popen(
            [sys.executable, str(PROJECT_ROOT / filename), "--config", str(self.config_path)],
            cwd=PROJECT_ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.processes.append(process)
        wait_for_port(port)
        return process

    def primary_call(self, request: dict) -> dict:
        return rpc_call("127.0.0.1", PRIMARY_PORT, request)

    def backup_call(self, request: dict) -> dict:
        return rpc_call("127.0.0.1", BACKUP_PORT, request)

    def test_replication_reads_delete_and_health(self) -> None:
        put = self.primary_call({"op": "put", "key": "city", "value": "Toronto"})
        self.assertTrue(put["ok"])
        self.assertTrue(put["replicated"])

        self.assertEqual(self.primary_call({"op": "get", "key": "city"})["value"], "Toronto")
        self.assertEqual(self.backup_call({"op": "get", "key": "city"})["value"], "Toronto")
        self.assertEqual(self.backup_call({"op": "ping"})["role"], "backup")

        deleted = self.primary_call({"op": "delete", "key": "city"})
        self.assertTrue(deleted["ok"])
        self.assertFalse(self.backup_call({"op": "get", "key": "city"})["ok"])

    def test_backup_rejects_direct_writes(self) -> None:
        response = self.backup_call({"op": "put", "key": "x", "value": 1})
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"], "READ_ONLY")

    def test_failed_replication_rolls_back_primary(self) -> None:
        self.backup.terminate()
        self.backup.wait(timeout=2)

        response = self.primary_call({"op": "put", "key": "unsafe", "value": "value"})
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"], "REPLICATION_FAILED")

        read_back = self.primary_call({"op": "get", "key": "unsafe"})
        self.assertFalse(read_back["ok"])
        self.assertEqual(read_back["error"], "NOT_FOUND")

    def test_backup_data_survives_restart(self) -> None:
        response = self.primary_call({"op": "put", "key": "persistent", "value": {"n": 42}})
        self.assertTrue(response["ok"])

        self.backup.terminate()
        self.backup.wait(timeout=2)
        self.backup = self.start_server("backup_server.py", BACKUP_PORT)

        response = self.backup_call({"op": "get", "key": "persistent"})
        self.assertEqual(response["value"], {"n": 42})


if __name__ == "__main__":
    unittest.main(verbosity=2)
