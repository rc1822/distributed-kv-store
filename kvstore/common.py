from __future__ import annotations

import json
import logging
import os
import socket
from pathlib import Path
from typing import Any

MAX_MESSAGE_BYTES = 64 * 1024
DEFAULT_TIMEOUT_SECONDS = 3.0


def configure_logger(name: str, path: str) -> logging.Logger:
    log_path = Path(path)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not logger.handlers:
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(file_handler)
    return logger


def recv_json_line(conn: socket.socket) -> dict[str, Any]:
    data = bytearray()
    while True:
        chunk = conn.recv(4096)
        if not chunk:
            break
        data.extend(chunk)
        if len(data) > MAX_MESSAGE_BYTES:
            raise ValueError("message exceeds maximum size")
        if b"\n" in chunk:
            break

    if not data:
        raise ConnectionError("connection closed before a request was received")

    raw = bytes(data).split(b"\n", 1)[0]
    try:
        message = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("message must be valid UTF-8 JSON") from exc

    if not isinstance(message, dict):
        raise ValueError("message must be a JSON object")
    return message


def send_json_line(conn: socket.socket, message: dict[str, Any]) -> None:
    payload = json.dumps(message, separators=(",", ":"), ensure_ascii=False).encode("utf-8") + b"\n"
    conn.sendall(payload)


def rpc_call(
    host: str,
    port: int,
    request: dict[str, Any],
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        send_json_line(sock, request)
        return recv_json_line(sock)


def ok(**payload: Any) -> dict[str, Any]:
    return {"ok": True, **payload}


def error(code: str, message: str) -> dict[str, Any]:
    return {"ok": False, "error": code, "message": message}


def load_config(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        config = json.load(fh)
    if not isinstance(config, dict):
        raise ValueError("configuration must contain a JSON object")
    return config


def env_or(value: Any, env_name: str, cast=str) -> Any:
    raw = os.getenv(env_name)
    return cast(raw) if raw is not None else value
