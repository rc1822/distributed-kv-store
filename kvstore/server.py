from __future__ import annotations

import socket
import threading
from collections.abc import Callable
from typing import Any

from .common import error, recv_json_line, send_json_line

RequestHandler = Callable[[dict[str, Any], str], dict[str, Any]]


def serve_tcp(host: str, port: int, handler: RequestHandler, logger, label: str) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((host, port))
        server.listen()
        logger.info("%s_started host=%s port=%s", label, host, port)
        print(f"{label.capitalize()} listening on {host}:{port}", flush=True)

        while True:
            conn, address = server.accept()
            thread = threading.Thread(
                target=_client_worker,
                args=(conn, address, handler, logger),
                daemon=True,
            )
            thread.start()


def _client_worker(conn: socket.socket, address, handler: RequestHandler, logger) -> None:
    peer = f"{address[0]}:{address[1]}"
    with conn:
        try:
            request = recv_json_line(conn)
            response = handler(request, peer)
        except (ValueError, ConnectionError, socket.timeout) as exc:
            response = error("BAD_REQUEST", str(exc))
            logger.warning("peer=%s bad_request error=%s", peer, exc)
        except Exception as exc:
            response = error("INTERNAL_ERROR", "unexpected server error")
            logger.exception("peer=%s unexpected_error=%s", peer, exc)

        try:
            send_json_line(conn, response)
        except OSError:
            logger.warning("peer=%s response_send_failed", peer)
