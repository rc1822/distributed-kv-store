from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Iterator


class KeyLockManager:
    """Provides one mutex per key so unrelated writes can proceed concurrently."""

    def __init__(self) -> None:
        self._locks: dict[str, threading.Lock] = {}
        self._guard = threading.Lock()

    def _lock_for(self, key: str) -> threading.Lock:
        with self._guard:
            return self._locks.setdefault(key, threading.Lock())

    @contextmanager
    def acquire(self, key: str) -> Iterator[None]:
        lock = self._lock_for(key)
        lock.acquire()
        try:
            yield
        finally:
            lock.release()
