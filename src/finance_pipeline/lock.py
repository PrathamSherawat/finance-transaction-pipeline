"""Single-writer lock file for incremental runs."""
from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

from finance_pipeline.errors import LockError

log = logging.getLogger(__name__)


class RunLock:
    """Context manager: atomically creates the lock file, removes it on exit.

    A lock older than max_age_seconds is treated as left behind by a dead run.
    """

    def __init__(self, path, max_age_seconds: float = 6 * 3600):
        self.path = Path(path)
        self.max_age_seconds = max_age_seconds
        self._held = False

    def _is_stale(self) -> bool:
        try:
            age = time.time() - self.path.stat().st_mtime
        except FileNotFoundError:
            return True
        return age > self.max_age_seconds

    def __enter__(self) -> "RunLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        for attempt in (1, 2):
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                if attempt == 1 and self._is_stale():
                    log.warning("removing stale lock %s", self.path)
                    self.path.unlink(missing_ok=True)
                    continue
                raise LockError(
                    f"another run holds {self.path}; if that run is dead, delete the file"
                ) from None
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump({"pid": os.getpid(), "created": time.time()}, f)
            self._held = True
            return self
        raise LockError(f"could not acquire {self.path}")

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._held:
            self.path.unlink(missing_ok=True)
            self._held = False
