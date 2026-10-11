"""Logging setup: records go to stderr so stdout stays reserved for the JSON summary."""
from __future__ import annotations

import logging
import sys

_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


class _StderrHandler(logging.Handler):
    """Looks up sys.stderr at emit time, so a stream swapped by a test never goes stale."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            sys.stderr.write(self.format(record) + "\n")
        except Exception:
            self.handleError(record)


def setup_logging(level: str = "INFO") -> None:
    """Configure the 'finance_pipeline' logger. Safe to call repeatedly."""
    name = str(level).upper()
    if name not in logging.getLevelNamesMapping():
        raise ValueError(f"unknown log level {level!r}")
    logger = logging.getLogger("finance_pipeline")
    logger.setLevel(name)
    if not any(isinstance(h, _StderrHandler) for h in logger.handlers):
        handler = _StderrHandler()
        handler.setFormatter(logging.Formatter(_FORMAT))
        logger.addHandler(handler)
    logger.propagate = False
