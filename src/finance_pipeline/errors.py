"""Exception hierarchy and the single mapping from exceptions to CLI exit codes."""
from __future__ import annotations


class PipelineError(Exception):
    """Base class for expected pipeline failures."""


class QualityCheckError(PipelineError):
    """Raised when at least one data-quality check failed."""


class BatchConflictError(PipelineError):
    """A file name is already in the ledger but its contents changed."""


class LockError(PipelineError):
    """Another run holds the lock file."""


def exit_code_for(exc: BaseException) -> int:
    """0 ok (never here), 1 quality check failed, 2 bad input, 3 batch conflict, 4 locked."""
    if isinstance(exc, QualityCheckError):
        return 1
    if isinstance(exc, BatchConflictError):
        return 3
    if isinstance(exc, LockError):
        return 4
    return 2
