"""Ledger of processed batch files: what was processed and which snapshot is current."""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from finance_pipeline.errors import BatchConflictError

LEDGER_NAME = "ledger.json"


def file_sha256(path) -> str:
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def empty_ledger() -> dict:
    return {"version": 1, "current_snapshot": None, "batches": []}


def load_ledger(state_dir) -> dict:
    p = Path(state_dir) / LEDGER_NAME
    if not p.is_file():
        return empty_ledger()
    data = json.loads(p.read_text(encoding="utf-8-sig"))
    if data.get("version") != 1 or "batches" not in data or "current_snapshot" not in data:
        raise ValueError(f"{p} is not a valid ledger (expected version 1)")
    return data


def save_ledger(state_dir, ledger: dict) -> None:
    """Atomic write: temp file, then os.replace. Either the old or the new ledger exists."""
    d = Path(state_dir)
    d.mkdir(parents=True, exist_ok=True)
    tmp = d / (LEDGER_NAME + ".tmp")
    tmp.write_text(json.dumps(ledger, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, d / LEDGER_NAME)


def classify_batch(ledger: dict, name: str, sha256: str) -> str:
    """Return 'new' or 'skip'; raise BatchConflictError if the name was seen with other content."""
    for b in ledger["batches"]:
        if b["file"] == name and b["sha256"] != sha256:
            raise BatchConflictError(
                f"{name} was already processed with different contents; "
                "rename the file if it is a genuinely new batch"
            )
    return "skip" if any(b["sha256"] == sha256 for b in ledger["batches"]) else "new"


def record_batch(ledger: dict, *, file: str, sha256: str, input_rows: int, snapshot: str) -> dict:
    """Return a NEW ledger with the batch appended; the input is not mutated."""
    entry = {
        "file": file,
        "sha256": sha256,
        "input_rows": input_rows,
        "snapshot": snapshot,
        "processed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    return {**ledger, "current_snapshot": snapshot, "batches": [*ledger["batches"], entry]}
