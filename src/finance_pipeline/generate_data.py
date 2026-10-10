"""Synthetic finance transaction generator."""
from __future__ import annotations

import argparse
import csv
import math
import random
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple
from datetime import date, datetime, time, timedelta


def _require_int(name: str, value: object, minimum: int | None = None) -> None:
    # bool is a subclass of int, so it must be rejected explicitly.
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer, got {value!r}")
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}, got {value!r}")


def _require_rate(name: str, value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a number in [0, 1), got {value!r}")
    if not 0 <= value < 1:
        raise ValueError(f"{name} must be in [0, 1), got {value!r}")


@dataclass(frozen=True)
class GeneratorConfig:
    num_rows: int = 1000
    seed: int = 42
    invalid_rate: float = 0.05
    duplicate_rate: float = 0.08
    start_date: date = date(2025, 1, 1)
    window_days: int = 90

    def __post_init__(self) -> None:
        _require_int("num_rows", self.num_rows, minimum=1)
        _require_int("seed", self.seed)
        _require_int("window_days", self.window_days, minimum=1)
        _require_rate("invalid_rate", self.invalid_rate)
        _require_rate("duplicate_rate", self.duplicate_rate)
        # datetime is a subclass of date; we want a plain date.
        if isinstance(self.start_date, datetime) or not isinstance(self.start_date, date):
            raise ValueError(f"start_date must be a datetime.date, got {self.start_date!r}")
        total = self.invalid_rate + 2 * self.duplicate_rate
        if total >= 1:
            raise ValueError(
                "invalid_rate + 2 * duplicate_rate must sum to less than 1 "
                "(each duplicate needs its own valid base row, so duplicate rows "
                f"cannot exceed valid rows), got {total}"
            )



class RowCounts(NamedTuple):
    valid: int
    invalid: int
    duplicate: int


def compute_row_counts(config: GeneratorConfig) -> RowCounts:
    """Split config.num_rows into mutually exclusive categories.

    invalid and duplicate are floored (epsilon guards float error such as
    0.29 * 100 == 28.999...). valid is the remainder, so the three counts
    always sum to num_rows and valid >= 1 (guaranteed by the config's
    rate-sum check).
    """
    eps = 1e-9
    invalid = math.floor(config.num_rows * config.invalid_rate + eps)
    duplicate = math.floor(config.num_rows * config.duplicate_rate + eps)
    valid = config.num_rows - invalid - duplicate
    return RowCounts(valid=valid, invalid=invalid, duplicate=duplicate)



def format_amount(cents: int) -> str:
    """Format integer cents as a string with exactly two decimals."""
    if isinstance(cents, bool) or not isinstance(cents, int):
        raise ValueError(f"cents must be an integer, got {cents!r}")
    sign = "-" if cents < 0 else ""
    whole, frac = divmod(abs(cents), 100)
    return f"{sign}{whole}.{frac:02d}"


def format_ts(dt: datetime) -> str:
    """Format a naive datetime as YYYY-MM-DDTHH:MM:SS (treated as UTC)."""
    return dt.replace(microsecond=0).isoformat()


def make_account_pool(rng: random.Random, size: int) -> list[str]:
    """Return `size` synthetic account IDs: ACC-0001, ACC-0002, ..."""
    if isinstance(size, bool) or not isinstance(size, int) or size < 1:
        raise ValueError(f"size must be an integer >= 1, got {size!r}")
    return [f"ACC-{i:04d}" for i in range(1, size + 1)]



COLUMNS = (
    "transaction_id",
    "account_id",
    "transaction_ts",
    "transaction_type",
    "amount",
    "currency",
    "status",
    "source_updated_ts",
)

# (values, weights) pairs used with rng.choices
TYPE_CHOICES = (("CREDIT", "DEBIT"), (45, 55))
CURRENCY_CHOICES = (("INR", "USD"), (70, 30))
STATUS_CHOICES = (("POSTED", "PENDING", "FAILED"), (80, 12, 8))

MIN_CENTS = 100  # 1.00
MAX_CENTS = 500_000  # 5000.00
MAX_UPDATE_DELAY_SECONDS = 3600


def make_valid_transaction(
    rng: random.Random, index: int, accounts: list[str], config: GeneratorConfig
) -> dict[str, str]:
    """Build one clean transaction. All values are strings, keys follow COLUMNS."""
    if isinstance(index, bool) or not isinstance(index, int) or index < 1:
        raise ValueError(f"index must be an integer >= 1, got {index!r}")
    if not accounts:
        raise ValueError("accounts must not be empty")

    start = datetime.combine(config.start_date, time.min)
    txn_ts = start + timedelta(seconds=rng.randrange(config.window_days * 86400))
    updated_ts = txn_ts + timedelta(seconds=rng.randint(1, MAX_UPDATE_DELAY_SECONDS))

    return {
        "transaction_id": f"TXN-{index:06d}",
        "account_id": rng.choice(accounts),
        "transaction_ts": format_ts(txn_ts),
        "transaction_type": rng.choices(*TYPE_CHOICES)[0],
        "amount": format_amount(rng.randint(MIN_CENTS, MAX_CENTS)),
        "currency": rng.choices(*CURRENCY_CHOICES)[0],
        "status": rng.choices(*STATUS_CHOICES)[0],
        "source_updated_ts": format_ts(updated_ts),
    }


MUTABLE_AMOUNT_CHANGE_PROBABILITY = 0.30
MAX_VERSION_DELAY_SECONDS = 86_400
ALL_STATUSES = STATUS_CHOICES[0]


def make_duplicate_version(
    rng: random.Random, original: dict[str, str]
) -> dict[str, str]:
    """Return a strictly later version of `original`; does not mutate it."""
    missing = [c for c in COLUMNS if c not in original]
    if missing:
        raise ValueError(f"original is missing columns: {missing}")

    version = dict(original)
    version["status"] = rng.choice(
        [s for s in ALL_STATUSES if s != original["status"]]
    )
    if rng.random() < MUTABLE_AMOUNT_CHANGE_PROBABILITY:
        version["amount"] = format_amount(rng.randint(MIN_CENTS, MAX_CENTS))
    prev_updated = datetime.fromisoformat(original["source_updated_ts"])
    version["source_updated_ts"] = format_ts(
        prev_updated + timedelta(seconds=rng.randint(1, MAX_VERSION_DELAY_SECONDS))
    )
    return version


DEFECTS = (
    "missing_transaction_id",
    "missing_account_id",
    "malformed_transaction_ts",
    "invalid_transaction_type",
    "non_positive_amount",
    "unsupported_currency",
    "invalid_status",
)

BAD_TIMESTAMPS = ("2025-13-45T99:99:99", "not-a-timestamp", "2025/01/01 25:61")
BAD_CURRENCIES = ("EUR", "")
BAD_STATUSES = ("REVERSED", "UNKNOWN")


def apply_defect(
    rng: random.Random, record: dict[str, str], defect: str
) -> dict[str, str]:
    """Return a copy of `record` with exactly one field corrupted."""
    if defect not in DEFECTS:
        raise ValueError(f"unknown defect {defect!r}; valid defects: {list(DEFECTS)}")

    bad = dict(record)
    if defect == "missing_transaction_id":
        bad["transaction_id"] = ""
    elif defect == "missing_account_id":
        bad["account_id"] = ""
    elif defect == "malformed_transaction_ts":
        bad["transaction_ts"] = rng.choice(BAD_TIMESTAMPS)
    elif defect == "invalid_transaction_type":
        bad["transaction_type"] = "TRANSFER"
    elif defect == "non_positive_amount":
        if rng.random() < 0.5:
            bad["amount"] = format_amount(0)
        else:
            bad["amount"] = format_amount(-rng.randint(MIN_CENTS, MAX_CENTS))
    elif defect == "unsupported_currency":
        bad["currency"] = rng.choice(BAD_CURRENCIES)
    elif defect == "invalid_status":
        bad["status"] = rng.choice(BAD_STATUSES)
    return bad


def generate_rows(config: GeneratorConfig) -> list[dict[str, str]]:
    """Generate exactly config.num_rows rows: valid + duplicate versions + invalid."""
    counts = compute_row_counts(config)
    if counts.duplicate > counts.valid:
        raise ValueError(
            f"duplicate rows ({counts.duplicate}) exceed valid base rows "
            f"({counts.valid}); lower duplicate_rate"
        )

    rng = random.Random(config.seed)
    accounts = make_account_pool(rng, max(10, config.num_rows // 20))

    base = [
        make_valid_transaction(rng, i, accounts, config)
        for i in range(1, counts.valid + 1)
    ]
    duplicates = [
        make_duplicate_version(rng, original)
        for original in rng.sample(base, counts.duplicate)
    ]
    invalid = []
    for k in range(counts.invalid):
        clean = make_valid_transaction(rng, counts.valid + 1 + k, accounts, config)
        invalid.append(apply_defect(rng, clean, DEFECTS[k % len(DEFECTS)]))

    rows = base + duplicates + invalid
    rng.shuffle(rows)
    return rows



def write_csv(rows: list[dict[str, str]], path: str | Path) -> int:
    """Write rows to `path` with the exact COLUMNS schema; return rows written."""
    expected = list(COLUMNS)
    for i, row in enumerate(rows):
        if list(row.keys()) != expected and set(row.keys()) != set(expected):
            raise ValueError(
                f"row {i} has columns {list(row.keys())}; expected {expected}"
            )

    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)



DEFAULT_OUTPUT = "data/sample/transactions.csv"


def build_parser() -> argparse.ArgumentParser:
    defaults = GeneratorConfig()
    parser = argparse.ArgumentParser(
        prog="python -m finance_pipeline.generate_data",
        description="Generate synthetic finance transactions as CSV.",
    )
    parser.add_argument("--rows", type=int, default=defaults.num_rows,
                        help="total CSV data rows, including duplicates and invalid rows")
    parser.add_argument("--seed", type=int, default=defaults.seed)
    parser.add_argument("--output", default=DEFAULT_OUTPUT,
                        help="output CSV path (relative to the current directory)")
    parser.add_argument("--invalid-rate", type=float, default=defaults.invalid_rate)
    parser.add_argument("--duplicate-rate", type=float, default=defaults.duplicate_rate)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        config = GeneratorConfig(
            num_rows=args.rows,
            seed=args.seed,
            invalid_rate=args.invalid_rate,
            duplicate_rate=args.duplicate_rate,
        )
        rows = generate_rows(config)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    written = write_csv(rows, args.output)
    counts = compute_row_counts(config)
    print(
        f"wrote {written} rows to {args.output} "
        f"(valid={counts.valid}, duplicate_versions={counts.duplicate}, "
        f"invalid={counts.invalid}, seed={config.seed})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
