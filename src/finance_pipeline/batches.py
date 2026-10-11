"""Split generated rows into batch files, for incremental-processing demos and tests."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from finance_pipeline.generate_data import GeneratorConfig, generate_rows, write_csv


def split_into_batches(rows: list[dict], n: int) -> list[list[dict]]:
    """Split rows into n contiguous chunks (sizes differ by at most 1), keeping order.

    The generator shuffles rows, so older versions of a transaction naturally land
    in later batches than newer ones: late arrivals.
    """
    if isinstance(n, bool) or not isinstance(n, int) or not 1 <= n <= max(len(rows), 1):
        raise ValueError(f"n must be an integer between 1 and {max(len(rows), 1)}, got {n!r}")
    size, extra = divmod(len(rows), n)
    chunks, start = [], 0
    for i in range(n):
        end = start + size + (1 if i < extra else 0)
        chunks.append(rows[start:end])
        start = end
    return chunks


def write_batches(rows: list[dict], n: int, output_dir, prefix: str = "transactions") -> list[Path]:
    paths = []
    for i, chunk in enumerate(split_into_batches(rows, n), start=1):
        p = Path(output_dir) / f"{prefix}_{i:04d}.csv"
        write_csv(chunk, p)
        paths.append(p)
    return paths


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m finance_pipeline.batches",
                                description="Generate synthetic data split into batch files.")
    p.add_argument("--rows", type=int, default=1000)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--batches", type=int, default=3)
    p.add_argument("--output-dir", default="data/incoming")
    args = p.parse_args(argv)
    try:
        rows = generate_rows(GeneratorConfig(num_rows=args.rows, seed=args.seed))
        paths = write_batches(rows, args.batches, args.output_dir)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(f"wrote {len(paths)} batch files ({len(rows)} rows) to {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
