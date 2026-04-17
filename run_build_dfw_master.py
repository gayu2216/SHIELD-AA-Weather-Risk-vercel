#!/usr/bin/env python3
"""Build DFW hub-edge master CSV: AA flights with ORIGIN=DFW or DEST=DFW (no other airport filter)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from shield_pipeline.dfw_master import stream_dfw_hub_master  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser(description="Stream-filter raw BTS master to DFW hub-edge rows.")
    p.add_argument(
        "--raw",
        type=Path,
        default=Path("data/raw/bts_master_raw.csv"),
        help="Combined monthly BTS file (from combine_all.py).",
    )
    p.add_argument(
        "--out",
        type=Path,
        default=Path("data/processed/dfw_hub_flights_master.csv"),
        help="Output CSV (normalized column names).",
    )
    p.add_argument("--chunksize", type=int, default=100_000)
    args = p.parse_args()

    total_in, total_out = stream_dfw_hub_master(args.raw, args.out, chunksize=args.chunksize)
    print(f"Input rows scanned: {total_in:,}")
    print(f"DFW hub-edge rows written: {total_out:,}")
    print(f"Wrote: {args.out.resolve()}")


if __name__ == "__main__":
    main()
