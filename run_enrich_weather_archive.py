#!/usr/bin/env python3
"""
Add Open-Meteo historical weather at origin (departure) and destination (arrival).

Creates new columns wx_origin_dep_* and wx_dest_arr_* if missing; fills NaN unless --force.
Requires network access to archive-api.open-meteo.com (rate-limited with small sleeps).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from shield_pipeline.weather.historical_archive import ArchiveDayCache  # noqa: E402
from shield_pipeline.weather_enrichment import all_wx_column_names, enrich_chunk  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--input",
        type=Path,
        default=Path("data/processed/dfw_hub_flights_master.csv"),
        help="Hub-edge master from run_build_dfw_master.py",
    )
    ap.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Defaults to input path with _weather suffix if omitted",
    )
    ap.add_argument("--chunksize", type=int, default=2_000, help="Rows per batch (API cache-friendly).")
    ap.add_argument("--sleep", type=float, default=0.12, help="Seconds between new (airport,day) API calls.")
    ap.add_argument("--force", action="store_true", help="Overwrite existing wx_* values.")
    ap.add_argument("--max-rows", type=int, default=None, help="Stop after this many input rows (debug).")
    args = ap.parse_args()

    out_path = args.output
    if out_path is None:
        stem = args.input.stem + "_weather"
        out_path = args.input.with_name(stem + ".csv")

    if not args.input.exists():
        raise SystemExit(f"Missing input: {args.input}")

    if out_path.exists():
        out_path.unlink()

    cache = ArchiveDayCache(sleep_s=args.sleep)
    first = True
    written = 0

    reader = pd.read_csv(args.input, chunksize=args.chunksize, low_memory=False)
    for chunk in reader:
        if args.max_rows is not None:
            remaining = args.max_rows - written
            if remaining <= 0:
                break
            if len(chunk) > remaining:
                chunk = chunk.iloc[:remaining].copy()
        chunk = enrich_chunk(chunk, cache, force=args.force)
        chunk.to_csv(out_path, mode="a", index=False, header=first)
        first = False
        written += len(chunk)
        print(f"Wrote {written:,} rows → {out_path.name}", flush=True)
        if args.max_rows is not None and written >= args.max_rows:
            break

    print("Done. New columns (if added):")
    for c in all_wx_column_names():
        print(f"  - {c}")
    print(f"Output: {out_path.resolve()}")


if __name__ == "__main__":
    main()
