#!/usr/bin/env python3
"""Build a reduced flight-level dataset for weather-delay modeling."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from shield_pipeline.weather_delay_dataset import build_weather_delay_model_dataset  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--raw",
        type=Path,
        default=Path("data/raw/bts_master_raw.csv"),
        help="Combined BTS raw file.",
    )
    ap.add_argument(
        "--master",
        type=Path,
        default=Path("data/processed/dfw_hub_flights_master.csv"),
        help="Normalized DFW hub-edge master file. Built automatically if missing.",
    )
    ap.add_argument(
        "--output",
        type=Path,
        default=Path("data/processed/weather_delay_model_master.csv"),
        help="Reduced modeling dataset output path.",
    )
    ap.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("data/raw/noaa_global_hourly_cache"),
        help="On-disk cache for NOAA station metadata and station-year observations.",
    )
    ap.add_argument("--master-chunksize", type=int, default=100_000)
    ap.add_argument("--enrich-chunksize", type=int, default=10_000)
    ap.add_argument("--prefetch-workers", type=int, default=4, help="Parallel NOAA station-year cache warmers.")
    ap.add_argument("--max-rows", type=int, default=None, help="Optional smoke-test limit.")
    ap.add_argument(
        "--subset-rows",
        type=int,
        default=None,
        help="Build a smaller training subset instead of the full dataset.",
    )
    ap.add_argument(
        "--positive-fraction",
        type=float,
        default=0.35,
        help="Target share of WEATHER_DELAY>0 rows inside a subset build.",
    )
    ap.add_argument("--random-seed", type=int, default=42)
    args = ap.parse_args()

    written = build_weather_delay_model_dataset(
        raw_file=args.raw,
        master_file=args.master,
        output_file=args.output,
        cache_dir=args.cache_dir,
        master_chunksize=args.master_chunksize,
        enrich_chunksize=args.enrich_chunksize,
        prefetch_workers=args.prefetch_workers,
        max_rows=args.max_rows,
        subset_rows=args.subset_rows,
        positive_fraction=args.positive_fraction,
        random_seed=args.random_seed,
    )
    print(f"Wrote {written:,} rows to {args.output.resolve()}")
    print(f"NOAA cache: {args.cache_dir.resolve()}")


if __name__ == "__main__":
    main()
