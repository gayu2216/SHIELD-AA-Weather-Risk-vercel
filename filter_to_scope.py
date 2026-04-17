#!/usr/bin/env python3
"""
Legacy entrypoint — builds the DFW hub-edge master (AA only; ORIGIN or DEST = DFW).

For the same logic from the package, use: PYTHONPATH=src python run_build_dfw_master.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from shield_pipeline.dfw_master import stream_dfw_hub_master  # noqa: E402

INPUT_FILE = Path("data/raw/bts_master_raw.csv")
OUTPUT_FILE = Path("data/processed/dfw_hub_flights_master.csv")


if __name__ == "__main__":
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    total_in, total_out = stream_dfw_hub_master(INPUT_FILE, OUTPUT_FILE)
    print(f"Input rows scanned: {total_in:,}")
    print(f"DFW hub-edge rows written: {total_out:,}")
    print(f"Saved: {OUTPUT_FILE.resolve()}")
