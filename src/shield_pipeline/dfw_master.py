"""
American Airlines flights that touch DFW only (origin or destination = DFW).

No secondary airport filter — this is the hub-edge master used for a fresh modeling base.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from shield_pipeline.bts_schema import ALL_POSSIBLE_COLS, COLUMN_ALIASES, normalize_bts_columns


def scope_aa_dfw_hub_edges(df: pd.DataFrame) -> pd.DataFrame:
    """AA-only rows where the flight either departs from or arrives at DFW."""
    df = normalize_bts_columns(df)
    for col in ("REPORTING_AIRLINE", "ORIGIN", "DEST"):
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")
    aa = df[df["REPORTING_AIRLINE"] == "AA"].copy()
    mask = (aa["ORIGIN"] == "DFW") | (aa["DEST"] == "DFW")
    return aa.loc[mask].copy()


def stream_dfw_hub_master(
    raw_file: Path,
    out_file: Path,
    *,
    chunksize: int = 100_000,
) -> tuple[int, int]:
    """
    Read combined BTS master CSV in chunks, keep only AA flights with DFW as origin or dest,
    write normalized columns to `out_file`.

    Returns (total_input_rows, total_output_rows).
    """
    if not raw_file.exists():
        raise FileNotFoundError(f"Missing raw file: {raw_file}")

    out_file.parent.mkdir(parents=True, exist_ok=True)
    if out_file.exists():
        out_file.unlink()

    total_in = 0
    total_out = 0
    first_write = True

    reader = pd.read_csv(
        raw_file,
        usecols=lambda c: c in ALL_POSSIBLE_COLS,
        chunksize=chunksize,
        low_memory=False,
    )

    keep_cols = [c for c in COLUMN_ALIASES.keys()]

    for chunk in reader:
        total_in += len(chunk)
        try:
            hub = scope_aa_dfw_hub_edges(chunk)
        except ValueError:
            continue

        if hub.empty:
            continue

        hub = hub[[c for c in keep_cols if c in hub.columns]]
        total_out += len(hub)
        hub.to_csv(out_file, mode="a", index=False, header=first_write)
        first_write = False

    return total_in, total_out
