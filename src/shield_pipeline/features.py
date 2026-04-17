from __future__ import annotations

from typing import Iterable

import pandas as pd
from pathlib import Path

from shield_pipeline.bts_schema import ALL_POSSIBLE_COLS, normalize_bts_columns


TARGET_AIRPORTS: list[str] = [
    "LAX", "LAS", "ATL", "ORD", "DEN", "PHX", "MIA", "JFK",
    "LGA", "CLT", "MCO", "SEA", "BOS", "SFO", "IAH", "HOU",
    "SAN", "PHL", "DCA", "MSP", "DTW", "MDW", "SLC", "PDX",
    "AUS", "SAT", "ELP", "OKC", "TUL", "MSY", "MEM", "BNA",
    "RDU", "TPA", "FLL",
]


def _required_cols(df: pd.DataFrame, columns: Iterable[str]) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def scope_aa_dfw(data: pd.DataFrame) -> pd.DataFrame:
    """AA-only rows where the flight touches DFW (origin or destination). No other airport filter."""
    data = normalize_bts_columns(data)
    _required_cols(data, ["REPORTING_AIRLINE", "ORIGIN", "DEST"])
    aa = data[data["REPORTING_AIRLINE"] == "AA"].copy()
    return aa[(aa["ORIGIN"] == "DFW") | (aa["DEST"] == "DFW")].copy()


def process_raw_to_scoped_and_summary(
    raw_file: Path, scoped_file: Path, chunksize: int = 200_000
) -> pd.DataFrame:
    """
    Stream raw BTS master file in chunks, filter to scoped rows, write scoped CSV,
    and aggregate airport-month summary incrementally.
    """
    if not raw_file.exists():
        raise FileNotFoundError(f"Missing required file: {raw_file}")

    scoped_file.parent.mkdir(parents=True, exist_ok=True)
    if scoped_file.exists():
        scoped_file.unlink()

    first_write = True
    chunk_summaries: list[pd.DataFrame] = []

    reader = pd.read_csv(
        raw_file,
        usecols=lambda c: c in ALL_POSSIBLE_COLS,
        chunksize=chunksize,
        low_memory=False,
    )

    for chunk in reader:
        chunk = normalize_bts_columns(chunk)
        try:
            scoped = scope_aa_dfw(chunk)
        except ValueError:
            # Skip malformed chunks missing required columns.
            continue

        if scoped.empty:
            continue

        scoped.to_csv(scoped_file, mode="a", index=False, header=first_write)
        first_write = False

        for col in ["ARR_DELAY", "WEATHER_DELAY", "CANCELLED", "MONTH"]:
            if col not in scoped.columns:
                scoped[col] = 0

        for c in ["ARR_DELAY", "WEATHER_DELAY", "CANCELLED"]:
            if c in scoped.columns:
                scoped[c] = pd.to_numeric(scoped[c], errors="coerce")
        scoped["ARR_DELAY"] = scoped["ARR_DELAY"].fillna(0).clip(lower=0)
        scoped["WEATHER_DELAY"] = scoped["WEATHER_DELAY"].fillna(0).clip(lower=0)
        scoped["CANCELLED"] = scoped["CANCELLED"].fillna(0).clip(lower=0)
        scoped["weather_delay_flight_flag"] = (scoped["WEATHER_DELAY"] > 0).astype(int)

        chunk_summary = (
            scoped.groupby(["ORIGIN", "MONTH"], as_index=False)
            .agg(
                total_flights=("ORIGIN", "count"),
                arr_delay_sum=("ARR_DELAY", "sum"),
                weather_delay_sum=("WEATHER_DELAY", "sum"),
                weather_delay_flights=("weather_delay_flight_flag", "sum"),
                cancelled_flights=("CANCELLED", "sum"),
            )
        )
        chunk_summaries.append(chunk_summary)

    if not chunk_summaries:
        return pd.DataFrame(
            columns=[
                "ORIGIN",
                "MONTH",
                "total_flights",
                "avg_arr_delay",
                "avg_weather_delay",
                "weather_delay_flights",
                "cancelled_flights",
                "weather_delay_rate",
                "cancel_rate",
            ]
        )

    combined = pd.concat(chunk_summaries, ignore_index=True)
    grouped = (
        combined.groupby(["ORIGIN", "MONTH"], as_index=False)
        .agg(
            total_flights=("total_flights", "sum"),
            arr_delay_sum=("arr_delay_sum", "sum"),
            weather_delay_sum=("weather_delay_sum", "sum"),
            weather_delay_flights=("weather_delay_flights", "sum"),
            cancelled_flights=("cancelled_flights", "sum"),
        )
    )
    grouped["avg_arr_delay"] = grouped["arr_delay_sum"] / grouped["total_flights"]
    grouped["avg_weather_delay"] = grouped["weather_delay_sum"] / grouped["total_flights"]
    grouped["weather_delay_rate"] = grouped["weather_delay_flights"] / grouped["total_flights"]
    grouped["cancel_rate"] = grouped["cancelled_flights"] / grouped["total_flights"]

    return grouped[
        [
            "ORIGIN",
            "MONTH",
            "total_flights",
            "avg_arr_delay",
            "avg_weather_delay",
            "weather_delay_flights",
            "cancelled_flights",
            "weather_delay_rate",
            "cancel_rate",
        ]
    ]


def build_airport_month_summary(scoped: pd.DataFrame) -> pd.DataFrame:
    scoped = normalize_bts_columns(scoped)
    _required_cols(scoped, ["MONTH", "ORIGIN", "ARR_DELAY", "WEATHER_DELAY", "CANCELLED"])

    df = scoped.copy()
    for c in ["ARR_DELAY", "WEATHER_DELAY", "CANCELLED"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["ARR_DELAY"] = df["ARR_DELAY"].fillna(0).clip(lower=0)
    df["WEATHER_DELAY"] = df["WEATHER_DELAY"].fillna(0).clip(lower=0)
    df["CANCELLED"] = df["CANCELLED"].fillna(0).clip(lower=0)
    df["weather_delay_flight_flag"] = (df["WEATHER_DELAY"] > 0).astype(int)

    summary = (
        df.groupby(["ORIGIN", "MONTH"], as_index=False)
        .agg(
            total_flights=("ORIGIN", "count"),
            avg_arr_delay=("ARR_DELAY", "mean"),
            avg_weather_delay=("WEATHER_DELAY", "mean"),
            weather_delay_flights=("weather_delay_flight_flag", "sum"),
            cancelled_flights=("CANCELLED", "sum"),
        )
    )
    summary["weather_delay_rate"] = summary["weather_delay_flights"] / summary["total_flights"]
    summary["cancel_rate"] = summary["cancelled_flights"] / summary["total_flights"]
    return summary

