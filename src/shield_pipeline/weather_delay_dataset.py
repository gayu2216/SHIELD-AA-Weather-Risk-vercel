"""Create a reduced modeling dataset for weather-delay prediction."""

from __future__ import annotations

from pathlib import Path
import math

import pandas as pd

from shield_pipeline.bts_schema import normalize_bts_columns
from shield_pipeline.dfw_master import stream_dfw_hub_master
from shield_pipeline.weather.noaa_global_hourly import (
    NOAAGlobalHourlyCache,
    all_noaa_wx_column_names,
    enrich_with_noaa_weather,
)

KEEP_COLUMNS = [
    "FL_DATE",
    "MONTH",
    "DAY_OF_MONTH",
    "ORIGIN",
    "DEST",
    "DISTANCE",
    "WEATHER_DELAY",
    "CANCELLED",
    "CRS_DEP_TIME",
    "DEP_TIME",
    "CRS_ARR_TIME",
    "ARR_TIME",
    "CRS_ELAPSED_TIME",
    "ACTUAL_ELAPSED_TIME",
]


def _select_training_subset(
    chunk: pd.DataFrame,
    *,
    rows_needed: int | None,
    positive_fraction: float,
    random_seed: int,
) -> pd.DataFrame:
    if rows_needed is None or len(chunk) <= rows_needed:
        return chunk

    df = chunk.copy()
    weather_delay = pd.to_numeric(df.get("WEATHER_DELAY"), errors="coerce").fillna(0)
    positive_mask = weather_delay > 0
    positives = df.loc[positive_mask]
    negatives = df.loc[~positive_mask]

    target_positives = min(len(positives), math.ceil(rows_needed * positive_fraction))
    target_negatives = min(len(negatives), rows_needed - target_positives)

    if target_negatives < rows_needed - target_positives:
        extra = rows_needed - (target_positives + target_negatives)
        target_positives = min(len(positives), target_positives + extra)

    if target_positives < rows_needed - target_negatives:
        extra = rows_needed - (target_positives + target_negatives)
        target_negatives = min(len(negatives), target_negatives + extra)

    sampled_parts: list[pd.DataFrame] = []
    if target_positives > 0:
        sampled_parts.append(positives.sample(n=target_positives, random_state=random_seed))
    if target_negatives > 0:
        sampled_parts.append(negatives.sample(n=target_negatives, random_state=random_seed + 1))

    if not sampled_parts:
        return df.iloc[:0].copy()

    sampled = pd.concat(sampled_parts, ignore_index=False)
    sampled = sampled.sample(frac=1, random_state=random_seed + 2)
    return sampled.reset_index(drop=True)


def _load_global_training_subset(
    master_file: Path,
    *,
    subset_rows: int,
    positive_fraction: float,
    random_seed: int,
) -> pd.DataFrame:
    df = pd.read_csv(master_file, usecols=lambda c: c in KEEP_COLUMNS, low_memory=False)
    df = normalize_bts_columns(df)
    df = df[[c for c in KEEP_COLUMNS if c in df.columns]].copy()
    if "WEATHER_DELAY" in df.columns:
        df["WEATHER_DELAY"] = pd.to_numeric(df["WEATHER_DELAY"], errors="coerce")
    return _select_training_subset(
        df,
        rows_needed=subset_rows,
        positive_fraction=positive_fraction,
        random_seed=random_seed,
    )


def build_weather_delay_model_dataset(
    *,
    raw_file: Path,
    master_file: Path,
    output_file: Path,
    cache_dir: Path,
    master_chunksize: int = 100_000,
    enrich_chunksize: int = 10_000,
    prefetch_workers: int = 4,
    max_rows: int | None = None,
    subset_rows: int | None = None,
    positive_fraction: float = 0.35,
    random_seed: int = 42,
) -> int:
    if not master_file.exists():
        stream_dfw_hub_master(raw_file, master_file, chunksize=master_chunksize)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    if output_file.exists():
        output_file.unlink()

    cache = NOAAGlobalHourlyCache(cache_dir=cache_dir)
    written = 0
    first_write = True
    if subset_rows is not None:
        source_df = _load_global_training_subset(
            master_file,
            subset_rows=subset_rows,
            positive_fraction=positive_fraction,
            random_seed=random_seed,
        )
        source_iter = (
            source_df.iloc[start:start + enrich_chunksize].copy()
            for start in range(0, len(source_df), enrich_chunksize)
        )
        limit = subset_rows
    else:
        source_iter = pd.read_csv(master_file, chunksize=enrich_chunksize, low_memory=False)
        limit = max_rows

    for chunk in source_iter:
        chunk = normalize_bts_columns(chunk)
        chunk = chunk[[c for c in KEEP_COLUMNS if c in chunk.columns]].copy()
        for col in ["WEATHER_DELAY", "DISTANCE", "CANCELLED", "CRS_ELAPSED_TIME", "ACTUAL_ELAPSED_TIME"]:
            if col in chunk.columns:
                chunk[col] = pd.to_numeric(chunk[col], errors="coerce")

        if chunk.empty:
            continue

        chunk = enrich_with_noaa_weather(chunk, cache, prefetch_workers=prefetch_workers)
        chunk["weather_delay_target"] = chunk["WEATHER_DELAY"].fillna(0).clip(lower=0)
        chunk["weather_delay_binary"] = (chunk["weather_delay_target"] > 0).astype(int)
        chunk["weather_data_source"] = "NOAA_NCEI_Global_Hourly"
        chunk = chunk[
            [
                "FL_DATE",
                "MONTH",
                "DAY_OF_MONTH",
                "ORIGIN",
                "DEST",
                "DISTANCE",
                "weather_delay_target",
                "weather_delay_binary",
                "WEATHER_DELAY",
                "CANCELLED",
                "CRS_DEP_TIME",
                "DEP_TIME",
                "CRS_ARR_TIME",
                "ARR_TIME",
                "CRS_ELAPSED_TIME",
                "ACTUAL_ELAPSED_TIME",
                "weather_data_source",
                *all_noaa_wx_column_names(),
            ]
        ]

        if limit is not None:
            remaining = limit - written
            if remaining <= 0:
                break
            if len(chunk) > remaining:
                chunk = chunk.iloc[:remaining].copy()

        chunk.to_csv(output_file, mode="a", index=False, header=first_write)
        first_write = False
        written += len(chunk)
        if limit is not None and written >= limit:
            break

    return written
