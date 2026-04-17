"""Historical airport weather features from NOAA NCEI Global Hourly / ISD."""

from __future__ import annotations

from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests

from shield_pipeline.weather.airport_timezones import timezone_for_airport
from shield_pipeline.weather_enrichment import hhmm_to_minutes

NOAA_ISD_HISTORY_URL = "https://www.ncei.noaa.gov/pub/data/noaa/isd-history.txt"
NOAA_GLOBAL_HOURLY_URL = "https://www.ncei.noaa.gov/access/services/data/v1"
NOAA_USER_AGENT = "SHIELD-AA-Weather-Risk/0.1 (historical-weather-enrichment)"

NOAA_WX_FEATURE_COLUMNS = [
    "obs_time_utc",
    "obs_age_minutes",
    "temperature_c",
    "dewpoint_c",
    "visibility_m",
    "ceiling_ft",
    "wind_speed_ms",
    "sea_level_pressure_hpa",
    "precip_1h_flag",
    "ifr_flag",
    "station_id",
    "report_type",
]

NOAA_ORIGIN_PREFIX = "noaa_origin_dep_"
NOAA_DEST_PREFIX = "noaa_dest_arr_"

SPECIAL_IATA_TO_ICAO = {
    "ANC": "PANC",
    "HNL": "PHNL",
    "KOA": "PHKO",
    "LIH": "PHLI",
    "OGG": "PHOG",
    "SJU": "TJSJ",
    "STT": "TIST",
    "STX": "TISX",
}


@dataclass(frozen=True)
class ISDStation:
    station_id: str
    icao: str
    name: str
    latitude: float | None
    longitude: float | None
    begin: str
    end: str


def all_noaa_wx_column_names() -> list[str]:
    origin = [f"{NOAA_ORIGIN_PREFIX}{c}" for c in NOAA_WX_FEATURE_COLUMNS]
    dest = [f"{NOAA_DEST_PREFIX}{c}" for c in NOAA_WX_FEATURE_COLUMNS]
    return origin + dest


def iata_to_icao(code: str) -> str | None:
    clean = str(code or "").strip().upper()
    if len(clean) != 3:
        return None
    if clean in SPECIAL_IATA_TO_ICAO:
        return SPECIAL_IATA_TO_ICAO[clean]
    return f"K{clean}"


def _token_before_comma(raw: Any) -> str | None:
    text = str(raw or "").strip()
    if not text:
        return None
    return text.split(",", 1)[0].strip() or None


def _parse_scaled_value(raw: Any, *, scale: float = 10.0, missing: set[str] | None = None) -> float | None:
    token = _token_before_comma(raw)
    if token is None:
        return None
    if missing and token in missing:
        return None
    try:
        return int(token) / scale
    except ValueError:
        return None


def _parse_int_value(raw: Any, *, missing: set[str] | None = None) -> int | None:
    token = _token_before_comma(raw)
    if token is None:
        return None
    if missing and token in missing:
        return None
    try:
        return int(token)
    except ValueError:
        return None


def _parse_wind_speed_ms(raw: Any) -> float | None:
    text = str(raw or "").strip()
    if not text:
        return None
    parts = [p.strip() for p in text.split(",")]
    if len(parts) < 4:
        return None
    token = parts[3]
    if token in {"9999", "99999"}:
        return None
    try:
        return int(token) / 10.0
    except ValueError:
        return None


def _parse_precip_flag(raw: Any) -> float | None:
    text = str(raw or "").strip()
    if not text:
        return None
    parts = [p.strip() for p in text.split(",")]
    if len(parts) < 2:
        return None
    amount = parts[1]
    if amount in {"9999", "99999"}:
        return None
    try:
        return float(int(amount) > 0)
    except ValueError:
        return None


def _normalize_noaa_observations(rows: list[dict[str, Any]], station_id: str) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=NOAA_WX_FEATURE_COLUMNS)

    df = pd.DataFrame(rows)
    out = pd.DataFrame(
        {
            "obs_time_utc": pd.to_datetime(df.get("DATE"), errors="coerce", utc=True),
            "temperature_c": df.get("TMP", pd.Series(dtype=object)).map(
                lambda x: _parse_scaled_value(x, missing={"+9999", "-9999", "9999"})
            ),
            "dewpoint_c": df.get("DEW", pd.Series(dtype=object)).map(
                lambda x: _parse_scaled_value(x, missing={"+9999", "-9999", "9999"})
            ),
            "visibility_m": df.get("VIS", pd.Series(dtype=object)).map(
                lambda x: _parse_int_value(x, missing={"999999", "99999"})
            ),
            "ceiling_ft": df.get("CIG", pd.Series(dtype=object)).map(
                lambda x: _parse_int_value(x, missing={"99999"})
            ),
            "wind_speed_ms": df.get("WND", pd.Series(dtype=object)).map(_parse_wind_speed_ms),
            "sea_level_pressure_hpa": df.get("SLP", pd.Series(dtype=object)).map(
                lambda x: _parse_scaled_value(x, missing={"99999"})
            ),
            "precip_1h_flag": df.get("AA1", pd.Series(dtype=object)).map(_parse_precip_flag),
            "report_type": df.get("REPORT_TYPE", pd.Series(dtype=object)).astype(str),
        }
    )
    out["station_id"] = station_id
    out = out.dropna(subset=["obs_time_utc"]).copy()
    out["ifr_flag"] = (
        ((out["visibility_m"].notna()) & (out["visibility_m"] < 4_800))
        | ((out["ceiling_ft"].notna()) & (out["ceiling_ft"] < 1_000))
    ).astype(float)
    out["report_rank"] = out["report_type"].map({"FM-15": 0, "FM-16": 1, "FM-12": 2}).fillna(9)
    out = out.sort_values(["obs_time_utc", "report_rank"]).drop_duplicates("obs_time_utc", keep="first")
    out = out.drop(columns=["report_rank"]).reset_index(drop=True)
    return out


def _normalize_cached_obs_time(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["obs_time_utc"] = pd.to_datetime(out["obs_time_utc"], errors="coerce", utc=True)
    out["station_id"] = out["station_id"].astype(str)
    return out


def _empty_feature_frame(index: pd.Index, prefix: str) -> pd.DataFrame:
    cols: dict[str, pd.Series] = {}
    for col in NOAA_WX_FEATURE_COLUMNS:
        name = f"{prefix}{col}"
        if col == "obs_time_utc":
            cols[name] = pd.Series(pd.NaT, index=index, dtype="datetime64[ns, UTC]")
        elif col in {"station_id", "report_type"}:
            cols[name] = pd.Series(pd.NA, index=index, dtype="object")
        else:
            cols[name] = pd.Series(np.nan, index=index, dtype="float64")
    return pd.DataFrame(cols, index=index)


def _choose_operation_hhmm(df: pd.DataFrame, *, actual_col: str, scheduled_col: str) -> pd.Series:
    cancelled = pd.to_numeric(df.get("CANCELLED", 0), errors="coerce").fillna(0) != 0
    actual = df.get(actual_col)
    scheduled = df.get(scheduled_col)
    if actual is None:
        return scheduled
    if scheduled is None:
        return actual
    return actual.where(~cancelled & actual.notna(), scheduled)


def _hhmm_series_to_minutes(series: pd.Series) -> pd.Series:
    return series.map(hhmm_to_minutes).astype("float")


def _compute_local_event_fields(df: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
    dep_date = pd.to_datetime(df.get("FL_DATE"), errors="coerce").dt.normalize()
    dep_minutes = _hhmm_series_to_minutes(_choose_operation_hhmm(df, actual_col="DEP_TIME", scheduled_col="CRS_DEP_TIME"))
    arr_minutes = _hhmm_series_to_minutes(_choose_operation_hhmm(df, actual_col="ARR_TIME", scheduled_col="CRS_ARR_TIME"))

    arr_date = dep_date.copy()
    elapsed = pd.to_numeric(df.get("CRS_ELAPSED_TIME"), errors="coerce")
    dep_dt = dep_date + pd.to_timedelta(dep_minutes, unit="m")

    with_elapsed = dep_date.notna() & dep_minutes.notna() & elapsed.notna() & (elapsed > 0)
    arr_date.loc[with_elapsed] = (
        dep_dt.loc[with_elapsed] + pd.to_timedelta(elapsed.loc[with_elapsed], unit="m")
    ).dt.normalize()

    overnight = (~with_elapsed) & dep_date.notna() & dep_minutes.notna() & arr_minutes.notna() & (arr_minutes < dep_minutes)
    arr_date.loc[overnight] = dep_date.loc[overnight] + pd.Timedelta(days=1)
    return dep_date, dep_minutes, arr_date, arr_minutes


def _convert_local_fields_to_utc(
    airport_codes: pd.Series,
    local_dates: pd.Series,
    minutes: pd.Series,
) -> pd.Series:
    result = pd.Series(pd.NaT, index=airport_codes.index, dtype="datetime64[ns, UTC]")
    airport_codes = airport_codes.astype(str).str.upper()
    tz_series = airport_codes.map(timezone_for_airport)

    for tz_name in sorted(set(tz_series.dropna())):
        mask = tz_series.eq(tz_name) & local_dates.notna() & minutes.notna()
        if not mask.any():
            continue
        naive_local = local_dates.loc[mask] + pd.to_timedelta(minutes.loc[mask], unit="m")
        localized = naive_local.dt.tz_localize(tz_name, ambiguous="NaT", nonexistent="shift_forward").dt.tz_convert("UTC")
        result.loc[mask] = localized

    return result


def _build_weather_join_targets(
    df: pd.DataFrame,
    *,
    airport_col: str,
    target_times_utc: pd.Series,
    cache: "NOAAGlobalHourlyCache",
) -> pd.DataFrame:
    targets = pd.DataFrame(
        {
            "row_id": df.index,
            "station_id": df[airport_col].astype(str).str.upper().map(cache.station_id_for_airport),
            "target_time_utc": pd.to_datetime(target_times_utc, errors="coerce", utc=True),
        }
    )
    return targets.dropna(subset=["station_id", "target_time_utc"]).copy()


class ISDStationRegistry:
    def __init__(self, cache_dir: Path) -> None:
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_file = self.cache_dir / "isd-history.txt"
        self._stations_by_icao: dict[str, ISDStation] | None = None

    def _download_history(self) -> str:
        resp = requests.get(
            NOAA_ISD_HISTORY_URL,
            timeout=60,
            headers={"User-Agent": NOAA_USER_AGENT},
        )
        resp.raise_for_status()
        text = resp.text
        self.cache_file.write_text(text, encoding="utf-8")
        return text

    def _load_text(self) -> str:
        if self.cache_file.exists():
            return self.cache_file.read_text(encoding="utf-8")
        return self._download_history()

    def _load(self) -> dict[str, ISDStation]:
        if self._stations_by_icao is not None:
            return self._stations_by_icao

        stations: dict[str, ISDStation] = {}
        text = self._load_text()
        for line in text.splitlines():
            if not line or line.startswith("Integrated Surface Database"):
                continue
            if len(line) < 99:
                continue
            usaf = line[0:6].strip()
            wban = line[7:12].strip()
            icao = line[51:55].strip().upper()
            if not icao:
                continue
            station = ISDStation(
                station_id=f"{usaf}{wban}",
                icao=icao,
                name=line[13:43].strip(),
                latitude=float(line[57:64].strip()) if line[57:64].strip() else None,
                longitude=float(line[65:73].strip()) if line[65:73].strip() else None,
                begin=line[82:90].strip(),
                end=line[91:99].strip(),
            )
            current = stations.get(icao)
            if current is None or current.end < station.end:
                stations[icao] = station

        self._stations_by_icao = stations
        return stations

    def station_for_airport(self, airport_code: str) -> ISDStation | None:
        icao = iata_to_icao(airport_code)
        if icao is None:
            return None
        return self._load().get(icao)


class NOAAGlobalHourlyCache:
    def __init__(self, cache_dir: Path, *, max_open_station_years: int = 10) -> None:
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.station_registry = ISDStationRegistry(self.cache_dir)
        self.max_open_station_years = max_open_station_years
        self._memory_cache: OrderedDict[tuple[str, int], pd.DataFrame] = OrderedDict()

    def station_id_for_airport(self, airport_code: str) -> str | None:
        station = self.station_registry.station_for_airport(airport_code)
        return None if station is None else station.station_id

    def _parquet_cache_path(self, station_id: str, year: int) -> Path:
        return self.cache_dir / f"{station_id}_{year}.parquet"

    def _pickle_cache_path(self, station_id: str, year: int) -> Path:
        return self.cache_dir / f"{station_id}_{year}.pkl"

    def _csv_cache_path(self, station_id: str, year: int) -> Path:
        return self.cache_dir / f"{station_id}_{year}.csv"

    def _fetch_station_year(self, station_id: str, year: int) -> pd.DataFrame:
        params = {
            "dataset": "global-hourly",
            "stations": station_id,
            "startDate": f"{year}-01-01",
            "endDate": f"{year}-12-31T23:59:59",
            "format": "json",
            "units": "metric",
            "dataTypes": "DATE,REPORT_TYPE,TMP,DEW,VIS,CIG,WND,SLP,AA1,CALL_SIGN",
        }
        resp = requests.get(
            NOAA_GLOBAL_HOURLY_URL,
            params=params,
            timeout=120,
            headers={"User-Agent": NOAA_USER_AGENT},
        )
        resp.raise_for_status()
        return _normalize_noaa_observations(resp.json(), station_id)

    def _write_cache(self, station_id: str, year: int, df: pd.DataFrame) -> None:
        parquet_path = self._parquet_cache_path(station_id, year)
        try:
            df.to_parquet(parquet_path, index=False)
            return
        except Exception:
            pass

        pickle_path = self._pickle_cache_path(station_id, year)
        try:
            df.to_pickle(pickle_path)
            return
        except Exception:
            pass

        df.to_csv(self._csv_cache_path(station_id, year), index=False)

    def _read_cache(self, station_id: str, year: int) -> pd.DataFrame | None:
        parquet_path = self._parquet_cache_path(station_id, year)
        if parquet_path.exists():
            return _normalize_cached_obs_time(pd.read_parquet(parquet_path))

        pickle_path = self._pickle_cache_path(station_id, year)
        if pickle_path.exists():
            return _normalize_cached_obs_time(pd.read_pickle(pickle_path))

        csv_path = self._csv_cache_path(station_id, year)
        if csv_path.exists():
            return _normalize_cached_obs_time(pd.read_csv(csv_path, parse_dates=["obs_time_utc"]))
        return None

    def _ensure_station_year_cached(self, station_id: str, year: int) -> None:
        if self._read_cache(station_id, year) is not None:
            return
        self._write_cache(station_id, year, self._fetch_station_year(station_id, year))

    def _load_station_year(self, station_id: str, year: int) -> pd.DataFrame:
        key = (station_id, year)
        if key in self._memory_cache:
            df = self._memory_cache.pop(key)
            self._memory_cache[key] = df
            return df

        df = self._read_cache(station_id, year)
        if df is None:
            df = self._fetch_station_year(station_id, year)
            self._write_cache(station_id, year, df)

        self._memory_cache[key] = df
        while len(self._memory_cache) > self.max_open_station_years:
            self._memory_cache.popitem(last=False)
        return df

    def prefetch_station_years(self, station_years: set[tuple[str, int]], *, max_workers: int = 4) -> None:
        if not station_years:
            return
        items = sorted({item for item in station_years if item[0]})
        if max_workers <= 1:
            for station_id, year in items:
                self._ensure_station_year_cached(station_id, year)
            return
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            list(executor.map(lambda item: self._ensure_station_year_cached(*item), items))


def _merge_weather_for_targets(
    df: pd.DataFrame,
    *,
    targets: pd.DataFrame,
    prefix: str,
    cache: NOAAGlobalHourlyCache,
    prefetch_workers: int = 4,
) -> pd.DataFrame:
    out = _empty_feature_frame(df.index, prefix)
    if targets.empty:
        return out

    station_years = {
        (station_id, int(year))
        for station_id, year in zip(targets["station_id"], targets["target_time_utc"].dt.year, strict=False)
    }
    cache.prefetch_station_years(station_years, max_workers=prefetch_workers)

    obs_frames = [cache._load_station_year(station_id, year) for station_id, year in sorted(station_years)]
    obs = pd.concat(obs_frames, ignore_index=True)
    if obs.empty:
        return out

    merged = pd.merge_asof(
        targets.sort_values(["target_time_utc", "station_id"]),
        obs.sort_values(["obs_time_utc", "station_id"]),
        left_on="target_time_utc",
        right_on="obs_time_utc",
        by="station_id",
        direction="nearest",
    )
    merged["obs_age_minutes"] = (
        (merged["obs_time_utc"] - merged["target_time_utc"]).abs().dt.total_seconds() / 60.0
    )
    merged = merged.set_index("row_id")

    for col in NOAA_WX_FEATURE_COLUMNS:
        out.loc[merged.index, f"{prefix}{col}"] = merged[col]
    return out


def build_noaa_feature_record(row: pd.Series, cache: NOAAGlobalHourlyCache) -> dict[str, Any]:
    one = enrich_with_noaa_weather(pd.DataFrame([row]), cache, prefetch_workers=1)
    return one[all_noaa_wx_column_names()].iloc[0].to_dict()


def enrich_with_noaa_weather(
    df: pd.DataFrame,
    cache: NOAAGlobalHourlyCache,
    *,
    prefetch_workers: int = 4,
) -> pd.DataFrame:
    base = df.reset_index(drop=True).copy()
    dep_date, dep_minutes, arr_date, arr_minutes = _compute_local_event_fields(base)
    dep_utc = _convert_local_fields_to_utc(base["ORIGIN"], dep_date, dep_minutes)
    arr_utc = _convert_local_fields_to_utc(base["DEST"], arr_date, arr_minutes)

    dep_targets = _build_weather_join_targets(base, airport_col="ORIGIN", target_times_utc=dep_utc, cache=cache)
    arr_targets = _build_weather_join_targets(base, airport_col="DEST", target_times_utc=arr_utc, cache=cache)

    dep_features = _merge_weather_for_targets(
        base,
        targets=dep_targets,
        prefix=NOAA_ORIGIN_PREFIX,
        cache=cache,
        prefetch_workers=prefetch_workers,
    )
    arr_features = _merge_weather_for_targets(
        base,
        targets=arr_targets,
        prefix=NOAA_DEST_PREFIX,
        cache=cache,
        prefetch_workers=prefetch_workers,
    )
    return pd.concat([base, dep_features, arr_features], axis=1)
