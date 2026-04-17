"""BTS column name normalization (DOT on-time statistics) — shared by filters and master builds."""

from __future__ import annotations

import pandas as pd

# Standard name -> list of possible raw column names from different BTS file vintages
COLUMN_ALIASES: dict[str, list[str]] = {
    "YEAR": ["YEAR", "Year"],
    "MONTH": ["MONTH", "Month"],
    "DAY_OF_MONTH": ["DAY_OF_MONTH", "DayofMonth"],
    "FL_DATE": ["FL_DATE", "FlightDate"],
    "REPORTING_AIRLINE": ["REPORTING_AIRLINE", "Reporting_Airline", "OP_UNIQUE_CARRIER", "UniqueCarrier"],
    "ORIGIN": ["ORIGIN", "Origin"],
    "ORIGIN_CITY_NAME": ["ORIGIN_CITY_NAME", "OriginCityName"],
    "DEST": ["DEST", "Dest"],
    "DEST_CITY_NAME": ["DEST_CITY_NAME", "DestCityName"],
    "CRS_DEP_TIME": ["CRS_DEP_TIME", "CRSDepTime"],
    "DEP_TIME": ["DEP_TIME", "DepTime"],
    "DEP_DELAY": ["DEP_DELAY", "DepDelay"],
    "CRS_ARR_TIME": ["CRS_ARR_TIME", "CRSArrTime"],
    "ARR_TIME": ["ARR_TIME", "ArrTime"],
    "ARR_DELAY": ["ARR_DELAY", "ArrDelay"],
    "CANCELLED": ["CANCELLED", "Cancelled"],
    "CANCELLATION_CODE": ["CANCELLATION_CODE", "CancellationCode"],
    "DIVERTED": ["DIVERTED", "Diverted"],
    "CRS_ELAPSED_TIME": ["CRS_ELAPSED_TIME", "CRSElapsedTime"],
    "ACTUAL_ELAPSED_TIME": ["ACTUAL_ELAPSED_TIME", "ActualElapsedTime"],
    "DISTANCE": ["DISTANCE", "Distance"],
    "CARRIER_DELAY": ["CARRIER_DELAY", "CarrierDelay"],
    "WEATHER_DELAY": ["WEATHER_DELAY", "WeatherDelay"],
    "NAS_DELAY": ["NAS_DELAY", "NASDelay"],
    "SECURITY_DELAY": ["SECURITY_DELAY", "SecurityDelay"],
    "LATE_AIRCRAFT_DELAY": ["LATE_AIRCRAFT_DELAY", "LateAircraftDelay"],
}

ALL_POSSIBLE_COLS: set[str] = set()
for _aliases in COLUMN_ALIASES.values():
    ALL_POSSIBLE_COLS.update(_aliases)


def normalize_bts_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map: dict[str, str] = {}
    for standard_name, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            if alias in df.columns:
                rename_map[alias] = standard_name
                break
    return df.rename(columns=rename_map)
