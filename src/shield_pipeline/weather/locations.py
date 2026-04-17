# Approximate airport coordinates (deg lat, deg lon) for Open-Meteo.

from __future__ import annotations

from functools import lru_cache
from pathlib import Path


AIRPORT_LAT_LON: dict[str, tuple[float, float]] = {
    "DFW": (32.8968, -97.0380),
    "LAX": (33.9425, -118.4081),
    "LAS": (36.0801, -115.1523),
    "ATL": (33.6367, -84.4281),
    "ORD": (41.9786, -87.9047),
    "DEN": (39.8617, -104.6731),
    "EGE": (39.6426, -106.9177),
    "PHX": (33.4343, -112.0116),
    "MIA": (25.7933, -80.2906),
    "JFK": (40.6397, -73.7789),
    "LGA": (40.7773, -73.8726),
    "CLT": (35.2139, -80.9431),
    "MCO": (28.4294, -81.3089),
    "SEA": (47.4502, -122.3092),
    "BOS": (42.3646, -71.0053),
    "SFO": (37.6189, -122.3750),
    "IAH": (29.9844, -95.3414),
    "HOU": (29.6456, -95.2789),
    "SAN": (32.7336, -117.1897),
    "PHL": (39.8719, -75.2411),
    "DCA": (38.8522, -77.0378),
    "MSP": (44.8820, -93.2218),
    "DTW": (42.2124, -83.3534),
    "MDW": (41.7860, -87.7524),
    "SLC": (40.7884, -111.9778),
    "PDX": (45.5887, -122.5975),
    "AUS": (30.1945, -97.6699),
    "SAT": (29.5337, -98.4698),
    "ELP": (31.8073, -106.3776),
    "OKC": (35.3931, -97.6007),
    "TUL": (36.1984, -95.8881),
    "MSY": (29.9934, -90.2580),
    "MEM": (35.0424, -89.9767),
    "BNA": (36.1245, -86.6782),
    "RDU": (35.8776, -78.7875),
    "TPA": (27.9755, -82.5333),
    "FLL": (26.0726, -80.1528),
    "FCA": (48.3105, -114.2560),
}

NOAA_CACHE_DIR = Path("data/raw/noaa_global_hourly_cache")


@lru_cache(maxsize=1)
def _station_registry():
    from shield_pipeline.weather.noaa_global_hourly import ISDStationRegistry

    return ISDStationRegistry(NOAA_CACHE_DIR)


@lru_cache(maxsize=512)
def airport_lat_lon(code: str) -> tuple[float, float] | None:
    clean = str(code or "").strip().upper()
    if not clean:
        return None
    if clean in AIRPORT_LAT_LON:
        return AIRPORT_LAT_LON[clean]

    station = _station_registry().station_for_airport(clean)
    if station and station.latitude is not None and station.longitude is not None:
        return (station.latitude, station.longitude)
    return None
