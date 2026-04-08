# save as: filter_to_scope.py

import pandas as pd
import os

INPUT_FILE = 'data/raw/bts_master_raw.csv'
OUTPUT_FILE = 'data/processed/aa_dfw_scoped.csv'

os.makedirs('data/processed', exist_ok=True)

TARGET_AIRPORTS = [
    'LAX', 'LAS', 'ATL', 'ORD', 'DEN', 'PHX', 'MIA', 'JFK',
    'LGA', 'CLT', 'MCO', 'SEA', 'BOS', 'SFO', 'IAH', 'HOU',
    'SAN', 'PHL', 'DCA', 'MSP', 'DTW', 'MDW', 'SLC', 'PDX',
    'AUS', 'SAT', 'ELP', 'OKC', 'TUL', 'MSY', 'MEM', 'BNA',
    'RDU', 'TPA', 'FLL'
]

COLUMN_ALIASES = {
    'YEAR': ['YEAR', 'Year'],
    'MONTH': ['MONTH', 'Month'],
    'DAY_OF_MONTH': ['DAY_OF_MONTH', 'DayofMonth'],
    'FL_DATE': ['FL_DATE', 'FlightDate'],
    'REPORTING_AIRLINE': ['REPORTING_AIRLINE', 'Reporting_Airline', 'OP_UNIQUE_CARRIER', 'UniqueCarrier'],
    'ORIGIN': ['ORIGIN', 'Origin'],
    'ORIGIN_CITY_NAME': ['ORIGIN_CITY_NAME', 'OriginCityName'],
    'DEST': ['DEST', 'Dest'],
    'DEST_CITY_NAME': ['DEST_CITY_NAME', 'DestCityName'],
    'CRS_DEP_TIME': ['CRS_DEP_TIME', 'CRSDepTime'],
    'DEP_TIME': ['DEP_TIME', 'DepTime'],
    'DEP_DELAY': ['DEP_DELAY', 'DepDelay'],
    'CRS_ARR_TIME': ['CRS_ARR_TIME', 'CRSArrTime'],
    'ARR_TIME': ['ARR_TIME', 'ArrTime'],
    'ARR_DELAY': ['ARR_DELAY', 'ArrDelay'],
    'CANCELLED': ['CANCELLED', 'Cancelled'],
    'CANCELLATION_CODE': ['CANCELLATION_CODE', 'CancellationCode'],
    'DIVERTED': ['DIVERTED', 'Diverted'],
    'CRS_ELAPSED_TIME': ['CRS_ELAPSED_TIME', 'CRSElapsedTime'],
    'ACTUAL_ELAPSED_TIME': ['ACTUAL_ELAPSED_TIME', 'ActualElapsedTime'],
    'DISTANCE': ['DISTANCE', 'Distance'],
    'CARRIER_DELAY': ['CARRIER_DELAY', 'CarrierDelay'],
    'WEATHER_DELAY': ['WEATHER_DELAY', 'WeatherDelay'],
    'NAS_DELAY': ['NAS_DELAY', 'NASDelay'],
    'SECURITY_DELAY': ['SECURITY_DELAY', 'SecurityDelay'],
    'LATE_AIRCRAFT_DELAY': ['LATE_AIRCRAFT_DELAY', 'LateAircraftDelay']
}

ALL_POSSIBLE_COLS = set()
for aliases in COLUMN_ALIASES.values():
    ALL_POSSIBLE_COLS.update(aliases)

if os.path.exists(OUTPUT_FILE):
    os.remove(OUTPUT_FILE)

print("Starting filter process...")

total_input_rows = 0
total_aa_rows = 0
total_dfw_rows = 0
total_output_rows = 0
first_write = True
checked_columns = False

for chunk in pd.read_csv(
    INPUT_FILE,
    usecols=lambda c: c in ALL_POSSIBLE_COLS,
    low_memory=False,
    chunksize=100_000
):
    if not checked_columns:
        print("\nDetected columns:")
        print(list(chunk.columns))
        checked_columns = True

    rename_map = {}
    for standard_name, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            if alias in chunk.columns:
                rename_map[alias] = standard_name
                break

    chunk = chunk.rename(columns=rename_map)

    required_cols = ['REPORTING_AIRLINE', 'ORIGIN', 'DEST']
    missing_required = [c for c in required_cols if c not in chunk.columns]
    if missing_required:
        print(f"\nMissing required columns: {missing_required}")
        raise SystemExit

    keep_existing = [c for c in COLUMN_ALIASES.keys() if c in chunk.columns]
    chunk = chunk[keep_existing].copy()

    total_input_rows += len(chunk)

    df_aa = chunk[chunk['REPORTING_AIRLINE'] == 'AA'].copy()
    total_aa_rows += len(df_aa)

    df_dfw = df_aa[
        (df_aa['ORIGIN'] == 'DFW') | (df_aa['DEST'] == 'DFW')
    ].copy()
    total_dfw_rows += len(df_dfw)

    df_scoped = df_dfw[
        (df_dfw['ORIGIN'].isin(TARGET_AIRPORTS)) |
        (df_dfw['DEST'].isin(TARGET_AIRPORTS))
    ].copy()

    total_output_rows += len(df_scoped)

    if not df_scoped.empty:
        df_scoped.to_csv(
            OUTPUT_FILE,
            mode='a',
            index=False,
            header=first_write
        )
        first_write = False

    print(
        f"Processed {total_input_rows:,} rows | "
        f"AA: {total_aa_rows:,} | "
        f"DFW: {total_dfw_rows:,} | "
        f"Saved: {total_output_rows:,}"
    )

print("\nDone.")
print(f"Final saved file: {OUTPUT_FILE}")
print(f"Final rows saved: {total_output_rows:,}")

if os.path.exists(OUTPUT_FILE):
    sample = pd.read_csv(OUTPUT_FILE, nrows=5)
    print("\nSample data:")
    print(sample.head())
else:
    print("\nNo rows matched the filter.")