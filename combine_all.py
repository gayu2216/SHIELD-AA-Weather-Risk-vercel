# save as: combine_all.py

import pandas as pd
import glob
import os

files = sorted(glob.glob('data/raw/bts_monthly/bts_*.csv'))
output_file = 'data/raw/bts_master_raw.csv'

if not files:
    print("No monthly CSV files found.")
    raise SystemExit

print(f"Found {len(files)} monthly files\n")

if os.path.exists(output_file):
    os.remove(output_file)

total_rows = 0
first_write = True

for f in files:
    print(f"Loading {os.path.basename(f)}...")

    for chunk in pd.read_csv(f, low_memory=False, chunksize=100_000):
        total_rows += len(chunk)
        chunk.to_csv(
            output_file,
            mode='a',
            index=False,
            header=first_write
        )
        first_write = False

print(f"\nSaved: {output_file}")
print(f"Total rows written: {total_rows:,}")