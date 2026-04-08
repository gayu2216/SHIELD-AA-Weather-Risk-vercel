import pandas as pd
import glob
import os

files = sorted(glob.glob('data/raw/bts_monthly/bts_*.csv'))

if not files:
    print("No monthly files found.")
    raise SystemExit

first_file = files[0]
print("Checking:", os.path.basename(first_file))

df = pd.read_csv(first_file, nrows=0)
print("\nColumns in first monthly CSV:")
print(df.columns.tolist())