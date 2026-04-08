import requests
import os
import zipfile
import time
import shutil

os.makedirs("data/raw/bts_monthly", exist_ok=True)

years = [2023, 2024, 2025]
months = list(range(1, 13))

URL_PATTERNS = [
    "https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_Beginning_January_2018_{year}_{month}.zip",
    "https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_{year}_{month}.zip",
    "https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip",
]

session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0"})

print("Starting BTS download - all 36 months (2023-2025)")
print("This will take some time. Let it run!\n", flush=True)

success = 0
failed = []

for year in years:
    for month in months:
        out_csv = f"data/raw/bts_monthly/bts_{year}_{month:02d}.csv"

        if os.path.exists(out_csv):
            print(f"Already exists: {year}-{month:02d} - skipping", flush=True)
            success += 1
            continue

        downloaded = False

        for pattern in URL_PATTERNS:
            url = pattern.format(year=year, month=month)
            zip_path = f"data/raw/bts_monthly/bts_{year}_{month:02d}.zip"

            print(f"Trying {year}-{month:02d}: {url}", flush=True)

            try:
                with session.get(url, stream=True, timeout=180) as r:
                    content_type = r.headers.get("Content-Type", "").lower()

                    if r.status_code != 200:
                        continue

                    with open(zip_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                f.write(chunk)

                if not zipfile.is_zipfile(zip_path):
                    os.remove(zip_path)
                    continue

                with zipfile.ZipFile(zip_path, "r") as z:
                    csv_members = [n for n in z.namelist() if n.lower().endswith(".csv")]
                    if not csv_members:
                        raise Exception("ZIP downloaded, but no CSV found inside")

                    extracted_path = z.extract(csv_members[0], "data/raw/bts_monthly")
                    shutil.move(extracted_path, out_csv)

                os.remove(zip_path)
                size_mb = os.path.getsize(out_csv) / (1024 * 1024)
                print(f"Downloaded {year}-{month:02d} successfully ({size_mb:.1f} MB)", flush=True)

                success += 1
                downloaded = True
                break

            except Exception as e:
                if os.path.exists(zip_path):
                    os.remove(zip_path)
                print(f"Error for {year}-{month:02d} using this URL: {e}", flush=True)

        if not downloaded:
            print(f"FAILED: {year}-{month:02d}", flush=True)
            failed.append(f"{year}-{month:02d}")

        time.sleep(1)

print("\n" + "=" * 50)
print(f"Successfully downloaded: {success}/36 months")
if failed:
    print(f"Failed months: {failed}")
else:
    print("All months downloaded successfully")