# SHIELD AA Weather Risk

End-to-end project to identify risky pilot sequence pairs of the form:

`A -> DFW -> B`

where `A` and `B` are US airports and DFW is the hub connection point.

This repository now supports three scoring paths:

- deterministic layered SHIELD scoring (rule-based)
- unsupervised anomaly scoring (Isolation Forest)
- supervised multi-task scoring (cancel, severe delay, duty risk)

The final operational output can be **month-specific forbidden pairs**, so the same pair can be safe in one month and forbidden in another.

## Problem Objective

Minimize disruption risk from:

- delay propagation across sequence legs
- weather-driven delay/cancellation pressure
- tight duty buffers and compliance pressure
- downstream operational instability from pairing risky airports

## High-Level System Flow

1. Download BTS monthly on-time files.
2. Combine monthly files into one master raw file.
3. Stream/filter raw data to AA + DFW + target airports.
4. Build airport-month metrics.
5. Build pair-month risk features.
6. Generate final SHIELD scores.
7. Generate safe/forbidden recommendations (CSP style thresholding).
8. Optionally run ML models (unsupervised + multi-task supervised).
9. Export pair-level and airport-month outputs.
10. Optional: load **multi-day forecast** bundles (7/10/… days) for scheduling via Forecast API or export.
11. Optional: **React dashboard** (`frontend/`) — landing page, map + pair list, integrated risk with month and horizon controls (dev: Vite on 5173, API on 8765).

## Repository Layout

```text
.
├── auto_download_bts.py
├── combine_all.py
├── filter_to_scope.py
├── run_pipeline.py
├── run_threshold_sweep.py
├── run_ml_risk.py
├── run_multitask.py
├── run_weather_producer.py
├── run_weather_consumer.py
├── run_forecast_api.py
├── run_forecast_export.py
├── run_integrated_risk.py
├── docker-compose.weather.yml
├── frontend/
│   ├── package.json
│   ├── vite.config.ts
│   └── src/          # React landing + dashboard (Vite)
├── requirements.txt
├── pyproject.toml
├── src/shield_pipeline/
│   ├── __init__.py
│   ├── config.py
│   ├── io.py
│   ├── features.py
│   ├── scoring.py
│   ├── csp.py
│   ├── evaluation.py
│   ├── thresholds.py
│   ├── ml_risk.py
│   ├── multitask.py
│   ├── integrated_risk.py
│   ├── pipeline.py
│   └── weather/
│       ├── open_meteo.py
│       ├── forecast.py
│       ├── forecast_bundle.py
│       ├── producer.py
│       ├── consumer.py
│       ├── locations.py
│       └── kafka_settings.py
│   └── web/
│       └── app.py
└── data/
    ├── raw/
    └── processed/
```

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Data Scope and Filtering

### Source

- BTS On-Time Performance monthly files
- years currently pulled by script: 2023-2025

### Scope rules

Data is filtered to:

- `REPORTING_AIRLINE == "AA"`
- flights where origin or destination is `DFW`
- other endpoint in target airport list

### Column normalization

The pipeline handles BTS schema variants by alias mapping, for example:

- `Reporting_Airline` -> `REPORTING_AIRLINE`
- `Origin` -> `ORIGIN`
- `Dest` -> `DEST`
- `ArrDelay` -> `ARR_DELAY`
- `WeatherDelay` -> `WEATHER_DELAY`

## Performance Design (Large Data)

The main pipeline is optimized for large raw files (multi-GB):

- streams raw CSV in chunks (`read_chunksize` in `PipelineConfig`)
- reads only relevant columns (`usecols`)
- filters early before downstream processing
- incrementally aggregates airport-month metrics
- avoids loading full raw file into memory

## Core Pipeline (Deterministic SHIELD)

Run:

```bash
PYTHONPATH=src python run_pipeline.py
```

### Step A: Airport-month summary

Built in `features.py` from scoped data:

- `total_flights`
- `avg_arr_delay`
- `avg_weather_delay`
- `weather_delay_flights`
- `cancelled_flights`
- `weather_delay_rate = weather_delay_flights / total_flights`
- `cancel_rate = cancelled_flights / total_flights`

### Step B: Airport risk score

Built in `scoring.py`:

- `weather_risk_component = weather_delay_rate * avg_weather_delay`
- features normalized with MinMax scaling
- weighted score:

`risk_score = 0.35*weather_risk_component + 0.30*cancel_rate + 0.20*avg_weather_delay + 0.15*avg_arr_delay`

Then normalized to `0-1`.

### Step C: Pair risk score

For each pair `(A, B, month)`:

- `pair_risk_score = sqrt(risk_A * risk_B)`
- `buffer_risk = (flight_time_A + DFW_turn + flight_time_B) / 14`
- `SHIELD_pair_score = 0.70*pair_risk_score + 0.30*buffer_risk`

Then normalized to `0-1`.

### Step D: Duty-aware final risk

- estimate predicted delay hours by airport-month average weather delay
- compute:
  - `total_sequence_hours`
  - `duty_buffer_hours = 14 - total_sequence_hours`
  - `duty_risk_score = max(0, 1 - duty_buffer_hours/14)`
- combine:

`SHIELD_final_score = 0.50*SHIELD_pair_score + 0.30*duty_risk_score + 0.20*buffer_risk`

Then normalized to `0-1`.

### Step E: CSP-style classification

Using threshold (`forbidden_threshold`, default `0.55`):

- pair is forbidden if `avg_SHIELD_final >= threshold`
- otherwise safe

Also exports top safe monthly assignments that exclude forbidden pair edges.

## Threshold Sweep

Run:

```bash
PYTHONPATH=src python run_threshold_sweep.py
```

This creates `threshold_sweep.csv` to compare how forbidden/safe counts change across candidate thresholds.

## Unsupervised ML Path (Optional)

Run:

```bash
PYTHONPATH=src python run_ml_risk.py
```

### What it does

- trains `IsolationForest` on pair operational features
- computes:
  - `ml_risk_score` (0-1 anomaly-derived risk)
  - `ml_risk_class` (Safe/Forbidden per pair-month)
- aggregates to pair-level forbidden summary

### Key outputs

- `pairs_with_ml_risk.csv`
- `top_forbidden_pairs_ml.csv`

## Multi-Task Supervised ML Path (Recommended)

Run:

```bash
PYTHONPATH=src python run_multitask.py
```

### Model design

Three separate supervised models are trained on pair-month data:

- cancellation risk model
- severe-delay risk model
- duty-risk model

Each model is currently a `RandomForestClassifier` with class balancing.

### Inputs

Features include:

- pair features (`risk_A`, `risk_B`, `pair_risk_score`, `buffer_risk`, `SHIELD_pair_score`)
- duty features (`predicted_delay_hours`, `total_sequence_hours`, `duty_buffer_hours`, `duty_risk_score`)
- month cyclic encoding (`month_sin`, `month_cos`)
- airport A/B monthly context (`cancel_rate`, delays, weather delay rate)

### Targets (current proxy labels)

Because no direct ground-truth "bad pair" labels exist, proxy targets are built using top-quartile pressure logic:

- `target_cancel` from cancel-pressure
- `target_severe_delay` from severe-delay-pressure
- `target_duty_violation` from duty-pressure

These are trainable labels used to learn relative risk structure from current data.

### Output scores

For each pair-month:

- `cancel_risk_score`
- `severe_delay_risk_score`
- `duty_violation_risk_score`
- `multitask_combined_risk = 0.40*cancel + 0.35*severe_delay + 0.25*duty`

### Business rules

Two decision tables are exported:

- pair-level long-horizon class (`pairs_multitask_business_rules.csv`)
- month-specific class (`pairs_multitask_business_rules_monthly.csv`)

Month-specific table is the one to use for seasonal/month variability decisions.

## Month-Specific Forbidden Pair Logic

In `pairs_multitask_business_rules_monthly.csv`, each row is:

- `airport_A`
- `airport_B`
- `month`
- per-risk scores
- `multitask_combined_risk`
- `monthly_business_rule_class`

Current monthly decision rule:

- `Forbidden` if `multitask_combined_risk >= 0.60`
- else `Safe`

This means the same pair can be safe in one month and forbidden in another.

## Output Files and Meaning

All under `data/processed/`:

- `aa_dfw_scoped.csv`: filtered flight-level scoped data
- `airport_month_summary.csv`: airport-month operational aggregates
- `airport_risk_scores.csv`: deterministic airport risk scores
- `pair_risk_scores.csv`: deterministic pair risk components
- `pairs_final_with_duty.csv`: deterministic final pair-month SHIELD score + duty metrics
- `top_forbidden_pairs_final.csv`: top deterministic forbidden pair summary
- `safe_schedule_csp.csv`: deterministic top safe monthly assignments
- `evaluation_summary.csv`: pipeline summary metrics
- `threshold_sweep.csv`: threshold sensitivity table
- `pairs_with_ml_risk.csv`: unsupervised pair-month ML scores
- `top_forbidden_pairs_ml.csv`: unsupervised pair-level summary
- `pairs_multitask_scores.csv`: supervised multi-task pair-month scores
- `airport_month_multitask_scores.csv`: airport-by-month multi-task risk scores
- `pairs_multitask_business_rules.csv`: pair-level business rule class
- `pairs_multitask_business_rules_monthly.csv`: month-specific business rule class
- `pairs_integrated_risk_{N}d.csv`, `pairs_integrated_business_rules_monthly_{N}d.csv`: multitask + **forecast-window** weather at A and B (`run_integrated_risk.py --days N`)
- `weather_stream.jsonl`: append-only Kafka consumer log (when consumer is running)
- `weather_latest_by_airport.json`: latest observation per airport from Kafka consumer

## Main Config Controls

In `src/shield_pipeline/config.py`:

- `forbidden_threshold`: deterministic SHIELD threshold
- `read_chunksize`: raw streaming chunk size
- output file paths for deterministic and ML artifacts

## Practical Usage Recommendation

For planning decisions, use this order:

1. use `pairs_multitask_business_rules_monthly.csv` for month-specific forbidden/safe decisions
2. use deterministic outputs as interpretable baseline and sanity check
3. use threshold sweep and observed ops outcomes to tune cutoffs

## Real-time weather via Kafka (event stream, not app HTTP calls)

Your scoring pipeline can stay **decoupled from HTTP**: a small **producer** service pulls weather (or ingests from any upstream feed) and publishes **events** to Kafka. Your **application** only **consumes** messages from the bus, so the main app never blocks on weather API round-trips.

### Why this works

- **Producer** (background job or container): fetches weather on a schedule, pushes JSON to a topic.
- **Broker** (Kafka / Redpanda): holds the stream; consumers read at their own pace.
- **Consumer / your app**: subscribes to the topic and updates local state (files, DB, cache) or triggers feature refresh.

The ingestion service may still use HTTP **once** (Open-Meteo, NOAA, partner feed). That is normal. The important part is: **your product code does not call the weather API** — it reads from Kafka or from files the consumer writes.

### What is included here

- `docker-compose.weather.yml` — local **Redpanda** (Kafka-compatible) on `localhost:19092`.
- `run_weather_producer.py` — publishes current conditions per target airport to topic `weather.airport.observations` (default).
- `run_weather_consumer.py` — consumes events and appends to:
  - `data/processed/weather_stream.jsonl` (append-only log)
  - `data/processed/weather_latest_by_airport.json` (latest snapshot per airport)

Weather fields come from **Open-Meteo** (no API key) in `shield_pipeline/weather/open_meteo.py`.

### Environment variables

| Variable | Default | Meaning |
|----------|---------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:19092` | Broker list |
| `WEATHER_KAFKA_TOPIC` | `weather.airport.observations` | Topic name |
| `WEATHER_KAFKA_GROUP` | `shield-weather-consumers` | Consumer group (consumer) |
| `WEATHER_INTERVAL_SEC` | `300` | Producer sweep interval (via `--interval`) |
| `WEATHER_OUTPUT_JSONL` | `data/processed/weather_stream.jsonl` | Consumer append log |
| `WEATHER_OUTPUT_LATEST` | `data/processed/weather_latest_by_airport.json` | Snapshot file |

### How to run

```bash
# 1) Start broker
docker compose -f docker-compose.weather.yml up -d

# 2) Install deps (includes kafka-python)
pip install -r requirements.txt

# 3) One-time test publish (all airports once)
PYTHONPATH=src python run_weather_producer.py --once

# 4) In another terminal — consumer (keeps running)
PYTHONPATH=src python run_weather_consumer.py
```

Continuous producer (every 5 minutes by default):

```bash
PYTHONPATH=src python run_weather_producer.py --interval 300
```

### Wiring weather into risk scoring

- **Option A:** Read `weather_latest_by_airport.json` inside your feature builder (join by airport + month or use rolling stats).
- **Option B:** Import `kafka-python` in your service and consume the same topic directly (no files).
- **Option C:** Production: use **Kafka Connect** + NOAA/s3/bucket source instead of the Python producer (same topic contract).

### Production notes

- Use a managed Kafka (MSK, Confluent Cloud, Aiven) instead of local Redpanda.
- Add auth (SASL/SSL), dead-letter topic, and schema registry if you standardize payloads.
- Replace Open-Meteo with NOAA/AWC or an enterprise feed in the producer only.

## Multi-day forecast windows (scheduling, not “now-cast”)

Published schedules are set days or weeks ahead, so **current** weather does not explain those decisions. For planning you want **forecast horizons** (e.g. 7 vs 10 days): the same airport can look different under a 7-day vs 10-day prediction bundle.

### What is implemented

- **Daily forecast** from Open-Meteo for each target airport (typically **1–16 days** ahead on the free API).
- **REST API** so your app loads the right window when the user picks “7 days”, “10 days”, etc.
- **Web UI** with buttons for preset windows (7 / 10 / 14 / 16 days) that calls the API (no hardcoded single horizon in the client).
- **`forecast_disruption_hint`** per airport in `summary`: a simple 0–1 heuristic from max wind and max precipitation probability over the selected window (tune or replace with your model).
- **`/api/forecast/scheduling-hints`**: compact table (one row per airport) for joining to pair-based scheduling logic.

### Endpoints

| Endpoint | Purpose |
|----------|---------|
| `GET /api/windows` | Lists selectable horizons and max days |
| `GET /api/forecast?days=7` | Full daily series + per-airport summary for that horizon |
| `GET /api/forecast/scheduling-hints?days=10` | Same horizon, hints only (for schedulers) |
| `GET /api/risk/pairs?days=7&month=3` | Same, **March only** (`month` 1–12); omit `month` for all months (use `limit` if unfiltered) |
| `GET /api/risk/pairs?days=7&limit=100` | All months, capped rows (preview) |

### Run the Forecast API + React UI

**Terminal 1 — API (port 8765):**

```bash
pip install -r requirements.txt
PYTHONPATH=src python run_forecast_api.py
```

**Terminal 2 — React dashboard (port 5173, proxies `/api` to the API):**

```bash
cd frontend && npm install && npm run dev
```

Open **http://127.0.0.1:5173/** for the **landing page**, then **Dashboard** (`/dashboard`):

- **Defaults** button: **10-day** forecast + **current calendar month**, loads integrated pairs.
- Pick **month** and **forecast days**, then **Load**.
- **Sidebar**: scrollable pair list — **Forbidden** red accent, **Safe** green; click a pair to select.
- **Map**: faint **A–B** chords for all loaded pairs (color by status); selected pair shows **A → DFW → B** hub legs in bold.
- **Right panel**: full metrics for the selected pair + forecast summaries at A and B; **7d / 10d / 14d / 16d** buttons reload that pair’s context for a new horizon.

API root **http://127.0.0.1:8765/** returns JSON pointers to the UI (`/docs` for Swagger).

Production build: `cd frontend && npm run build` — serve `frontend/dist` with any static host and point `VITE_API_URL` or reverse-proxy `/api` to the FastAPI service.

### Export bundle to disk (batch jobs)

```bash
PYTHONPATH=src python run_forecast_export.py --days 7
```

Writes `data/processed/forecast_window_7d.json` (adjust with `--out`).

### Integrated pair risk (multitask + forecast at A and B)

Implemented in `src/shield_pipeline/integrated_risk.py` and CLI `run_integrated_risk.py`.

For the user-selected forecast horizon (`days`):

1. Fetch the same Open-Meteo daily bundle used elsewhere (`build_forecast_bundle(days)`).
2. For each airport, read `forecast_disruption_hint` from the bundle summary (0–1).
3. For each pair row `(airport_A, airport_B, month)`:
   - `forecast_hint_A`, `forecast_hint_B` — hints at both endpoints.
   - `pair_forecast_weather_risk = max(hint_A, hint_B)` (either leg can drive disruption; you can change to mean in code if preferred).
4. Blend with multitask output:
   - `integrated_risk_score = w_mt * multitask_combined_risk + w_fc * pair_forecast_weather_risk`
   - Defaults: `w_mt=0.6`, `w_fc=0.4` (override via `INTEGRATED_WEIGHT_MULTITASK`, `INTEGRATED_WEIGHT_FORECAST`).
5. `integrated_risk_class = Forbidden` if `integrated_risk_score >= INTEGRATED_FORBIDDEN_THRESHOLD` (default `0.6`).

**CLI (writes CSVs for the chosen horizon):**

```bash
PYTHONPATH=src python run_integrated_risk.py --days 10
PYTHONPATH=src python run_integrated_risk.py --days 10 --month 3
```

Produces e.g. `pairs_integrated_risk_10d.csv` (or `pairs_integrated_risk_10d_m3.csv` when `--month` is set), plus monthly rules and meta JSON.

**API:** `GET /api/risk/pairs?days=7&month=6` returns JSON with `meta`, `summary`, and `pairs`. Summary includes `month_filter`, `rows_before_month_filter`, and counts after filtering.

### Limits

- Forecast length is capped by the provider (here **16 days** max). Longer “planning windows” need a different data product or rolling refreshes.

## Current Limitations

- supervised targets are proxy labels, not external ground-truth outcomes
- model evaluation currently focuses on outputs, not full time-split validation reports
- integrated risk is **online blend** of multitask + forecast; retraining multitask models on historical forecast archives is not done here

## Next Enhancements

- retrain multitask models with forecast features as inputs (offline batch)
- merge live or forecast weather features into airport-month and multitask features
- add external NOAA/AWC-specific fields in the producer payload if required
- add time-based train/validation split and report PR-AUC, recall@K, precision@K
- calibrate monthly forbidden threshold by operational preference
- add configurable business rules from YAML/JSON (instead of hardcoded cutoff)
