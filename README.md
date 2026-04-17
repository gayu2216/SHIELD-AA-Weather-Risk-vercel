# SHIELD AA Weather Risk

SHIELD is a flight-pair risk system for American Airlines hub sequences of the form:

`A -> DFW -> B`

where:

- `A` is the first spoke airport
- `DFW` is the hub connection
- `B` is the second spoke airport

The repository contains:

- a deterministic monthly risk pipeline built from BTS operational data
- optional ML and analytics experiments
- a time-aware web application that uses weather at a user-selected date/time
- an XGBoost model that predicts weather-driven delay minutes for the two legs of a pair

The current app experience is:

1. the user selects a departure date and time
2. the backend fetches weather for that point in time
3. the XGBoost model predicts delay minutes for `A -> DFW` and `DFW -> B`
4. the pair is labeled `Forbidden` or `Safe`
5. the UI shows forbidden routes in red and safe routes in green

## What This System Is For

The system is designed to help evaluate whether a route pairing is operationally risky under weather conditions and historical operational patterns.

It answers questions like:

- Which `A -> DFW -> B` sequences look operationally fragile in a given month?
- For a selected departure date and time, which pairs are likely to incur weather delay?
- Which routes should be treated as more conservative or more acceptable from a planning perspective?

There are two major layers in the repo:

- an offline batch layer that builds monthly pair catalogs and deterministic risk features
- an online scoring layer that applies weather-aware XGBoost predictions at a user-selected timestamp

## System Overview

At a high level, the system works like this:

1. raw BTS on-time data is downloaded and combined
2. the raw data is filtered to AA flights touching DFW
3. monthly airport metrics and pair metrics are computed
4. a deterministic pair catalog is produced in `pairs_final_with_duty.csv`
5. a weather-delay training dataset is built from flight-level history plus NOAA weather
6. an XGBoost model is trained to predict weather delay minutes
7. the web app loads the deterministic pair catalog
8. when the user chooses a date and time, the backend pulls past or future weather
9. the backend predicts delay minutes for the pair’s two legs
10. the app shows the pair as safe or forbidden and colors it accordingly

## Current Decision Logic In The App

The current live app does not use multitask risk blending to classify pairs.

The app uses:

- the deterministic pair catalog as the list of candidate `A -> DFW -> B` routes
- the selected date and time from the user
- time-aware weather for the selected timestamp
- the XGBoost weather-delay model as the only decision model

For each pair:

- predict delay minutes for leg A: `A -> DFW`
- predict delay minutes for leg B: `DFW -> B`
- add them to get `xgboost_pair_delay_minutes`
- classify:
  - `Forbidden` if predicted pair delay minutes >= `XGBOOST_FORBIDDEN_DELAY_MINUTES`
  - otherwise `Safe`

Default threshold:

- `XGBOOST_FORBIDDEN_DELAY_MINUTES = 60`

That means the red/green route coloring in the UI is currently driven by predicted delay minutes, not by a normalized probability score.

## Data Sources

### 1. BTS On-Time Performance Data

Primary operational history comes from US BTS on-time performance data.

Used for:

- airline, airport, month, and route filtering
- arrival delay and weather delay fields
- cancellation fields
- flight timing fields such as scheduled elapsed time
- building airport-month summaries
- building pair-level deterministic features
- constructing the training dataset target for weather delay

Relevant columns include:

- `REPORTING_AIRLINE`
- `ORIGIN`
- `DEST`
- `MONTH`
- `ARR_DELAY`
- `WEATHER_DELAY`
- `CANCELLED`
- `CRS_DEP_TIME`
- `CRS_ARR_TIME`
- `CRS_ELAPSED_TIME`
- `DEP_TIME`
- `ARR_TIME`

### 2. NOAA NCEI Global Hourly / ISD

NOAA global hourly data is used for historical weather enrichment when building the weather-delay training dataset.

Used for:

- historical departure weather at the origin
- historical arrival weather at the destination
- model training features such as:
  - temperature
  - dew point
  - visibility
  - ceiling
  - wind speed
  - pressure
  - precipitation flag
  - IFR flag

Relevant code:

- `src/shield_pipeline/weather/noaa_global_hourly.py`
- `src/shield_pipeline/weather_delay_dataset.py`

### 3. Open-Meteo Forecast API

Open-Meteo is used for future weather when the selected departure date is in the forecast horizon.

Used for:

- hourly forecast rows for the user-selected date and time
- weather cards in the UI
- XGBoost runtime feature generation for future dates

Relevant code:

- `src/shield_pipeline/weather/forecast.py`
- `src/shield_pipeline/weather/forecast_bundle.py`

### 4. Open-Meteo Archive API

Open-Meteo archive is used when the selected departure date is in the past.

Used for:

- hourly archived weather rows for the chosen historical day
- XGBoost runtime feature generation for past dates
- time-specific weather display in the UI

Relevant code:

- `src/shield_pipeline/weather/historical_archive.py`
- `src/shield_pipeline/weather/forecast_bundle.py`

## Repository Layout

```text
.
├── auto_download_bts.py
├── combine_all.py
├── filter_to_scope.py
├── run_build_dfw_master.py
├── run_build_weather_delay_dataset.py
├── run_enrich_weather_archive.py
├── run_forecast_api.py
├── run_forecast_export.py
├── run_integrated_risk.py
├── run_ml_risk.py
├── run_multitask.py
├── run_pipeline.py
├── run_threshold_sweep.py
├── run_train_weather_delay_xgb.py
├── frontend/
├── src/shield_pipeline/
│   ├── bts_schema.py
│   ├── config.py
│   ├── csp.py
│   ├── dfw_master.py
│   ├── evaluation.py
│   ├── features.py
│   ├── integrated_risk.py
│   ├── io.py
│   ├── ml_risk.py
│   ├── multitask.py
│   ├── pipeline.py
│   ├── scoring.py
│   ├── weather_delay_dataset.py
│   ├── weather_delay_runtime.py
│   ├── weather_delay_xgb.py
│   ├── weather_enrichment.py
│   ├── weather/
│   │   ├── airport_timezones.py
│   │   ├── consumer.py
│   │   ├── forecast.py
│   │   ├── forecast_bundle.py
│   │   ├── historical_archive.py
│   │   ├── kafka_settings.py
│   │   ├── locations.py
│   │   ├── noaa_global_hourly.py
│   │   ├── open_meteo.py
│   │   └── producer.py
│   └── web/
│       └── app.py
└── data/
    ├── raw/
    └── processed/
```

## Setup

### Python

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Frontend

```bash
cd frontend
npm install
```

## Core Offline Pipeline

The deterministic pipeline creates the pair catalog that the app later scores online.

Run:

```bash
PYTHONPATH=src python run_pipeline.py
```

This produces:

- `data/processed/airport_month_summary.csv`
- `data/processed/airport_risk_scores.csv`
- `data/processed/pair_risk_scores.csv`
- `data/processed/pairs_final_with_duty.csv`
- `data/processed/top_forbidden_pairs_final.csv`
- `data/processed/safe_schedule_csp.csv`
- `data/processed/evaluation_summary.csv`

### Deterministic Pipeline Stages

#### Step 1. Build the scoped DFW master

The pipeline filters the raw BTS file to:

- American Airlines only
- flights where `ORIGIN == DFW` or `DEST == DFW`

This creates:

- `data/processed/dfw_hub_flights_master.csv`

Helper script:

```bash
PYTHONPATH=src python run_build_dfw_master.py
```

#### Step 2. Build airport-month summary

From the scoped master, monthly airport aggregates are computed:

- total flights
- average arrival delay
- average weather delay
- weather delay rate
- cancellation rate

Implemented in:

- `src/shield_pipeline/features.py`

#### Step 3. Score airports

The system creates a deterministic monthly airport risk score using weighted normalized components.

Implemented in:

- `src/shield_pipeline/scoring.py`

#### Step 4. Build pair-month rows

For each airport pair and month:

- join airport-level risks
- compute pair geometric risk
- compute buffer risk from route durations
- compute duty-related constraints

Output:

- `pairs_final_with_duty.csv`

This file is especially important because it is the current catalog consumed by the live app.

## Weather-Delay Model Training

The XGBoost model is trained on flight-level rows, not pair-level rows.

### Training Dataset Build

Run:

```bash
PYTHONPATH=src python run_build_weather_delay_dataset.py
```

This creates a training-ready dataset containing:

- flight date fields
- origin and destination
- schedule fields
- observed weather delay target
- enriched NOAA weather features at:
  - origin departure
  - destination arrival

Typical output:

- `data/processed/weather_delay_model_master.csv`

Optional subset builds are supported for faster experimentation.

### Model Target

The current XGBoost model predicts:

- `weather_delay_target`

which represents delay minutes attributable to weather.

This is a regression target, not a binary classification target.

### Model Features

The trained model uses:

- calendar features:
  - `MONTH`
  - `DAY_OF_MONTH`
- route/schedule features:
  - `ORIGIN`
  - `DEST`
  - `DISTANCE`
  - `CRS_DEP_TIME`
  - `CRS_ARR_TIME`
  - `CRS_ELAPSED_TIME`
- timepoint weather features for origin departure
- timepoint weather features for destination arrival

### Train the Model

Run:

```bash
PYTHONPATH=src python run_train_weather_delay_xgb.py \
  --input data/processed/weather_delay_model_subset.csv \
  --output-dir data/processed/weather_delay_xgb_20k
```

Artifacts:

- `weather_delay_xgb_pipeline.joblib`
- `weather_delay_xgb_metrics.json`
- `weather_delay_xgb_feature_importance.csv`

Implemented in:

- `src/shield_pipeline/weather_delay_xgb.py`

## Time-Aware Runtime Scoring

This is the most important part of the current system behavior.

### User Input

The user selects:

- departure date
- departure time

The app then determines the month from the selected date and filters the pair catalog to that month.

### Past vs Future Weather

If the selected date is in the past:

- the backend uses Open-Meteo archive hourly weather

If the selected date is in the future:

- the backend uses Open-Meteo forecast hourly weather

If a route airport is not in the small hardcoded coordinate table:

- the backend falls back to NOAA station metadata to resolve coordinates

This is why weather coverage is now much broader than before.

### Runtime Flow

For each monthly pair row:

1. compute the first-leg schedule `A -> DFW`
2. compute the second-leg schedule `DFW -> B`
3. choose the nearest hourly weather row to each relevant event time
4. build model feature rows for each leg
5. predict delay minutes for each leg with the trained XGBoost model
6. sum the two legs into a pair-level delay value
7. classify as `Forbidden` or `Safe`

Implemented in:

- `src/shield_pipeline/weather_delay_runtime.py`

### Batch Optimization

The runtime scorer avoids one model call per pair row.

Instead, it batches:

- one prediction for each unique `A -> DFW` leg
- one prediction for each unique `DFW -> B` leg

Then it maps those leg predictions across all pair rows in the selected month.

This matters because the monthly pair catalog can contain thousands of rows.

## Web Application

### Backend

Run:

```bash
PYTHONPATH=src python run_forecast_api.py
```

The backend runs on:

- `http://127.0.0.1:8765`

Main file:

- `src/shield_pipeline/web/app.py`

### Frontend

Run:

```bash
cd frontend
npm run dev
```

The frontend runs on:

- `http://127.0.0.1:5173`

### What The App Does

The dashboard:

- lets the user choose a departure date
- lets the user choose a departure time
- loads the month implied by that date
- scores all pair rows for that month
- shows:
  - a scrollable pair list
  - a map
  - weather cards
  - detailed metrics for a selected pair

### UI Colors

- `Forbidden` pairs are shown in red
- `Safe` pairs are shown in green

### Important API Endpoints

#### `GET /api/forecast`

Returns weather bundle data for the selected date/time context.

Examples:

```bash
curl 'http://127.0.0.1:8765/api/forecast?days=1&date=2026-04-16&time=08:00'
```

#### `GET /api/risk/pairs`

Returns the pair list and XGBoost classification results.

Examples:

```bash
curl 'http://127.0.0.1:8765/api/risk/pairs?days=1&date=2026-04-16&time=08:00&limit=10'
```

Response includes:

- `meta`
- `summary`
- `pairs`

Important pair fields include:

- `xgboost_pair_delay_minutes`
- `xgboost_pair_risk_class`
- `leg_a_predicted_weather_delay_minutes`
- `leg_b_predicted_weather_delay_minutes`
- `pair_predicted_weather_delay_minutes`
- `selected_departure_local`

## Other Modeling Paths In The Repo

These still exist for analysis and experimentation, but they are not the current live app decision path.

### Unsupervised ML

Run:

```bash
PYTHONPATH=src python run_ml_risk.py
```

This uses `IsolationForest` on pair features.

### Multi-Task Supervised Model

Run:

```bash
PYTHONPATH=src python run_multitask.py
```

This trains separate pair-month proxy models for:

- cancellations
- severe delays
- duty pressure

Useful outputs:

- `pairs_multitask_scores.csv`
- `pairs_multitask_business_rules.csv`
- `pairs_multitask_business_rules_monthly.csv`

### Integrated Risk

Run:

```bash
PYTHONPATH=src python run_integrated_risk.py --days 10
```

This path combines multitask outputs with forecast hints.

It remains in the repository for experimentation, but it is not the current source of truth for the live red/green classification in the dashboard.

## Weather Streaming via Kafka

The repo also includes a Kafka/Redpanda weather event path.

This is separate from the current web app’s direct timepoint weather lookup.

Included scripts:

- `run_weather_producer.py`
- `run_weather_consumer.py`
- `docker-compose.weather.yml`

Outputs:

- `data/processed/weather_stream.jsonl`
- `data/processed/weather_latest_by_airport.json`

Use this path if you want an event-stream architecture instead of the app fetching weather per request.

## Important Files and Outputs

### Core deterministic outputs

- `data/processed/dfw_hub_flights_master.csv`
- `data/processed/airport_month_summary.csv`
- `data/processed/airport_risk_scores.csv`
- `data/processed/pair_risk_scores.csv`
- `data/processed/pairs_final_with_duty.csv`

### Weather-delay model outputs

- `data/processed/weather_delay_xgb_20k/weather_delay_xgb_pipeline.joblib`
- `data/processed/weather_delay_xgb_20k/weather_delay_xgb_metrics.json`
- `data/processed/weather_delay_xgb_20k/weather_delay_xgb_feature_importance.csv`

### App-facing runtime values

- `xgboost_pair_delay_minutes`
- `xgboost_pair_risk_class`
- `leg_a_predicted_weather_delay_minutes`
- `leg_b_predicted_weather_delay_minutes`

## Configuration Notes

### Main config class

See:

- `src/shield_pipeline/config.py`

### Important environment variables

- `WEATHER_DELAY_MODEL_PATH`
  - override the path to the trained XGBoost artifact
- `XGBOOST_FORBIDDEN_DELAY_MINUTES`
  - pair delay threshold used to classify safe vs forbidden

## Known Limitations

- The deterministic pair catalog is still month-based, so the live app filters by the selected date’s month rather than rebuilding pair structure for each exact day.
- The app classification threshold is a simple delay-minute cutoff. It is easy to understand, but it is not yet learned from downstream business outcomes.
- The same pair can still have deterministic fields such as `SHIELD_final_score` visible in the UI even though those fields are not used for the live red/green decision.
- Open-Meteo future forecasts are horizon-limited.
- Archive and forecast weather quality can vary by airport and timestamp.
- The XGBoost runtime currently predicts weather delay contribution, not total operational disruption.

## Recommended Reading Order For New Contributors

If you are new to the codebase, read in this order:

1. `src/shield_pipeline/pipeline.py`
2. `src/shield_pipeline/features.py`
3. `src/shield_pipeline/scoring.py`
4. `src/shield_pipeline/weather_delay_dataset.py`
5. `src/shield_pipeline/weather_delay_xgb.py`
6. `src/shield_pipeline/weather_delay_runtime.py`
7. `src/shield_pipeline/weather/forecast_bundle.py`
8. `src/shield_pipeline/web/app.py`
9. `frontend/src/pages/Dashboard.tsx`
10. `frontend/src/components/PairDetail.tsx`

## Quick Start

### 1. Build deterministic catalog

```bash
PYTHONPATH=src python run_pipeline.py
```

### 2. Train or refresh the XGBoost weather-delay model

```bash
PYTHONPATH=src python run_train_weather_delay_xgb.py \
  --input data/processed/weather_delay_model_subset.csv \
  --output-dir data/processed/weather_delay_xgb_20k
```

### 3. Start backend

```bash
PYTHONPATH=src python run_forecast_api.py
```

### 4. Start frontend

```bash
cd frontend
npm run dev
```

### 5. Open the app

- [http://127.0.0.1:5173](http://127.0.0.1:5173)

Choose a date and time, then inspect:

- the weather cards
- the pair list
- the selected route map
- the predicted delay minutes and safe/forbidden classification
