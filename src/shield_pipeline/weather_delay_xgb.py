from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from xgboost import XGBRegressor

NUMERIC_FEATURES = [
    "MONTH",
    "DAY_OF_MONTH",
    "DISTANCE",
    "CRS_DEP_TIME",
    "CRS_ARR_TIME",
    "CRS_ELAPSED_TIME",
    "noaa_origin_dep_obs_age_minutes",
    "noaa_origin_dep_temperature_c",
    "noaa_origin_dep_dewpoint_c",
    "noaa_origin_dep_visibility_m",
    "noaa_origin_dep_ceiling_ft",
    "noaa_origin_dep_wind_speed_ms",
    "noaa_origin_dep_sea_level_pressure_hpa",
    "noaa_origin_dep_precip_1h_flag",
    "noaa_origin_dep_ifr_flag",
    "noaa_dest_arr_obs_age_minutes",
    "noaa_dest_arr_temperature_c",
    "noaa_dest_arr_dewpoint_c",
    "noaa_dest_arr_visibility_m",
    "noaa_dest_arr_ceiling_ft",
    "noaa_dest_arr_wind_speed_ms",
    "noaa_dest_arr_sea_level_pressure_hpa",
    "noaa_dest_arr_precip_1h_flag",
    "noaa_dest_arr_ifr_flag",
]

CATEGORICAL_FEATURES = [
    "ORIGIN",
    "DEST",
]

TARGET_COLUMN = "weather_delay_target"


@dataclass
class TrainingArtifacts:
    model_path: Path
    metrics_path: Path
    feature_importance_path: Path


def _load_training_frame(dataset_path: Path) -> pd.DataFrame:
    df = pd.read_csv(dataset_path, low_memory=False)
    if TARGET_COLUMN not in df.columns:
        if "WEATHER_DELAY" not in df.columns:
            raise ValueError(f"Missing target columns in {dataset_path}")
        df[TARGET_COLUMN] = pd.to_numeric(df["WEATHER_DELAY"], errors="coerce").fillna(0).clip(lower=0)
    return df


def train_weather_delay_xgboost(
    *,
    dataset_path: Path,
    output_dir: Path,
    test_size: float = 0.2,
    random_state: int = 42,
) -> TrainingArtifacts:
    df = _load_training_frame(dataset_path)
    for col in NUMERIC_FEATURES:
        if col not in df.columns:
            df[col] = np.nan
    for col in CATEGORICAL_FEATURES:
        if col not in df.columns:
            raise ValueError(f"Missing categorical feature column: {col}")

    X = df[NUMERIC_FEATURES + CATEGORICAL_FEATURES].copy()
    y = pd.to_numeric(df[TARGET_COLUMN], errors="coerce").fillna(0).clip(lower=0)

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=test_size,
        random_state=random_state,
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", SimpleImputer(strategy="median"), NUMERIC_FEATURES),
            (
                "cat",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("onehot", OneHotEncoder(handle_unknown="ignore")),
                    ]
                ),
                CATEGORICAL_FEATURES,
            ),
        ]
    )

    model = XGBRegressor(
        n_estimators=500,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=3,
        reg_lambda=1.0,
        objective="reg:squarederror",
        random_state=random_state,
        n_jobs=-1,
    )

    pipeline = Pipeline(
        steps=[
            ("preprocess", preprocessor),
            ("model", model),
        ]
    )
    pipeline.fit(X_train, y_train)

    y_pred = np.clip(pipeline.predict(X_test), 0, None)

    metrics = {
        "rows_total": int(len(df)),
        "rows_train": int(len(X_train)),
        "rows_test": int(len(X_test)),
        "target_mean_total_minutes": float(y.mean()),
        "target_mean_train_minutes": float(y_train.mean()),
        "target_mean_test_minutes": float(y_test.mean()),
        "mae_minutes": float(mean_absolute_error(y_test, y_pred)),
        "rmse_minutes": float(mean_squared_error(y_test, y_pred) ** 0.5),
        "r2": float(r2_score(y_test, y_pred)),
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    model_path = output_dir / "weather_delay_xgb_pipeline.joblib"
    metrics_path = output_dir / "weather_delay_xgb_metrics.json"
    feature_importance_path = output_dir / "weather_delay_xgb_feature_importance.csv"

    joblib.dump(pipeline, model_path)
    with metrics_path.open("w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    feature_names = pipeline.named_steps["preprocess"].get_feature_names_out()
    importances = pipeline.named_steps["model"].feature_importances_
    importance_df = (
        pd.DataFrame({"feature": feature_names, "importance": importances})
        .sort_values("importance", ascending=False)
        .reset_index(drop=True)
    )
    importance_df.to_csv(feature_importance_path, index=False)

    return TrainingArtifacts(
        model_path=model_path,
        metrics_path=metrics_path,
        feature_importance_path=feature_importance_path,
    )
