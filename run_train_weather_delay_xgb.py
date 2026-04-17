#!/usr/bin/env python3
"""Train an XGBoost classifier for weather-delay occurrence."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from shield_pipeline.weather_delay_xgb import train_weather_delay_xgboost  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--input",
        type=Path,
        default=Path("data/processed/weather_delay_model_subset.csv"),
        help="Weather-delay modeling dataset.",
    )
    ap.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/processed/weather_delay_xgb"),
        help="Directory for trained model and metrics.",
    )
    ap.add_argument("--test-size", type=float, default=0.2)
    ap.add_argument("--random-state", type=int, default=42)
    args = ap.parse_args()

    artifacts = train_weather_delay_xgboost(
        dataset_path=args.input,
        output_dir=args.output_dir,
        test_size=args.test_size,
        random_state=args.random_state,
    )
    print(f"Model: {artifacts.model_path.resolve()}")
    print(f"Metrics: {artifacts.metrics_path.resolve()}")
    print(f"Importances: {artifacts.feature_importance_path.resolve()}")


if __name__ == "__main__":
    main()
