"""Merge multitask pair scores with forecast-window weather for integrated risk."""

import argparse
import json
from pathlib import Path

import pandas as pd

from shield_pipeline.config import PipelineConfig
from shield_pipeline.integrated_risk import build_integrated_monthly_rules, integrate_forecast_into_pairs
from shield_pipeline.weather.forecast import FORECAST_DAYS_MAX


def main() -> None:
    cfg = PipelineConfig()
    p = argparse.ArgumentParser(description="Integrated risk = multitask + forecast at A and B for chosen horizon.")
    p.add_argument("--days", type=int, default=7, help=f"Forecast horizon 1..{FORECAST_DAYS_MAX}")
    p.add_argument(
        "--input",
        type=Path,
        default=cfg.multitask_pair_scores_file,
        help="Input pairs with multitask columns (default: pairs_multitask_scores.csv)",
    )
    p.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output CSV (default: data/processed/pairs_integrated_risk_{days}d.csv)",
    )
    p.add_argument(
        "--out-monthly",
        type=Path,
        default=None,
        help="Monthly rules CSV (default: data/processed/pairs_integrated_business_rules_monthly_{days}d.csv)",
    )
    p.add_argument(
        "--month",
        type=int,
        default=None,
        choices=list(range(1, 13)),
        help="If set, only include rows for this calendar month (1–12).",
    )
    args = p.parse_args()

    days = max(1, min(args.days, FORECAST_DAYS_MAX))
    pairs = pd.read_csv(args.input)
    integrated, meta = integrate_forecast_into_pairs(pairs, days)

    if args.month is not None and "month" in integrated.columns:
        m = pd.to_numeric(integrated["month"], errors="coerce")
        integrated = integrated[m == args.month].copy()

    suffix = f"_m{args.month}" if args.month is not None else ""
    out = args.out or Path(f"data/processed/pairs_integrated_risk_{days}d{suffix}.csv")
    out_monthly = args.out_monthly or Path(
        f"data/processed/pairs_integrated_business_rules_monthly_{days}d{suffix}.csv"
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    integrated.to_csv(out, index=False)

    monthly = build_integrated_monthly_rules(integrated)
    monthly.to_csv(out_monthly, index=False)

    meta_out = dict(meta)
    if args.month is not None:
        meta_out["month_filter"] = args.month
    meta_path = Path(f"data/processed/integrated_risk_meta_{days}d{suffix}.json")
    meta_path.write_text(json.dumps(meta_out, indent=2), encoding="utf-8")

    forbidden = int((integrated["integrated_risk_class"] == "Forbidden").sum())
    print(f"Wrote {out} ({len(integrated)} rows, {forbidden} Forbidden)")
    print(f"Wrote {out_monthly}")
    print(f"Meta: {meta_path}")


if __name__ == "__main__":
    main()
