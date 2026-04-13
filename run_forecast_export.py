"""Export forecast bundle to JSON (same payload as GET /api/forecast?days=N)."""

import argparse
import json
from pathlib import Path

from shield_pipeline.weather.forecast import FORECAST_DAYS_MAX
from shield_pipeline.weather.forecast_bundle import build_forecast_bundle


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=7, help=f"1..{FORECAST_DAYS_MAX}")
    p.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output path (default: data/processed/forecast_window_{days}d.json)",
    )
    args = p.parse_args()
    out = args.out or Path(f"data/processed/forecast_window_{args.days}d.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    bundle = build_forecast_bundle(args.days)
    out.write_text(json.dumps(bundle, indent=2), encoding="utf-8")
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
