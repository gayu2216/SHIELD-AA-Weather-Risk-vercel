from pathlib import Path

from shield_pipeline.io import read_csv, write_csv
from shield_pipeline.ml_risk import build_ml_forbidden_pairs, score_pairs_with_isolation_forest


def main() -> None:
    input_path = Path("data/processed/pairs_final_with_duty.csv")
    df = read_csv(input_path)

    scored = score_pairs_with_isolation_forest(df, contamination=0.15, random_state=42)
    pair_summary = build_ml_forbidden_pairs(scored)

    scored_out = Path("data/processed/pairs_with_ml_risk.csv")
    summary_out = Path("data/processed/top_forbidden_pairs_ml.csv")
    write_csv(scored, scored_out)
    write_csv(pair_summary, summary_out)

    forbidden_count = int((pair_summary["ml_pair_class"] == "Forbidden").sum())
    print(f"Saved ML-scored pairs: {scored_out}")
    print(f"Saved ML forbidden summary: {summary_out}")
    print(f"ML forbidden pairs: {forbidden_count}")


if __name__ == "__main__":
    main()

