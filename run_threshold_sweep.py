from pathlib import Path

from shield_pipeline.io import read_csv, write_csv
from shield_pipeline.thresholds import sweep_thresholds


def main() -> None:
    final_pairs = read_csv(Path("data/processed/pairs_final_with_duty.csv"))
    threshold_grid = [round(x, 2) for x in [0.45, 0.5, 0.55, 0.6, 0.65, 0.7]]
    summary = sweep_thresholds(final_pairs, threshold_grid)
    output = Path("data/processed/threshold_sweep.csv")
    write_csv(summary, output)
    print(f"Saved threshold sweep to {output}")


if __name__ == "__main__":
    main()

