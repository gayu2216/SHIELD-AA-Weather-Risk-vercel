import pandas as pd

from shield_pipeline.config import PipelineConfig
from shield_pipeline.features import build_airport_month_summary
from shield_pipeline.io import read_csv, write_csv
from shield_pipeline.multitask import run_multitask_scoring
from shield_pipeline.pipeline import run_pipeline
from shield_pipeline.scoring import score_airports, score_final_with_duty, score_pairs


def _ensure_multitask_inputs(cfg: PipelineConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    if cfg.final_pairs_file.exists() and cfg.airport_summary_file.exists():
        return read_csv(cfg.final_pairs_file), read_csv(cfg.airport_summary_file)

    if cfg.scoped_file.exists():
        scoped = read_csv(cfg.scoped_file)
        airport_summary = build_airport_month_summary(scoped)
        airport_risk = score_airports(airport_summary)
        pair_risk = score_pairs(airport_risk)
        final_pairs = score_final_with_duty(pair_risk, airport_summary)

        write_csv(airport_summary, cfg.airport_summary_file)
        write_csv(airport_risk, cfg.airport_risk_file)
        write_csv(pair_risk, cfg.pair_risk_file)
        write_csv(final_pairs, cfg.final_pairs_file)
        return final_pairs, airport_summary

    run_pipeline(cfg)
    return read_csv(cfg.final_pairs_file), read_csv(cfg.airport_summary_file)


def main() -> None:
    cfg = PipelineConfig()
    pairs_final, airport_summary = _ensure_multitask_inputs(cfg)

    pair_scores, airport_scores, pair_rules, monthly_pair_rules = run_multitask_scoring(
        pairs_final, airport_summary
    )
    write_csv(pair_scores, cfg.multitask_pair_scores_file)
    write_csv(airport_scores, cfg.multitask_airport_scores_file)
    write_csv(pair_rules, cfg.multitask_business_rules_file)
    write_csv(monthly_pair_rules, cfg.multitask_business_rules_monthly_file)

    forbidden = int((pair_rules["business_rule_class"] == "Forbidden").sum())
    forbidden_monthly = int((monthly_pair_rules["monthly_business_rule_class"] == "Forbidden").sum())
    print(f"Saved: {cfg.multitask_pair_scores_file}")
    print(f"Saved: {cfg.multitask_airport_scores_file}")
    print(f"Saved: {cfg.multitask_business_rules_file}")
    print(f"Saved: {cfg.multitask_business_rules_monthly_file}")
    print(f"Total forbidden pairs by business rule: {forbidden}")
    print(f"Total forbidden pair-months: {forbidden_monthly}")


if __name__ == "__main__":
    main()
