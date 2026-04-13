from shield_pipeline.config import PipelineConfig
from shield_pipeline.io import read_csv, write_csv
from shield_pipeline.multitask import run_multitask_scoring


def main() -> None:
    cfg = PipelineConfig()
    pairs_final = read_csv(cfg.final_pairs_file)
    airport_summary = read_csv(cfg.airport_summary_file)

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

