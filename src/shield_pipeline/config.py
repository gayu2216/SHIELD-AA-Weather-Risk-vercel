from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PipelineConfig:
    raw_file: Path = Path("data/raw/bts_master_raw.csv")
    """AA flights with ORIGIN=DFW or DEST=DFW (hub-edge master; no other airport filter)."""
    scoped_file: Path = Path("data/processed/dfw_hub_flights_master.csv")
    route_stats_file: Path = Path("data/processed/route_stats.csv")
    airport_summary_file: Path = Path("data/processed/airport_month_summary.csv")
    airport_risk_file: Path = Path("data/processed/airport_risk_scores.csv")
    pair_risk_file: Path = Path("data/processed/pair_risk_scores.csv")
    final_pairs_file: Path = Path("data/processed/pairs_final_with_duty.csv")
    top_forbidden_file: Path = Path("data/processed/top_forbidden_pairs_final.csv")
    safe_schedule_file: Path = Path("data/processed/safe_schedule_csp.csv")
    evaluation_file: Path = Path("data/processed/evaluation_summary.csv")
    multitask_pair_scores_file: Path = Path("data/processed/pairs_multitask_scores.csv")
    multitask_airport_scores_file: Path = Path("data/processed/airport_month_multitask_scores.csv")
    multitask_business_rules_file: Path = Path("data/processed/pairs_multitask_business_rules.csv")
    multitask_business_rules_monthly_file: Path = Path("data/processed/pairs_multitask_business_rules_monthly.csv")
    integrated_pair_scores_file: Path = Path("data/processed/pairs_integrated_risk.csv")
    integrated_monthly_rules_file: Path = Path("data/processed/pairs_integrated_business_rules_monthly.csv")

    forbidden_threshold: float = 0.55
    top_n_schedule_per_month: int = 10
    top_n_forbidden_pairs: int = 50
    read_chunksize: int = 200_000
