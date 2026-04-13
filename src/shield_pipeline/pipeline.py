from __future__ import annotations

from .config import PipelineConfig
from .csp import build_forbidden_pairs, build_monthly_safe_schedule
from .evaluation import summarize_outputs
from .features import process_raw_to_scoped_and_summary
from .io import write_csv
from .scoring import score_airports, score_final_with_duty, score_pairs


def run_pipeline(cfg: PipelineConfig) -> None:
    airport_summary = process_raw_to_scoped_and_summary(
        cfg.raw_file, cfg.scoped_file, chunksize=cfg.read_chunksize
    )
    write_csv(airport_summary, cfg.airport_summary_file)

    airport_risk = score_airports(airport_summary)
    write_csv(airport_risk, cfg.airport_risk_file)

    pair_risk = score_pairs(airport_risk)
    write_csv(pair_risk, cfg.pair_risk_file)

    final_pairs = score_final_with_duty(pair_risk, airport_summary)
    write_csv(final_pairs, cfg.final_pairs_file)

    forbidden = build_forbidden_pairs(final_pairs, cfg.forbidden_threshold)
    write_csv(forbidden.head(cfg.top_n_forbidden_pairs), cfg.top_forbidden_file)

    safe_schedule = build_monthly_safe_schedule(
        final_pairs, forbidden, cfg.forbidden_threshold, cfg.top_n_schedule_per_month
    )
    write_csv(safe_schedule, cfg.safe_schedule_file)

    evaluation = summarize_outputs(final_pairs, forbidden, cfg.forbidden_threshold)
    write_csv(evaluation, cfg.evaluation_file)

