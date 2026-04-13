from __future__ import annotations

import pandas as pd


def summarize_outputs(final_pairs: pd.DataFrame, forbidden_summary: pd.DataFrame, threshold: float) -> pd.DataFrame:
    forbidden_count = int((forbidden_summary["avg_SHIELD_final"] >= threshold).sum())
    safe_count = int((forbidden_summary["avg_SHIELD_final"] < threshold).sum())
    violation_rate = float(final_pairs["duty_violation_flag"].mean()) if len(final_pairs) else 0.0
    avg_final_risk = float(final_pairs["SHIELD_final_score"].mean()) if len(final_pairs) else 0.0
    avg_buffer = float(final_pairs["duty_buffer_hours"].mean()) if len(final_pairs) else 0.0

    return pd.DataFrame(
        [
            {"metric": "total_pair_month_rows", "value": float(len(final_pairs))},
            {"metric": "forbidden_pairs", "value": float(forbidden_count)},
            {"metric": "safe_pairs", "value": float(safe_count)},
            {"metric": "duty_violation_rate", "value": round(violation_rate, 6)},
            {"metric": "avg_final_risk", "value": round(avg_final_risk, 6)},
            {"metric": "avg_duty_buffer_hours", "value": round(avg_buffer, 6)},
        ]
    )

