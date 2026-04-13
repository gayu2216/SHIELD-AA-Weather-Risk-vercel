from __future__ import annotations

import pandas as pd


def build_forbidden_pairs(final_pairs: pd.DataFrame, threshold: float) -> pd.DataFrame:
    top = (
        final_pairs.groupby(["airport_A", "airport_B"], as_index=False)
        .agg(
            avg_SHIELD_final=("SHIELD_final_score", "mean"),
            avg_pair_risk=("SHIELD_pair_score", "mean"),
            avg_duty_risk=("duty_risk_score", "mean"),
            duty_violations=("duty_violation_flag", "sum"),
        )
        .sort_values("avg_SHIELD_final", ascending=False)
    )
    top["risk_class"] = top["avg_SHIELD_final"].apply(
        lambda x: "Forbidden" if x >= threshold else "Safe"
    )
    return top


def build_monthly_safe_schedule(
    final_pairs: pd.DataFrame, forbidden_summary: pd.DataFrame, threshold: float, top_n: int
) -> pd.DataFrame:
    forbidden = forbidden_summary[forbidden_summary["avg_SHIELD_final"] >= threshold][
        ["airport_A", "airport_B"]
    ]
    forbidden_set = set(tuple(r) for r in forbidden.itertuples(index=False, name=None))

    all_rows: list[pd.DataFrame] = []
    for month in sorted(final_pairs["month"].unique().tolist()):
        month_df = final_pairs[final_pairs["month"] == month].copy()
        safe = month_df[
            ~month_df.apply(
                lambda r: (r["airport_A"], r["airport_B"]) in forbidden_set
                or (r["airport_B"], r["airport_A"]) in forbidden_set,
                axis=1,
            )
        ]
        safe = safe.sort_values("SHIELD_final_score").head(top_n)
        all_rows.append(safe)
    return pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()

