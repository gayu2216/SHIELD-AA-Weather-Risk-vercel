from __future__ import annotations

from itertools import combinations

import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler


FLIGHT_TIMES: dict[str, float] = {
    "LAX": 3.5, "LAS": 2.75, "ATL": 2.25, "ORD": 2.5, "DEN": 2.0,
    "PHX": 2.25, "MIA": 2.75, "JFK": 3.25, "LGA": 3.25, "CLT": 2.5,
    "MCO": 2.5, "SEA": 3.75, "BOS": 3.5, "SFO": 3.75, "IAH": 1.25,
    "HOU": 1.25, "SAN": 3.25, "PHL": 3.25, "DCA": 3.0, "MSP": 2.25,
    "DTW": 2.5, "MDW": 2.25, "SLC": 2.5, "PDX": 3.5, "AUS": 0.75,
    "SAT": 1.0, "ELP": 1.5, "OKC": 1.25, "TUL": 1.25, "MSY": 1.5,
    "MEM": 1.5, "BNA": 1.75, "RDU": 2.75, "TPA": 2.5, "FLL": 2.75,
}
DEFAULT_FLIGHT_TIME = 2.5
DFW_TURNAROUND_HOURS = 1.0
FAA_MAX_DUTY_HOURS = 14.0


def score_airports(summary_df: pd.DataFrame) -> pd.DataFrame:
    df = summary_df.copy()
    df["weather_risk_component"] = df["weather_delay_rate"] * df["avg_weather_delay"]
    features = ["weather_risk_component", "cancel_rate", "avg_weather_delay", "avg_arr_delay"]
    for col in features:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).clip(lower=0)

    scaler = MinMaxScaler()
    scaled = df.copy()
    scaled[features] = scaler.fit_transform(df[features])
    scaled["risk_score"] = (
        0.35 * scaled["weather_risk_component"]
        + 0.30 * scaled["cancel_rate"]
        + 0.20 * scaled["avg_weather_delay"]
        + 0.15 * scaled["avg_arr_delay"]
    )
    mn, mx = scaled["risk_score"].min(), scaled["risk_score"].max()
    scaled["risk_score"] = 0.0 if mx == mn else (scaled["risk_score"] - mn) / (mx - mn)
    return scaled


def score_pairs(airport_risk_df: pd.DataFrame) -> pd.DataFrame:
    airports = sorted(airport_risk_df["ORIGIN"].unique().tolist())
    rows: list[dict] = []
    for month in sorted(airport_risk_df["MONTH"].unique().tolist()):
        month_data = airport_risk_df[airport_risk_df["MONTH"] == month].set_index("ORIGIN")
        for a, b in combinations(airports, 2):
            if a in month_data.index and b in month_data.index:
                risk_a = float(month_data.loc[a, "risk_score"])
                risk_b = float(month_data.loc[b, "risk_score"])
                pair_risk = np.sqrt(risk_a * risk_b)
                ft_a = FLIGHT_TIMES.get(a, DEFAULT_FLIGHT_TIME)
                ft_b = FLIGHT_TIMES.get(b, DEFAULT_FLIGHT_TIME)
                buffer_risk = (ft_a + DFW_TURNAROUND_HOURS + ft_b) / FAA_MAX_DUTY_HOURS
                rows.append(
                    {
                        "airport_A": a,
                        "airport_B": b,
                        "month": int(month),
                        "risk_A": round(risk_a, 4),
                        "risk_B": round(risk_b, 4),
                        "pair_risk_score": round(pair_risk, 4),
                        "buffer_risk": round(buffer_risk, 4),
                    }
                )
    pairs = pd.DataFrame(rows)
    pairs["SHIELD_pair_score"] = 0.70 * pairs["pair_risk_score"] + 0.30 * pairs["buffer_risk"]
    mn, mx = pairs["SHIELD_pair_score"].min(), pairs["SHIELD_pair_score"].max()
    pairs["SHIELD_pair_score"] = 0.0 if mx == mn else (pairs["SHIELD_pair_score"] - mn) / (mx - mn)
    pairs["SHIELD_pair_score"] = pairs["SHIELD_pair_score"].round(4)
    return pairs


def score_final_with_duty(pairs_df: pd.DataFrame, airport_summary_df: pd.DataFrame) -> pd.DataFrame:
    delay_lookup = airport_summary_df[["ORIGIN", "MONTH", "avg_weather_delay"]].copy()
    delay_lookup["avg_delay_hours"] = delay_lookup["avg_weather_delay"] / 60.0
    delay_lookup = delay_lookup.set_index(["ORIGIN", "MONTH"])

    def _duty(row: pd.Series) -> pd.Series:
        a, b, month = row["airport_A"], row["airport_B"], int(row["month"])
        ft_a = FLIGHT_TIMES.get(a, DEFAULT_FLIGHT_TIME)
        ft_b = FLIGHT_TIMES.get(b, DEFAULT_FLIGHT_TIME)
        try:
            predicted_delay = float(delay_lookup.loc[(a, month), "avg_delay_hours"])
        except KeyError:
            predicted_delay = 0.25
        total = ft_a + predicted_delay + DFW_TURNAROUND_HOURS + ft_b
        buffer_hours = FAA_MAX_DUTY_HOURS - total
        duty_risk = max(0.0, 1 - (buffer_hours / FAA_MAX_DUTY_HOURS))
        return pd.Series(
            {
                "predicted_delay_hours": round(predicted_delay, 2),
                "total_sequence_hours": round(total, 2),
                "duty_buffer_hours": round(buffer_hours, 2),
                "duty_violation_flag": int(total > FAA_MAX_DUTY_HOURS),
                "duty_risk_score": round(duty_risk, 4),
            }
        )

    final_df = pairs_df.copy()
    final_df = pd.concat([final_df, final_df.apply(_duty, axis=1)], axis=1)
    final_df["SHIELD_final_score"] = (
        0.50 * final_df["SHIELD_pair_score"]
        + 0.30 * final_df["duty_risk_score"]
        + 0.20 * final_df["buffer_risk"]
    )
    mn, mx = final_df["SHIELD_final_score"].min(), final_df["SHIELD_final_score"].max()
    final_df["SHIELD_final_score"] = (
        0.0 if mx == mn else (final_df["SHIELD_final_score"] - mn) / (mx - mn)
    )
    final_df["SHIELD_final_score"] = final_df["SHIELD_final_score"].round(4)
    return final_df

