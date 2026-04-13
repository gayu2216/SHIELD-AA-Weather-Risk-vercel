import type { PairRow, ForecastBundle } from "../api/client";

const RISK_KEYS = [
  "integrated_risk_score",
  "integrated_risk_class",
  "multitask_class",
  "multitask_combined_risk",
  "cancel_risk_score",
  "severe_delay_risk_score",
  "duty_violation_risk_score",
  "SHIELD_final_score",
  "SHIELD_pair_score",
  "pair_risk_score",
  "buffer_risk",
  "duty_risk_score",
  "forecast_hint_A",
  "forecast_hint_B",
  "pair_forecast_weather_risk",
  "duty_buffer_hours",
  "predicted_delay_hours",
];

function fmt(k: string, v: unknown): string {
  if (v === null || v === undefined) return "—";
  if (k.endsWith("class")) return String(v);
  const n = Number(v);
  return Number.isFinite(n) ? n.toFixed(4) : String(v);
}

export default function PairDetail({
  pair,
  forecast,
  forecastDays,
  onChangeWindow,
}: {
  pair: PairRow | null;
  forecast: ForecastBundle | null;
  forecastDays: number;
  onChangeWindow: (days: number) => void;
}) {
  if (!pair) {
    return (
      <div className="detail-panel">
        <h2>Pair detail</h2>
        <p className="detail-empty">Select a pair from the list to see routing on the map, forecast context for the chosen horizon, and all risk metrics.</p>
      </div>
    );
  }

  const a = String(pair.airport_A ?? "");
  const b = String(pair.airport_B ?? "");
  const fa = forecast?.airports?.[a];
  const fb = forecast?.airports?.[b];

  return (
    <div className="detail-panel">
      <h2>
        {a} → DFW → {b}
      </h2>
      <p className="detail-empty" style={{ marginBottom: "1rem" }}>
        Month <strong>{String(pair.month)}</strong> · Forecast horizon <strong>{forecastDays} days</strong>
      </p>

      <div className="detail-section">
        <h3>Change forecast window</h3>
        <div style={{ display: "flex", flexWrap: "wrap", gap: "0.5rem", alignItems: "center" }}>
          {[7, 10, 14, 16].map((d) => (
            <button
              key={d}
              type="button"
              className="dash-btn dash-btn-secondary"
              style={{ opacity: d === forecastDays ? 1 : 0.75 }}
              onClick={() => onChangeWindow(d)}
            >
              {d}d
            </button>
          ))}
        </div>
      </div>

      <div className="stat-grid">
        {RISK_KEYS.map((k) => (
          <div className="stat" key={k}>
            <div className="k">{k.replace(/_/g, " ")}</div>
            <div className="v">{fmt(k, pair[k])}</div>
          </div>
        ))}
      </div>

      <div className="detail-section">
        <h3>Forecast summary @ {a}</h3>
        {fa?.error ? (
          <p className="detail-empty">{fa.error}</p>
        ) : (
          <pre className="detail-pre">{JSON.stringify(fa?.summary ?? {}, null, 2)}</pre>
        )}
      </div>
      <div className="detail-section">
        <h3>Forecast summary @ {b}</h3>
        {fb?.error ? (
          <p className="detail-empty">{fb.error}</p>
        ) : (
          <pre className="detail-pre">{JSON.stringify(fb?.summary ?? {}, null, 2)}</pre>
        )}
      </div>
      <div className="detail-section">
        <h3>Daily forecast sample @ {a} (first 5 days)</h3>
        <pre className="detail-pre">{JSON.stringify((fa?.daily as unknown[])?.slice(0, 5) ?? [], null, 2)}</pre>
      </div>
      <div className="detail-section">
        <h3>Daily forecast sample @ {b} (first 5 days)</h3>
        <pre className="detail-pre">{JSON.stringify((fb?.daily as unknown[])?.slice(0, 5) ?? [], null, 2)}</pre>
      </div>
    </div>
  );
}
