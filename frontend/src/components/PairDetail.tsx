import type { PairRow, ForecastBundle } from "../api/client";
import RouteWeather from "./RouteWeather";

function fmtValue(v: unknown, digits = 2): string {
  if (v === null || v === undefined) return "—";
  const n = Number(v);
  return Number.isFinite(n) ? n.toFixed(digits) : String(v);
}

function fmtMinutes(v: unknown): string {
  if (v === null || v === undefined) return "—";
  const n = Number(v);
  return Number.isFinite(n) ? `${n.toFixed(2)} min` : String(v);
}

export default function PairDetail({
  pair,
  forecast,
  selectedDate,
  selectedTime,
  loading,
}: {
  pair: PairRow | null;
  forecast: ForecastBundle | null;
  selectedDate: string;
  selectedTime: string;
  loading: boolean;
}) {
  if (!pair) {
    return (
      <div className="detail-panel">
        <h2>Pair detail</h2>
        <p className="detail-empty">
          Select a pair from the list to see the route, current weather context, and the small set of metrics used for the live decision.
        </p>
      </div>
    );
  }

  const a = String(pair.airport_A ?? "").trim().toUpperCase();
  const b = String(pair.airport_B ?? "").trim().toUpperCase();
  const pairDelay = pair.xgboost_pair_delay_minutes ?? pair.pair_predicted_weather_delay_minutes;
  const legADelay = pair.leg_a_predicted_weather_delay_minutes;
  const legBDelay = pair.leg_b_predicted_weather_delay_minutes;
  const pairClass = String(pair.xgboost_pair_risk_class ?? pair.integrated_risk_class ?? "—");
  const departureLocal = pair.selected_departure_local;

  return (
    <div className="detail-panel">
      <h2>
        {a} → DFW → {b}
      </h2>
      <p className="detail-empty" style={{ marginBottom: "1rem" }}>
        Month <strong>{String(pair.month)}</strong>
        {" · "}
        Departure <strong>{selectedDate}</strong> at <strong>{selectedTime}</strong>
        {forecast && !loading && (
          <>
            {" "}
            (weather bundle ready)
          </>
        )}
        {loading && <span className="detail-updating"> · Updating…</span>}
      </p>

      <RouteWeather
        airportA={a}
        airportB={b}
        forecast={forecast}
        selectedDate={selectedDate}
        selectedTime={selectedTime}
      />

      <div className="detail-section">
        <h3>Decision Summary</h3>
        <div className="stat-grid">
          <div className="stat stat--emphasis">
            <div className="k">Pair status</div>
            <div className="v">{pairClass}</div>
          </div>
          <div className="stat stat--emphasis">
            <div className="k">Predicted pair delay</div>
            <div className="v">{fmtMinutes(pairDelay)}</div>
          </div>
          <div className="stat">
            <div className="k">Leg A delay</div>
            <div className="v">{fmtMinutes(legADelay)}</div>
          </div>
          <div className="stat">
            <div className="k">Leg B delay</div>
            <div className="v">{fmtMinutes(legBDelay)}</div>
          </div>
          <div className="stat">
            <div className="k">Departure local</div>
            <div className="v">{departureLocal ? String(departureLocal) : "—"}</div>
          </div>
          <div className="stat">
            <div className="k">Month bucket</div>
            <div className="v">{fmtValue(pair.month, 0)}</div>
          </div>
        </div>
      </div>

      <div className="stat-grid">
        <div className="stat">
          <div className="k">Airport A</div>
          <div className="v">{a}</div>
        </div>
        <div className="stat">
          <div className="k">Airport B</div>
          <div className="v">{b}</div>
        </div>
      </div>
    </div>
  );
}
