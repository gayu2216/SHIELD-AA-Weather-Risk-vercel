import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import MapBoard from "../components/MapBoard";
import PairDetail from "../components/PairDetail";
import { fetchForecast, fetchPairs } from "../api/client";
import type { PairRow, ForecastBundle, PairsApiResponse } from "../api/client";
import "../styles/dashboard.css";

const DEFAULT_REQUEST_DAYS = 1;

function todayLocalDate(): string {
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

export default function Dashboard() {
  const [selectedDate, setSelectedDate] = useState<string>(todayLocalDate());
  const [selectedTime, setSelectedTime] = useState<string>("08:00");
  const [pairs, setPairs] = useState<PairRow[]>([]);
  const [summary, setSummary] = useState<PairsApiResponse["summary"] | null>(null);
  const [forecast, setForecast] = useState<ForecastBundle | null>(null);
  const [selected, setSelected] = useState<PairRow | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const loadSeq = useRef(0);

  const sortedPairs = useMemo(() => {
    const copy = [...pairs];
    copy.sort((x, y) => {
      const ax = Number(x.xgboost_pair_delay_minutes ?? x.xgboost_pair_risk_score ?? x.pair_predicted_weather_delay_minutes ?? 0);
      const ay = Number(y.xgboost_pair_delay_minutes ?? y.xgboost_pair_risk_score ?? y.pair_predicted_weather_delay_minutes ?? 0);
      return ay - ax;
    });
    return copy;
  }, [pairs]);

  const load = useCallback(async (date: string, time: string) => {
    const seq = ++loadSeq.current;
    setLoading(true);
    setError(null);
    try {
      const [pr, fc] = await Promise.all([
        fetchPairs(DEFAULT_REQUEST_DAYS, date, time),
        fetchForecast(DEFAULT_REQUEST_DAYS, date, time),
      ]);
      if (seq !== loadSeq.current) return;
      setPairs(pr.pairs);
      setSummary(pr.summary ?? null);
      setForecast(fc);
      setSelected((cur) => {
        if (!cur) return null;
        const hit = pr.pairs.find((p) => samePairRow(p, cur));
        return hit ?? null;
      });
    } catch (e) {
      if (seq !== loadSeq.current) return;
      setError(e instanceof Error ? e.message : String(e));
      setPairs([]);
      setForecast(null);
    } finally {
      if (seq === loadSeq.current) setLoading(false);
    }
  }, []);

  const applyDefaultsAndLoad = () => {
    const date = todayLocalDate();
    const time = "08:00";
    setSelectedDate(date);
    setSelectedTime(time);
    void load(date, time);
  };

  const loadFromControls = () => {
    void load(selectedDate, selectedTime);
  };

  useEffect(() => {
    void load(selectedDate, selectedTime);
  }, [load]);

  return (
    <div className="dashboard">
      <header className="dash-header">
        <Link to="/" className="dash-brand">
          SHIELD
        </Link>
        <div className="dash-controls">
          <label>
            Departure date
            <input type="date" value={selectedDate} onChange={(e) => setSelectedDate(e.target.value)} />
          </label>
          <label>
            Departure time
            <input type="time" value={selectedTime} onChange={(e) => setSelectedTime(e.target.value)} />
          </label>
          <button type="button" className="dash-btn dash-btn-secondary" onClick={applyDefaultsAndLoad}>
            Defaults (today · 08:00)
          </button>
          <button type="button" className="dash-btn dash-btn-primary" onClick={loadFromControls} disabled={loading}>
            {loading ? "Loading…" : "Load"}
          </button>
          <Link className="dash-link" to="/">
            ← Home
          </Link>
        </div>
      </header>

      <div className={`dash-status ${error ? "error" : ""}`}>
        {error
          ? error
          : summary
            ? `Rows ${summary.returned_rows ?? pairs.length} · Forbidden ${summary.forbidden_count ?? "—"} · XGBoost-only · month ${summary.month_filter ?? "—"} · ${selectedDate} ${selectedTime}`
            : "Choose departure date and time, then Load."}
      </div>

      <div className="dash-body">
        <aside className="pair-sidebar">
          <h2>Pairs ({sortedPairs.length})</h2>
          <div className="pair-scroll">
            {sortedPairs.map((p, i) => {
              const forbidden = String(p.xgboost_pair_risk_class ?? p.integrated_risk_class) === "Forbidden";
              const sel = selected ? samePairRow(p, selected) : false;
              return (
                <button
                  key={`${p.airport_A}-${p.airport_B}-${p.month}-${i}`}
                  type="button"
                  className={`pair-item ${forbidden ? "forbidden" : "safe"}${sel ? " selected" : ""}`}
                  onClick={() => setSelected(p)}
                >
                  <div className="route">
                    {String(p.airport_A)} → DFW → {String(p.airport_B)}
                  </div>
                  <div className="meta">
                    delay {fmtMinutes(p.xgboost_pair_delay_minutes ?? p.xgboost_pair_risk_score ?? p.pair_predicted_weather_delay_minutes)} · {String(p.xgboost_pair_risk_class ?? p.integrated_risk_class ?? "—")}
                  </div>
                </button>
              );
            })}
          </div>
        </aside>

        <MapBoard pairs={sortedPairs} selected={selected} />

        <PairDetail
          pair={selected}
          forecast={forecast}
          selectedDate={selectedDate}
          selectedTime={selectedTime}
          loading={loading}
        />
      </div>
    </div>
  );
}

function fmtMinutes(v: unknown): string {
  const n = Number(v);
  return Number.isFinite(n) ? `${n.toFixed(1)} min` : "—";
}

function normAp(v: unknown): string {
  return String(v ?? "")
    .trim()
    .toUpperCase();
}

function monthKey(v: unknown): number {
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : NaN;
}

function samePairRow(p: PairRow, cur: PairRow): boolean {
  return (
    normAp(p.airport_A) === normAp(cur.airport_A) &&
    normAp(p.airport_B) === normAp(cur.airport_B) &&
    monthKey(p.month) === monthKey(cur.month)
  );
}
