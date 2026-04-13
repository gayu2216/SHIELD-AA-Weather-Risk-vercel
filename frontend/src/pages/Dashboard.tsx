import { useCallback, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import MapBoard from "../components/MapBoard";
import PairDetail from "../components/PairDetail";
import { fetchForecast, fetchPairs } from "../api/client";
import type { PairRow, ForecastBundle, PairsApiResponse } from "../api/client";
import "../styles/dashboard.css";

function currentMonth(): number {
  return new Date().getMonth() + 1;
}

export default function Dashboard() {
  const [month, setMonth] = useState<number>(currentMonth());
  const [days, setDays] = useState<number>(10);
  const [pairs, setPairs] = useState<PairRow[]>([]);
  const [summary, setSummary] = useState<PairsApiResponse["summary"] | null>(null);
  const [forecast, setForecast] = useState<ForecastBundle | null>(null);
  const [selected, setSelected] = useState<PairRow | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const sortedPairs = useMemo(() => {
    const copy = [...pairs];
    copy.sort((x, y) => {
      const ax = Number(x.integrated_risk_score ?? 0);
      const ay = Number(y.integrated_risk_score ?? 0);
      return ay - ax;
    });
    return copy;
  }, [pairs]);

  const load = useCallback(
    async (d: number, m: number) => {
      setLoading(true);
      setError(null);
      try {
        const [pr, fc] = await Promise.all([fetchPairs(d, m), fetchForecast(d)]);
        setPairs(pr.pairs);
        setSummary(pr.summary ?? null);
        setForecast(fc);
        setSelected((cur) => {
          if (!cur) return null;
          const hit = pr.pairs.find(
            (p) =>
              String(p.airport_A) === String(cur.airport_A) &&
              String(p.airport_B) === String(cur.airport_B) &&
              Number(p.month) === Number(cur.month)
          );
          return hit ?? null;
        });
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
        setPairs([]);
        setForecast(null);
      } finally {
        setLoading(false);
      }
    },
    []
  );

  const applyDefaultsAndLoad = () => {
    const m = currentMonth();
    setMonth(m);
    setDays(10);
    void load(10, m);
  };

  const loadFromControls = () => {
    void load(days, month);
  };

  const changeWindowForSelectedPair = (newDays: number) => {
    setDays(newDays);
    void load(newDays, month);
  };

  return (
    <div className="dashboard">
      <header className="dash-header">
        <Link to="/" className="dash-brand">
          SHIELD
        </Link>
        <div className="dash-controls">
          <label>
            Month
            <select value={month} onChange={(e) => setMonth(Number(e.target.value))}>
              {Array.from({ length: 12 }, (_, i) => i + 1).map((mo) => (
                <option key={mo} value={mo}>
                  {mo}
                </option>
              ))}
            </select>
          </label>
          <label>
            Forecast days
            <input
              type="number"
              min={1}
              max={16}
              value={days}
              onChange={(e) => setDays(Math.min(16, Math.max(1, Number(e.target.value) || 1)))}
            />
          </label>
          <button type="button" className="dash-btn dash-btn-secondary" onClick={applyDefaultsAndLoad}>
            Defaults (10 days · this month)
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
            ? `Rows ${summary.returned_rows ?? pairs.length} · Forbidden ${summary.forbidden_count ?? "—"} · horizon ${summary.forecast_window_days ?? days}d · month ${summary.month_filter ?? month}`
            : "Choose month and forecast window, then Load (or use defaults)."}
      </div>

      <div className="dash-body">
        <aside className="pair-sidebar">
          <h2>Pairs ({sortedPairs.length})</h2>
          <div className="pair-scroll">
            {sortedPairs.map((p, i) => {
              const forbidden = String(p.integrated_risk_class) === "Forbidden";
              const sel =
                selected &&
                String(selected.airport_A) === String(p.airport_A) &&
                String(selected.airport_B) === String(p.airport_B) &&
                Number(selected.month) === Number(p.month);
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
                    integrated {fmtScore(p.integrated_risk_score)} · {String(p.integrated_risk_class ?? "—")}
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
          forecastDays={days}
          onChangeWindow={changeWindowForSelectedPair}
        />
      </div>
    </div>
  );
}

function fmtScore(v: unknown): string {
  const n = Number(v);
  return Number.isFinite(n) ? n.toFixed(3) : "—";
}
