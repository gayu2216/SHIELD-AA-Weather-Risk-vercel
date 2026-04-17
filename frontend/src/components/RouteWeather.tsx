import type { ForecastAirport, ForecastBundle } from "../api/client";

/** Open-Meteo returns °C; show °F in the UI. */
function cToF(c: number): number {
  return (c * 9) / 5 + 32;
}

function normCode(v: unknown): string {
  return String(v ?? "")
    .trim()
    .toUpperCase();
}

function nearestHourlyRow(rows: Record<string, unknown>[] | undefined, time: string | undefined) {
  if (!rows?.length || !time) return null;
  let best: Record<string, unknown> | null = null;
  let bestDelta = Number.POSITIVE_INFINITY;
  const target = new Date(time).getTime();
  for (const row of rows) {
    const rowTime = typeof row.time === "string" ? Date.parse(row.time) : NaN;
    if (!Number.isFinite(rowTime)) continue;
    const delta = Math.abs(rowTime - target);
    if (delta < bestDelta) {
      best = row;
      bestDelta = delta;
    }
  }
  return best;
}

function hourlyHintLabel(data: ForecastAirport | undefined, selectedTime: string | undefined): string {
  const row = nearestHourlyRow(data?.hourly, selectedTime);
  if (!row) return "—";
  const precip = Number(row.precipitation_probability);
  const wind = Number(row.wind_speed_10m);
  const vis = Number(row.visibility);
  const risk = Math.max(
    Number.isFinite(precip) ? precip / 100 : 0,
    Number.isFinite(wind) ? Math.min(1, wind / 15) : 0,
    Number.isFinite(vis) ? (vis < 4800 ? 1 : 0) : 0,
  );
  return `${Math.round(risk * 100)}% hourly hint`;
}

function WeatherCard({
  role,
  code,
  data,
  selectedTime,
}: {
  role: string;
  code: string;
  data: ForecastBundle["airports"][string] | undefined;
  selectedTime?: string;
}) {
  if (!data) {
    return (
      <div className="weather-card weather-card--empty">
        <div className="weather-card__role">{role}</div>
        <div className="weather-card__code">{code}</div>
        <p className="weather-card__msg">No forecast for this airport.</p>
      </div>
    );
  }
  if (data.error) {
    return (
      <div className="weather-card weather-card--error">
        <div className="weather-card__role">{role}</div>
        <div className="weather-card__code">{code}</div>
        <p className="weather-card__msg">{data.error}</p>
      </div>
    );
  }
  if ((data as { skipped?: boolean }).skipped) {
    return (
      <div className="weather-card weather-card--empty">
        <div className="weather-card__role">{role}</div>
        <div className="weather-card__code">{code}</div>
        <p className="weather-card__msg">Coordinates not configured.</p>
      </div>
    );
  }

  const s = data.summary ?? {};
  const hint = s.forecast_disruption_hint;
  const hintNum = typeof hint === "number" ? hint : Number(hint);
  const hintLabel = Number.isFinite(hintNum) ? `${(hintNum * 100).toFixed(0)}% disruption hint` : "—";
  const hourly = nearestHourlyRow(data.hourly, selectedTime);
  const hourlyTemp = Number(hourly?.temperature_2m);
  const hourlyWind = Number(hourly?.wind_speed_10m);
  const hourlyVis = Number(hourly?.visibility);
  const hourlyPrecip = Number(hourly?.precipitation_probability);
  const useHourly = !!hourly && !!selectedTime;

  return (
    <div className="weather-card">
      <div className="weather-card__role">{role}</div>
      <div className="weather-card__code">{code}</div>
      <ul className="weather-card__stats">
        <li>
          <span>{useHourly ? "Precip chance" : "Max precip chance"}</span>
          <strong>
            {useHourly
              ? (Number.isFinite(hourlyPrecip) ? `${hourlyPrecip}%` : "—")
              : s.max_precip_probability_pct != null
                ? `${s.max_precip_probability_pct}%`
                : "—"}
          </strong>
        </li>
        <li>
          <span>{useHourly ? "Wind (10 m)" : "Max wind (10 m)"}</span>
          <strong>
            {useHourly
              ? (Number.isFinite(hourlyWind) ? `${hourlyWind.toFixed(1)} m/s` : "—")
              : s.max_wind_speed_ms != null
                ? `${Number(s.max_wind_speed_ms).toFixed(1)} m/s`
                : "—"}
          </strong>
        </li>
        <li>
          <span>{useHourly ? "Temperature" : "Temp range"}</span>
          <strong>
            {useHourly
              ? (Number.isFinite(hourlyTemp) ? `${Math.round(cToF(hourlyTemp))} °F` : "—")
              : s.min_temp_c != null && s.max_temp_c != null
                ? `${Math.round(cToF(Number(s.min_temp_c)))}–${Math.round(cToF(Number(s.max_temp_c)))} °F`
                : "—"}
          </strong>
        </li>
        <li>
          <span>{useHourly ? "Visibility" : "Total precip"}</span>
          <strong>
            {useHourly
              ? (Number.isFinite(hourlyVis) ? `${Math.round(hourlyVis)} m` : "—")
              : s.total_precipitation_mm != null
                ? `${s.total_precipitation_mm} mm`
                : "—"}
          </strong>
        </li>
        <li className="weather-card__hint">
          <span>{useHourly ? "Time-specific hint" : "Scheduling hint"}</span>
          <strong>{useHourly ? hourlyHintLabel(data, selectedTime) : hintLabel}</strong>
        </li>
      </ul>
    </div>
  );
}

export default function RouteWeather({
  airportA,
  airportB,
  forecast,
  selectedDate,
  selectedTime,
}: {
  airportA: string;
  airportB: string;
  forecast: ForecastBundle | null;
  selectedDate?: string;
  selectedTime?: string;
}) {
  const a = normCode(airportA);
  const b = normCode(airportB);
  const ap = forecast?.airports ?? {};
  const selectedDateTime =
    selectedDate && selectedTime ? `${selectedDate}T${selectedTime}` : undefined;

  return (
    <div className="route-weather">
      <h3 className="route-weather__title">Weather along route</h3>
      <p className="route-weather__sub">
        {selectedDateTime
          ? `Open-Meteo hourly forecast nearest ${selectedDate} ${selectedTime} local at each airport. Temperatures shown in Fahrenheit.`
          : "Open-Meteo daily forecast for the selected horizon at origin and destination. Temperatures shown in Fahrenheit."}
      </p>
      <div className="route-weather__grid">
        <WeatherCard role="Origin (A)" code={a} data={ap[a]} selectedTime={selectedDateTime} />
        <WeatherCard role="Destination (B)" code={b} data={ap[b]} selectedTime={selectedDateTime} />
      </div>
    </div>
  );
}
