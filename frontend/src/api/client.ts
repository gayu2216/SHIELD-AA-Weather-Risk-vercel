export type WindowsResponse = {
  windows: number[];
  max_days: number;
  min_days: number;
  months?: number[];
  month_names?: string[];
};

export type PairRow = Record<string, unknown>;

export type ForecastAirport = {
  daily?: Record<string, unknown>[];
  hourly?: Record<string, unknown>[];
  summary?: Record<string, unknown>;
  error?: string;
  latitude?: number;
  longitude?: number;
};

export type PairsApiResponse = {
  meta: Record<string, unknown>;
  summary: {
    rows?: number;
    rows_before_month_filter?: number;
    month_filter?: number | null;
    forbidden_count?: number;
    returned_rows?: number;
    forecast_window_days?: number;
  };
  pairs: PairRow[];
};

export type ForecastBundle = {
  window_days: number;
  generated_at_utc?: string;
  selected_departure_date?: string;
  selected_departure_time?: string;
  airports: Record<string, ForecastAirport>;
};

export async function fetchWindows(): Promise<WindowsResponse> {
  const r = await fetch("/api/windows");
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function fetchPairs(
  days: number,
  date?: string,
  time?: string,
): Promise<PairsApiResponse> {
  const q = new URLSearchParams({ days: String(days) });
  if (date) q.set("date", date);
  if (time) q.set("time", time);
  const r = await fetch(`/api/risk/pairs?${q.toString()}`);
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function fetchForecast(days: number, date?: string, time?: string): Promise<ForecastBundle> {
  const q = new URLSearchParams({ days: String(days) });
  if (date) q.set("date", date);
  if (time) q.set("time", time);
  const r = await fetch(`/api/forecast?${q.toString()}`);
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}
