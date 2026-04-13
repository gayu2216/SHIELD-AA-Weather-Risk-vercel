export type WindowsResponse = {
  windows: number[];
  max_days: number;
  min_days: number;
  months?: number[];
  month_names?: string[];
};

export type PairRow = Record<string, unknown>;

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
  airports: Record<
    string,
    {
      daily?: unknown[];
      summary?: Record<string, unknown>;
      error?: string;
      latitude?: number;
      longitude?: number;
    }
  >;
};

export async function fetchWindows(): Promise<WindowsResponse> {
  const r = await fetch("/api/windows");
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function fetchPairs(days: number, month: number): Promise<PairsApiResponse> {
  const q = new URLSearchParams({ days: String(days), month: String(month) });
  const r = await fetch(`/api/risk/pairs?${q.toString()}`);
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function fetchForecast(days: number): Promise<ForecastBundle> {
  const r = await fetch(`/api/forecast?days=${encodeURIComponent(String(days))}`);
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}
