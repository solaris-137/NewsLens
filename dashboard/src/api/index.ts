import type {
  FeedResponse,
  HealthResponse,
  SentimentHour,
  StockCurrent,
  StockHistoryResponse,
} from "@/types";

const BASE = import.meta.env.VITE_API_BASE_URL;

async function apiFetch<T>(
  path: string,
  params?: Record<string, string>,
): Promise<T> {
  const url = new URL(BASE + path);
  if (params) {
    Object.entries(params).forEach(([key, value]) => {
      url.searchParams.set(key, value);
    });
  }

  const response = await fetch(url.toString());
  if (!response.ok) {
    throw new Error(`API error ${response.status}: ${path}`);
  }
  return response.json() as Promise<T>;
}

export async function apiFetchText(path: string): Promise<string> {
  const url = new URL(BASE + path);
  const response = await fetch(url.toString());
  if (!response.ok) {
    throw new Error(`API error ${response.status}: ${path}`);
  }
  return response.text();
}

export const fetchFeed = (params: {
  limit?: number;
  offset?: number;
  category?: string;
  sentiment?: string;
}) =>
  apiFetch<FeedResponse>("/api/feed", {
    limit: String(params.limit ?? 20),
    offset: String(params.offset ?? 0),
    ...(params.category && { category: params.category }),
    ...(params.sentiment && { sentiment: params.sentiment }),
  });

export const fetchSentimentSummary = () =>
  apiFetch<SentimentHour[]>("/api/sentiment/summary");

export const fetchStockHistory = () =>
  apiFetch<StockHistoryResponse>("/api/stock/history");

export const fetchStockCurrent = () =>
  apiFetch<StockCurrent>("/api/stock/current");

export const fetchHealth = () => apiFetch<HealthResponse>("/api/health");

export const fetchMetricsText = () => apiFetchText("/api/metrics");
