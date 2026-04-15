export interface Article {
  id: string;
  url: string;
  source: string;
  title: string;
  summary: string;
  published_at: string;
  sentiment_label: "positive" | "negative" | "neutral";
  sentiment_score: number;
  sentiment_raw: {
    positive: number;
    negative: number;
    neutral: number;
  };
  category: string;
}

export interface FeedResponse {
  total: number;
  limit: number;
  offset: number;
  articles: Article[];
}

export interface SentimentHour {
  hour: string;
  count: number;
  avg_composite: number;
  positive_count: number;
  negative_count: number;
  neutral_count: number;
}

export interface StockCandle {
  timestamp: string;
  open: number;
  close: number;
  high: number;
  low: number;
  volume: number;
}

export interface StockHistoryResponse {
  history: StockCandle[];
  market_closed: boolean;
  cached: boolean;
}

export interface StockCurrent {
  price: number;
  previous_close: number;
  change_pct: number;
  timestamp: string;
}

export interface DependencyCheck {
  status: string;
  latency_ms?: number;
  error?: string;
  note?: string;
  meta_error?: string;
}

export interface HealthResponse {
  status: "ok" | "degraded" | "down";
  checks: {
    database: DependencyCheck;
    redis: DependencyCheck;
    service_bus: DependencyCheck;
    last_ingestion: string | null;
    articles_24hr: number | null;
  };
}

export interface WsMessage {
  type: "new_article";
  data: Article;
}
