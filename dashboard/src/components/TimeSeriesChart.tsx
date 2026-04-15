import { useQuery } from "@tanstack/react-query";
import { format, parseISO } from "date-fns";
import {
  Bar,
  CartesianGrid,
  Cell,
  ComposedChart,
  Legend,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { fetchSentimentSummary, fetchStockHistory } from "@/api";
import type { SentimentHour, StockCandle } from "@/types";

function toHourKey(isoString: string) {
  return format(parseISO(isoString), "yyyy-MM-dd HH:00");
}

function mergeChartData(stock: StockCandle[], sentiment: SentimentHour[]) {
  const sentimentMap = new Map(
    sentiment.map((hour) => [
      toHourKey(hour.hour),
      { sentiment: hour.avg_composite, count: hour.count },
    ]),
  );

  return stock.map((candle) => {
    const sentimentPoint = sentimentMap.get(toHourKey(candle.timestamp));
    return {
      time: format(parseISO(candle.timestamp), "HH:mm"),
      price: candle.close,
      sentiment: sentimentPoint?.sentiment ?? null,
      articleCount: sentimentPoint?.count ?? 0,
    };
  });
}

export default function TimeSeriesChart() {
  const { data: stockResponse } = useQuery({
    queryKey: ["stock-history"],
    queryFn: fetchStockHistory,
    refetchInterval: 5 * 60 * 1000,
  });
  const { data: sentiment } = useQuery({
    queryKey: ["sentiment-summary"],
    queryFn: fetchSentimentSummary,
    refetchInterval: 5 * 60 * 1000,
  });

  const chartData =
    stockResponse && sentiment
      ? mergeChartData(stockResponse.history, sentiment)
      : [];

  return (
    <section className="rounded-3xl border border-slate-200/70 bg-white/85 p-6 shadow-sm">
      <div className="mb-4 flex items-start justify-between gap-4">
        <div>
          <h2 className="text-lg font-semibold text-slate-900">
            AAPL Price vs Sentiment (Last 24h)
          </h2>
          <p className="mt-1 text-sm text-slate-500">
            Stock candles and news mood are aligned on hourly sentiment buckets.
          </p>
        </div>
        {stockResponse?.market_closed && (
          <span className="rounded-full bg-amber-50 px-3 py-1 text-xs font-medium text-amber-700">
            Market closed
          </span>
        )}
      </div>

      <ResponsiveContainer width="100%" height={320}>
        <ComposedChart data={chartData}>
          <CartesianGrid stroke="#e2e8f0" strokeDasharray="3 3" />
          <XAxis dataKey="time" tick={{ fontSize: 12 }} />
          <YAxis
            yAxisId="price"
            orientation="left"
            domain={["auto", "auto"]}
            tickFormatter={(value) => `$${value}`}
            tick={{ fontSize: 12 }}
          />
          <YAxis
            yAxisId="sentiment"
            orientation="right"
            domain={[-1, 1]}
            tickFormatter={(value) => Number(value).toFixed(1)}
            tick={{ fontSize: 12 }}
          />
          <Tooltip
            content={({ active, payload, label }) => {
              if (!active || !payload?.length) {
                return null;
              }

              const datum = payload[0]?.payload as
                | {
                    price?: number;
                    sentiment?: number | null;
                    articleCount?: number;
                  }
                | undefined;

              return (
                <div className="rounded-xl border border-slate-200 bg-white p-3 text-sm shadow-lg">
                  <p className="font-medium text-slate-900">{label}</p>
                  <p className="text-blue-700">
                    AAPL: ${datum?.price?.toFixed(2) ?? "n/a"}
                  </p>
                  <p className="text-slate-600">
                    Sentiment:{" "}
                    {datum?.sentiment != null
                      ? datum.sentiment.toFixed(3)
                      : "n/a"}
                  </p>
                  <p className="text-slate-400">
                    {datum?.articleCount ?? 0} articles
                  </p>
                </div>
              );
            }}
          />
          <Legend />
          <Line
            yAxisId="price"
            type="monotone"
            dataKey="price"
            stroke="#185FA5"
            dot={false}
            strokeWidth={2}
            name="AAPL Price (USD)"
          />
          <Bar
            yAxisId="sentiment"
            dataKey="sentiment"
            name="Avg Sentiment Score"
          >
            {chartData.map((entry, index) => (
              <Cell
                key={`${entry.time}-${index}`}
                fill={
                  entry.sentiment == null
                    ? "#cbd5e1"
                    : entry.sentiment >= 0
                      ? "#22c55e"
                      : "#ef4444"
                }
              />
            ))}
          </Bar>
        </ComposedChart>
      </ResponsiveContainer>
    </section>
  );
}
