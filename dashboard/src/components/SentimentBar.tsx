import { useQuery } from "@tanstack/react-query";

import { fetchSentimentSummary } from "@/api";
import type { SentimentHour } from "@/types";

function computeTotals(hours: SentimentHour[]) {
  const total = hours.reduce((sum, hour) => sum + hour.count, 0);
  const positive = hours.reduce((sum, hour) => sum + hour.positive_count, 0);
  const negative = hours.reduce((sum, hour) => sum + hour.negative_count, 0);
  const neutral = hours.reduce((sum, hour) => sum + hour.neutral_count, 0);
  return { total, positive, negative, neutral };
}

export default function SentimentBar({
  isConnected,
}: {
  isConnected: boolean;
}) {
  const { data, dataUpdatedAt } = useQuery({
    queryKey: ["sentiment-summary"],
    queryFn: fetchSentimentSummary,
    refetchInterval: 5 * 60 * 1000,
  });

  const totals = data ? computeTotals(data) : null;
  const minutesAgo = dataUpdatedAt
    ? Math.floor((Date.now() - dataUpdatedAt) / 60000)
    : null;

  return (
    <section className="rounded-3xl border border-slate-200/70 bg-white/85 p-6 shadow-sm">
      <div className="mb-4 flex items-center gap-2">
        <span
          className={`h-2.5 w-2.5 rounded-full ${
            isConnected ? "animate-pulse bg-green-500" : "bg-slate-400"
          }`}
        />
        <span className="text-sm text-slate-500">
          {isConnected ? "Live" : "Disconnected"}
        </span>
        {minutesAgo !== null && (
          <span className="ml-auto text-sm text-slate-400">
            Updated {minutesAgo === 0 ? "just now" : `${minutesAgo}m ago`}
          </span>
        )}
      </div>

      <div className="grid gap-4 md:grid-cols-3">
        {[
          {
            label: "Positive",
            count: totals?.positive,
            color: "text-green-600",
            bg: "bg-green-50",
          },
          {
            label: "Negative",
            count: totals?.negative,
            color: "text-red-600",
            bg: "bg-red-50",
          },
          {
            label: "Neutral",
            count: totals?.neutral,
            color: "text-slate-600",
            bg: "bg-slate-50",
          },
        ].map(({ label, count, color, bg }) => (
          <div key={label} className={`${bg} rounded-2xl p-5`}>
            <p className="text-sm text-slate-500">{label}</p>
            <p className={`text-3xl font-bold ${color}`}>{count ?? "-"}</p>
            <p className="text-sm text-slate-400">
              {totals?.total && count != null
                ? `${((count / totals.total) * 100).toFixed(1)}%`
                : ""}
            </p>
          </div>
        ))}
      </div>
    </section>
  );
}
