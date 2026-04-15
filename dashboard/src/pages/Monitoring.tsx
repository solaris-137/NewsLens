import { useMemo } from "react";

import { useQuery } from "@tanstack/react-query";
import { formatDistanceToNow, parseISO } from "date-fns";
import {
  Bar,
  BarChart,
  Cell,
  Legend,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { fetchFeed, fetchHealth, fetchSentimentSummary } from "@/api";

const STATUS_DOT: Record<string, string> = {
  ok: "bg-green-500",
  degraded: "bg-amber-400",
  down: "bg-red-500",
};

export default function Monitoring() {
  const { data: health } = useQuery({
    queryKey: ["health"],
    queryFn: fetchHealth,
    refetchInterval: 30 * 1000,
  });
  const { data: sentimentData } = useQuery({
    queryKey: ["sentiment-summary"],
    queryFn: fetchSentimentSummary,
    refetchInterval: 5 * 60 * 1000,
  });
  const { data: feedData } = useQuery({
    queryKey: ["feed-monitoring"],
    queryFn: () => fetchFeed({ limit: 100, offset: 0 }),
    refetchInterval: 5 * 60 * 1000,
  });

  const sourceData = useMemo(() => {
    if (!feedData) {
      return [];
    }

    const counts: Record<string, number> = {};
    feedData.articles.forEach((article) => {
      counts[article.source] = (counts[article.source] ?? 0) + 1;
    });

    return Object.entries(counts).map(([source, count]) => ({ source, count }));
  }, [feedData]);

  const donutData = useMemo(() => {
    if (!sentimentData) {
      return [];
    }

    const positive = sentimentData.reduce(
      (sum, hour) => sum + hour.positive_count,
      0,
    );
    const negative = sentimentData.reduce(
      (sum, hour) => sum + hour.negative_count,
      0,
    );
    const neutral = sentimentData.reduce(
      (sum, hour) => sum + hour.neutral_count,
      0,
    );

    return [
      { name: "Positive", value: positive, color: "#22c55e" },
      { name: "Negative", value: negative, color: "#ef4444" },
      { name: "Neutral", value: neutral, color: "#9ca3af" },
    ];
  }, [sentimentData]);

  return (
    <div className="space-y-8">
      <h1 className="text-2xl font-bold tracking-tight text-slate-900">
        System Monitoring
      </h1>

      <section className="rounded-3xl border border-slate-200/70 bg-white/85 p-6 shadow-sm">
        <h2 className="mb-4 font-semibold text-slate-900">Health Checks</h2>
        <table className="w-full text-sm">
          <tbody>
            {health &&
              Object.entries(health.checks).map(([key, value]) => {
                if (typeof value !== "object" || value === null) {
                  return null;
                }
                const check = value as {
                  status?: string;
                  latency_ms?: number;
                };
                const status = check.status ?? "unknown";
                return (
                  <tr key={key} className="border-b border-slate-100">
                    <td className="py-2 pr-4 capitalize text-slate-600">
                      {key.replace(/_/g, " ")}
                    </td>
                    <td className="py-2 pr-4">
                      <span
                        className={`inline-block h-2.5 w-2.5 rounded-full ${
                          STATUS_DOT[status] ?? "bg-slate-400"
                        }`}
                      />
                      <span className="ml-2 capitalize">{status}</span>
                    </td>
                    <td className="py-2 text-slate-400">
                      {check.latency_ms != null ? `${check.latency_ms}ms` : ""}
                    </td>
                  </tr>
                );
              })}
          </tbody>
        </table>

        <div className="mt-6 grid gap-4 text-sm md:grid-cols-3">
          <div className="rounded-lg bg-slate-50 p-4">
            <p className="text-slate-500">Last Ingestion</p>
            <p className="mt-1 font-semibold text-slate-900">
              {health?.checks.last_ingestion
                ? formatDistanceToNow(parseISO(health.checks.last_ingestion), {
                    addSuffix: true,
                  })
                : "-"}
            </p>
          </div>
          <div className="rounded-lg bg-slate-50 p-4">
            <p className="text-slate-500">Articles (24hr)</p>
            <p className="mt-1 font-semibold text-slate-900">
              {health?.checks.articles_24hr ?? "-"}
            </p>
          </div>
          <div className="rounded-lg bg-slate-50 p-4">
            <p className="text-slate-500">System Status</p>
            <p
              className={`mt-1 font-semibold ${
                health?.status === "ok"
                  ? "text-green-600"
                  : health?.status === "degraded"
                    ? "text-amber-500"
                    : "text-red-600"
              }`}
            >
              {health?.status ?? "-"}
            </p>
          </div>
        </div>
      </section>

      <div className="grid gap-8 lg:grid-cols-2">
        <section className="rounded-3xl border border-slate-200/70 bg-white/85 p-6 shadow-sm">
          <h2 className="mb-4 font-semibold text-slate-900">
            Sentiment Distribution
          </h2>
          <ResponsiveContainer width="100%" height={280}>
            <PieChart>
              <Pie
                data={donutData}
                dataKey="value"
                nameKey="name"
                cx="50%"
                cy="50%"
                innerRadius={55}
                outerRadius={80}
                paddingAngle={3}
              >
                {donutData.map((entry) => (
                  <Cell key={entry.name} fill={entry.color} />
                ))}
              </Pie>
              <Tooltip formatter={(value, name) => [`${value} articles`, name]} />
              <Legend />
            </PieChart>
          </ResponsiveContainer>
        </section>

        <section className="rounded-3xl border border-slate-200/70 bg-white/85 p-6 shadow-sm">
          <h2 className="mb-4 font-semibold text-slate-900">
            Articles by Source
          </h2>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={sourceData}>
              <XAxis dataKey="source" tick={{ fontSize: 12 }} />
              <YAxis tick={{ fontSize: 12 }} allowDecimals={false} />
              <Tooltip />
              <Bar dataKey="count" fill="#2563eb" radius={[6, 6, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </section>
      </div>
    </div>
  );
}
