import { useQuery } from "@tanstack/react-query";
import { format, parseISO } from "date-fns";
import {
  Bar,
  BarChart,
  Cell,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { fetchFeed, fetchHealth, fetchMetricsText, fetchSentimentSummary } from "@/api";

const STATUS_DOT: Record<string, string> = {
  ok: "bg-green-500",
  degraded: "bg-amber-400",
  down: "bg-red-500",
};

function parseMetric(metricsText: string | undefined, metricName: string) {
  if (!metricsText) {
    return null;
  }

  const line = metricsText
    .split("\n")
    .find((entry) => entry.startsWith(`${metricName} `));

  if (!line) {
    return null;
  }

  const value = Number(line.split(" ").at(-1));
  return Number.isFinite(value) ? value : null;
}

export default function Monitoring() {
  const { data: health } = useQuery({
    queryKey: ["health"],
    queryFn: fetchHealth,
    refetchInterval: 30 * 1000,
  });
  const { data: sentiment } = useQuery({
    queryKey: ["sentiment-summary"],
    queryFn: fetchSentimentSummary,
  });
  const { data: feed } = useQuery({
    queryKey: ["feed", "monitoring", 100],
    queryFn: () => fetchFeed({ limit: 100, offset: 0 }),
  });
  const { data: metricsText } = useQuery({
    queryKey: ["metrics-text"],
    queryFn: fetchMetricsText,
    refetchInterval: 30 * 1000,
  });

  const donutData = sentiment
    ? [
        {
          name: "Positive",
          value: sentiment.reduce((sum, hour) => sum + hour.positive_count, 0),
          fill: "#22c55e",
        },
        {
          name: "Negative",
          value: sentiment.reduce((sum, hour) => sum + hour.negative_count, 0),
          fill: "#ef4444",
        },
        {
          name: "Neutral",
          value: sentiment.reduce((sum, hour) => sum + hour.neutral_count, 0),
          fill: "#94a3b8",
        },
      ]
    : [];

  const sourceMap = new Map<string, number>();
  for (const article of feed?.articles ?? []) {
    sourceMap.set(article.source, (sourceMap.get(article.source) ?? 0) + 1);
  }
  const sourceData = Array.from(sourceMap.entries()).map(([source, count]) => ({
    source,
    count,
  }));

  const latestLatency = parseMetric(metricsText, "pipeline_latency_ms");

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
                const status = value.status ?? "unknown";
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
                      {value.latency_ms != null ? `${value.latency_ms}ms` : ""}
                    </td>
                  </tr>
                );
              })}
          </tbody>
        </table>

        <div className="mt-4 grid gap-4 text-sm text-slate-500 md:grid-cols-2 xl:grid-cols-5">
          <div>
            Last ingestion
            <br />
            <span className="font-medium text-slate-800">
              {health?.checks.last_ingestion
                ? format(parseISO(health.checks.last_ingestion), "HH:mm:ss")
                : "-"}
            </span>
          </div>
          <div>
            Articles today
            <br />
            <span className="font-medium text-slate-800">
              {health?.checks.articles_24hr ?? "-"}
            </span>
          </div>
          <div>
            Overall status
            <br />
            <span
              className={`font-medium ${
                health?.status === "ok"
                  ? "text-green-600"
                  : health?.status === "degraded"
                    ? "text-amber-500"
                    : "text-red-600"
              }`}
            >
              {health?.status ?? "-"}
            </span>
          </div>
          <div>
            Pipeline latency
            <br />
            <span className="font-medium text-slate-800">
              {latestLatency != null ? `${latestLatency.toFixed(0)} ms` : "-"}
            </span>
          </div>
          <div>
            Extraction fail rate
            <br />
            <span className="font-medium text-slate-800">Not exposed</span>
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
                innerRadius={68}
                outerRadius={104}
                paddingAngle={3}
              >
                {donutData.map((entry) => (
                  <Cell key={entry.name} fill={entry.fill} />
                ))}
              </Pie>
              <Tooltip />
            </PieChart>
          </ResponsiveContainer>
          <div className="mt-4 flex gap-4 text-sm text-slate-500">
            {donutData.map((entry) => (
              <div key={entry.name} className="flex items-center gap-2">
                <span
                  className="inline-block h-2.5 w-2.5 rounded-full"
                  style={{ backgroundColor: entry.fill }}
                />
                <span>
                  {entry.name}: {entry.value}
                </span>
              </div>
            ))}
          </div>
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
