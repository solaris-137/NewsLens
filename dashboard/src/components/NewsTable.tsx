import {
  keepPreviousData,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";
import { format, parseISO } from "date-fns";
import { useDeferredValue, useEffect, useState } from "react";

import { fetchFeed } from "@/api";
import type { Article, FeedResponse, WsMessage } from "@/types";

const CATEGORY_STYLES: Record<string, string> = {
  earnings: "bg-[#EEEDFE] text-[#3C3489]",
  products: "bg-[#E6F1FB] text-[#0C447C]",
  legal: "bg-[#FCEBEB] text-[#791F1F]",
  regulatory: "bg-[#FAEEDA] text-[#633806]",
  macroeconomic: "bg-[#F1EFE8] text-[#444441]",
  competition: "bg-[#E1F5EE] text-[#085041]",
  executive: "bg-[#FAECE7] text-[#712B13]",
  supply_chain: "bg-[#EAF3DE] text-[#27500A]",
  general: "bg-[#F1EFE8] text-[#444441]",
};

const SENTIMENT_STYLES: Record<string, string> = {
  positive: "bg-[#EAF3DE] text-[#3B6D11]",
  negative: "bg-[#FCEBEB] text-[#A32D2D]",
  neutral: "bg-[#F1EFE8] text-[#5F5E5A]",
};

const CATEGORY_OPTIONS = [
  "earnings",
  "products",
  "legal",
  "regulatory",
  "macroeconomic",
  "competition",
  "executive",
  "supply_chain",
  "general",
];

const LIMIT = 20;

export default function NewsTable({
  lastMessage,
}: {
  lastMessage: WsMessage | null;
}) {
  const [page, setPage] = useState(0);
  const [categoryFilter, setCategory] = useState("");
  const [sentimentFilter, setSentiment] = useState("");
  const [search, setSearch] = useState("");
  const [newIds, setNewIds] = useState<Set<string>>(new Set());
  const [selected, setSelected] = useState<Article | null>(null);
  const deferredSearch = useDeferredValue(search);
  const queryClient = useQueryClient();

  useEffect(() => {
    setPage(0);
  }, [categoryFilter, sentimentFilter]);

  const { data, isLoading, isFetching } = useQuery({
    queryKey: ["feed", page, categoryFilter, sentimentFilter],
    queryFn: () =>
      fetchFeed({
        limit: LIMIT,
        offset: page * LIMIT,
        category: categoryFilter || undefined,
        sentiment: sentimentFilter || undefined,
      }),
    placeholderData: keepPreviousData,
  });

  useEffect(() => {
    if (!lastMessage || lastMessage.type !== "new_article") {
      return;
    }

    queryClient.setQueryData<FeedResponse>(["feed", 0, "", ""], (old) =>
      old
        ? {
            ...old,
            total: old.total + 1,
            articles: [lastMessage.data, ...old.articles.slice(0, LIMIT - 1)],
          }
        : old,
    );
    setNewIds((previous) => new Set(previous).add(lastMessage.data.id));

    const timer = window.setTimeout(() => {
      setNewIds((previous) => {
        const next = new Set(previous);
        next.delete(lastMessage.data.id);
        return next;
      });
    }, 5000);

    return () => window.clearTimeout(timer);
  }, [lastMessage, queryClient]);

  const filteredArticles =
    data?.articles.filter((article) => {
      if (!deferredSearch.trim()) {
        return true;
      }
      const searchText = deferredSearch.toLowerCase();
      return (
        article.title.toLowerCase().includes(searchText) ||
        article.summary.toLowerCase().includes(searchText) ||
        article.source.toLowerCase().includes(searchText)
      );
    }) ?? [];

  const totalPages = data ? Math.ceil(data.total / LIMIT) : 0;
  const safeTotalPages = Math.max(totalPages, 1);

  return (
    <>
      <section className="rounded-3xl border border-slate-200/70 bg-white/85 p-6 shadow-sm">
        <div className="mb-5 flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <h2 className="text-lg font-semibold text-slate-900">Latest Headlines</h2>
            <p className="mt-1 text-sm text-slate-500">
              Browse incoming Apple coverage, then open a story for the short summary.
            </p>
          </div>
          <div className="grid gap-3 sm:grid-cols-3">
            <input
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="Search this page"
              className="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700 outline-none ring-0 transition focus:border-blue-400"
            />
            <select
              value={categoryFilter}
              onChange={(event) => setCategory(event.target.value)}
              className="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700 outline-none transition focus:border-blue-400"
            >
              <option value="">All categories</option>
              {CATEGORY_OPTIONS.map((category) => (
                <option key={category} value={category}>
                  {category}
                </option>
              ))}
            </select>
            <select
              value={sentimentFilter}
              onChange={(event) => setSentiment(event.target.value)}
              className="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700 outline-none transition focus:border-blue-400"
            >
              <option value="">All sentiment</option>
              <option value="positive">positive</option>
              <option value="negative">negative</option>
              <option value="neutral">neutral</option>
            </select>
          </div>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-200 text-left text-slate-500">
                <th className="py-3 pr-4 font-medium">Time</th>
                <th className="py-3 pr-4 font-medium">Source</th>
                <th className="py-3 pr-4 font-medium">Headline</th>
                <th className="py-3 pr-4 font-medium">Category</th>
                <th className="py-3 pr-4 font-medium">Sentiment</th>
                <th className="py-3 font-medium">Score</th>
              </tr>
            </thead>
            <tbody>
              {isLoading && (
                <tr>
                  <td className="py-6 text-slate-400" colSpan={6}>
                    Loading latest articles...
                  </td>
                </tr>
              )}
              {!isLoading && filteredArticles.length === 0 && (
                <tr>
                  <td className="py-6 text-slate-400" colSpan={6}>
                    No articles match the current filters.
                  </td>
                </tr>
              )}
              {filteredArticles.map((article) => (
                <tr
                  key={article.id}
                  onClick={() => setSelected(article)}
                  className="cursor-pointer border-b border-slate-100 transition-colors hover:bg-slate-50"
                >
                  <td className="whitespace-nowrap py-3 pr-4 text-slate-400">
                    {format(parseISO(article.published_at), "HH:mm")}
                  </td>
                  <td className="py-3 pr-4 capitalize">{article.source}</td>
                  <td className="max-w-md py-3 pr-4">
                    <span className="line-clamp-2">{article.title}</span>
                    {newIds.has(article.id) && (
                      <span className="ml-2 rounded bg-blue-100 px-1.5 py-0.5 text-xs font-medium text-blue-700">
                        NEW
                      </span>
                    )}
                  </td>
                  <td className="py-3 pr-4">
                    <span
                      className={`rounded px-2 py-0.5 text-xs font-medium ${
                        CATEGORY_STYLES[article.category] ?? CATEGORY_STYLES.general
                      }`}
                    >
                      {article.category}
                    </span>
                  </td>
                  <td className="py-3 pr-4">
                    <span
                      className={`rounded px-2 py-0.5 text-xs font-medium ${
                        SENTIMENT_STYLES[article.sentiment_label]
                      }`}
                    >
                      {article.sentiment_label}
                    </span>
                  </td>
                  <td className="py-3 font-mono text-sm">
                    {article.sentiment_score.toFixed(2)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="mt-4 flex items-center justify-between">
          <button
            onClick={() => setPage((current) => Math.max(0, current - 1))}
            disabled={page === 0}
            className="rounded border border-slate-300 px-3 py-1.5 text-sm disabled:opacity-40"
          >
            Previous
          </button>
          <span className="text-sm text-slate-500">
            Page {page + 1} of {safeTotalPages}
            {isFetching ? " - refreshing" : ""}
          </span>
          <button
            onClick={() =>
              setPage((current) => Math.min(safeTotalPages - 1, current + 1))
            }
            disabled={page >= safeTotalPages - 1}
            className="rounded border border-slate-300 px-3 py-1.5 text-sm disabled:opacity-40"
          >
            Next
          </button>
        </div>
      </section>

      {selected && (
        <div className="fixed inset-0 z-50 flex justify-end">
          <div
            className="absolute inset-0 bg-black/30"
            onClick={() => setSelected(null)}
          />
          <div className="relative flex h-full w-full max-w-lg flex-col gap-4 overflow-y-auto bg-white p-6 shadow-xl">
            <button
              onClick={() => setSelected(null)}
              className="self-start text-sm text-slate-400 transition hover:text-slate-600"
            >
              X Close
            </button>
            <p className="text-xs text-slate-400">
              {selected.source} · {format(parseISO(selected.published_at), "PPpp")}
            </p>
            <h2 className="text-lg font-semibold text-slate-900">{selected.title}</h2>
            <div className="flex gap-2">
              <span
                className={`rounded px-2 py-0.5 text-xs font-medium ${
                  CATEGORY_STYLES[selected.category] ?? CATEGORY_STYLES.general
                }`}
              >
                {selected.category}
              </span>
              <span
                className={`rounded px-2 py-0.5 text-xs font-medium ${
                  SENTIMENT_STYLES[selected.sentiment_label]
                }`}
              >
                {selected.sentiment_label}
              </span>
            </div>
            <p className="leading-relaxed text-slate-700">{selected.summary}</p>
            <a
              href={selected.url}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-block text-sm text-blue-600 underline"
            >
              Read original article
            </a>
          </div>
        </div>
      )}
    </>
  );
}
