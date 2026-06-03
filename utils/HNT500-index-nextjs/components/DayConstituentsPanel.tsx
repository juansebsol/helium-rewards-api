"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import type { IndexPoint } from "@/lib/types";

type Constituent = {
  rank: number;
  device_id: string;
  total_hnt: number;
  total_dc: number;
  weight: number;
};

type ApiResponse = {
  ok: boolean;
  error?: string;
  day?: string;
  day_meta?: {
    basket_hnt: number;
    basket_usd: number;
    hnt_price: number;
    constituent_count: number;
    fetch_complete: boolean;
  };
  summary?: { count: number; sum_hnt: number };
  items?: Constituent[];
};

const PAGE_SIZE = 50;

function fmtNum(n: number, d = 2) {
  return n.toLocaleString("en-US", { maximumFractionDigits: d });
}

function fmtUsd(n: number, d = 2) {
  return (
    "$" +
    n.toLocaleString("en-US", { maximumFractionDigits: d, minimumFractionDigits: d })
  );
}

function fmtDay(day: string) {
  return new Date(`${day}T12:00:00`).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

function shortId(id: string) {
  return id.length > 20 ? `${id.slice(0, 12)}…${id.slice(-6)}` : id;
}

export function DayConstituentsPanel({
  selectedDay,
  seriesByDay,
  n,
  weightMode,
  layout = "stacked",
}: {
  selectedDay: string;
  seriesByDay: Map<string, IndexPoint>;
  n: number;
  weightMode: string;
  layout?: "stacked" | "split";
}) {
  const [data, setData] = useState<ApiResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState(0);
  const [search, setSearch] = useState("");

  const load = useCallback(async () => {
    if (!selectedDay) return;
    setLoading(true);
    setError(null);
    setPage(0);
    try {
      const url = `/api/constituents?day=${encodeURIComponent(selectedDay)}&n=${n}&weight_mode=${encodeURIComponent(weightMode)}`;
      const res = await fetch(url);
      const json = (await res.json()) as ApiResponse;
      if (!res.ok || !json.ok) {
        setData(null);
        setError(json.error || `HTTP ${res.status}`);
        return;
      }
      setData(json);
    } catch (e) {
      setData(null);
      setError(e instanceof Error ? e.message : "Failed to load");
    } finally {
      setLoading(false);
    }
  }, [selectedDay, n, weightMode]);

  useEffect(() => {
    load();
  }, [load]);

  const items = data?.items || [];
  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return items;
    return items.filter((c) => c.device_id.toLowerCase().includes(q));
  }, [items, search]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const pageItems = filtered.slice(page * PAGE_SIZE, page * PAGE_SIZE + PAGE_SIZE);
  const dayPoint = seriesByDay.get(selectedDay);

  const panelClass =
    layout === "split"
      ? "panel constituents-panel daily-explorer-col"
      : "panel constituents-panel";

  return (
    <div className={panelClass}>
      <div className="daily-explorer-panel-header constituents-header">
        <div>
          <div className="chart-title">Day breakdown</div>
          <div className="chart-sub">{fmtDay(selectedDay)} · {n} constituents</div>
        </div>
        {data?.summary && (
          <div className="constituents-summary-badges">
            <span className="badge">{data.summary.count} devices</span>
            <span className="badge">{fmtNum(data.summary.sum_hnt, 0)} HNT</span>
            {data.day_meta && (
              <span className="badge">{fmtUsd(data.day_meta.basket_usd)}</span>
            )}
          </div>
        )}
      </div>

      <div className="constituents-body">
        {loading && <p className="constituents-status">Loading constituents…</p>}
        {error && (
          <div className="alert-banner error constituents-error">
            <strong>{error}</strong>
            <p>
              Run <code>npm run rebuild</code> to store per-device snapshots for each day.
            </p>
          </div>
        )}

        {!loading && !error && data?.items && (
          <>
          <div className="constituents-toolbar">
            <input
              type="search"
              className="constituents-search"
              placeholder="Filter by device_id…"
              value={search}
              onChange={(e) => {
                setSearch(e.target.value);
                setPage(0);
              }}
            />
            <span className="constituents-page-info">
              {filtered.length} shown · page {page + 1}/{totalPages}
            </span>
            <div className="constituents-pager">
              <button
                type="button"
                className="btn-ghost"
                disabled={page <= 0}
                onClick={() => setPage((p) => Math.max(0, p - 1))}
              >
                Prev
              </button>
              <button
                type="button"
                className="btn-ghost"
                disabled={page >= totalPages - 1}
                onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
              >
                Next
              </button>
            </div>
          </div>

          <div className="daily-explorer-scroll constituents-table-wrap">
            <table className="history-table constituents-table">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Device</th>
                  <th>HNT</th>
                  <th>Wt</th>
                  <th>Share</th>
                </tr>
              </thead>
              <tbody>
                {pageItems.map((c) => {
                  const share =
                    dayPoint && dayPoint.basket_hnt > 0
                      ? (c.total_hnt / dayPoint.basket_hnt) * 100
                      : 0;
                  return (
                    <tr key={`${c.rank}-${c.device_id}`}>
                      <td>{c.rank}</td>
                      <td className="mono" title={c.device_id}>
                        {shortId(c.device_id)}
                      </td>
                      <td>{fmtNum(c.total_hnt, 4)}</td>
                      <td>{fmtNum(c.weight * 100, 2)}%</td>
                      <td>{fmtNum(share, 2)}%</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          </>
        )}
      </div>
    </div>
  );
}
