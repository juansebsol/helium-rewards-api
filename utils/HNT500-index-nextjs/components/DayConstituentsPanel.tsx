"use client";

import { Fragment, useCallback, useEffect, useMemo, useState } from "react";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Filler,
  Tooltip,
  Legend,
} from "chart.js";
import type { ChartOptions } from "chart.js";
import { Bar, Line } from "react-chartjs-2";
import type { IndexPoint } from "@/lib/types";
import { heliumWorldHotspotUrl } from "@/lib/helium-world";

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Filler,
  Tooltip,
  Legend
);

type Constituent = {
  rank: number;
  device_id: string;
  entity_name: string | null;
  total_hnt: number;
  total_dc: number;
  weight: number;
  status: "new" | "stayed";
  delta_hnt: number | null;
  days_in_index: number;
  streak_days: number;
};

type ApiResponse = {
  ok: boolean;
  error?: string;
  day?: string;
  prev_day?: string | null;
  day_meta?: {
    basket_hnt: number;
    basket_usd: number;
    hnt_price: number;
    constituent_count: number;
    fetch_complete: boolean;
  };
  summary?: {
    count: number;
    sum_hnt: number;
    basket_delta_hnt: number | null;
    new_count: number;
    exited_count: number;
    new_hnt: number;
    outperform_hnt: number;
  };
  items?: Constituent[];
};

type HistoryPoint = {
  day: string;
  rank: number;
  total_hnt: number;
  weight: number;
  delta_hnt: number | null;
  share: number | null;
  basket_hnt: number | null;
  hnt_price: number | null;
};

type HistoryResponse = {
  ok: boolean;
  error?: string;
  summary?: {
    days_in_window: number;
    window_days: number;
    sum_hnt: number;
    avg_hnt: number;
  };
  items?: HistoryPoint[];
};

const PAGE_SIZE = 50;
const HISTORY_DAY_OPTIONS = [7, 14, 30] as const;
const HISTORY_VIEWS = ["line", "bars", "table"] as const;
type HistoryView = (typeof HISTORY_VIEWS)[number];
const HISTORY_METRICS = ["hnt", "rank", "share"] as const;
type HistoryMetric = (typeof HISTORY_METRICS)[number];

const HISTORY_VIEW_LABELS: Record<HistoryView, string> = {
  line: "Line",
  bars: "Bars",
  table: "Table",
};

const HISTORY_METRIC_LABELS: Record<HistoryMetric, string> = {
  hnt: "HNT",
  rank: "Rank",
  share: "Share",
};

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

function fmtDelta(n: number | null | undefined, d = 2) {
  if (n == null) return "—";
  const sign = n > 0 ? "+" : "";
  return `${sign}${fmtNum(n, d)}`;
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
  const [nameById, setNameById] = useState<Record<string, string>>({});
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [historyDays, setHistoryDays] = useState<(typeof HISTORY_DAY_OPTIONS)[number]>(14);
  const [historyView, setHistoryView] = useState<HistoryView>("line");
  const [historyMetric, setHistoryMetric] = useState<HistoryMetric>("hnt");

  const load = useCallback(async () => {
    if (!selectedDay) return;
    setLoading(true);
    setError(null);
    setPage(0);
    setNameById({});
    setExpandedId(null);
    try {
      const url =
        `/api/constituents?day=${encodeURIComponent(selectedDay)}` +
        `&n=${n}&weight_mode=${encodeURIComponent(weightMode)}&resolve_names=0`;
      const res = await fetch(url);
      const json = (await res.json()) as ApiResponse;
      if (!res.ok || !json.ok) {
        setData(null);
        setError(json.error || `HTTP ${res.status}`);
        return;
      }
      setData(json);
      const seeded: Record<string, string> = {};
      for (const item of json.items || []) {
        if (item.entity_name) seeded[item.device_id] = item.entity_name;
      }
      if (Object.keys(seeded).length) setNameById(seeded);
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
    return items.filter((c) => {
      const name = (nameById[c.device_id] || c.entity_name || "").toLowerCase();
      return c.device_id.toLowerCase().includes(q) || name.includes(q);
    });
  }, [items, search, nameById]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const pageItems = filtered.slice(page * PAGE_SIZE, page * PAGE_SIZE + PAGE_SIZE);
  const dayPoint = seriesByDay.get(selectedDay);
  const summary = data?.summary;

  useEffect(() => {
    const missing = pageItems
      .map((c) => c.device_id)
      .filter((id) => !nameById[id]);
    if (!missing.length) return;

    let cancelled = false;
    (async () => {
      try {
        const res = await fetch("/api/entity-names", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ device_ids: missing }),
        });
        const json = (await res.json()) as {
          ok?: boolean;
          names?: Record<string, string>;
        };
        if (cancelled || !json.ok || !json.names) return;
        setNameById((prev) => ({ ...prev, ...json.names }));
      } catch {
        // names are optional
      }
    })();

    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedDay, page, filtered.length, pageItems.map((c) => c.device_id).join("|")]);

  const panelClass =
    layout === "split"
      ? "panel constituents-panel daily-explorer-col"
      : "panel constituents-panel";

  return (
    <div className={panelClass}>
      <div className="daily-explorer-panel-header constituents-header">
        <div>
          <div className="chart-title">Day breakdown</div>
          <div className="chart-sub">
            {fmtDay(selectedDay)} · {n} constituents
            {data?.prev_day ? ` · vs ${fmtDay(data.prev_day)}` : ""}
          </div>
        </div>
        {summary && (
          <div className="constituents-summary-badges">
            <span className="badge">{summary.count} devices</span>
            <span className="badge">{fmtNum(summary.sum_hnt, 0)} HNT</span>
            {summary.basket_delta_hnt != null && (
              <span className="badge">
                {fmtDelta(summary.basket_delta_hnt, 0)} HNT vs prior
              </span>
            )}
            {summary.new_count > 0 && (
              <span className="badge">{summary.new_count} new</span>
            )}
            {data?.day_meta && (
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
                placeholder="Filter by name or device_id…"
                value={search}
                onChange={(e) => {
                  setSearch(e.target.value);
                  setPage(0);
                  setExpandedId(null);
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
                  onClick={() => {
                    setPage((p) => Math.max(0, p - 1));
                    setExpandedId(null);
                  }}
                >
                  Prev
                </button>
                <button
                  type="button"
                  className="btn-ghost"
                  disabled={page >= totalPages - 1}
                  onClick={() => {
                    setPage((p) => Math.min(totalPages - 1, p + 1));
                    setExpandedId(null);
                  }}
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
                    <th>1d chg</th>
                    <th>Days</th>
                    <th>Wt</th>
                    <th>Share</th>
                    <th className="col-expand" aria-label="History" />
                  </tr>
                </thead>
                <tbody>
                  {pageItems.map((c) => {
                    const share =
                      dayPoint && dayPoint.basket_hnt > 0
                        ? (c.total_hnt / dayPoint.basket_hnt) * 100
                        : 0;
                    const animalName = nameById[c.device_id] || c.entity_name;
                    const worldUrl = heliumWorldHotspotUrl(c.device_id);
                    const open = expandedId === c.device_id;
                    return (
                      <Fragment key={`${c.rank}-${c.device_id}`}>
                        <tr className={open ? "row-expanded" : undefined}>
                          <td>{c.rank}</td>
                          <td title={c.device_id}>
                            <div className="device-cell">
                              {worldUrl ? (
                                <a
                                  className={`device-name device-link${animalName ? "" : " pending"}`}
                                  href={worldUrl}
                                  target="_blank"
                                  rel="noopener noreferrer"
                                  title="Open in Helium World"
                                >
                                  {animalName || "…"}
                                </a>
                              ) : (
                                <span className={`device-name${animalName ? "" : " pending"}`}>
                                  {animalName || "…"}
                                </span>
                              )}
                              <span className="device-id mono">{shortId(c.device_id)}</span>
                            </div>
                          </td>
                          <td>{fmtNum(c.total_hnt, 4)}</td>
                          <td
                            className={
                              c.status === "new"
                                ? "delta-new"
                                : (c.delta_hnt ?? 0) > 0
                                  ? "delta-up"
                                  : (c.delta_hnt ?? 0) < 0
                                    ? "delta-down"
                                    : ""
                            }
                          >
                            {c.status === "new" ? "new" : fmtDelta(c.delta_hnt, 3)}
                          </td>
                          <td title={`${c.streak_days}d streak`}>{c.days_in_index}</td>
                          <td>{fmtNum(c.weight * 100, 2)}%</td>
                          <td>{fmtNum(share, 2)}%</td>
                          <td className="col-expand">
                            <button
                              type="button"
                              className={`expand-btn${open ? " open" : ""}`}
                              aria-expanded={open}
                              aria-label={open ? "Hide device history" : "Show device history"}
                              onClick={() =>
                                setExpandedId((id) => (id === c.device_id ? null : c.device_id))
                              }
                            >
                              <span aria-hidden>▾</span>
                            </button>
                          </td>
                        </tr>
                        {open && (
                          <tr className="device-history-row">
                            <td colSpan={8}>
                              <DeviceHistoryPanel
                                deviceId={c.device_id}
                                deviceName={animalName}
                                throughDay={selectedDay}
                                n={n}
                                weightMode={weightMode}
                                days={historyDays}
                                onDaysChange={setHistoryDays}
                                view={historyView}
                                onViewChange={setHistoryView}
                                metric={historyMetric}
                                onMetricChange={setHistoryMetric}
                              />
                            </td>
                          </tr>
                        )}
                      </Fragment>
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

function DeviceHistoryPanel({
  deviceId,
  deviceName,
  throughDay,
  n,
  weightMode,
  days,
  onDaysChange,
  view,
  onViewChange,
  metric,
  onMetricChange,
}: {
  deviceId: string;
  deviceName: string | null | undefined;
  throughDay: string;
  n: number;
  weightMode: string;
  days: (typeof HISTORY_DAY_OPTIONS)[number];
  onDaysChange: (d: (typeof HISTORY_DAY_OPTIONS)[number]) => void;
  view: HistoryView;
  onViewChange: (v: HistoryView) => void;
  metric: HistoryMetric;
  onMetricChange: (m: HistoryMetric) => void;
}) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [payload, setPayload] = useState<HistoryResponse | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    (async () => {
      try {
        const url =
          `/api/device-history?device_id=${encodeURIComponent(deviceId)}` +
          `&through=${encodeURIComponent(throughDay)}` +
          `&n=${n}&weight_mode=${encodeURIComponent(weightMode)}&days=${days}`;
        const res = await fetch(url);
        const json = (await res.json()) as HistoryResponse;
        if (cancelled) return;
        if (!res.ok || !json.ok) {
          setPayload(null);
          setError(json.error || `HTTP ${res.status}`);
          return;
        }
        setPayload(json);
      } catch (e) {
        if (!cancelled) {
          setPayload(null);
          setError(e instanceof Error ? e.message : "Failed to load history");
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [deviceId, throughDay, n, weightMode, days]);

  const chronological = useMemo(
    () => [...(payload?.items || [])].reverse(),
    [payload?.items]
  );

  return (
    <div className="device-history">
      <div className="device-history-head">
        <div>
          <div className="device-history-title">
            {deviceName || shortId(deviceId)} · index history
          </div>
          <div className="device-history-sub">
            Days this device appeared in HNT{n}, ending {fmtDay(throughDay)}
          </div>
        </div>
        <div className="device-history-controls">
          <div className="device-history-toggles" role="group" aria-label="History view">
            {HISTORY_VIEWS.map((v) => (
              <button
                key={v}
                type="button"
                className={`device-history-toggle${view === v ? " active" : ""}`}
                aria-pressed={view === v}
                onClick={() => onViewChange(v)}
              >
                {HISTORY_VIEW_LABELS[v]}
              </button>
            ))}
          </div>
          {view !== "table" && (
            <div className="device-history-toggles" role="group" aria-label="Chart metric">
              {HISTORY_METRICS.map((m) => (
                <button
                  key={m}
                  type="button"
                  className={`device-history-toggle${metric === m ? " active" : ""}`}
                  aria-pressed={metric === m}
                  onClick={() => onMetricChange(m)}
                >
                  {HISTORY_METRIC_LABELS[m]}
                </button>
              ))}
            </div>
          )}
          <label className="device-history-days">
            <span>Lookback</span>
            <select
              value={days}
              onChange={(e) =>
                onDaysChange(Number(e.target.value) as (typeof HISTORY_DAY_OPTIONS)[number])
              }
            >
              {HISTORY_DAY_OPTIONS.map((d) => (
                <option key={d} value={d}>
                  {d} days
                </option>
              ))}
            </select>
          </label>
        </div>
      </div>

      {loading && <p className="device-history-status">Loading history…</p>}
      {error && <p className="device-history-status error">{error}</p>}

      {!loading && !error && payload?.summary && (
        <div className="device-history-meta">
          <span>
            In index {payload.summary.days_in_window}/{payload.summary.window_days} days
          </span>
          <span>Σ {fmtNum(payload.summary.sum_hnt, 2)} HNT</span>
          <span>Avg {fmtNum(payload.summary.avg_hnt, 2)} HNT/day in</span>
        </div>
      )}

      {!loading && !error && payload?.items && (
        payload.items.length === 0 ? (
          <p className="device-history-status">No index appearances in this window.</p>
        ) : view === "table" ? (
          <DeviceHistoryTable items={payload.items} />
        ) : (
          <DeviceHistoryChart items={chronological} view={view} metric={metric} />
        )
      )}
    </div>
  );
}

function DeviceHistoryTable({ items }: { items: HistoryPoint[] }) {
  return (
    <table className="device-history-table">
      <thead>
        <tr>
          <th>Day</th>
          <th>#</th>
          <th>HNT</th>
          <th>1d chg</th>
          <th>Share</th>
        </tr>
      </thead>
      <tbody>
        {items.map((row) => (
          <tr key={row.day}>
            <td>{fmtDay(row.day)}</td>
            <td>{row.rank}</td>
            <td>{fmtNum(row.total_hnt, 4)}</td>
            <td
              className={
                (row.delta_hnt ?? 0) > 0
                  ? "delta-up"
                  : (row.delta_hnt ?? 0) < 0
                    ? "delta-down"
                    : ""
              }
            >
              {fmtDelta(row.delta_hnt, 3)}
            </td>
            <td>{row.share != null ? `${fmtNum(row.share, 2)}%` : "—"}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function DeviceHistoryChart({
  items,
  view,
  metric,
}: {
  items: HistoryPoint[];
  view: Exclude<HistoryView, "table">;
  metric: HistoryMetric;
}) {
  const labels = items.map((r) =>
    new Date(`${r.day}T12:00:00`).toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
    })
  );

  const values = items.map((r) => {
    if (metric === "rank") return r.rank;
    if (metric === "share") return r.share ?? 0;
    return r.total_hnt;
  });

  const palette =
    metric === "rank"
      ? { stroke: "rgba(148, 163, 184, 0.95)", fill: "rgba(148, 163, 184, 0.08)", bar: "rgba(148, 163, 184, 0.55)" }
      : metric === "share"
        ? { stroke: "rgba(52, 211, 153, 0.95)", fill: "rgba(52, 211, 153, 0.08)", bar: "rgba(52, 211, 153, 0.55)" }
        : { stroke: "rgba(232, 201, 106, 0.95)", fill: "rgba(201, 162, 39, 0.1)", bar: "rgba(232, 201, 106, 0.55)" };

  const yFormat = (v: number) => {
    if (metric === "rank") return `#${Math.round(v)}`;
    if (metric === "share") return `${v.toFixed(1)}%`;
    return v.toLocaleString("en-US", { maximumFractionDigits: 1 });
  };

  const baseOptions = {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: "index" as const, intersect: false },
    plugins: {
      legend: { display: false },
      tooltip: {
        backgroundColor: "#111827",
        borderColor: "rgba(255,255,255,0.08)",
        borderWidth: 1,
        titleFont: { family: "var(--font-sans)", size: 12 },
        bodyFont: { family: "var(--font-mono)", size: 12 },
        padding: 12,
        cornerRadius: 10,
        displayColors: false,
        callbacks: {
          title: (tooltipItems: { dataIndex: number }[]) => {
            const row = items[tooltipItems[0]?.dataIndex ?? 0];
            return row ? fmtDay(row.day) : "";
          },
          label: (ctx: { dataIndex: number }) => {
            const row = items[ctx.dataIndex];
            if (!row) return "";
            return [
              ` HNT ${fmtNum(row.total_hnt, 4)}`,
              ` Rank #${row.rank}`,
              ` Share ${row.share != null ? `${fmtNum(row.share, 2)}%` : "—"}`,
              ` 1d ${fmtDelta(row.delta_hnt, 3)}`,
            ];
          },
        },
      },
    },
    scales: {
      x: {
        grid: { display: false },
        border: { display: false },
        ticks: {
          color: "#64748b",
          maxRotation: 0,
          autoSkip: true,
          maxTicksLimit: 8,
          font: { size: 11 },
          padding: 6,
        },
      },
      y: {
        reverse: metric === "rank",
        grid: { color: "rgba(255, 255, 255, 0.04)" },
        border: { display: false },
        ticks: {
          color: "#64748b",
          callback: (v: string | number) => yFormat(Number(v)),
          font: { size: 11, family: "var(--font-mono)" },
          padding: 8,
        },
      },
    },
  };

  if (view === "line") {
    const options = baseOptions as ChartOptions<"line">;
    return (
      <div className="device-history-chart-wrap">
        <Line
          data={{
            labels,
            datasets: [
              {
                label: HISTORY_METRIC_LABELS[metric],
                data: values,
                borderColor: palette.stroke,
                backgroundColor: palette.fill,
                borderWidth: 2.25,
                pointRadius: 3,
                pointHoverRadius: 5,
                pointBackgroundColor: palette.stroke,
                tension: 0.35,
                fill: true,
              },
            ],
          }}
          options={options}
        />
      </div>
    );
  }

  const options = baseOptions as ChartOptions<"bar">;
  return (
    <div className="device-history-chart-wrap">
      <Bar
        data={{
          labels,
          datasets: [
            {
              label: HISTORY_METRIC_LABELS[metric],
              data: values,
              backgroundColor: palette.bar,
              borderRadius: 4,
              borderSkipped: false,
            },
          ],
        }}
        options={options}
      />
    </div>
  );
}
