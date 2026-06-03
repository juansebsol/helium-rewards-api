"use client";

import { useMemo, useState } from "react";
import type { IndexPoint } from "@/lib/types";
import type { IndexStats } from "@/lib/types";
import { DashboardUsdChart, DashboardHntChart } from "@/components/DashboardCharts";
import { DayConstituentsPanel } from "@/components/DayConstituentsPanel";

function fmtUsd(n: number, d = 2) {
  return (
    "$" +
    n.toLocaleString("en-US", { maximumFractionDigits: d, minimumFractionDigits: d })
  );
}

function fmtNum(n: number, d = 2) {
  return n.toLocaleString("en-US", { maximumFractionDigits: d });
}

function fmtPct(n: number | null, d = 2) {
  if (n == null || !Number.isFinite(n)) return "—";
  const sign = n > 0 ? "+" : "";
  return `${sign}${n.toFixed(d)}%`;
}

function fmtDay(day: string) {
  return new Date(`${day}T12:00:00`).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

export function DashboardExplorer({
  series,
  stats,
  config,
}: {
  series: IndexPoint[];
  stats: IndexStats;
  config: { n: number; weightMode: string; historyDays: number };
}) {
  const latestDay = series[series.length - 1]?.day;
  const [selectedDay, setSelectedDay] = useState(latestDay || "");

  const seriesByDay = useMemo(
    () => new Map(series.map((r) => [r.day, r])),
    [series]
  );

  const historyRows = useMemo(() => [...series].reverse(), [series]);
  const selectedLabel = selectedDay ? fmtDay(selectedDay) : "";

  return (
    <>
      <section className="dashboard-section" aria-labelledby="performance-heading">
        <div className="section-head">
          <div>
            <h2 id="performance-heading">Performance</h2>
            <p>Basket value over {stats.days} trading days · click the chart to drill into a day</p>
          </div>
        </div>

        <DashboardUsdChart
          series={series}
          changePct={stats.changePct}
          n={config.n}
          selectedDay={selectedDay}
          onDaySelect={setSelectedDay}
          selectedLabel={selectedLabel}
        />

        <div className="chart-secondary-row">
          <DashboardHntChart series={series} n={config.n} />
          <div className="panel chart-panel">
            <div className="chart-header">
              <div className="chart-title">Methodology</div>
              <div className="chart-sub">
                Daily-reconstituted top {config.n}, like the S&P 500
              </div>
            </div>
            <div className="chart-footnote" style={{ marginTop: 0, borderTop: "none", paddingTop: 0 }}>
              <p style={{ marginBottom: 14 }}>
                Each session ranks that calendar day&apos;s highest earners from Nexus. Basket HNT
                is the sum of <strong>total_hnt</strong> across constituents; USD applies the
                day&apos;s CoinGecko HNT price.
              </p>
              <p>
                Weight mode: <strong>{config.weightMode}</strong> · API{" "}
                <a href="/api/index" style={{ color: "var(--accent-bright)" }}>
                  /api/index
                </a>
              </p>
            </div>
          </div>
        </div>
      </section>

      <section className="dashboard-section" aria-labelledby="constituents-heading">
        <div className="section-head">
          <div>
            <h2 id="constituents-heading">Constituents</h2>
            <p>
              Global basket history and per-device rewards
              {selectedDay ? ` · viewing ${selectedLabel}` : ""}
            </p>
          </div>
        </div>

        <div className="daily-explorer">
          <div className="daily-explorer-grid">
            <div className="panel history-panel daily-explorer-col">
              <div className="daily-explorer-panel-header">
                <div>
                  <div className="chart-title">Global history</div>
                  <div className="chart-sub">Aggregate top-{config.n} basket by day</div>
                </div>
                <span className="daily-explorer-count">{historyRows.length} days</span>
              </div>
              <div className="daily-explorer-scroll history-table-wrap">
                <table className="history-table history-table-compact">
                  <thead>
                    <tr>
                      <th>Date</th>
                      <th>Value (USD)</th>
                      <th>HNT</th>
                      <th>Load</th>
                    </tr>
                  </thead>
                  <tbody>
                    {historyRows.map((row, i) => {
                      const prev = historyRows[i + 1];
                      const chg =
                        prev && prev.basket_usd > 0
                          ? ((row.basket_usd - prev.basket_usd) / prev.basket_usd) * 100
                          : null;
                      const selected = row.day === selectedDay;
                      return (
                        <tr
                          key={row.day}
                          className={
                            selected ? "history-row-selected" : "history-row-clickable"
                          }
                          onClick={() => setSelectedDay(row.day)}
                          onKeyDown={(e) => {
                            if (e.key === "Enter" || e.key === " ") {
                              e.preventDefault();
                              setSelectedDay(row.day);
                            }
                          }}
                          tabIndex={0}
                          role="button"
                          aria-selected={selected}
                        >
                          <td className="history-day-cell">{fmtDay(row.day)}</td>
                          <td>
                            <span className="history-primary">{fmtUsd(row.basket_usd)}</span>
                            {chg != null && (
                              <span
                                className={
                                  chg >= 0 ? "history-delta up" : "history-delta down"
                                }
                              >
                                {fmtPct(chg)}
                              </span>
                            )}
                          </td>
                          <td>{fmtNum(row.basket_hnt, 0)}</td>
                          <td className="history-load-cell">
                            {row.constituent_count}/{row.requested_n}
                            {!row.fetch_complete && (
                              <span className="history-partial" title="Incomplete fetch">
                                !
                              </span>
                            )}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>

            {selectedDay ? (
              <DayConstituentsPanel
                selectedDay={selectedDay}
                seriesByDay={seriesByDay}
                n={config.n}
                weightMode={config.weightMode}
                layout="split"
              />
            ) : (
              <div className="panel daily-explorer-col daily-explorer-placeholder">
                <p>Select a date on the chart or in global history.</p>
              </div>
            )}
          </div>
        </div>
      </section>
    </>
  );
}
