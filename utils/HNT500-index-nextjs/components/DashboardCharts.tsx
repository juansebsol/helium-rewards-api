"use client";

import type { IndexPoint } from "@/lib/types";
import { BasketUsdChart, BasketHntChart } from "@/components/IndexCharts";

export function DashboardUsdChart({
  series,
  changePct,
  n,
  selectedDay,
  onDaySelect,
  selectedLabel,
}: {
  series: IndexPoint[];
  changePct: number | null;
  n: number;
  selectedDay?: string | null;
  onDaySelect?: (day: string) => void;
  selectedLabel?: string;
}) {
  return (
    <div className="panel chart-panel chart-panel-primary">
      <div className="chart-header">
        <div>
          <div className="chart-title">Index value (USD)</div>
          <div className="chart-sub">
            Top {n} aggregate HNT rewards × daily USD price
            {selectedLabel ? ` · selected ${selectedLabel}` : ""}
          </div>
        </div>
        {changePct != null && (
          <span
            className={`chart-change ${changePct >= 0 ? "up" : "down"}`}
          >
            {changePct >= 0 ? "+" : ""}
            {changePct.toFixed(2)}% 1d
          </span>
        )}
      </div>
      <BasketUsdChart
        series={series}
        variant="usd"
        selectedDay={selectedDay}
        onDaySelect={onDaySelect}
      />
      <p className="chart-footnote">
        <strong>Tip:</strong> Click any point on the curve to load that day&apos;s constituent
        breakdown in the panel below.
      </p>
    </div>
  );
}

export function DashboardHntChart({ series, n }: { series: IndexPoint[]; n: number }) {
  return (
    <div className="panel chart-panel">
      <div className="chart-header">
        <div className="chart-title">Index value (HNT)</div>
        <div className="chart-sub">Raw token rewards for top {n} each day</div>
      </div>
      <BasketHntChart series={series} />
    </div>
  );
}

export function HomePreviewChart({ series, n }: { series: IndexPoint[]; n: number }) {
  if (series.length < 2) return null;
  return (
    <div className="panel chart-panel">
      <div className="chart-header">
        <div>
          <div className="chart-title">Index preview</div>
          <div className="chart-sub">
            {series[0].day} → {series[series.length - 1].day}
          </div>
        </div>
      </div>
      <BasketUsdChart series={series} variant="preview" />
    </div>
  );
}
