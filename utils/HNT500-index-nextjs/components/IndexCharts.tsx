"use client";

import { useRef } from "react";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Filler,
  Tooltip,
  Legend,
} from "chart.js";
import type { ChartOptions } from "chart.js";
import { Line } from "react-chartjs-2";
import type { IndexPoint } from "@/lib/types";

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Filler,
  Tooltip,
  Legend
);

const gridColor = "rgba(255, 255, 255, 0.04)";
const tickColor = "#64748b";
const lineGold = "rgba(232, 201, 106, 0.95)";
const fillGold = "rgba(201, 162, 39, 0.08)";
const lineHnt = "rgba(148, 163, 184, 0.85)";
const fillHnt = "rgba(148, 163, 184, 0.06)";
const selectPoint = "#e8c96a";

function baseOptions(yFormat: (v: number) => string, height?: "sm" | "lg") {
  return {
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
        bodyFont: { family: "var(--font-mono)", size: 13 },
        padding: 14,
        cornerRadius: 10,
        displayColors: false,
      },
    },
    scales: {
      x: {
        grid: { display: false },
        border: { display: false },
        ticks: {
          color: tickColor,
          maxRotation: 0,
          autoSkip: true,
          maxTicksLimit: height === "sm" ? 6 : 8,
          font: { size: 11 },
          padding: 8,
        },
      },
      y: {
        grid: { color: gridColor },
        border: { display: false },
        ticks: {
          color: tickColor,
          callback: (v: string | number) => yFormat(Number(v)),
          font: { size: 11, family: "var(--font-mono)" },
          padding: 10,
        },
      },
    },
  };
}

type Props = {
  series: IndexPoint[];
  variant?: "usd" | "hnt" | "preview";
  selectedDay?: string | null;
  onDaySelect?: (day: string) => void;
};

export function BasketUsdChart({
  series,
  variant = "usd",
  selectedDay = null,
  onDaySelect,
}: Props) {
  const chartRef = useRef<ChartJS<"line">>(null);
  const labels = series.map((r) => r.day);
  const data = series.map((r) => r.basket_usd);
  const interactive = variant === "usd" && !!onDaySelect;

  const heightClass =
    variant === "preview" ? "chart-wrap sm" : variant === "usd" ? "chart-wrap lg" : "chart-wrap";

  const base = baseOptions(
    (v) =>
      "$" +
      v.toLocaleString("en-US", {
        maximumFractionDigits: 0,
      }),
    variant === "preview" ? "sm" : "lg"
  );

  const options: ChartOptions<"line"> = {
    ...base,
    onClick: interactive
      ? (_e, elements) => {
          if (!elements.length) return;
          const idx = elements[0].index;
          const day = series[idx]?.day;
          if (day) onDaySelect(day);
        }
      : undefined,
    plugins: {
      ...base.plugins,
      tooltip: {
        ...base.plugins?.tooltip,
        callbacks: {
          title: (items) => {
            const row = series[items[0]?.dataIndex ?? 0];
            return row?.day ?? "";
          },
          label: (ctx) => {
            const row = series[ctx.dataIndex];
            return ` ${row.basket_usd.toLocaleString("en-US", {
              style: "currency",
              currency: "USD",
              maximumFractionDigits: 2,
            })}`;
          },
          afterLabel: interactive ? () => "Click to view constituents" : undefined,
        },
      },
    },
  };

  return (
    <div className={`${heightClass}${interactive ? " chart-interactive" : ""}`}>
      <Line
        ref={chartRef}
        data={{
          labels,
          datasets: [
            {
              label: "Basket USD",
              data,
              borderColor: lineGold,
              backgroundColor: fillGold,
              borderWidth: variant === "preview" ? 1.5 : 2.5,
              pointRadius:
                variant === "preview"
                  ? 0
                  : labels.map((d) => (d === selectedDay ? 8 : 0)),
              pointHoverRadius: 6,
              pointBackgroundColor: labels.map((d) =>
                d === selectedDay ? selectPoint : lineGold
              ),
              pointBorderColor: labels.map((d) =>
                d === selectedDay ? "#0a0c10" : "transparent"
              ),
              pointBorderWidth: labels.map((d) => (d === selectedDay ? 2 : 0)),
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

export function BasketHntChart({ series }: { series: IndexPoint[] }) {
  const labels = series.map((r) => r.day);
  const data = series.map((r) => r.basket_hnt);

  return (
    <div className="chart-wrap">
      <Line
        data={{
          labels,
          datasets: [
            {
              label: "Basket HNT",
              data,
              borderColor: lineHnt,
              backgroundColor: fillHnt,
              borderWidth: 2,
              pointRadius: 0,
              pointHoverRadius: 4,
              tension: 0.35,
              fill: true,
            },
          ],
        }}
        options={{
          ...baseOptions((v) =>
            v.toLocaleString("en-US", { maximumFractionDigits: 0 })
          ),
          plugins: {
            ...baseOptions(() => "").plugins,
            tooltip: {
              ...baseOptions(() => "").plugins?.tooltip,
              callbacks: {
                title: (items) => {
                  const row = series[items[0]?.dataIndex ?? 0];
                  return row?.day ?? "";
                },
                label: (ctx) => {
                  const row = series[ctx.dataIndex];
                  return ` ${row.basket_hnt.toLocaleString("en-US", { maximumFractionDigits: 2 })} HNT`;
                },
              },
            },
          },
        }}
      />
    </div>
  );
}
