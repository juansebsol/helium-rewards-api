import { fmtPct } from "@/lib/format";

export function ChangePill({ pct }: { pct: number | null }) {
  if (pct == null) return <span className="change-pill">—</span>;
  const up = pct >= 0;
  return (
    <span className={`change-pill ${up ? "up" : "down"}`}>
      {up ? "▲" : "▼"} {fmtPct(pct)}
    </span>
  );
}
