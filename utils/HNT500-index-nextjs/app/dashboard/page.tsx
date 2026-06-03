import Link from "next/link";
import { loadIndexPayload } from "@/lib/index-data";
import { fmtNum, fmtDay } from "@/lib/format";
import { IndexQuoteBand } from "@/components/IndexQuoteBand";
import { DashboardExplorer } from "@/components/DashboardExplorer";

export const dynamic = "force-dynamic";

export const metadata = {
  title: "Index Dashboard | HNT500",
  description: "Live HNT500 basket USD, HNT, and daily history.",
};

export default function DashboardPage() {
  let payload: ReturnType<typeof loadIndexPayload> | null = null;
  let error: string | null = null;

  try {
    payload = loadIndexPayload();
  } catch (e) {
    error = e instanceof Error ? e.message : "Failed to load index";
  }

  if (error || !payload) {
    return (
      <main className="dashboard-main">
        <div className="container dashboard-empty-header">
          <h1>Index dashboard</h1>
          <p>Unable to load data.</p>
        </div>
        <div className="container">
          <div className="panel empty-state">
            <p>{error || "No database"}</p>
            <Link href="/" className="btn-ghost">
              ← Back to overview
            </Link>
          </div>
        </div>
      </main>
    );
  }

  const { series, stats, meta, config } = payload;
  const latest = stats.latest;

  if (!series.length || !latest) {
    return (
      <main className="dashboard-main">
        <div className="container dashboard-empty-header">
          <h1>HNT500 Index</h1>
          <p>No index rows yet.</p>
        </div>
        <div className="container">
          <div className="panel empty-state">
            <p>
              Run a build: <code>npm run rebuild</code> or wait for cron.
            </p>
            <Link href="/" className="btn-ghost">
              ← Overview
            </Link>
          </div>
        </div>
      </main>
    );
  }

  const rangeLabel = `${fmtDay(series[0].day)} — ${fmtDay(series[series.length - 1].day)}`;
  const updatedAt = meta.latestBuild?.finished_at
    ? new Date(meta.latestBuild.finished_at).toLocaleString()
    : latest.day;

  const incompleteDays = stats.incompleteDays;
  const lowHnt =
    latest.requested_n >= 100 && latest.basket_hnt < latest.requested_n * 0.5;

  return (
    <main className="dashboard-main">
      <IndexQuoteBand
        latest={latest}
        stats={stats}
        n={config.n}
        rangeLabel={rangeLabel}
        updatedAt={updatedAt}
        incompleteDays={incompleteDays}
      />

      <div className="dashboard-body container container-wide">
        {(incompleteDays > 0 || lowHnt) && (
          <div className={`alert-banner${lowHnt ? " error" : ""}`}>
            {lowHnt ? (
              <>
                <strong>Index totals look too low.</strong> Latest basket is only{" "}
                {fmtNum(latest.basket_hnt, 0)} HNT for top {latest.requested_n}. Run{" "}
                <code>npm run rebuild</code> after updating (sum of <code>total_hnt</code>).
              </>
            ) : (
              <>
                <strong>{incompleteDays} day(s) have incomplete data.</strong> Run{" "}
                <code>npm run rebuild</code> to refresh from Nexus.
              </>
            )}
          </div>
        )}

        <DashboardExplorer series={series} stats={stats} config={config} />
      </div>
    </main>
  );
}
