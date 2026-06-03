import Link from "next/link";
import { loadIndexPayload } from "@/lib/index-data";
import { fmtUsd, fmtNum, fmtDay } from "@/lib/format";
import { ChangePill } from "@/components/ChangePill";
import { HomePreviewChart } from "@/components/DashboardCharts";

export const dynamic = "force-dynamic";

export default function HomePage() {
  let series: Awaited<ReturnType<typeof loadIndexPayload>>["series"] = [];
  let stats: Awaited<ReturnType<typeof loadIndexPayload>>["stats"] | null = null;
  let meta: Awaited<ReturnType<typeof loadIndexPayload>>["meta"] | null = null;
  let config: Awaited<ReturnType<typeof loadIndexPayload>>["config"] | null = null;
  let hasData = false;

  try {
    const payload = loadIndexPayload();
    series = payload.series;
    stats = payload.stats;
    meta = payload.meta;
    config = payload.config;
    hasData = series.length > 0;
  } catch {
    hasData = false;
  }

  const latest = stats?.latest;

  return (
    <>
      {hasData && latest && (
        <div className="ticker-strip container container-wide">
          <div className="ticker-item">
            <span className="ticker-label">HNT500</span>
            <span>{fmtUsd(latest.basket_usd)}</span>
            <ChangePill pct={stats?.changePct ?? null} />
          </div>
          <div className="ticker-item">
            <span className="ticker-label">Basket HNT</span>
            <span>{fmtNum(latest.basket_hnt, 0)}</span>
          </div>
          <div className="ticker-item">
            <span className="ticker-label">Coverage</span>
            <span>
              {latest.constituent_count}/{latest.requested_n}
            </span>
          </div>
          <div className="ticker-item">
            <span className="ticker-label">As of</span>
            <span>{fmtDay(latest.day)}</span>
          </div>
          <div className="ticker-item">
            <span className="badge-live">Live</span>
          </div>
        </div>
      )}

      <section className="hero">
        <div className="container">
          <p className="hero-eyebrow">Helium Network · Fractals</p>
          <h1>
            The <span>HNT500</span> Index
          </h1>
          <p className="hero-lead">
            A market-style benchmark for Helium rewards — the daily performance of the
            network&apos;s top 500 earners, reconstituted every session like the S&amp;P 500.
          </p>
          <div className="hero-actions">
            <Link href="/dashboard" className="btn-primary">
              View index dashboard →
            </Link>
            <a href="#methodology" className="btn-ghost">
              How it works
            </a>
          </div>
        </div>
      </section>

      {hasData && series.length > 1 && config && (
        <section className="section" style={{ paddingTop: 0 }}>
          <div className="container">
            <div className="section-head" style={{ marginBottom: 20 }}>
              <h2 className="section-title" style={{ marginBottom: 0 }}>
                Live preview
              </h2>
              <Link href="/dashboard" className="btn-ghost" style={{ padding: "10px 18px", fontSize: 13 }}>
                Full dashboard →
              </Link>
            </div>
            <HomePreviewChart series={series} n={config.n} />
          </div>
        </section>
      )}

      <section className="section" id="methodology">
        <div className="container">
          <h2 className="section-title">What is HNT500?</h2>
          <div className="explainer-grid">
            <article className="explainer-card">
              <h3>Daily reconstitution</h3>
              <p>
                Each calendar day we rank Helium&apos;s top 500 earners for that specific day.
                Membership changes daily — devices enter and leave the basket, just like the S&amp;P
                500.
              </p>
            </article>
            <article className="explainer-card">
              <h3>Aggregate basket</h3>
              <p>
                Each day we sum <code>total_hnt</code> for that day&apos;s top 500 — aggregate
                network rewards at the top, priced in USD using daily HNT/USD.
              </p>
            </article>
            <article className="explainer-card">
              <h3>Why it matters</h3>
              <p>
                One snapshot over 30 days would bias toward survivors. HNT500 measures what the top
                of the network actually earned each day — a fair benchmark for Helium performance.
              </p>
            </article>
          </div>
        </div>
      </section>

      <section className="section" style={{ paddingTop: 0 }}>
        <div className="container">
          <div className="panel panel-elevated cta-panel">
            <div className="cta-panel-inner">
              <div>
                <h3>Index dashboard</h3>
                <p>
                  Full performance charts, global history, and per-device constituent breakdown —
                  updated on a schedule from Nexus top-earner data.
                </p>
              </div>
              <Link href="/dashboard" className="btn-primary">
                Open dashboard
              </Link>
            </div>
          </div>
        </div>
      </section>

      {!hasData && (
        <section className="section">
          <div className="container">
            <div className="panel empty-state">
              <p style={{ fontSize: 17, fontWeight: 600, color: "var(--text)" }}>
                Index data building…
              </p>
              <p>
                Start the server with <code>NEXUS_API_KEY</code> and wait for the first cron build,
                or run <code>npm run rebuild</code>.
              </p>
            </div>
          </div>
        </section>
      )}

      {meta?.latestBuild && (
        <section style={{ paddingBottom: 40 }}>
          <div className="container">
            <p style={{ fontSize: 12, color: "var(--text-dim)", textAlign: "center" }}>
              Last build: {meta.latestBuild.status} · {meta.latestBuild.days_built} days ·{" "}
              {meta.latestBuild.finished_at
                ? new Date(meta.latestBuild.finished_at).toLocaleString()
                : "in progress"}
            </p>
          </div>
        </section>
      )}
    </>
  );
}
