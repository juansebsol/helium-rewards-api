import { fmtUsd, fmtNum, fmtDay } from "@/lib/format";
import { ChangePill } from "@/components/ChangePill";
import type { IndexPoint, IndexStats } from "@/lib/types";

type Props = {
  latest: IndexPoint;
  stats: IndexStats;
  n: number;
  rangeLabel: string;
  updatedAt: string;
  incompleteDays: number;
};

export function IndexQuoteBand({
  latest,
  stats,
  n,
  rangeLabel,
  updatedAt,
  incompleteDays,
}: Props) {
  const coverage = `${latest.constituent_count}/${latest.requested_n}`;

  return (
    <header className="index-quote">
      <div className="container container-wide index-quote-inner">
        <div className="index-quote-primary">
          <div className="index-quote-meta">
            <span className="index-symbol">HNT500</span>
            <span className="index-quote-divider" aria-hidden />
            <span className="index-quote-tag">Top {n} earners · daily reconstituted</span>
          </div>
          <div className="index-quote-value-row">
            <span className="index-quote-value">{fmtUsd(latest.basket_usd)}</span>
            <ChangePill pct={stats.changePct} />
          </div>
          <p className="index-quote-asof">
            As of <time dateTime={latest.day}>{fmtDay(latest.day)}</time>
            <span className="index-quote-dot" aria-hidden>
              ·
            </span>
            {fmtNum(latest.basket_hnt, 0)} HNT aggregate
          </p>
        </div>

        <dl className="index-stat-strip">
          <div className="index-stat-item">
            <dt>HNT price</dt>
            <dd>{fmtUsd(latest.hnt_price, 4)}</dd>
          </div>
          <div className="index-stat-item">
            <dt>{stats.days}-day USD</dt>
            <dd>{fmtUsd(stats.periodSumUsd)}</dd>
          </div>
          <div className="index-stat-item">
            <dt>{stats.days}-day HNT</dt>
            <dd>{fmtNum(stats.periodSumHnt, 0)}</dd>
          </div>
          <div className="index-stat-item">
            <dt>Coverage</dt>
            <dd>{coverage}</dd>
          </div>
        </dl>
      </div>

      <div className="container container-wide index-quote-footer">
        <span>{rangeLabel}</span>
        <span className="index-quote-updated">
          <span className="badge-live">Live</span>
          Updated {updatedAt}
        </span>
        {incompleteDays > 0 && (
          <span className="index-quote-warn">{incompleteDays} incomplete day(s)</span>
        )}
      </div>
    </header>
  );
}
