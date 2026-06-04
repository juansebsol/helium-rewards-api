import { fmtUsd, fmtPct } from "@/lib/format";

const REPEAT = 8;

function TickerQuote({
  symbol,
  price,
  changePct,
}: {
  symbol: string;
  price: string;
  changePct: number | null;
}) {
  const up = changePct != null && changePct >= 0;
  const down = changePct != null && changePct < 0;

  return (
    <span className="market-ticker-quote">
      <span className="market-ticker-symbol">{symbol}</span>
      <span className="market-ticker-price">{price}</span>
      <span
        className={`market-ticker-change${up ? " up" : ""}${down ? " down" : ""}`}
      >
        {changePct == null ? "—" : fmtPct(changePct)}
      </span>
    </span>
  );
}

export function LiveTicker({
  basketUsd,
  changePct,
  symbol = "HNT500",
}: {
  basketUsd: number;
  changePct: number | null;
  symbol?: string;
}) {
  const price = fmtUsd(basketUsd);
  const segment = (keyPrefix: string) =>
    Array.from({ length: REPEAT }, (_, i) => (
      <TickerQuote
        key={`${keyPrefix}-${i}`}
        symbol={symbol}
        price={price}
        changePct={changePct}
      />
    ));

  return (
    <div className="market-ticker" role="marquee" aria-label={`${symbol} ${price}, ${fmtPct(changePct)}`}>
      <div className="market-ticker-track">
        <div className="market-ticker-group">{segment("a")}</div>
        <div className="market-ticker-group" aria-hidden="true">
          {segment("b")}
        </div>
      </div>
    </div>
  );
}
