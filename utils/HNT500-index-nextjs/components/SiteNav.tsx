import Link from "next/link";

type Props = { active?: "home" | "dashboard" };

export function SiteNav({ active }: Props) {
  return (
    <header className="site-nav">
      <div className="container container-wide site-nav-inner">
        <Link href="/" className="brand">
          <span className="brand-mark">500</span>
          <span>HNT500</span>
        </Link>
        <nav className="nav-links">
          <Link href="/" className={`nav-link ${active === "home" ? "active" : ""}`}>
            Overview
          </Link>
          <Link
            href="/dashboard"
            className={`nav-link ${active === "dashboard" ? "active" : ""}`}
          >
            Index
          </Link>
          {active !== "dashboard" && (
            <Link href="/dashboard" className="nav-cta">
              View live data
            </Link>
          )}
        </nav>
      </div>
    </header>
  );
}
