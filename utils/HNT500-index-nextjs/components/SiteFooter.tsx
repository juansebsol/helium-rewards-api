export function SiteFooter({ note }: { note?: string }) {
  return (
    <footer className="site-footer">
      <div className="container container-wide footer-inner">
        <span>HNT500 Index · Daily-reconstituted Helium top-earner basket</span>
        {note && <span>{note}</span>}
      </div>
    </footer>
  );
}
