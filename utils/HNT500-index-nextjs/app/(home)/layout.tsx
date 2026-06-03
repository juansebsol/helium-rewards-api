import { SiteNav } from "@/components/SiteNav";

export default function HomeLayout({ children }: { children: React.ReactNode }) {
  return (
    <>
      <SiteNav active="home" />
      {children}
    </>
  );
}
