import type { NextConfig } from "next";
import path from "path";

const nextConfig: NextConfig = {
  serverExternalPackages: ["better-sqlite3"],
  outputFileTracingRoot: path.join(process.cwd()),
  // `app/index/` breaks clientReferenceManifest in Next.js — use /dashboard + rewrite
  async rewrites() {
    return [{ source: "/index", destination: "/dashboard" }];
  },
};

export default nextConfig;
