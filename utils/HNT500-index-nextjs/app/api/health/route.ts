import { NextResponse } from "next/server";
import { getMeta } from "@/lib/db";
import { isBuilding } from "@/lib/build-index";
import { env } from "@/lib/env";

export const dynamic = "force-dynamic";

export async function GET() {
  const meta = getMeta();
  return NextResponse.json({
    ok: true,
    building: isBuilding(),
    config: {
      index_size: env.indexSize,
      weight_mode: env.weightMode,
      history_days: env.historyDays,
      cron_schedule: env.cronSchedule,
    },
    meta,
  });
}
