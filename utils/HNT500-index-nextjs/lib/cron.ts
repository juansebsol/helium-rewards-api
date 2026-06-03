import cron from "node-cron";
import { env } from "@/lib/env";
import { buildIndexHistory } from "@/lib/build-index";

let started = false;

export function startCron(): void {
  if (started) return;
  started = true;

  const schedule = env.cronSchedule;
  if (!cron.validate(schedule)) {
    console.error(`[cron] Invalid CRON_SCHEDULE: ${schedule}`);
    return;
  }

  console.log(`[cron] Index updates scheduled: ${schedule}`);

  cron.schedule(schedule, () => {
    console.log("[cron] Starting scheduled index build...");
    buildIndexHistory().catch((err) => {
      console.error("[cron] Build failed:", err);
    });
  });

  if (env.runBuildOnStart) {
    console.log("[cron] RUN_BUILD_ON_START=true — building now...");
    setImmediate(() => {
      buildIndexHistory().catch((err) => {
        console.error("[cron] Startup build failed:", err);
      });
    });
  }
}
