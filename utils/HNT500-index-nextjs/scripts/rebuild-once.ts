import { buildIndexHistory } from "../lib/build-index";

buildIndexHistory()
  .then((r) => {
    console.log("Done:", r);
    if (r.issues.length) {
      console.warn("Issues:\n" + r.issues.join("\n"));
    }
    process.exit(r.daysBuilt > 0 ? 0 : 1);
  })
  .catch((e) => {
    console.error(e);
    process.exit(1);
  });
