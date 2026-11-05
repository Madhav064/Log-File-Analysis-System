// The two kinds of hole in the data, shared by every time chart. gap_runs come from /api/anomalies.
const Gaps = {
  text: {
    missing_data: "No log data: the July log file stops here and the August file starts on 1 Aug, so these hours are in neither file.",
    outage: "Server outage: inside the August file the server logged nothing for these hours (it was shut down).",
  },
  title: { missing_data: "No log data", outage: "Server outage" },

  async runs() {
    try { return (await API.get("/anomalies", { kind: "gap" })).gap_runs; } catch (e) { return []; }
  },

  // Bands for a chart whose x labels are hour timestamps or dates (granularity "hour" or "day").
  bands(runs, granularity, c) {
    return runs.map((r) => ({
      from: granularity === "hour" ? r.start : r.start.slice(0, 10),
      to: granularity === "hour" ? r.end : r.end.slice(0, 10),
      color: r.cause === "outage" ? c.outage : c.gap,
      label: Gaps.title[r.cause],
    }));
  },

  notes(el, runs) {
    if (!el) return;
    el.innerHTML = runs.map((r) => `
      <div class="note ${r.cause === "outage" ? "outage" : "gap"}"><div>
        <strong>${Gaps.title[r.cause]}: ${fmt.ts(r.start, false)} → ${fmt.ts(r.end, false)} (${r.hours} h)</strong><br>
        <span class="muted">${Gaps.text[r.cause]}</span></div></div>`).join("");
  },
};
