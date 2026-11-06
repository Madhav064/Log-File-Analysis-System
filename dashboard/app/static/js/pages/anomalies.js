// 08 Anomalies: counts, hourly chart with spikes/drops/gaps, and the full list of flagged hours.
(async function () {
  const all = await load("#counts", "/anomalies", null, ({ counts }, el) => {
    const cell = (metric, kind, label, cls) => `<div class="card kpi"><div class="label">${label}</div>
      <div class="value ${cls}">${fmt.int(counts[`${metric}:${kind}`] || 0)} <small>h</small></div></div>`;
    const total = Object.values(counts).reduce((a, b) => a + b, 0);
    el.innerHTML = `<div class="grid g3" style="margin:0">
        ${cell("requests", "gap", "Gaps (no requests)", "")}
        ${cell("requests", "spike", "Request spikes", "")}
        ${cell("requests", "drop", "Request drops", "")}
        ${cell("error_rate", "spike", "Error-rate spikes", "")}
        ${cell("error_rate", "drop", "Error-rate drops", "")}
        <div class="card kpi"><div class="label">Total flagged</div><div class="value">${fmt.int(total)} <small>h</small></div></div>
      </div>
      <p class="muted" style="margin-top:12px">Error-rate spikes are often small in absolute terms: when the usual rate is
        tiny, a few extra errors already give a high z-score. Read them together with the request numbers.</p>`;
  });
  const runs = all ? all.gap_runs : [];
  Gaps.notes($("#gap-notes"), runs);
  const flagged = all ? all.data : [];

  let range = {};
  const draw = () => load("#chart-box", "/traffic/timeline", { granularity: "hour", ...range }, ({ data }, el) => {
    if (!el.querySelector("canvas")) el.innerHTML = '<canvas id="anomaly-chart"></canvas>';
    const marks = (kind) => data.map((r) => {
      const a = flagged.find((f) => f.metric === "requests" && f.kind === kind && f.hour_ts === r.hour_ts);
      return a ? r.requests : null;
    });
    Charts.make("anomaly-chart", (c) => ({
      type: "line",
      data: { labels: data.map((r) => r.hour_ts), datasets: [
        { label: "Requests per hour", data: data.map((r) => r.requests), borderColor: c.c1, backgroundColor: c.c1, borderWidth: 1, pointRadius: 0 },
        { label: "Spike", data: marks("spike"), showLine: false, pointStyle: "triangle", pointRadius: 6, borderColor: c.error, backgroundColor: c.error },
        { label: "Drop", data: marks("drop"), showLine: false, pointStyle: "triangle", rotation: 180, pointRadius: 6, borderColor: c.warning, backgroundColor: c.warning },
      ] },
      options: { interaction: { mode: "index", intersect: false },
        plugins: { bands: { items: Gaps.bands(runs, "hour", c) },
          tooltip: { filter: (i) => i.raw != null || i.datasetIndex === 0, callbacks: { title: (i) => fmt.ts(i[0].label),
            label: (i) => {
              if (i.datasetIndex === 0) return i.raw == null ? "no requests (gap)" : `${fmt.int(i.raw)} requests`;
              const a = flagged.find((f) => f.metric === "requests" && f.hour_ts === i.label);
              return a ? `${i.dataset.label}: z = ${fmt.dec(a.z_score, 2)} (usual ${fmt.int(Math.round(a.baseline_mean))})` : "";
            } } } },
        scales: { x: { ...Charts.thin(c), ticks: { maxTicksLimit: 12, callback(v) { return tickLabel(this.getLabelForValue(v), "day"); } } },
          y: Charts.count(c, "requests per hour") } },
    }));
  });
  range = (await dateRange((r) => { range = r; draw(); })) || {};
  draw();

  // the list, filtered client-side and paged 25 at a time
  let metric = "", kind = "", page = 0;
  const PAGE = 25;
  const list = () => {
    const rows = flagged.filter((r) => (!metric || r.metric === metric) && (!kind || r.kind === kind));
    const pages = Math.max(1, Math.ceil(rows.length / PAGE));
    page = Math.min(page, pages - 1);
    const shown = rows.slice(page * PAGE, page * PAGE + PAGE);
    const val = (r, v) => (v == null ? "–" : r.metric === "error_rate" ? fmt.rate(v, 2) : fmt.int(Math.round(v)));
    const KIND = { spike: '<span class="chip bad">▲ spike</span>', drop: '<span class="chip warn">▼ drop</span>', gap: '<span class="chip">gap</span>' };
    table("#list", shown, [
      { label: "Hour (server time)", html: (r) => `<span class="mono">${esc(fmt.ts(r.hour_ts))}</span>` },
      { label: "Metric", html: (r) => `<span class="mono">${r.metric === "requests" ? "requests" : "error rate"}</span>` },
      { label: "Kind", html: (r) => KIND[r.kind] },
      { label: "Value", right: true, html: (r) => val(r, r.value) },
      { label: "Usual (mean)", right: true, html: (r) => (r.kind === "gap" ? '<span class="faint">no data</span>' : val(r, r.baseline_mean)) },
      { label: "Spread (std)", right: true, html: (r) => (r.kind === "gap" ? '<span class="faint">no data</span>' : val(r, r.baseline_std)) },
      { label: "z-score", right: true, html: (r) => (r.z_score == null ? '<span class="faint">–</span>' : (r.z_score > 0 ? "+" : "") + fmt.dec(r.z_score, 2)) },
    ], "No flagged hours match these filters");
    $("#pager").innerHTML = `${fmt.int(rows.length)} rows · page ${page + 1} of ${pages}
      <button class="btn" data-p="-1" ${page ? "" : "disabled"}>Previous</button>
      <button class="btn" data-p="1" ${page < pages - 1 ? "" : "disabled"}>Next</button>`;
  };
  $("#pager").addEventListener("click", (e) => { const b = e.target.closest("[data-p]"); if (b) { page += +b.dataset.p; list(); } });
  segmented("metric", (v) => { metric = v; page = 0; list(); });
  segmented("kind", (v) => { kind = v; page = 0; list(); });
  if (all) list(); else showError($("#list"), { code: 503, message: "anomalies: not exported" });
})();
