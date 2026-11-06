// 05 Errors: status codes, error rate over time, top error resources and hosts.
(async function () {
  const MEANING = { 200: "OK", 302: "Found (redirect)", 304: "Not modified (use the cached copy)", 400: "Bad request",
    403: "Forbidden", 404: "Not found", 500: "Internal server error", 501: "Not implemented" };
  const classColor = (c, s) => (s >= 500 ? c.error : s >= 400 ? c.warning : s >= 300 ? c.c1 : c.success);

  load("#status-table", "/errors/status", null, ({ data }, el) => {
    Charts.make("status-chart", (c) => ({
      type: "bar",
      data: { labels: data.map((r) => String(r.status)),
        datasets: [{ data: data.map((r) => r.requests), backgroundColor: data.map((r) => classColor(c, r.status)), borderWidth: 0 }] },
      options: { plugins: { legend: { display: false },
        tooltip: { callbacks: { label: (i) => `${fmt.int(i.raw)} requests (${fmt.pct(data[i.dataIndex].pct, 3)})` } } },
        scales: { x: { ...Charts.thin(c), title: { display: true, text: "HTTP status code" } },
          y: { ...Charts.thin(c), type: "logarithmic", title: { display: true, text: "requests (log scale)" },
            ticks: { callback: (v) => ([1, 10, 100, 1e3, 1e4, 1e5, 1e6, 1e7].includes(v) ? fmt.int(v) : "") } } } },
    }));
    table(el, data, [
      { label: "Code", html: (r) => `<span class="mono">${r.status}</span>` },
      { label: "Meaning", html: (r) => esc(MEANING[r.status] || "") },
      { label: "Requests", right: true, html: (r) => fmt.int(r.requests) },
      { label: "Share", right: true, html: (r) => fmt.pct(r.pct, r.pct < 0.01 ? 4 : 2) },
    ]);
  });

  let granularity = "day", range = {};
  const drawTimeline = () => load("#timeline-box", "/errors/timeline", { granularity, ...range }, ({ data }, el) => {
    if (!el.querySelector("canvas")) el.innerHTML = '<canvas id="timeline-chart"></canvas>';
    const key = granularity === "hour" ? "hour_ts" : "log_date";
    const rate = (r, f) => (r.requests ? (100 * r[f]) / r.requests : null);
    Charts.make("timeline-chart", (c) => ({
      type: "line",
      data: { labels: data.map((r) => r[key]), datasets: [
        { label: "4xx client errors", data: data.map((r) => rate(r, "errors_4xx")), borderColor: c.warning, backgroundColor: c.warning, borderWidth: 1.5, pointRadius: granularity === "day" ? 2 : 0 },
        { label: "5xx server errors", data: data.map((r) => rate(r, "errors_5xx")), borderColor: c.error, backgroundColor: c.error, borderWidth: 1.5, pointRadius: granularity === "day" ? 2 : 0 },
      ] },
      options: { interaction: { mode: "index", intersect: false },
        plugins: { tooltip: { callbacks: { title: (i) => fmt.ts(i[0].label),
          label: (i) => `${i.dataset.label}: ${i.raw == null ? "no requests" : fmt.dec(i.raw, 2) + "%"}`,
          footer: (i) => { const r = data[i[0].dataIndex]; return r.requests ? `${fmt.int(r.errors_4xx + r.errors_5xx)} errors in ${fmt.int(r.requests)} requests` : ""; } } } },
        scales: { x: { ...Charts.thin(c), ticks: { maxTicksLimit: 12, callback(v) { return tickLabel(this.getLabelForValue(v), granularity); } } },
          y: { ...Charts.thin(c), beginAtZero: true, title: { display: true, text: "% of requests" } } } },
    }));
  });
  segmented("granularity", (g) => { granularity = g; drawTimeline(); });
  range = (await dateRange((r) => { range = r; drawTimeline(); })) || {};
  drawTimeline();

  const sel = $("#status-filter");
  const drawResources = () => load("#top-resources", "/errors/top-resources", { n: 10, status: sel.value }, ({ data, statuses }, el) => {
    if (!sel.options.length) {
      sel.innerHTML = statuses.map((s) => `<option value="${s}" ${s === 404 ? "selected" : ""}>${s} ${esc(MEANING[s] || "")}</option>`).join("");
      if (sel.value) return drawResources();
    }
    table(el, data, [
      { label: "#", right: true, html: (r) => String(data.indexOf(r) + 1) },
      { label: "Resource", html: (r) => `<div class="mono truncate" title="${esc(r.resource)}">${esc(r.resource)}</div>` },
      { label: "Status", html: (r) => `<span class="chip ${r.status >= 500 ? "bad" : "warn"}">${r.status}</span>` },
      { label: "Requests", right: true, html: (r) => fmt.int(r.requests) },
    ]);
  });
  sel.addEventListener("change", drawResources);
  drawResources();

  load("#top-hosts", "/errors/top-hosts", { n: 10 }, ({ data }, el) => table(el, data, [
    { label: "#", right: true, html: (r) => String(data.indexOf(r) + 1) },
    { label: "Host", html: (r) => `<span class="mono">${esc(r.host)}</span>` },
    { label: "Errors", right: true, html: (r) => fmt.int(r.errors) },
  ]));
})();
