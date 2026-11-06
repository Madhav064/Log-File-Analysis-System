// 06 Traffic: requests over time with the data holes, daily and weekly rhythm, busiest periods.
(async function () {
  load("#kpis", "/summary", null, ({ data: s }, el) => {
    const kpi = (label, value, note) => `<div class="card kpi"><div class="label">${label}</div><div class="value">${value}</div><div class="note">${note}</div></div>`;
    el.innerHTML = [
      kpi("Requests", fmt.int(s.total_requests), `over ${s.days_with_traffic} days with traffic`),
      kpi("Data served", fmt.bytes(s.total_bytes), "total size of all answers"),
      kpi("Average per day", fmt.int(Math.round(s.total_requests / s.days_with_traffic)), "requests, on days with traffic"),
      kpi("Hours with traffic", fmt.int(s.hours_with_traffic), `of ${fmt.int(s.calendar_days * 24)} hours in the date range`),
    ].join("");
  });

  const runs = await Gaps.runs();
  Gaps.notes($("#gap-notes"), runs);

  let granularity = "day", range = {};
  const draw = () => load("#timeline-box", "/traffic/timeline", { granularity, ...range }, ({ data }, el) => {
    if (!el.querySelector("canvas")) el.innerHTML = '<canvas id="timeline-chart"></canvas>';
    const key = granularity === "hour" ? "hour_ts" : "log_date";
    Charts.make("timeline-chart", (c) => ({
      type: "line",
      data: { labels: data.map((r) => r[key]), datasets: [
        { label: "Requests", data: data.map((r) => r.requests), borderColor: c.c1, backgroundColor: c.c1,
          borderWidth: 1.3, pointRadius: granularity === "day" ? 2 : 0, yAxisID: "y" },
        { label: "Unique hosts", data: data.map((r) => r.unique_hosts), borderColor: c.c2, backgroundColor: c.c2,
          borderWidth: 1, borderDash: [4, 3], pointRadius: 0, yAxisID: "y" },
      ] },
      options: { interaction: { mode: "index", intersect: false },
        plugins: { bands: { items: Gaps.bands(runs, granularity, c) },
          tooltip: { callbacks: { title: (i) => fmt.ts(i[0].label),
            label: (i) => `${i.dataset.label}: ${i.raw == null ? "nothing logged" : fmt.int(i.raw)}`,
            footer: (i) => { const r = data[i[0].dataIndex]; return r.bytes != null ? `${fmt.bytes(r.bytes)} served` : ""; } } } },
        scales: { x: { ...Charts.thin(c), ticks: { maxTicksLimit: 12, callback(v) { return tickLabel(this.getLabelForValue(v), granularity); } } },
          y: Charts.count(c, granularity === "hour" ? "per hour" : "per day") } },
    }));
  });
  segmented("granularity", (g) => { granularity = g; draw(); });
  range = (await dateRange((r) => { range = r; draw(); })) || {};
  draw();

  load("#hour-box", "/traffic/profile", { by: "hour" }, ({ data }) => {
    const top = data.reduce((a, r) => (r.avg_requests_per_day > a.avg_requests_per_day ? r : a));
    Charts.make("hour-chart", (c) => ({
      type: "bar",
      data: { labels: data.map((r) => String(r.hour).padStart(2, "0")),
        datasets: [{ data: data.map((r) => r.avg_requests_per_day), backgroundColor: data.map((r) => (r === top ? c.c1 : c.c1 + "99")), borderWidth: 0 }] },
      options: { plugins: { legend: { display: false }, tooltip: { callbacks: { title: (i) => i[0].label + ":00",
        label: (i) => `${fmt.int(Math.round(i.raw))} requests on an average day (${fmt.int(data[i.dataIndex].total_requests)} in total)` } } },
        scales: { x: { ...Charts.thin(c), title: { display: true, text: "hour of day (server time)" } }, y: Charts.count(c, "average requests") } },
    }));
  });

  load("#weekday-box", "/traffic/profile", { by: "weekday" }, ({ data }) => {
    Charts.make("weekday-chart", (c) => ({
      type: "bar",
      data: { labels: data.map((r) => fmt.weekday(r.day_of_week)),
        datasets: [{ data: data.map((r) => r.avg_requests), backgroundColor: data.map((r) => (r.day_of_week >= 6 ? c.c2 : c.c1)), borderWidth: 0 }] },
      options: { plugins: { legend: { display: false }, tooltip: { callbacks: {
        label: (i) => `${fmt.int(Math.round(i.raw))} requests on an average ${i.label} (${fmt.int(data[i.dataIndex].total_requests)} in total)` } } },
        scales: { x: Charts.thin(c), y: Charts.count(c, "average requests") } },
    }));
  });

  const drawPeaks = (g) => load("#peaks", "/traffic/peaks", { granularity: g }, ({ data }, el) => table(el, data, [
    { label: "Rank", right: true, html: (r) => String(r.rank) },
    { label: g === "day" ? "Day" : g === "hour" ? "Hour starting" : "Minute starting",
      html: (r) => `<span class="mono">${esc(g === "day" ? fmt.ts(r.period_start, false).slice(0, 11) + " · " + new Date(r.period_start).toLocaleDateString("en-US", { weekday: "short", timeZone: "UTC" }) : fmt.ts(r.period_start))}</span>` },
    { label: "Requests", right: true, html: (r) => fmt.int(r.requests) },
  ]));
  segmented("peaks", drawPeaks);
  drawPeaks("hour");
})();
