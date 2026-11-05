// 01 Overview: KPIs, daily requests with the two data holes, status classes, findings, pipeline summary.
(async function () {
  const CLASS_COLOR = { "2xx": "success", "3xx": "c1", "4xx": "warning", "5xx": "error" };

  const sum = await load("#kpis", "/summary", null, ({ data: s }, el) => {
    const kpi = (label, value, note) => `<div class="card kpi"><div class="label">${label}</div>
      <div class="value">${value}</div><div class="note">${note}</div></div>`;
    el.innerHTML = [
      kpi("Total requests", fmt.int(s.total_requests), `${fmt.int(s.total_lines)} log lines, ${fmt.int(s.rejected)} unreadable`),
      kpi("Unique hosts", fmt.int(s.unique_hosts), "distinct computers that made requests (exact count)"),
      kpi("Data served", fmt.bytes(s.total_bytes), `${fmt.int(s.total_bytes)} bytes`),
      kpi("Error rate", fmt.rate(s.error_rate), "share of requests with status 400 or higher"),
      kpi("Busiest hour", fmt.int(s.peak_hour && s.peak_hour.requests) + " <small>req</small>",
          s.peak_hour ? fmt.ts(s.peak_hour.period_start) : "–"),
      kpi("Date range", `${s.calendar_days} <small>days</small>`,
          `${s.days_with_traffic} days have traffic · ${fmt.day(s.min_ts)} → ${fmt.day(s.max_ts)}`),
    ].join("");
    el.classList.remove("loading");
  });

  const runs = await Gaps.runs();
  Gaps.notes($("#gap-notes"), runs);

  load("#daily-box", "/traffic/timeline", { granularity: "day" }, ({ data }) => {
    Charts.make("daily-chart", (c) => ({
      type: "line",
      data: { labels: data.map((d) => d.log_date),
        datasets: [{ label: "Requests", data: data.map((d) => d.requests), borderColor: c.c1, backgroundColor: c.c1,
          borderWidth: 1.5, pointRadius: 2, spanGaps: false }] },
      options: {
        plugins: { legend: { display: false }, bands: { items: Gaps.bands(runs, "day", c) },
          tooltip: { callbacks: { title: (i) => fmt.ts(i[0].label) + " · " + fmt.weekday(data[i[0].dataIndex].day_of_week || 1),
            label: (i) => i.raw == null ? "no requests logged" : fmt.int(i.raw) + " requests" } } },
        scales: { x: { ...Charts.thin(c), ticks: { callback(v) { return fmt.ts(this.getLabelForValue(v)).replace(" 1995", ""); }, maxTicksLimit: 10 } },
          y: Charts.count(c, "requests per day") },
      },
    }));
  });

  load("#class-table", "/errors/status", null, ({ classes }, el) => {
    Charts.make("class-chart", (c) => ({
      type: "doughnut",
      data: { labels: classes.map((k) => k.status_class),
        datasets: [{ data: classes.map((k) => k.requests), backgroundColor: classes.map((k) => c[CLASS_COLOR[k.status_class]] || c.faint),
          borderColor: c.surface, borderWidth: 2 }] },
      options: { cutout: "65%", plugins: { legend: { position: "right" },
        tooltip: { callbacks: { label: (i) => `${i.label}: ${fmt.int(i.raw)} requests` } } } },
    }));
    table(el, classes, [
      { label: "Class", html: (r) => `<span class="mono">${esc(r.status_class)}</span>` },
      { label: "Requests", right: true, html: (r) => fmt.int(r.requests) },
      { label: "Share", right: true, html: (r) => fmt.pct(r.pct, r.pct < 0.1 ? 3 : 1) },
      { label: "Codes", html: (r) => `<span class="mono muted">${r.codes.join(", ")}</span>` },
    ]);
  });

  // Findings: each sentence is built from a table value, nothing is typed in.
  (async () => {
    const el = $("#findings");
    try {
      const [weekday, hours, top] = await Promise.all([API.get("/traffic/profile", { by: "weekday" }),
        API.get("/traffic/profile", { by: "hour" }), API.get("/top/resources", { by: "hits", n: 1 })]);
      if (!sum) throw Object.assign(new Error("summary missing"), { code: 503 });
      const s = sum.data;
      const wk = weekday.data, avg = (xs) => xs.reduce((a, r) => a + r.avg_requests, 0) / xs.length;
      const weekendRatio = avg(wk.filter((r) => r.day_of_week >= 6)) / avg(wk.filter((r) => r.day_of_week <= 5));
      const busiestHour = hours.data.reduce((a, r) => (r.avg_requests_per_day > a.avg_requests_per_day ? r : a));
      const t = top.data[0];
      const items = [
        ["Busiest day", `${fmt.ts(s.peak_day.period_start, false).slice(0, 11)} with <span class="num">${fmt.int(s.peak_day.requests)}</span> requests; its busiest minute had <span class="num">${fmt.int(s.peak_minute.requests)}</span>.`],
        ["Daily rhythm", `Traffic peaks at <span class="num">${String(busiestHour.hour).padStart(2, "0")}:00</span> server time, about <span class="num">${fmt.int(Math.round(busiestHour.avg_requests_per_day))}</span> requests on an average day.`],
        ["Weekends", `An average weekend day gets <span class="num">${Math.round(weekendRatio * 100)}%</span> of an average weekday's traffic.`],
        ["Most requested file", `<code>${esc(t.resource)}</code> was requested <span class="num">${fmt.int(t.requests)}</span> times.`],
      ];
      el.innerHTML = items.map(([h, p], i) => `<div style="display:flex;gap:12px;margin-bottom:12px">
        <span class="chip info">${i + 1}</span><div><h3>${h}</h3><p class="muted" style="margin-top:2px">${p}</p></div></div>`).join("");
    } catch (err) { showError(el, err); }
  })();

  // Pipeline at a glance: one row per stage from the recorded runs, linked to its page.
  (async () => {
    const el = $("#glance");
    try {
      const runs = (await API.get("/pipeline/runs")).data;
      const STAGE = { ingest: ["Ingestion", "HDFS", "ingestion"], etl: ["Parsing & ETL", "Spark", "etl"],
        analytics: ["Analytics (4 jobs)", "Spark", "errors"], mapreduce: ["MapReduce (3 jobs)", "YARN", "engines"],
        compare: ["Spark vs MapReduce", "Spark + Python", "engines"], export: ["Export to JSON", "Spark", "export"] };
      const rows = Object.entries(STAGE).map(([key, [name, engine, slug]]) => {
        const rs = runs.filter((r) => r.stage === key);
        return { name, engine, slug, n: rs.length, secs: rs.length ? rs.reduce((a, r) => a + r.duration_sec, 0) : null,
          ok: rs.length && rs.every((r) => r.status === "ok"), ran: rs.length > 0 };
      });
      table(el, rows, [
        { label: "Stage", html: (r) => `<a href="/${r.slug}">${esc(r.name)}</a>` },
        { label: "Engine", html: (r) => `<span class="mono muted">${esc(r.engine)}</span>` },
        { label: "Runtime", right: true, html: (r) => fmt.secs(r.secs) },
        { label: "Result", html: (r) => !r.ran ? `<span class="chip">not run</span>` : r.ok ? `<span class="chip ok">ok</span>` : `<span class="chip bad">failed</span>` },
      ]);
    } catch (err) { showError(el, err); }
  })();
})();
