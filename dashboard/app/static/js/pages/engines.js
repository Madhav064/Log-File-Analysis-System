// 09 Engine comparison: do Spark and MapReduce agree (results_match), and how long does each take.
(async function () {
  const JOBS = {
    status_count: ["Requests per status code", "status_counts"],
    hourly_traffic: ["Requests per hour", "traffic_hourly"],
    top_resources: ["Top 20 resources", "top_resources_by_hits"],
  };
  const body = await load("#verdicts", "/pipeline/benchmark", null, ({ data }, el) => {
    el.innerHTML = Object.entries(JOBS).map(([job, [what, table]]) => {
      const rows = data.filter((r) => r.job === job);
      const spark = rows.find((r) => r.engine === "spark"), mr = rows.find((r) => r.engine === "mapreduce");
      const match = rows.length && rows.every((r) => r.results_match);
      return `<section class="card"><div class="card-head"><div><h2 class="mono">${job}</h2><p>${what} · Spark table <code>${table}</code></p></div>
          <span class="chip ${match ? "ok" : "bad"}">${match ? "✓ results match" : "✗ results differ"}</span></div>
        <div class="card-body"><div class="grid g2" style="margin:0">
          <div><div class="label">Spark rows</div><div class="num" style="font-size:18px">${fmt.int(spark && spark.output_rows)}</div></div>
          <div><div class="label">MapReduce rows</div><div class="num" style="font-size:18px">${fmt.int(mr && mr.output_rows)}</div></div>
        </div><p class="muted" style="margin-top:10px">${match ? "Every key and every count is identical in both engines." : "See compare_engines.py output for the differences."}</p></div></section>`;
    }).join("");
  });
  if (!body) { ["#bench-box", "#bench-table"].forEach((s) => showError($(s), { code: 503, message: "engine_benchmark: not exported; run scripts/compare_engines.py then the export" })); return; }
  const data = body.data;
  const jobs = Object.keys(JOBS).filter((j) => data.some((r) => r.job === j));
  const time = (job, engine) => (data.find((r) => r.job === job && r.engine === engine) || {}).runtime_sec ?? null;

  Charts.make("bench-chart", (c) => ({
    type: "bar",
    data: { labels: jobs, datasets: [
      { label: "Spark", data: jobs.map((j) => time(j, "spark")), backgroundColor: c.c1, borderWidth: 0 },
      { label: "MapReduce", data: jobs.map((j) => time(j, "mapreduce")), backgroundColor: c.c2, borderWidth: 0 },
    ] },
    options: { indexAxis: "y", plugins: { legend: { display: false },
      tooltip: { callbacks: { label: (i) => `${i.dataset.label}: ${fmt.secs(i.raw)}` } } },
      scales: { x: { ...Charts.count(c, "seconds"), ticks: { callback: (v) => v + " s" } },
        y: { ...Charts.thin(c), ticks: { font: { family: getComputedStyle(document.documentElement).getPropertyValue("--font-mono") } } } } },
  }));

  table("#bench-table", data, [
    { label: "Job", html: (r) => `<span class="mono">${esc(r.job)}</span>` },
    { label: "Engine", html: (r) => `<span class="chip ${r.engine === "spark" ? "info" : "purple"}">${esc(r.engine)}</span>` },
    { label: "Runtime", right: true, html: (r) => fmt.secs(r.runtime_sec) },
    { label: "Output rows", right: true, html: (r) => fmt.int(r.output_rows) },
    { label: "Results match", html: (r) => (r.results_match ? '<span class="chip ok">✓ yes</span>' : '<span class="chip bad">✗ no</span>') },
    { label: "Ratio", right: true, html: (r) => {
      const s = time(r.job, "spark"), m = time(r.job, "mapreduce");
      return r.engine === "mapreduce" && s && m ? `${fmt.dec(m / s, 1)}× Spark's time` : "";
    } },
  ]);
})();
