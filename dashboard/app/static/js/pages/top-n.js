// 07 Top N: top resources by hits or bytes, top hosts, file-type mix.
(function () {
  let by = "hits", n = 10;
  const short = (s, max = 48) => (s.length > max ? "…" + s.slice(-max + 1) : s);

  const drawResources = () => load("#res-box", "/top/resources", { by, n }, ({ data, source }, el) => {
    if (!el.querySelector("canvas")) el.innerHTML = '<canvas id="res-chart"></canvas>';
    const key = by === "hits" ? "requests" : "bytes";
    $("#res-title").textContent = by === "hits" ? "Most requested resources" : "Resources that sent the most data";
    $("#res-sub").textContent = by === "hits" ? "A hit is one request for that page or file."
      : "Total size of every answer for that page or file (hits × size).";
    $("#res-source").textContent = source[0];
    el.style.height = Math.max(240, data.length * 26 + 40) + "px";
    Charts.make("res-chart", (c) => ({
      type: "bar",
      data: { labels: data.map((r) => r.resource), datasets: [{ data: data.map((r) => r[key]), backgroundColor: by === "hits" ? c.c1 : c.c2, borderWidth: 0 }] },
      options: { indexAxis: "y",
        plugins: { legend: { display: false }, tooltip: { callbacks: { title: (i) => i[0].label,
          label: (i) => (by === "hits" ? fmt.int(i.raw) + " requests" : fmt.bytes(i.raw) + ` (${fmt.int(i.raw)} B)`) } } },
        scales: { x: { ...Charts.count(c, by === "hits" ? "requests" : "bytes"),
            ticks: { callback: (v) => (by === "hits" ? Number(v).toLocaleString("en-US") : fmt.bytes(v)) } },
          y: { ...Charts.thin(c), ticks: { font: { family: getComputedStyle(document.documentElement).getPropertyValue("--font-mono") },
            callback(v) { return short(this.getLabelForValue(v)); } } } } },
    }));
  });

  const drawHosts = () => load("#hosts", "/top/hosts", { n }, ({ data }, el) => table(el, data, [
    { label: "#", right: true, html: (r) => String(data.indexOf(r) + 1) },
    { label: "Host", html: (r) => `<div class="mono truncate" title="${esc(r.host)}">${esc(r.host)}</div>` },
    { label: "Requests", right: true, html: (r) => fmt.int(r.requests) },
    { label: "Data", right: true, html: (r) => fmt.bytes(r.bytes) },
  ]));

  load("#types-table", "/top/resource-types", null, ({ data, chart }, el) => {
    Charts.make("types-chart", (c) => {
      const palette = [c.c1, c.c2, c.c3, c.c4, c.success, c.warning, c.error, c.purple, c.faint];
      return { type: "doughnut",
        data: { labels: chart.map((r) => r.resource_type), datasets: [{ data: chart.map((r) => r.requests),
          backgroundColor: chart.map((_, i) => palette[i % palette.length]), borderColor: c.surface, borderWidth: 2 }] },
        options: { cutout: "60%", plugins: { legend: { position: "right" },
          tooltip: { callbacks: { label: (i) => `${i.label}: ${fmt.int(i.raw)} requests (${fmt.pct(chart[i.dataIndex].pct, 1)})` } } } } };
    });
    table(el, chart, [
      { label: "Type", html: (r) => `<span class="mono">${esc(r.resource_type)}</span>` },
      { label: "Requests", right: true, html: (r) => fmt.int(r.requests) },
      { label: "Share", right: true, html: (r) => fmt.pct(r.pct, 1) },
    ]);
    el.insertAdjacentHTML("beforeend", `<p class="faint" style="margin-top:8px">${data.length} file types seen in total.</p>`);
  });

  segmented("by", (v) => { by = v; drawResources(); });
  segmented("n", (v) => { n = +v; drawResources(); drawHosts(); });
  drawResources();
  drawHosts();
})();
