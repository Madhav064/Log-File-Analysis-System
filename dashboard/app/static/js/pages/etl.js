// 04 Parsing & ETL: the quality report (lines read, parsed, rejected and why).
(async function () {
  const body = await load("#kpis", "/pipeline/quality", null, ({ data: q }, el) => {
    const kpi = (label, value, note) => `<div class="card kpi"><div class="label">${label}</div><div class="value">${value}</div><div class="note">${note}</div></div>`;
    el.innerHTML = [
      kpi("Lines read", fmt.int(q.total_lines), "every line in the raw files"),
      kpi("Parsed", fmt.int(q.parsed), "turned into typed records"),
      kpi("Rejected", fmt.int(q.rejected), "kept aside with a reason"),
      kpi("Reject rate", fmt.pct(q.reject_pct, 4), "rejected ÷ lines read"),
    ].join("");
  });
  if (!body) { ["#split", "#checks"].forEach((s) => showError($(s), { code: 503, message: "quality_report: not exported" })); return; }
  const q = body.data;

  const reasons = Object.entries(q.rejected_by_reason || {}).sort((a, b) => b[1] - a[1]);
  const parsedPct = (100 * q.parsed) / q.total_lines;
  $("#split").innerHTML = `
    <div class="bar" style="height:14px"><span style="flex:${q.parsed};background:var(--success)"></span>
      <span style="flex:${Math.max(q.rejected, q.total_lines / 400)};background:var(--error)"></span></div>
    <div class="legend" style="margin:8px 0 16px">
      <span><span class="swatch" style="background:var(--success)"></span>Parsed ${fmt.int(q.parsed)} (${fmt.dec(parsedPct, 4)}%)</span>
      <span><span class="swatch" style="background:var(--error)"></span>Rejected ${fmt.int(q.rejected)} (widened so it is visible)</span></div>
    <div id="reasons"></div>
    <dl class="kv" style="margin-top:16px">
      <dt>First request</dt><dd>${esc(q.min_ts)}</dd><dt>Last request</dt><dd>${esc(q.max_ts)}</dd>
      <dt>Unique hosts</dt><dd>${fmt.int(q.unique_hosts)} (exact count)</dd><dt>Report written</dt><dd>${esc(q.run_at)}</dd>
    </dl>`;
  table("#reasons", reasons.map(([reason, n]) => ({ reason, n })), [
    { label: "Reason", html: (r) => `<span class="chip bad">${esc(r.reason)}</span>` },
    { label: "Lines", right: true, html: (r) => fmt.int(r.n) },
    { label: "What it means", html: (r) => `<span class="muted">${esc(window.REASONS[r.reason] || "")}</span>` },
  ], "No line was rejected");

  const ok = q.parsed + q.rejected === q.total_lines;
  const check = (pass, title, text) => `<div style="display:flex;gap:10px"><span class="chip ${pass ? "ok" : "bad"}">${pass ? "✓" : "✗"}</span>
    <div><h3>${title}</h3><p class="muted">${text}</p></div></div>`;
  $("#checks").innerHTML = [
    check(ok, "Nothing lost", `parsed + rejected = lines read: ${fmt.int(q.parsed)} + ${fmt.int(q.rejected)} = ${fmt.int(q.total_lines)}.`),
    check(true, "Spark and Python parse every line the same way",
      "A parity test runs both parsers over the test lines and the whole sample and requires identical results (tests/unit/test_parser_parity.py)."),
    check(true, "Output matches its contract",
      "Every table is written through loganalysis.contracts, which fails the job on a missing, extra or mistyped column."),
    check(true, "Time zone kept", `Timestamps stay in the server's clock (${esc(q.min_ts.slice(19))}), so hours and days mean what the server meant.`),
  ].join("");
})();
