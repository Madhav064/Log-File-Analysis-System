// 10 Export & API: the manifest of exported files and a live API explorer.
(async function () {
  try {
    const res = await fetch("/api/health");
    const h = await res.json();
    if (!res.ok) throw Object.assign(new Error(h.error), { code: h.code });
    const rows = Object.entries(h.tables).map(([name, e]) => ({ name, ...e }));
    $("#files").innerHTML = `<p style="margin-bottom:12px"><span class="chip ${h.missing.length ? "warn" : "ok"}">
        ${h.files_present} of ${h.files_listed} files present</span>
        <span class="muted"> · exported ${esc(fmt.ts(h.generated_at))} · env=${esc(h.env)} · ${esc(h.dataset)}</span></p><div id="files-table"></div>`;
    table("#files-table", rows, [
      { label: "Table", html: (r) => `<span class="mono">${esc(r.name)}</span>` },
      { label: "File", html: (r) => (r.missing ? '<span class="chip warn">not exported</span>' : `<span class="mono muted">${esc(r.file)}</span>`) },
      { label: "Rows", right: true, html: (r) => fmt.int(r.rows) },
      { label: "Columns", html: (r) => `<span class="mono faint">${r.columns.map(esc).join(", ")}</span>` },
    ]);
  } catch (err) { showError($("#files"), err); }

  const out = $("#try-out"), url = $("#try-url");
  document.addEventListener("click", async (e) => {
    const a = e.target.closest("[data-try]");
    if (!a) return;
    e.preventDefault();
    url.textContent = "GET " + a.dataset.try;
    out.textContent = "…";
    const res = await fetch(a.dataset.try);
    let body = await res.text();
    try {
      const j = JSON.parse(body);
      if (Array.isArray(j.data) && j.data.length > 20) { j.data = j.data.slice(0, 20); j["…"] = "first 20 rows shown"; }
      body = JSON.stringify(j, null, 2);
    } catch (_) { /* not JSON */ }
    out.textContent = `HTTP ${res.status}\n\n${body}`;
  });
})();
