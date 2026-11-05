// 03 Ingestion: one card per raw file and the HDFS block summary, from the ingestion_report.
(async function () {
  const body = await load("#files", "/pipeline/ingestion", null, ({ data }, el) => {
    el.innerHTML = data.map((f) => `
      <section class="card">
        <div class="card-head"><div><h2 class="mono">${esc(f.file)}</h2><p class="mono">${esc(f.hdfs_path)}</p></div>
          ${f.checksum_verified ? '<span class="chip ok">✓ checksum verified</span>' : '<span class="chip bad">checksum not verified</span>'}</div>
        <div class="card-body">
          <div class="grid g3" style="margin:0 0 12px">
            <div><div class="label">Lines</div><div class="num" style="font-size:18px">${fmt.int(f.lines)}</div></div>
            <div><div class="label">Size</div><div class="num" style="font-size:18px">${fmt.bytes(f.hdfs_bytes)}</div>
              <div class="mono faint">${fmt.int(f.hdfs_bytes)} B</div></div>
            <div><div class="label">Partition</div><div class="mono" style="font-size:14px">month=${esc(f.month)}</div></div>
          </div>
          <dl class="kv">
            <dt>SHA-256</dt><dd>${esc(f.sha256)}</dd>
            <dt>Blocks</dt><dd>${f.blocks} × up to ${fmt.bytes(f.block_size)} · replication ${f.replication}</dd>
            <dt>Size check</dt><dd>${f.local_bytes === f.hdfs_bytes ? '<span class="chip ok">local = HDFS</span>' : '<span class="chip bad">sizes differ</span>'}
              ${fmt.int(f.local_bytes)} B on disk</dd>
          </dl>
        </div>
      </section>`).join("");
  });
  if (!body) { showError($("#storage"), { code: 503, message: "ingestion_report: missing from the last export" }); return; }

  const files = body.data;
  const total = files.reduce((a, f) => a + f.hdfs_bytes, 0);
  const blocks = files.reduce((a, f) => a + f.blocks, 0);
  const status = files[0].fsck_status;
  // one segment per block, sized by the bytes it actually holds (the last block of a file is partly full)
  const segs = [];
  files.forEach((f, fi) => {
    for (let b = 0; b < f.blocks; b++) {
      const size = Math.min(f.block_size, f.hdfs_bytes - b * f.block_size);
      segs.push({ size, file: f.file, color: fi ? "var(--chart-2)" : "var(--chart-1)", b });
    }
  });
  $("#storage").innerHTML = `
    <div class="grid g4" style="margin:0 0 16px">
      <div><div class="label">Stored</div><div class="num" style="font-size:18px">${fmt.bytes(total)}</div><div class="mono faint">${fmt.int(total)} B</div></div>
      <div><div class="label">Blocks</div><div class="num" style="font-size:18px">${blocks}</div><div class="mono faint">${fmt.bytes(files[0].block_size)} each at most</div></div>
      <div><div class="label">Replication</div><div class="num" style="font-size:18px">${files[0].replication}×</div><div class="mono faint">copies of each block</div></div>
      <div><div class="label">fsck</div><div style="margin-top:4px"><span class="chip ${status === "HEALTHY" ? "ok" : "bad"}">${esc(status)}</span></div>
        <div class="mono faint">loaded ${fmt.ts(files[0].loaded_at)}</div></div>
    </div>
    <div class="label" style="margin-bottom:6px">Blocks, to scale</div>
    <div class="bar" style="height:28px;gap:2px;background:transparent">
      ${segs.map((s) => `<span title="${esc(s.file)} block ${s.b + 1}: ${fmt.int(s.size)} B"
        style="flex:${s.size};background:${s.color};opacity:${0.55 + 0.45 * (s.size / files[0].block_size)}"></span>`).join("")}
    </div>
    <div class="legend" style="margin-top:8px">${files.map((f, i) => `<span><span class="swatch" style="background:var(--chart-${i + 1})"></span>${esc(f.file)} · ${f.blocks} blocks</span>`).join("")}</div>
    <p class="muted" style="margin-top:12px">Because the files are split into blocks, Spark and MapReduce can read
      ${blocks} pieces at the same time instead of one file from start to end.</p>`;
})();
