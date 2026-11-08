// Fetching from the dashboard's API, number formatting, and the empty / error states (plan M8b).
const API = {
  async get(path, params) {
    const qs = params ? "?" + new URLSearchParams(Object.entries(params).filter(([, v]) => v !== "" && v != null)) : "";
    const res = await fetch("/api" + path + qs);
    const body = await res.json().catch(() => ({ error: "The server sent an unreadable answer", code: res.status }));
    if (!res.ok) { const e = new Error(body.error || res.statusText); e.code = body.code || res.status; throw e; }
    return body;
  },
};

const fmt = {
  int: (v) => (v == null ? "–" : Number(v).toLocaleString("en-US")),
  dec: (v, d = 1) => (v == null ? "–" : Number(v).toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d })),
  pct: (v, d = 2) => (v == null ? "–" : fmt.dec(v, d) + "%"),          // v already 0-100
  rate: (v, d = 2) => (v == null ? "–" : fmt.dec(v * 100, d) + "%"),   // v is 0-1
  bytes(v) {
    if (v == null) return "–";
    const units = ["B", "KB", "MB", "GB", "TB"];
    let i = 0, n = Number(v);
    while (n >= 1000 && i < units.length - 1) { n /= 1000; i++; }
    return (i ? n.toFixed(n >= 100 ? 0 : n >= 10 ? 1 : 2) : n) + " " + units[i];
  },
  mib: (v) => (v == null ? "�" : fmt.int(Math.round(v / 1048576)) + " MiB"),   // HDFS block sizes are powers of 2
  secs(v) {
    if (v == null) return "–";
    const s = Number(v);
    return s < 90 ? s.toFixed(1) + " s" : Math.floor(s / 60) + " min " + Math.round(s % 60) + " s";
  },
  // "1995-07-13T09:00:00-04:00" -> "13 Jul 1995 09:00 -04:00"; dates kept in the server's own clock
  ts(v, withOffset = true) {
    if (!v) return "–";
    const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    const [d, t] = v.split("T");
    const [y, m, day] = d.split("-");
    let out = `${+day} ${months[+m - 1]} ${y}`;
    if (t) out += " " + t.slice(0, 5) + (withOffset ? " " + t.slice(8) : "");
    return out;
  },
  day(v) { return fmt.ts(v.slice(0, 10)); },
  weekday: (n) => ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][n - 1],
};

function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

function $(sel, root = document) { return root.querySelector(sel); }

// Show the right state in a container when a request fails: 503 = no export yet, anything else = error.
function showError(el, err) {
  if (!el) return;
  if (err.code === 503) {
    el.innerHTML = `<div class="state"><strong>No data yet</strong><span>${esc(err.message)}</span>
      <span>Run: <code>python scripts/pipeline.py export</code></span></div>`;
  } else {
    el.innerHTML = `<div class="state error"><strong>Could not load this part</strong><span>${esc(err.message)}</span></div>`;
  }
}

// Load one endpoint into one area: render(body) on success, the empty/error state otherwise.
async function load(el, path, params, render) {
  if (typeof el === "string") el = $(el);
  try {
    const body = await API.get(path, params);
    render(body, el);
    return body;
  } catch (err) {
    showError(el, err);
    console.warn(path, err.message);
    return null;
  }
}

// Fill a <table class="data"> from rows: cols = [{key, label, right, html: (row) => string}]
function table(el, rows, cols, emptyText = "No rows") {
  if (typeof el === "string") el = $(el);
  if (!rows.length) { el.innerHTML = `<div class="state">${esc(emptyText)}</div>`; return; }
  const head = cols.map((c) => `<th class="${c.right ? "r" : ""}">${esc(c.label)}</th>`).join("");
  const body = rows.map((r) => "<tr>" + cols.map((c) => {
    const v = c.html ? c.html(r) : esc(r[c.key]);
    return `<td class="${c.right ? "r" : ""}">${v}</td>`;
  }).join("") + "</tr>").join("");
  el.innerHTML = `<div class="table-wrap"><table class="data"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
}

// Segmented toggle: <div class="seg" data-seg="name"><button data-v="hour">...</button></div>
function segmented(name, onChange) {
  const seg = document.querySelector(`[data-seg="${name}"]`);
  if (!seg) return;
  seg.addEventListener("click", (e) => {
    const b = e.target.closest("button[data-v]");
    if (!b) return;
    seg.querySelectorAll("button").forEach((x) => x.classList.toggle("on", x === b));
    onChange(b.dataset.v);
  });
}

// Date range inputs <input type=date data-range="from|to">, limited to the dates in the data (from the
// quality report). Calls onChange({from, to}) whenever either changes.
async function dateRange(onChange) {
  const from = document.querySelector('[data-range="from"]'), to = document.querySelector('[data-range="to"]');
  if (!from || !to) return null;
  try {
    const q = (await API.get("/pipeline/quality")).data;
    for (const el of [from, to]) { el.min = q.min_ts.slice(0, 10); el.max = q.max_ts.slice(0, 10); }
    from.value = from.min; to.value = to.max;
  } catch (e) { /* no data yet: the charts show the empty state */ }
  const fire = () => onChange({ from: from.value, to: to.value });
  from.addEventListener("change", fire);
  to.addEventListener("change", fire);
  return { from: from.value, to: to.value };
}

// x-axis tick label for hour or day labels: "13 Jul" or "13 Jul 09:00"
function tickLabel(label, granularity) {
  const s = fmt.ts(label, false).replace(" 1995", "");
  return granularity === "hour" ? s : s.slice(0, 6);
}
