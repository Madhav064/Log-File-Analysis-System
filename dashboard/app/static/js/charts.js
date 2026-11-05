// Chart helpers. Colours come from the theme's CSS variables; every chart is rebuilt when the theme changes.
const Charts = (() => {
  const registry = new Map();   // canvas id -> {chart, build}

  function css(name) { return getComputedStyle(document.documentElement).getPropertyValue(name).trim(); }

  function colors() {
    return {
      text: css("--text"), muted: css("--text-muted"), faint: css("--text-faint"), grid: css("--grid"),
      border: css("--border"), surface: css("--surface"),
      c1: css("--chart-1"), c2: css("--chart-2"), c3: css("--chart-3"), c4: css("--chart-4"),
      success: css("--success"), warning: css("--warning"), error: css("--error"), purple: css("--purple"),
      gap: css("--gap"), outage: css("--outage"),
    };
  }

  // Shaded vertical bands over a category axis: options.plugins.bands = [{from, to, color, label}],
  // where from/to are x labels. Used for the data gap and the outage.
  const bandPlugin = {
    id: "bands",
    beforeDatasetsDraw(chart, args, opts) {
      const bands = (opts && opts.items) || [];
      const x = chart.scales.x, area = chart.chartArea, ctx = chart.ctx;
      if (!x || !bands.length) return;
      const labels = chart.data.labels;
      ctx.save();
      for (const b of bands) {
        const i0 = labels.indexOf(b.from), i1 = labels.indexOf(b.to);
        if (i0 < 0 || i1 < 0) continue;
        const half = labels.length > 1 ? (x.getPixelForValue(1) - x.getPixelForValue(0)) / 2 : 0;
        const left = Math.max(area.left, x.getPixelForValue(i0) - half);
        const right = Math.min(area.right, x.getPixelForValue(i1) + half);
        ctx.fillStyle = b.color;
        ctx.fillRect(left, area.top, right - left, area.bottom - area.top);
        if (b.label) {
          ctx.fillStyle = colors().muted;
          ctx.font = "11px " + css("--font-mono");
          ctx.save();
          ctx.translate(left + 4, area.top + 6);
          ctx.rotate(Math.PI / 2);
          ctx.fillText(b.label, 0, 0);
          ctx.restore();
        }
      }
      ctx.restore();
    },
  };

  function defaults(c) {
    Chart.defaults.font.family = css("--font-sans");
    Chart.defaults.font.size = 12;
    Chart.defaults.color = c.muted;
    Chart.defaults.borderColor = c.grid;
    Chart.defaults.animation = false;
    Chart.defaults.maintainAspectRatio = false;
    Chart.defaults.plugins.legend.labels.boxWidth = 12;
    Chart.defaults.plugins.tooltip.backgroundColor = c.surface;
    Chart.defaults.plugins.tooltip.titleColor = c.text;
    Chart.defaults.plugins.tooltip.bodyColor = c.text;
    Chart.defaults.plugins.tooltip.borderColor = c.border;
    Chart.defaults.plugins.tooltip.borderWidth = 1;
    Chart.defaults.plugins.tooltip.titleFont = { family: css("--font-mono"), size: 12 };
  }

  function make(id, build) {
    const canvas = document.getElementById(id);
    if (!canvas) return null;
    const old = registry.get(id);
    if (old) old.chart.destroy();
    const c = colors();
    defaults(c);
    const chart = new Chart(canvas, build(c));
    registry.set(id, { chart, build });
    return chart;
  }

  document.addEventListener("themechange", () => {
    for (const [id, { build }] of registry) make(id, build);
  });

  if (window.Chart) Chart.register(bandPlugin);

  // Axis helpers
  const thin = (c) => ({ grid: { color: c.grid }, border: { color: c.border } });
  const count = (c, title) => ({ ...thin(c), beginAtZero: true, title: { display: !!title, text: title },
    ticks: { callback: (v) => Number(v).toLocaleString("en-US") } });

  return { make, colors, thin, count };
})();
