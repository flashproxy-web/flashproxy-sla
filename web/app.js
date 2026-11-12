// One script for three pages. Each page's init runs only when its own root
// element exists, so no page fetches or sets timers for another:
//   "/"          #components  -> cards from /api/overview (package bars; a vantage
//                                 chip splits each bar: package above, vantage below)
//   "/<package>" #pkg-card    -> the same card (initCards) for this package only
//                #detail      -> response-time chart + scenarios for one package
//   "/incidents" #inc-filter  -> client-side package filter

function fmt(v) { return (v == null || isNaN(v)) ? "—" : Math.round(v); }
function shortV(v) { return String(v).replace(/^aws-/, ""); }
function esc(s) {
  return String(s).replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));
}
// Fixed-width text for the index cards. The fill is U+00A0 so HTML keeps it.
const NBSP = "\u00a0";
// "1234ms" / "   1ms": number right-aligned in 4 characters, then "ms". A value
// above 9999 is shown as it is (the field just gets wider). No reading: a dash in
// the same 6 characters.
function msField(v) {
  if (v == null || isNaN(v)) return ("—").padStart(4, NBSP) + NBSP + NBSP;
  return String(Math.round(v)).padStart(4, NBSP) + "ms";
}
// Status word right-aligned: padded from the left to the longest label
// ("operational", 11 characters).
const STATUS_W = 11;
function statusField(status) { return String(status).replace(/_/g, " ").padStart(STATUS_W, NBSP); }
function isVisible() { return document.visibilityState === "visible"; }

// ------------------------------------------- cards: "/" and "/<package>"

// The product cards, filled from /api/overview every 15 s. only = "" on "/": one
// card per product, each name a link to its page. only = a package key on that
// package's page: just that card, and its name is plain text (no link).
function initCards(host, only) {
  only = only ? String(only) : "";
  // package -> vantage shown in the lower half of the card's bars. No entry = plain
  // package bars. Only a chip click changes it, so the 15 s refresh keeps the choice.
  const vantageSel = Object.create(null);
  const cards = new Map(); // package -> { el, sig }
  let overview = null;
  let ssrCleared = false;

  // The vantage the card is filtered to, or undefined when no filter is on (or
  // the chosen vantage is not in this payload: the package bars show meanwhile).
  function vantageOf(c) {
    const want = vantageSel[c.package];
    if (!want) return undefined;
    return (c.vantages || []).find((v) => v && v.vantage === want);
  }

  // Status as one of four fixed values; anything else reads as no_data.
  function statusKey(b) {
    const st = b ? b.status : "";
    return st === "operational" || st === "degraded" || st === "down" ? st : "no_data";
  }
  function hasReading(b) { return !!b && statusKey(b) !== "no_data" && !!b.samples; }
  function ms(b) { return Math.round(Number(b.response_ms_avg) || 0); }

  // Tooltip part for one reading (fixed status words and numbers only).
  function readingTitle(b) {
    if (!hasReading(b)) return "no data";
    return `${statusKey(b)} · ${ms(b)}ms round-trip · ${Math.round(Number(b.success_pct) || 0)}% ok · n=${Math.round(Number(b.samples) || 0)}`;
  }
  // Screen-reader part for one reading: status in words.
  function readingWord(b) { return hasReading(b) ? statusKey(b) : "no data"; }

  function barTitle(b) {
    return new Date(b.t * 1000).toLocaleTimeString() + " · " + readingTitle(b);
  }

  // Screen-reader text for one bar: minute, status in words, response time.
  function barLabel(b) {
    const t = new Date(b.t * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
    return hasReading(b) ? `${t}, ${statusKey(b)}, ${ms(b)} ms` : t + ", no data";
  }

  // One split bar: upper half = the package's minute, lower half = the chosen
  // vantage's minute (vb). No vantage bar for that minute: both halves take the
  // package colour and the text says so.
  function splitBarHTML(b, vb) {
    const t = new Date(b.t * 1000);
    const pk = statusKey(b);
    const vTitle = vb ? readingTitle(vb) : "no bar for this minute";
    const vWord = vb ? readingWord(vb) : "not available";
    const title = `${t.toLocaleTimeString()} · package ${readingTitle(b)} · vantage ${vTitle}`;
    const label = `${t.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}, package ${readingWord(b)}, vantage ${vWord}`;
    const lower = vb
      ? `<span class="half van" data-status="${esc(statusKey(vb))}"></span>`
      : `<span class="half" data-status="${esc(pk)}"></span>`;
    return `<i class="bar split" role="img" aria-label="${esc(label)}" title="${esc(title)}">` +
      `<span class="half" data-status="${esc(pk)}"></span>${lower}</i>`;
  }

  // The package's bars as markup. With a vantage chosen (v), every bar is split:
  // package above, that vantage below, matched by minute t.
  // Display only: the newest minute is often still filling in, so drop that one
  // bar when the package has no_data there. Verdicts and uptime come from the
  // server unchanged.
  function barSeries(series) { return (Array.isArray(series) ? series : []).filter(Boolean); }
  function barsHTML(series, v) {
    const bars = barSeries(series);
    if (bars.length && bars[bars.length - 1].status === "no_data") bars.pop();
    if (!v) {
      return bars.map((b) =>
        `<i class="bar" role="img" aria-label="${esc(barLabel(b))}" data-status="${esc(statusKey(b))}" title="${esc(barTitle(b))}"></i>`
      ).join("");
    }
    const byT = new Map();
    (Array.isArray(v.bars) ? v.bars : []).forEach((vb) => { if (vb) byT.set(vb.t, vb); });
    return bars.map((b) => splitBarHTML(b, byT.get(b.t))).join("");
  }

  // Stepped block size for a row of n blocks in a container w pixels wide (the
  // caller passes DEVICE pixels): whole pixels, block b and gap g. From large to small, g = max(1, round(b / 4)) (gap
  // about 20% of the pitch b + g, accepted between 10% and 25%); the largest b
  // whose row n*b + (n-1)*g fits wins. Below that (b = 2 and b = 1 cannot meet
  // the ratio with a whole-pixel gap) the gap stays 1px; when even 1 + 1 does
  // not fit, the row keeps 1 + 1 and scrolls. w = 0 (not laid out yet) gives the
  // minimum size. Never throws.
  function barLayout(w, n) {
    w = Math.floor(Number(w)) || 0;
    n = Math.max(1, Math.floor(Number(n)) || 0);
    const total = (b, g) => n * b + (n - 1) * g;
    for (let b = Math.floor(w / n); b >= 1; b--) {
      const g = Math.max(1, Math.round(b * 0.25));
      const ratio = g / (b + g);
      if (ratio < 0.10 || ratio > 0.25) continue;
      if (total(b, g) <= w) return { b, g, scroll: false };
    }
    if (total(2, 1) <= w) return { b: 2, g: 1, scroll: false };
    return { b: 1, g: 1, scroll: w > 0 && total(1, 1) > w };
  }

  // The screen's device pixels per CSS pixel (display scaling, browser zoom).
  function pixelRatio() {
    const r = Number(typeof window !== "undefined" ? window.devicePixelRatio : 1);
    return isFinite(r) && r > 0 ? r : 1;
  }

  // Size and place every card's bars in DEVICE pixels: block b and gap g are
  // whole device pixels (barLayout on the container's width in device pixels),
  // and each block is positioned absolutely at a whole device-pixel offset, so
  // every block and gap is drawn the same size at any scaling or zoom. The row
  // is centred in the container. The result is
  // kept on the card and keyed (width, n, ratio, blocks drawn), so a data
  // refresh with nothing changed neither recomputes nor moves anything.
  function layoutBars() {
    const dpr = pixelRatio();
    cards.forEach((card) => {
      if (!card || !card.el) return;
      const el = card.el.querySelector(".bars");
      if (!el) return;
      const w = el.clientWidth || 0;
      const n = Number(el.dataset.n) || 0;
      if (!w || !n) return; // not laid out yet: keep the minimum / last size
      const kids = el.children;
      const m = kids ? kids.length : 0;
      const wDev = Math.floor(w * dpr);
      const key = [wDev, n, dpr, m].join(":");
      let lay = card.lay;
      if (lay && lay.key === key && el.dataset.lay === key) return; // already placed
      if (!lay || lay.w !== wDev || lay.n !== n) {
        lay = barLayout(wDev, n);
        lay.w = wDev; lay.n = n;
      }
      lay.key = key; lay.dpr = dpr;
      card.lay = lay;
      const pitch = lay.b + lay.g;
      // Centre the full row of n blocks: the leftover width is split evenly and
      // the offset is a whole device pixel. The full series is used (not the m
      // blocks drawn), so hiding the newest block moves nothing.
      const total = n * lay.b + (n - 1) * lay.g;
      const off = lay.scroll ? 0 : Math.max(0, Math.floor((wDev - total) / 2));
      const width = (lay.b / dpr) + "px";
      for (let i = 0; i < m; i++) {
        const k = kids[i];
        if (!k || !k.style) continue;
        k.style.left = ((off + i * pitch) / dpr) + "px";
        k.style.width = width;
      }
      el.style.setProperty("--bw", width);
      el.style.setProperty("--bg", (lay.g / dpr) + "px");
      el.dataset.lay = key;
      el.classList.add("abs");
      el.classList.toggle("scroll", !!lay.scroll);
      if (lay.scroll) el.scrollLeft = el.scrollWidth; // newest block in view
    });
  }
  let layoutQueued = false;
  function queueLayout() {
    if (layoutQueued) return;
    layoutQueued = true;
    const run = () => { layoutQueued = false; layoutBars(); };
    if (typeof requestAnimationFrame === "function") requestAnimationFrame(run);
    else setTimeout(run, 50);
  }
  // The bars' width follows the card list's width, so one observer on the list
  // covers every card (the bars elements themselves are replaced on re-render).
  if (typeof ResizeObserver === "function") {
    try { new ResizeObserver(queueLayout).observe(host); } catch (e) { /* resize listener below */ }
  }
  window.addEventListener("resize", queueLayout);
  // The pixel ratio can change with no width change (window moved to another
  // screen, zoom): a resolution query matches only the current ratio, so its
  // change event means a new ratio. Lay out again and watch the new ratio.
  // layoutBars also reads the ratio on every run, so a missed event is caught
  // by the next resize or refresh.
  function watchPixelRatio() {
    if (typeof window.matchMedia !== "function") return;
    try {
      const mq = window.matchMedia(`(resolution: ${pixelRatio()}dppx)`);
      if (!mq) return;
      const onChange = () => {
        if (typeof mq.removeEventListener === "function") mq.removeEventListener("change", onChange);
        else if (typeof mq.removeListener === "function") mq.removeListener(onChange);
        queueLayout();
        watchPixelRatio();
      };
      if (typeof mq.addEventListener === "function") mq.addEventListener("change", onChange);
      else if (typeof mq.addListener === "function") mq.addListener(onChange);
    } catch (e) { /* layoutBars re-reads the ratio on every run */ }
  }
  watchPixelRatio();

  // v = the chosen vantage (undefined = none). It only adds that vantage's lower
  // half to each bar; the status square, the status word and the uptime are
  // always the package's.
  // lay = the card's last computed bar layout (none before the first draw: the
  // stylesheet's minimum size applies until layoutBars has measured the row).
  function cardHTML(c, v, win, lay) {
    const status = c.status || "no_data";
    const barsMarkup = barsHTML(c.bars, v);
    // Sized for the whole series, so hiding the newest block moves nothing.
    const nBars = barSeries(c.bars).length;
    // lay.b / lay.g are device pixels; the markup carries them as CSS pixels for
    // the moment before layoutBars places the blocks (same call, see renderComponents).
    const sized = lay && lay.b >= 1 && lay.g >= 1 && lay.dpr > 0;
    const barsAttr = ` data-n="${nBars}"` + (sized
      ? ` style="--bw:${Number(lay.b) / lay.dpr}px;--bg:${Number(lay.g) / lay.dpr}px"`
      : "");
    const barsCls = "bars" + (sized && lay.scroll ? " scroll" : "");

    const chips = (c.vantages || []).filter(Boolean).map((vv) => {
      const on = !!v && vv.vantage === v.vantage;
      const best = vv.vantage === c.default_vantage;
      const cls = "vchip" + (on ? " sel" : "") + (best ? " best" : "");
      const med = msField(vv.status === "no_data" ? null : vv.response_ms_avg);
      const title = `${String(vv.status).replace("_", " ")} · ${(vv.success_pct ?? 0).toFixed(1)}% ok · n=${fmt(vv.samples)}/min`;
      return `<button type="button" class="${cls}" data-v="${esc(vv.vantage)}" data-status="${esc(vv.status)}" ` +
             `aria-pressed="${on ? "true" : "false"}" title="${esc(title)}"><span class="vmark" aria-hidden="true">${best ? "★" : NBSP}</span><span class="vn">${esc(shortV(vv.vantage))}</span> <span class="vms">${med}</span></button>`;
    }).join("");

    const uptime = c.uptime_pct;
    // Footer middle slot: the package's Degraded threshold (a fixed fact about the
    // product, not a reading). Omitted when the API sends no usable value.
    const dms = Number(c.degraded_ms);
    const mid = dms > 0 ? `marked degraded above ${esc(String(+dms.toFixed(1)))} ms` : "";
    return `<div class="comp-top">` +
        `<span class="sq" data-status="${esc(status)}" aria-hidden="true"></span>` +
        (only
          ? `<span class="comp-name">${esc(c.display_name || c.package)}</span>`
          : `<a class="comp-name" href="/${encodeURIComponent(c.package)}">${esc(c.display_name || c.package)}</a>`) +
        `<span class="spacer"></span>` +
        (chips ? `<span class="vchips">${chips}</span>` : "") +
        `<span class="comp-stat" data-status="${esc(status)}">${esc(statusField(status))}</span>` +
      `</div>` +
      `<div class="${barsCls}"${barsAttr}>${barsMarkup}</div>` +
      `<div class="comp-bot">` +
        `<span class="left">${win}m ago</span>` +
        `<span class="mid">${mid}</span>` +
        `<span class="right">uptime ${(uptime ?? 100).toFixed(1)}% · now</span>` +
      `</div>`;
  }

  // Rebuild only the cards whose data (or chosen vantage) changed, and put keyboard
  // focus back on the same control when the focused card had to be rebuilt.
  function renderComponents() {
    if (!overview) return;
    const win = overview.window_minutes || 90;
    const all = (Array.isArray(overview.components) ? overview.components : []).filter((c) => c && c.package);
    const comps = only ? all.filter((c) => c.package === only) : all;
    // Package page: the overview does not list this package (yet). Keep what is
    // shown (the server-rendered card, or the last live one).
    if (only && !comps.length) return;
    if (!ssrCleared) { host.textContent = ""; ssrCleared = true; } // drop the server-rendered rows once
    const seen = new Set();

    comps.forEach((c, i) => {
      seen.add(c.package);
      const v = vantageOf(c);
      const sig = JSON.stringify([c, v ? v.vantage : "", win]);

      let card = cards.get(c.package);
      if (!card) {
        const el = document.createElement("div");
        el.className = "comp";
        el.dataset.pkg = c.package;
        card = { el, sig: null, lay: null };
        cards.set(c.package, card);
      }
      if (card.sig !== sig) {
        const act = document.activeElement;
        let focus = null;
        if (act && card.el.contains(act)) {
          focus = act.classList.contains("vchip") ? { v: act.dataset.v } : { name: true };
        }
        card.el.innerHTML = cardHTML(c, v, win, card.lay);
        card.sig = sig;
        if (focus) {
          const target = focus.name
            ? card.el.querySelector(".comp-name")
            : Array.from(card.el.querySelectorAll(".vchip")).find((b) => b.dataset.v === focus.v);
          if (target) target.focus();
        }
      }
      if (host.children[i] !== card.el) host.insertBefore(card.el, host.children[i] || null);
    });

    cards.forEach((card, pkg) => {
      if (!seen.has(pkg)) { card.el.remove(); cards.delete(pkg); }
    });
    layoutBars(); // after the cards are in the document: sizes new rows, no-op at an unchanged width
  }

  // One delegated handler (survives re-renders). A chip filters its card's bars to
  // that vantage; the active chip again turns the filter off; another chip
  // switches it. It sits above the card link and never navigates.
  host.addEventListener("click", (e) => {
    const chip = e.target.closest(".vchip");
    if (chip && host.contains(chip)) {
      const row = chip.closest(".comp");
      const pkg = row ? row.dataset.pkg : "";
      const van = chip.dataset.v;
      if (pkg && van) {
        if (vantageSel[pkg] === van) delete vantageSel[pkg];
        else vantageSel[pkg] = van;
        renderComponents();
      }
      return;
    }
    // The bars sit above the stretched link (to keep their tooltips), so follow
    // the card's link for pointer clicks on them (the package page's card has no
    // link, so nothing happens there).
    const bars = e.target.closest(".bars");
    if (bars && host.contains(bars)) {
      const comp = bars.closest(".comp");
      const link = comp ? comp.querySelector("a.comp-name") : null;
      if (link && link.href) window.location.assign(link.href);
    }
  });

  let loading = false;
  async function loadOverview() {
    if (loading) return;
    loading = true;
    let d;
    try {
      const r = await fetch("/api/overview");
      if (!r.ok) throw new Error("http " + r.status);
      d = await r.json();
    } catch (e) { return; } finally { loading = false; }
    if (!d || typeof d !== "object") return;
    overview = d;

    const updated = document.getElementById("updated");
    if (updated && d && d.generated_at) updated.textContent = "updated " + new Date(d.generated_at).toLocaleTimeString();
    const banner = document.getElementById("banner");
    if (banner && d.overall) {
      banner.dataset.status = d.overall.status;
      const bt = document.getElementById("banner-text");
      if (bt) bt.textContent = d.overall.label;
    }
    renderComponents();
  }

  loadOverview();
  setInterval(() => { if (isVisible()) loadOverview(); }, 15000);
  document.addEventListener("visibilitychange", () => { if (isVisible()) loadOverview(); });
}

function initMeta(box) {
  (async () => {
    let d;
    try {
      const r = await fetch("/api/meta");
      if (!r.ok) throw new Error("http " + r.status);
      d = await r.json();
    } catch (e) { return; }
    const p = d.public_clickhouse || {};
    box.innerHTML =
      `Query the same data this page is built from — ClickHouse HTTP, read-only, capped concurrency:<br><br>` +
      `host&nbsp;&nbsp;&nbsp; <b>${esc(p.url || "—")}</b><br>` +
      `db&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; <b>${esc(p.db || "—")}</b><br>` +
      `user&nbsp;&nbsp;&nbsp; <b>${esc(p.user || "—")}</b><br>` +
      `pass&nbsp;&nbsp;&nbsp; <b>${esc(p.password || "—")}</b><br><br>` +
      `<span class="muted">${esc(p.note || "")}</span>`;
  })();
}

// ------------------------------------------------------- detail "/<package>"

const SERIES_STYLE = {
  connect: { label: "via proxy", color: "#2f9e44" },
  connect_direct: { label: "direct (no proxy)", color: "#3b82f6" },
};

const SCN_ORDER = ["net_rtt", "connect", "streaming", "large_object", "hifreq_small", "scraping", "long_session"];
const SCN_LABEL = {
  net_rtt: "network RTT", connect: "connect", streaming: "streaming", large_object: "large object",
  hifreq_small: "hi-freq small", scraping: "scraping", long_session: "long session",
};
const SCN_KPI = {
  net_rtt: (s) => `${fmt(s.connect_ms_avg)}ms rtt`,
  connect: (s) => `${fmt(s.connect_ms_avg)}ms connect`,
  streaming: (s) => `${fmt(s.throughput_mbps_avg)} Mbps`,
  large_object: (s) => `${fmt(s.ttfb_ms_median)}ms ttfb`,
  hifreq_small: (s) => `${fmt(s.connect_ms_avg)}ms/conn · ${fmt(s.success_pct)}%`,
  scraping: (s) => `${fmt(s.connect_ms_avg)}ms avg`,
  long_session: (s) => `${fmt(s.success_pct)}% held · ${fmt(s.total_ms_median / 1000)}s`,
};

function renderScenarios(host, list) {
  const by = {};
  (list || []).forEach((s) => { by[s.scenario] = s; });
  host.innerHTML = SCN_ORDER.map((name) => {
    const p = by[name];                  // via proxy
    const dr = by[name + "_direct"];     // direct baseline (no proxy)
    const have = p && p.samples;
    const kpi = have ? SCN_KPI[name](p) : "— no data";
    const ok = have ? (p.success_pct >= 98 ? "op" : (p.success_pct >= 90 ? "deg" : "dn")) : "nd";
    let sub = "";
    if (name !== "net_rtt") {
      const haveD = dr && dr.samples;
      sub += `<div class="scn-direct">direct: ${haveD ? SCN_KPI[name](dr) : "—"}</div>`;
      // proxy overhead for latency-style scenarios: the proxy pays the vantage->proxy
      // hop (dial_ms) PLUS the proxy->target establishment (connect_ms); the direct
      // baseline pays only its own connect. So overhead = (proxy dial+connect) - direct connect.
      if (have && haveD && name !== "streaming" && name !== "large_object") {
        const d = Math.round((+p.connect_ms_avg + (+p.dial_ms_avg || 0)) - +dr.connect_ms_avg);
        sub += `<div class="scn-delta">proxy ${d >= 0 ? "+" : ""}${d}ms</div>`;
      }
    }
    const nn = have ? `n=${fmt(p.samples)}` : "";
    return `<div class="scn" data-st="${ok}"><div class="scn-name">${SCN_LABEL[name]}</div>` +
           `<div class="scn-kpi">${kpi}</div>${sub}<div class="scn-n">${nn}</div></div>`;
  }).join("");
}

// The SVG holds only lines (it stretches to the box); every label is an HTML
// element positioned in % over it, so text is never distorted.
function drawChart(svg, labels, legend, series) {
  const W = 1000, H = 220;
  const names = Object.keys(series).filter((n) => series[n] && series[n].length);
  labels.textContent = "";
  const label = (cls, text, style) => {
    const el = document.createElement("span");
    el.className = cls;
    el.textContent = text;
    Object.assign(el.style, style || {});
    labels.appendChild(el);
  };
  if (!names.length) {
    svg.innerHTML = "";
    label("empty", "no data yet");
    legend.innerHTML = "&nbsp;";
    return;
  }
  const allT = [], allY = [];
  names.forEach((n) => series[n].forEach((p) => { allT.push(+p.t); allY.push(+p.value); }));
  const minX = Math.min(...allT), maxX = Math.max(...allT);
  const maxY = Math.max(10, Math.ceil(Math.max(...allY) * 1.25));
  const X = (t) => (maxX === minX ? 0 : (t - minX) / (maxX - minX)) * W;
  const Y = (v) => H - (v / maxY) * H;
  const mid = maxY / 2;

  const grid = [[0, "#d7dcd2"], [mid, "#eceee9"], [maxY, "#eceee9"]].map(([v, c]) =>
    `<line x1="0" y1="${Y(v)}" x2="${W}" y2="${Y(v)}" stroke="${c}" vector-effect="non-scaling-stroke"/>`
  ).join("");
  const paths = names.map((n) => {
    const color = (SERIES_STYLE[n] || {}).color || "#888";
    const line = series[n].map((p, i) => (i ? "L" : "M") + X(+p.t).toFixed(1) + " " + Y(+p.value).toFixed(1)).join(" ");
    return `<path d="${line}" fill="none" stroke="${color}" stroke-width="1.5" vector-effect="non-scaling-stroke"/>`;
  }).join("");
  svg.innerHTML = grid + paths;

  label("yl", maxY + " ms", { top: "0%" });
  label("yl", String(Math.round(mid)), { top: "50%" });
  label("yl", "0", { top: "100%" });
  const tf = (t) => new Date(t * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  label("xl", tf(minX), { left: "0" });
  label("xl end", tf(maxX));

  // legend: colored swatch + latest value per series
  legend.innerHTML = names.map((n) => {
    const st = SERIES_STYLE[n] || { label: esc(n), color: "#888" };
    const pts = series[n];
    const last = pts[pts.length - 1];
    return `<span style="color:${st.color}">■</span> ${st.label} <b>${Math.round(+last.value)}ms</b>`;
  }).join(" &nbsp;&nbsp; ");
}

function initDetail(root) {
  const pkg = root.dataset.pkg;
  if (!pkg) return;
  const svg = document.getElementById("chart");
  const labels = document.getElementById("chart-labels");
  const legend = document.getElementById("chart-legend");
  const scnHost = document.getElementById("scenarios");
  const note = document.getElementById("detail-note");
  if (!svg || !labels || !legend || !scnHost) return;

  let vantage = root.dataset.vantage || ""; // "" = all vantages
  let minutes = 60;
  let ctl = null; // in-flight request, if any

  // load aborts whatever is in flight and fetches the current range + vantage.
  async function load() {
    if (ctl) ctl.abort();
    const mine = ctl = new AbortController();
    const q = `package=${encodeURIComponent(pkg)}&vantage=${encodeURIComponent(vantage)}`;
    try {
      const [sr, cr] = await Promise.all([
        fetch(`/api/series?${q}&minutes=${minutes}`, { signal: mine.signal }),
        fetch(`/api/scenarios?${q}`, { signal: mine.signal }),
      ]);
      if (!sr.ok || !cr.ok) throw new Error("http " + sr.status + "/" + cr.status);
      const s = await sr.json(), c = await cr.json();
      if (mine !== ctl) return; // superseded while reading the body
      drawChart(svg, labels, legend, s.series || {});
      renderScenarios(scnHost, c.scenarios);
      if (note) note.hidden = true;
    } catch (e) {
      if (mine !== ctl) return; // aborted by a newer load: not an error
      if (note) note.hidden = false;
    } finally {
      if (mine === ctl) ctl = null;
    }
  }

  // Range / vantage changes reload at once but leave the 30 s timer alone.
  root.querySelectorAll("#ranges button").forEach((b) => {
    b.addEventListener("click", () => {
      root.querySelectorAll("#ranges button").forEach((x) => {
        x.classList.toggle("active", x === b);
        x.setAttribute("aria-pressed", x === b ? "true" : "false");
      });
      minutes = +b.dataset.min;
      load();
    });
  });
  root.querySelectorAll("#detail-vantages .vchip").forEach((b) => {
    b.addEventListener("click", () => {
      root.querySelectorAll("#detail-vantages .vchip").forEach((x) => {
        x.classList.toggle("sel", x === b);
        x.setAttribute("aria-pressed", x === b ? "true" : "false");
      });
      vantage = b.dataset.v;
      load();
    });
  });

  load();
  setInterval(() => { if (isVisible() && !ctl) load(); }, 30000);
  document.addEventListener("visibilitychange", () => { if (isVisible() && !ctl) load(); });
}

// ------------------------------------------------------------- "/incidents"

function initIncidentFilter(bar) {
  const chips = Array.from(bar.querySelectorAll("button[data-pkg]"));
  const rows = Array.from(document.querySelectorAll(".inc[data-pkg]"));
  const list = document.getElementById("inc-list");
  const count = document.getElementById("inc-count");
  const empty = document.getElementById("inc-empty");

  function apply() {
    const on = new Set(chips.filter((c) => c.getAttribute("aria-pressed") === "true").map((c) => c.dataset.pkg));
    let n = 0;
    rows.forEach((r) => {
      const show = on.size === 0 || on.has(r.dataset.pkg);
      r.hidden = !show;
      if (show) n++;
    });
    if (count) count.textContent = `${n} incident${n === 1 ? "" : "s"}`;
    const none = rows.length > 0 && n === 0;
    if (empty) empty.hidden = !none;
    if (list) list.hidden = none;
  }

  chips.forEach((c) => {
    c.addEventListener("click", () => {
      c.setAttribute("aria-pressed", c.getAttribute("aria-pressed") === "true" ? "false" : "true");
      apply();
    });
  });

  // ?package=<name> pre-selects that chip, only when it is one of the chips. The
  // value is only ever compared, never written into the page.
  let want = null;
  try { want = new URLSearchParams(window.location.search).get("package"); } catch (e) { want = null; }
  if (want) {
    const chip = chips.find((c) => c.dataset.pkg === want);
    if (chip) chip.setAttribute("aria-pressed", "true");
  }
  apply();
}

// --------------------------------------------------------------------- boot

(function boot() {
  const comps = document.getElementById("components");
  if (comps) initCards(comps, "");
  const pkgCard = document.getElementById("pkg-card");
  if (pkgCard && pkgCard.dataset.pkg) initCards(pkgCard, pkgCard.dataset.pkg);
  const pub = document.getElementById("public-box");
  if (pub) initMeta(pub);
  const detail = document.getElementById("detail");
  if (detail) initDetail(detail);
  const filter = document.getElementById("inc-filter");
  if (filter) initIncidentFilter(filter);
})();
