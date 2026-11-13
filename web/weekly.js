// "/weekly": place each product's 84 blocks on whole device pixels (bars.js),
// the same way as the overview bars, so the gaps are even at any scaling or
// zoom. Without this script the CSS grid in style.css lays the row out.
(function () {
  const N = 84; // blocks per row (weekly.go: 7 days of 2-hour blocks)
  const fb = window.flashBars;
  const host = document.querySelector(".wk");
  if (!host) return;
  if (!fb) { console.error("bars.js is not loaded: the weekly bars keep the CSS grid"); return; }
  function layout() {
    const dpr = fb.pixelRatio();
    host.querySelectorAll(".wk-bars").forEach((el) => {
      const w = el.clientWidth || 0;
      if (!w || el.children.length !== N) return;
      const wDev = Math.floor(w * dpr);
      const key = [wDev, N, dpr].join(":");
      if (el.dataset.lay === key) return; // already placed
      const lay = fb.barLayout(wDev, N);
      fb.placeBars(el, N, lay, wDev, dpr);
      el.dataset.lay = key;
      if (lay.scroll) el.scrollLeft = el.scrollWidth; // newest block in view
    });
  }
  fb.watchLayout(host, layout);
  layout();
})();
