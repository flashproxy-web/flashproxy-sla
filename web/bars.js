// Device-pixel layout for a row of status blocks, shared by app.js (cards on
// "/" and "/<package>") and weekly.js ("/weekly"). Blocks and gaps sized in CSS
// pixels land on fractions of a device pixel when the screen is scaled or
// zoomed, so gaps look uneven and the row shimmers (moire). Here block and gap
// are whole DEVICE pixels and every block gets its own whole device-pixel offset.
(function () {
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

  // Place el's children as a row of n blocks (lay = barLayout(wDev, n), wDev =
  // el's width in device pixels): each child is positioned absolutely (class
  // "abs") at its own whole device-pixel offset, so no rounding adds up along
  // the row. The full row of n blocks is centred: the leftover width is split
  // evenly and the offset is a whole device pixel (0 when the row scrolls).
  function placeBars(el, n, lay, wDev, dpr) {
    const kids = el.children;
    const m = kids ? kids.length : 0;
    const pitch = lay.b + lay.g;
    const total = n * lay.b + (n - 1) * lay.g;
    const off = lay.scroll ? 0 : Math.max(0, Math.floor((wDev - total) / 2));
    const width = (lay.b / dpr) + "px";
    for (let i = 0; i < m; i++) {
      const k = kids[i];
      if (!k || !k.style) continue;
      k.style.left = ((off + i * pitch) / dpr) + "px";
      k.style.width = width;
    }
    el.classList.add("abs");
    el.classList.toggle("scroll", !!lay.scroll);
  }

  // Run layout() (at most once per frame) when target is resized, the window is
  // resized or the pixel ratio changes. Returns the function that queues a run.
  function watchLayout(target, layout) {
    let queued = false;
    function queue() {
      if (queued) return;
      queued = true;
      const run = () => { queued = false; layout(); };
      if (typeof requestAnimationFrame === "function") requestAnimationFrame(run);
      else setTimeout(run, 50);
    }
    if (typeof ResizeObserver === "function") {
      try { new ResizeObserver(queue).observe(target); } catch (e) { /* resize listener below */ }
    }
    window.addEventListener("resize", queue);
    // The pixel ratio can change with no width change (window moved to another
    // screen, zoom): a resolution query matches only the current ratio, so its
    // change event means a new ratio. Lay out again and watch the new ratio.
    // The layout also reads the ratio on every run, so a missed event is caught
    // by the next resize or refresh.
    function watchPixelRatio() {
      if (typeof window.matchMedia !== "function") return;
      try {
        const mq = window.matchMedia(`(resolution: ${pixelRatio()}dppx)`);
        if (!mq) return;
        const onChange = () => {
          if (typeof mq.removeEventListener === "function") mq.removeEventListener("change", onChange);
          else if (typeof mq.removeListener === "function") mq.removeListener(onChange);
          queue();
          watchPixelRatio();
        };
        if (typeof mq.addEventListener === "function") mq.addEventListener("change", onChange);
        else if (typeof mq.addListener === "function") mq.addListener(onChange);
      } catch (e) { /* the layout re-reads the ratio on every run */ }
    }
    watchPixelRatio();
    return queue;
  }

  window.flashBars = { barLayout, pixelRatio, placeBars, watchLayout };
})();
