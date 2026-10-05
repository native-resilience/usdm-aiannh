/* ============================================================================
   Native Resilience · Tribal Drought Dashboard · app.js
   Built on mco-web-style (window.MCO, window.MCO.map) + MapLibre GL 5.
   Section references (§) are to the kit's HOUSE-STYLE.md. Classic script,
   external file so the page's CSP can pin script-src 'self'.

   Data (written weekly by ../usdm-aiannh.R):
     dashboard/areas.json   schema usdm-aiannh-areas/1 — one entry per AIANNH
                            entity (AIANNHCE), components R first, each with
                            its current worst class (index into classes)
     dashboard/<GEOID>.json schema usdm-aiannh-area/1 — cumulative percent of
                            the component at or above D0…D4, one value per
                            USDM Tuesday from week0; null = component absent
                            from that week's boundary vintage; plus the
                            overlapping counties, each with its worst
                            class every week (usdm-max-class/1 encoding)
     dashboard/mask.geojson the world outside the Tribal areas: drawn over
                            the full-color USDM to fade it
     usdm-aiannh.json       schema usdm-max-class-aiannh/1 — every
                            component's worst class every week (loaded
                            only when an earlier week is picked)
   Weekly USDM map: sustainable-fsa data-tiles, USDM_<date>-geo.topojson.
   Boundaries: census-aiannh's newest vintage, simplified (display only).
   ========================================================================== */
(function () {
  'use strict';

  /* ── Constants ─────────────────────────────────────────────────────────── */

  // The archive (CloudFront E2O1CP3LOBQEN8). Local development (serving the
  // repo root) reads the freshly built ../dashboard/ instead.
  const ARCHIVE = 'https://data.native-resilience.com/usdm-aiannh';
  const LOCAL = ['localhost', '127.0.0.1'].includes(location.hostname);
  const DATA = LOCAL ? '../dashboard' : ARCHIVE + '/dashboard';
  const BOUNDARIES =
    'https://data.sustainable-fsa.com/census-aiannh/census-aiannh_simple.topojson';
  const D3DROUGHT = 'https://d3drought.org/#spi/30d/rolling-30/AIANNH:';
  const WEB_JSON = LOCAL ? '../usdm-aiannh.json' : ARCHIVE + '/usdm-aiannh.json';
  const USDM_MAP = (date) =>
    `https://data.sustainable-fsa.com/data-tiles/usdm/USDM_${date}-geo.topojson`;

  // Full extent of the areas: the Aleutians to Maine, Hawaiʻi to the North
  // Slope. It sets the zoom floor; the default view is the lower 48, where
  // most areas are (the data-tiles frame box).
  const BOUNDS = [[-174.24, 18.91], [-67.04, 71.34]];
  const CONUS = [[-125.0, 24.0], [-66.5, 49.6]];
  const FIT_OPTS = { padding: 24, animate: false };
  const SRC = 'aiannh';
  const MS_PER_WEEK = 7 * 86400000;

  // USDM classes. Drought hexes are the NDMC's own, unchanged so the map
  // reads like every other USDM map (hue-only by design — §6: class names
  // always accompany color in the legend, cards, and tables). "None" is a
  // warm off-white rather than gray so it can't be read as "no data"
  // (lfp-explorer's convention).
  const CLASSES = ['None', 'D0', 'D1', 'D2', 'D3', 'D4'];
  const CLASS_NAMES = ['No drought', 'Abnormally dry', 'Moderate drought',
    'Severe drought', 'Extreme drought', 'Exceptional drought'];
  const CLASS_COLORS = ['#f0ead8', '#ffff00', '#fcd37f', '#ffaa00', '#e60000', '#730000'];
  // Areas on the map with no record this week (boundary newer than the
  // week's vintage). Neutral gray, distinct from None in lightness and hue.
  const NODATA_COLOR = '#9aa3ad';
  const CUM = ['D0', 'D1', 'D2', 'D3', 'D4'];

  /* ── DOM ───────────────────────────────────────────────────────────────── */

  const $ = (id) => document.getElementById(id);
  const tooltip = $('tooltip');
  const noteEl = $('app-note');
  const input = $('area-search');
  const datalist = $('area-names');
  const overview = $('overview');
  const areaEl = $('area');
  const compsEl = $('components');
  const srSection = $('sr-area-section');
  const srTable = $('sr-area-table');

  const params = MCO.urlParams();
  const live = MCO.createLiveRegion();            // §5.1
  const recCache = MCO.promiseCache();

  /* ── State ─────────────────────────────────────────────────────────────── */

  let meta = null;                                // areas.json minus areas
  let areas = [];
  const byCode = new Map();                       // AIANNHCE -> area
  const byLabel = new Map();                      // lower-case label -> area
  const classByGeoid = new Map();
  let areaFC = null;
  let maskFC = null;                              // outside the Tribal areas
  let usdmOk = false;                             // this week's USDM map loaded
  let wk = 0;                                     // selected week (index from week0)
  let lastWk = 0;                                 // latest week
  let webIdx = null;                              // usdm-aiannh.json: geoid -> series
  // Map view: 'usdm' (the USDM map, full color inside Tribal areas) or
  // 'worst' (each area by its most severe class).
  let view = params.get('view') === 'worst' ? 'worst' : 'usdm';
  let selected = null;                            // AIANNHCE
  let hoverId = null;
  let opener = null;

  const token = (name) =>
    getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  const esc = (s) => MCO.escapeHTML(String(s));

  function note(html) {
    noteEl.hidden = !html;
    noteEl.innerHTML = html || '';
  }

  // Search and display label: the first component's Census name with its
  // legal/statistical description ("Navajo Nation Reservation", "Akiak
  // ANVSA") — unique across entities, unlike the bare Name.
  const labelOf = (a) => a.components[0].name_lsad;
  // Heading: the bare Name when the entity has R and T parts to tell apart.
  const titleOf = (a) => (a.components.length > 1 ? a.name : labelOf(a));
  // Most severe class of the area's parts present this week; null if none is.
  function worstOf(a) {
    const cs = a.components.map((c) => classByGeoid.get(c.geoid)).filter((c) => c != null);
    return cs.length ? Math.max(...cs) : null;
  }

  /* ── Dates (UTC only: USDM weeks are calendar dates, not instants) ─────── */

  let week0 = 0;
  const weekDate = (j) => new Date(week0 + j * MS_PER_WEEK);
  const fmtDate = (d) => d.toLocaleDateString('en-US',
    { year: 'numeric', month: 'long', day: 'numeric', timeZone: 'UTC' });
  const isoDate = (d) => d.toISOString().slice(0, 10);

  /* ── Map init (§7) ─────────────────────────────────────────────────────── */

  let map = null;
  let zoomFloor = null;
  try {
    map = new maplibregl.Map({
      container: 'map',
      style: MCO.map.cartoStyleUrl(),
      ...MCO.map.initialCamera(params, { bounds: CONUS, fitOpts: FIT_OPTS }),
    });
    MCO.map.addNavigation(map);                  // house default: no compass
    MCO.map.addFitControl(map, {
      bounds: CONUS, fitOpts: FIT_OPTS,
      title: 'Zoom to the contiguous US',
    });
    zoomFloor = MCO.map.installZoomFloor(map, { bounds: BOUNDS, fitOpts: FIT_OPTS });
  } catch (e) {
    map = null;                                  // no WebGL → table fallback
  }

  function fillColor() {
    const expr = ['match', ['get', 'GEOID']];
    for (const [geoid, cls] of classByGeoid) expr.push(geoid, CLASS_COLORS[cls]);
    expr.push(NODATA_COLOR);
    return expr;
  }
  // data-tiles USDM polygons are nested as the USDM publishes them (D0
  // contains D1, …) and arrive in class order, so later classes draw on top.
  const usdmColor = ['match', ['get', 'usdm_class'],
    ...CUM.flatMap((k, i) => [k, CLASS_COLORS[i + 1]]), NODATA_COLOR];
  const EMPTY = { type: 'FeatureCollection', features: [] };

  // The basemap's own background, so the fade reads as "less USDM", not as
  // a tint; the theme token if the style has no background layer.
  function paperColor() {
    const bg = map.getStyle().layers.find((l) => l.type === 'background');
    return (bg && map.getPaintProperty(bg.id, 'background-color')) || token('--bg-deep');
  }

  function applyView() {
    if (!map || !map.getLayer('aiannh-fill')) return;
    const usdm = view === 'usdm' && usdmOk && !!maskFC;
    for (const id of ['aiannh-base', 'usdm', 'usdm-mask']) {
      if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', usdm ? 'visible' : 'none');
    }
    map.setLayoutProperty('aiannh-fill', 'visibility', usdm ? 'none' : 'visible');
  }

  // Everything map.setStyle() wipes gets re-added here (theme switch — §4).
  // Layer order (§7): basemap → hillshade → fills → basemap labels → lines →
  // selection → area names. Two fill stacks share the slot under the
  // basemap labels; applyView() shows one:
  //   USDM map: each Tribal area as "None", the week's USDM in full color,
  //     then everything outside the Tribal areas veiled in the basemap's
  //     own background — so drought reads at full strength only inside.
  //   Most severe by area: one flat fill per area.
  // The boundaries carry identity in both: a halo under a line, in theme
  // tokens, which holds against every class color and both basemaps.
  function addCustomLayers() {
    if (!areaFC) return;
    MCO.map.addHillshade(map);
    const below = MCO.map.firstSymbolLayerId(map);
    if (!map.getSource(SRC)) {
      map.addSource(SRC, { type: 'geojson', data: areaFC, promoteId: 'GEOID' });
    }
    if (maskFC) {
      if (!map.getSource('usdm')) map.addSource('usdm', { type: 'geojson', data: usdmData || EMPTY });
      if (!map.getSource('usdm-mask')) map.addSource('usdm-mask', { type: 'geojson', data: maskFC });
      map.addLayer({
        id: 'aiannh-base', type: 'fill', source: SRC,
        paint: { 'fill-color': CLASS_COLORS[0], 'fill-opacity': 0.9 },
      }, below);
      map.addLayer({
        id: 'usdm', type: 'fill', source: 'usdm',
        paint: { 'fill-color': usdmColor, 'fill-opacity': 0.9 },
      }, below);
      map.addLayer({
        id: 'usdm-mask', type: 'fill', source: 'usdm-mask',
        paint: { 'fill-color': paperColor(), 'fill-opacity': 0.68, 'fill-antialias': false },
      }, below);
    }
    map.addLayer({
      id: 'aiannh-fill', type: 'fill', source: SRC,
      paint: { 'fill-color': fillColor(), 'fill-opacity': 0.85 },
    }, below);
    // Pointer target in both views: transparent fills are still queryable.
    map.addLayer({
      id: 'aiannh-hit', type: 'fill', source: SRC,
      paint: { 'fill-color': '#000', 'fill-opacity': 0 },
    });
    // Theme tokens, so the halo and line invert with the basemap (§4).
    const ink = token('--text-primary'), paper = token('--bg-deep');
    map.addLayer({
      id: 'aiannh-casing', type: 'line', source: SRC,
      paint: {
        'line-color': paper,
        'line-width': ['interpolate', ['linear'], ['zoom'], 3, 1.5, 8, 3.5],
        'line-opacity': 0.85,
      },
    });
    map.addLayer({
      id: 'aiannh-line', type: 'line', source: SRC,
      paint: {
        'line-color': ink,
        // zoom must be the outermost input; hover is decided per stop
        'line-width': ['interpolate', ['linear'], ['zoom'],
          3, ['case', ['boolean', ['feature-state', 'hover'], false], 2.2, 0.6],
          8, ['case', ['boolean', ['feature-state', 'hover'], false], 3, 1.2]],
      },
    });
    map.addLayer({
      id: 'aiannh-selected', type: 'line', source: SRC,
      filter: ['==', ['get', 'AIANNHCE'], selected ?? ''],
      paint: { 'line-color': token('--selection-ring'), 'line-width': 3 },
    });
    // Area names once the map is zoomed to a region: one point per
    // component, so a many-part area isn't labeled on every piece.
    if (!map.getSource('aiannh-label')) map.addSource('aiannh-label', { type: 'geojson', data: labelPoints() });
    map.addLayer({
      id: 'aiannh-label', type: 'symbol', source: 'aiannh-label', minzoom: 6.5,
      layout: {
        'text-field': ['get', 'NameLSAD'],
        'text-font': ['Open Sans Bold'],
        'text-size': ['interpolate', ['linear'], ['zoom'], 6.5, 10, 10, 13],
        'text-max-width': 8,
        'symbol-placement': 'point',
      },
      paint: { 'text-color': ink, 'text-halo-color': paper, 'text-halo-width': 1.4 },
    });
    applyView();
  }

  // Label anchor per component: the area centroid of its largest part
  // (planar lng/lat is fine at this scale), its bbox center if degenerate.
  let labelFC = null;
  function labelPoints() {
    if (labelFC) return labelFC;
    const ringArea = (r) => {
      let a = 0;
      for (let i = 0, j = r.length - 1; i < r.length; j = i++) a += (r[j][0] + r[i][0]) * (r[j][1] - r[i][1]);
      return Math.abs(a / 2);
    };
    const centroid = (r) => {
      let a = 0, x = 0, y = 0;
      for (let i = 0, j = r.length - 1; i < r.length; j = i++) {
        const f = r[j][0] * r[i][1] - r[i][0] * r[j][1];
        a += f; x += (r[j][0] + r[i][0]) * f; y += (r[j][1] + r[i][1]) * f;
      }
      if (!a) {
        const xs = r.map((p) => p[0]), ys = r.map((p) => p[1]);
        return [(Math.min(...xs) + Math.max(...xs)) / 2, (Math.min(...ys) + Math.max(...ys)) / 2];
      }
      return [x / (3 * a), y / (3 * a)];
    };
    labelFC = { type: 'FeatureCollection', features: areaFC.features.flatMap((f) => {
      const g = f.geometry;
      if (!g) return [];
      const polys = g.type === 'Polygon' ? [g.coordinates] : g.coordinates;
      const big = polys.reduce((m, p) => (ringArea(p[0]) > ringArea(m[0]) ? p : m));
      return [{ type: 'Feature', properties: { NameLSAD: f.properties.NameLSAD },
                geometry: { type: 'Point', coordinates: centroid(big[0]) } }];
    }) };
    return labelFC;
  }

  /* ── Weekly USDM map (data-tiles), a few weeks kept for stepping ───────── */

  let usdmData = null;
  const usdmCache = new Map();
  function loadUsdm(date) {
    if (!usdmCache.has(date)) {
      usdmCache.set(date, MCO.fetchJSON(USDM_MAP(date), { timeoutMs: 60000 })
        .then((t) => topojson.feature(t, t.objects.usdm)));
      if (usdmCache.size > 6) usdmCache.delete(usdmCache.keys().next().value);
    }
    return usdmCache.get(date).catch((e) => { usdmCache.delete(date); throw e; });
  }
  async function showUsdm(j) {
    if (!map || !maskFC) return;
    const date = isoDate(weekDate(j));
    try {
      const fc = await loadUsdm(date);
      if (wk !== j) return;                       // a newer week won
      usdmData = fc;
      usdmOk = true;
    } catch (e) {
      if (wk !== j) return;
      usdmData = EMPTY;
      usdmOk = false;
      if (view === 'usdm') MCO.showToast('The USDM map for this week didn’t load; showing areas by most severe class.', 4000);
    }
    const src = map.getSource('usdm');
    if (src) src.setData(usdmData);
    applyView();
    renderLegend();
  }

  /* ── Legend (kit panel; text labels carry the classes — §6) ────────────── */

  function renderLegend() {
    const usdm = view === 'usdm' && usdmOk && !!maskFC;
    $('legend-title').textContent = usdm ? 'US Drought Monitor' : 'Most severe class';
    const counts = new Array(CLASSES.length).fill(0);
    let absent = 0;
    for (const a of areas) {
      const w = worstOf(a);
      if (w == null) absent += 1; else counts[w] += 1;
    }
    const rows = CLASSES.map((c, i) =>
      `<div class="legend-row"><span class="legend-swatch" data-cls="${i}" aria-hidden="true"></span>` +
      `<span>${c === 'None' ? 'None' : `${c} ${esc(CLASS_NAMES[i])}`}</span>` +
      (usdm ? '' : `<span class="legend-count">${counts[i]}</span>`) + '</div>').join('');
    $('legend-rows').innerHTML = rows + (usdm
      ? '<p class="legend-note">Full color inside Tribal areas; faded outside.</p>'
      : '<div class="legend-row"><span class="legend-swatch" data-cls="na" aria-hidden="true"></span>' +
        `<span>Not in this week’s map</span><span class="legend-count">${absent}</span></div>` +
        '<p class="legend-note">Counts are Tribal areas, by their most severe part.</p>');
    for (const b of document.querySelectorAll('.view-btns [data-view]')) {
      b.setAttribute('aria-pressed', String(b.dataset.view === view));
      b.disabled = b.dataset.view === 'usdm' && !maskFC && meta !== null;
    }
  }
  for (const b of document.querySelectorAll('.view-btns [data-view]')) {
    b.addEventListener('click', () => {
      if (view === b.dataset.view) return;
      view = b.dataset.view;
      applyView();
      renderLegend();
      pushState();
      live.announce(view === 'usdm'
        ? 'Map shows the US Drought Monitor, full color inside Tribal areas.'
        : 'Map shows each Tribal area by its most severe drought class.');
    });
  }
  MCO.initCollapsible({ toggle: $('legend-toggle'), body: $('legend-body'),
                       autoCollapseOnCompact: true });

  /* ── Overview (no area selected) ───────────────────────────────────────── */

  function renderOverview() {
    $('overview-date').textContent = validText();
    const present = areas.filter((a) => worstOf(a) != null);
    const inDrought = present.filter((a) => worstOf(a) >= 2).length;
    const severe = present.filter((a) => worstOf(a) >= 4).length;
    const now = wk === lastWk;
    $('overview-summary').innerHTML =
      `<strong>${inDrought} of ${present.length}</strong> Tribal areas ${now ? 'have' : 'had'} land in drought (D1 or worse) ${now ? 'this week' : 'that week'}` +
      (severe ? `, including <strong>${severe}</strong> with extreme or exceptional drought (D3–D4).` : '.') +
      (present.length < areas.length
        ? ` ${areas.length - present.length} of today’s areas had no boundary in that week’s map.` : '');
  }

  /* ── Area panel ────────────────────────────────────────────────────────── */

  // Categorical shares from the cumulative series at week j: None, D0…D4.
  function categorical(cum, j) {
    const c = CUM.map((k) => cum[k][j]);
    if (c[0] == null) return null;
    return [100 - c[0], c[0] - c[1], c[1] - c[2], c[2] - c[3], c[3] - c[4], c[4]]
      .map((v) => Math.max(0, v));
  }

  const pct = (v) => `${v.toFixed(1)}%`;
  function change(now, then) {
    if (then == null) return 'no record';
    const d = now - then;
    if (Math.abs(d) < 0.05) return 'no change';
    return `${d > 0 ? '+' : '−'}${Math.abs(d).toFixed(1)} pts`;
  }

  function stackedBar(cat) {
    let x = 0;
    const rects = cat.map((v, i) => {
      const r = `<rect x="${x.toFixed(3)}" y="0" width="${v.toFixed(3)}" height="10" fill="${CLASS_COLORS[i]}"/>`;
      x += v;
      return r;
    }).join('');
    return `<svg class="bar" viewBox="0 0 100 10" preserveAspectRatio="none" aria-hidden="true" focusable="false">${rects}</svg>`;
  }

  // drought.gov's history graph: each cumulative class drawn as its own area
  // from zero, D0 behind through D4 in front, so the top edge of each color
  // is the share of the area at or above that class.
  function historyChart(cum, weeks, at) {
    const W = 380, H = 180, L = 34, R = 6, T = 6, B = 20;   // ≈ 1:1 in the details column
    const x = (j) => L + (j / (weeks - 1)) * (W - L - R);
    const y = (v) => T + (1 - v / 100) * (H - T - B);
    const base = y(0).toFixed(1);
    const paths = CUM.map((k, i) => {
      const s = cum[k];
      let d = '', run = false, last = 0;
      for (let j = 0; j < weeks; j++) {
        if (s[j] == null) {
          if (run) { d += `L${x(last).toFixed(1)},${base}Z`; run = false; }
          continue;
        }
        const px = x(j).toFixed(1), py = y(s[j]).toFixed(1);
        d += run ? `L${px},${py}` : `M${px},${base}L${px},${py}`;
        run = true; last = j;
      }
      if (run) d += `L${x(last).toFixed(1)},${base}Z`;
      return `<path d="${d}" fill="${CLASS_COLORS[i + 1]}"/>`;
    }).join('');
    const grid = [0, 25, 50, 75, 100].map((v) =>
      `<line class="grid" x1="${L}" x2="${W - R}" y1="${y(v)}" y2="${y(v)}"/>` +
      `<text class="tick" x="${L - 6}" y="${y(v) + 3.5}" text-anchor="end">${v}%</text>`).join('');
    const first = weekDate(0).getUTCFullYear();
    const lastYear = weekDate(weeks - 1).getUTCFullYear();
    let xt = '';
    for (let yr = Math.ceil(first / 5) * 5; yr <= lastYear; yr += 5) {
      const j = (Date.UTC(yr, 0, 1) - week0) / MS_PER_WEEK;
      if (j < 0 || j > weeks - 1) continue;
      xt += `<line class="axis" x1="${x(j)}" x2="${x(j)}" y1="${H - B}" y2="${H - B + 4}"/>` +
            `<text class="tick" x="${x(j)}" y="${H - 6}" text-anchor="middle">${yr}</text>`;
    }
    // The selected week, when it isn't the latest (the right edge already is)
    const now = at < weeks - 1
      ? `<line class="now" x1="${x(at).toFixed(1)}" x2="${x(at).toFixed(1)}" y1="${T}" y2="${H - B}"/>` : '';
    return `<svg class="history" viewBox="0 0 ${W} ${H}" aria-hidden="true" focusable="false">` +
      `${paths}${grid}<line class="axis" x1="${L}" x2="${W - R}" y1="${H - B}" y2="${H - B}"/>${xt}${now}</svg>`;
  }

  // Table twin of the chart (§5.2): annual means of each cumulative share.
  function annualTable(cum, weeks, caption) {
    const years = new Map();
    for (let j = 0; j < weeks; j++) {
      if (cum.D0[j] == null) continue;
      const yr = weekDate(j).getUTCFullYear();
      if (!years.has(yr)) years.set(yr, { n: 0, s: [0, 0, 0, 0, 0] });
      const e = years.get(yr);
      e.n += 1;
      CUM.forEach((k, i) => { e.s[i] += cum[k][j]; });
    }
    const rows = [...years].reverse().map(([yr, e]) =>
      `<tr><th scope="row">${yr}</th>${e.s.map((v) => `<td>${(v / e.n).toFixed(1)}</td>`).join('')}</tr>`).join('');
    return `<table class="data-table"><caption>${esc(caption)}</caption>` +
      '<thead><tr><th scope="col">Year</th>' +
      CUM.map((k) => `<th scope="col">${k === 'D4' ? 'D4' : `${k}–D4`}</th>`).join('') + '</tr></thead>' +
      `<tbody>${rows}</tbody></table>`;
  }

  // RFC 4180 field: names can hold commas and apostrophes.
  function csvField(v) {
    const t = v == null ? '' : String(v);
    return /[",\n]/.test(t) ? `"${t.replace(/"/g, '""')}"` : t;
  }

  // Readable download name, like the projections zips: "Coeur d'Alene
  // Reservation" -> "Coeur_d_Alene_Reservation".
  function fileStem(name) {
    return name.normalize('NFD').replace(/[\u0300-\u036f]/g, '')
      .replace(/[^A-Za-z0-9-]+/g, '_').replace(/^_+|_+$/g, '');
  }

  function csvFor(comp, rec) {
    const lines = ['name,geoid,date,census_year,' + CUM.map((k) => k === 'D4' ? 'D4_percent' : `${k}_D4_percent`).join(',')];
    const cy = rec.census_year;
    let ci = 0;
    for (let j = 0; j < rec.weeks; j++) {
      if (rec.cumulative.D0[j] == null) continue;
      while (ci + 1 < cy.length && cy[ci + 1].from_week <= j) ci++;
      lines.push([comp.name_lsad, comp.geoid, isoDate(weekDate(j)), cy[ci].census_year,
        ...CUM.map((k) => rec.cumulative[k][j])].map(csvField).join(','));
    }
    return lines.join('\n') + '\n';
  }

  // Overlapping counties (largest share first) and each county's own most
  // severe class: USDA drought programs key on county drought, so this is
  // the bridge from the Tribal area to the programs that apply to it.
  const COUNTY_ROWS = 5;
  function countyTable(comp, rec, j) {
    const cs = rec.counties || [];
    if (!cs.length) return '';
    const clsCell = (ch) => {
      if (ch == null || ch === '.') return '<td>No record</td>';
      const k = ch.charCodeAt(0) - 48;
      return `<td><span class="swatch" data-cls="${k}" aria-hidden="true"></span>` +
        `${k === 0 ? 'None' : CLASSES[k]} <span class="cls-name">${esc(CLASS_NAMES[k])}</span></td>`;
    };
    const row = (c) =>
      `<tr><th scope="row">${esc(c.name)}, ${esc(c.state)}</th>` +
      `<td>${c.share < 0.1 ? '&lt;0.1%' : pct(c.share)}</td>${clsCell(c.series[j])}</tr>`;
    const head = '<thead><tr><th scope="col">County</th><th scope="col">Share of area</th>' +
      '<th scope="col">County’s most severe class</th></tr></thead>';
    const caption = (t) => `<caption class="sr-only">${esc(t)}</caption>`;
    const asOf = Date.parse(meta.county_date + 'T00:00:00Z') < weekDate(j).getTime()
      ? ` County data run through ${fmtDate(new Date(meta.county_date + 'T00:00:00Z'))}.` : '';
    const more = cs.length > COUNTY_ROWS
      ? `<details><summary>${cs.length - COUNTY_ROWS} more ${cs.length - COUNTY_ROWS === 1 ? 'county' : 'counties'}</summary>` +
        `<table class="county-table">${caption(`More counties overlapping ${comp.name_lsad}`)}${head}` +
        `<tbody>${cs.slice(COUNTY_ROWS).map(row).join('')}</tbody></table></details>`
      : '';
    return `<div class="counties">
      <h4>Overlapping counties</h4>
      <table class="county-table">${caption(`Counties overlapping ${comp.name_lsad}, with each county’s most severe drought class`)}${head}
        <tbody>${cs.slice(0, COUNTY_ROWS).map(row).join('')}</tbody></table>
      ${more}
      <p class="county-note">USDA drought programs use county drought levels. Counties are today’s
         ${esc(String(meta.county_census_year))} Census boundaries.${asOf}</p>
    </div>`;
  }

  function componentCard(comp, rec, multi, L) {
    const cum = rec.cumulative;
    const cat = categorical(cum, L);
    const id = `c-${comp.geoid}`;
    const heading = multi
      ? (comp.comptyp === 'R' ? 'Reservation' : 'Off-reservation trust land')
      : 'Current conditions';
    if (!cat) {
      return `<article class="comp" aria-labelledby="${id}-h"><h3 id="${id}-h">${esc(heading)}</h3>` +
        `<p class="comp-name">${esc(comp.name_lsad)}</p><p>Not in this week’s map: the Census boundary for this part
          is newer than that week’s.</p></article>`;
    }
    const drought = cum.D1[L];
    const rows = CLASSES.map((c, i) =>
      `<tr><th scope="row"><span class="swatch" data-cls="${i}" aria-hidden="true"></span>` +
      `${c === 'None' ? 'None' : c} <span class="cls-name">${esc(CLASS_NAMES[i])}</span></th>` +
      `<td>${pct(cat[i])}</td></tr>`).join('');
    const csvName = `${fileStem(comp.name_lsad)}_USDM_${isoDate(weekDate(rec.weeks - 1))}.csv`;
    const prior = (k) => (L - k >= 0 ? cum.D1[L - k] : null);
    return `<article class="comp" aria-labelledby="${id}-h">
      <h3 id="${id}-h">${esc(heading)}</h3>
      ${multi ? `<p class="comp-name">${esc(comp.name_lsad)}</p>` : ''}
      <div class="stat">
        <span class="stat-value">${pct(drought)}</span>
        <span class="stat-label">of the area in drought (D1–D4)</span>
      </div>
      <p class="stat-change">${esc(change(drought, prior(1)))} from a week earlier ·
         ${esc(change(drought, prior(4)))} from four weeks earlier</p>
      ${stackedBar(cat)}
      <table class="class-table"><caption class="sr-only">Share of ${esc(comp.name_lsad)} in each drought class</caption>
        <tbody>${rows}</tbody></table>
      ${countyTable(comp, rec, L)}
      <figure class="chart">
        <figcaption>Percent of area by drought class, ${weekDate(0).getUTCFullYear()}–present</figcaption>
        ${historyChart(cum, rec.weeks, L)}
        <div class="chart-key" aria-hidden="true">${CUM.map((k, i) =>
          `<span><span class="swatch" data-cls="${i + 1}"></span>${k === 'D4' ? 'D4' : `${k}–D4`}</span>`).join('')}</div>
      </figure>
      <details>
        <summary>Yearly averages (table)</summary>
        ${annualTable(cum, rec.weeks, `Average percent of ${comp.name_lsad} at or above each drought class, by year`)}
      </details>
      <div class="comp-links">
        <a class="nav-btn" href="${esc(D3DROUGHT + encodeURIComponent(comp.geoid))}" target="_blank" rel="noopener noreferrer">
          Latest drought indicators on d3drought.org<span class="sr-only"> (opens in a new tab)</span></a>
        <a class="nav-btn" download="${esc(csvName)}" data-csv="${esc(comp.geoid)}" href="#">Download weekly data (CSV)</a>
      </div>
    </article>`;
  }

  async function selectArea(code, { fly = false, focus = true } = {}) {
    const a = byCode.get(code);
    if (!a) return;
    selected = code;
    if (focus) opener = document.activeElement;
    if (map && map.getLayer('aiannh-selected')) {
      map.setFilter('aiannh-selected', ['==', ['get', 'AIANNHCE'], code]);
    }
    if (map && fly) {
      // Camera animation gated on the LIVE reduced-motion flag — §5.3
      map.fitBounds([[a.bbox[0], a.bbox[1]], [a.bbox[2], a.bbox[3]]],
        { padding: 60, maxZoom: 9, animate: !MCO.reducedMotion() });
    }
    pushState();

    $('area-title').textContent = titleOf(a);
    compsEl.innerHTML = '<p class="loading">Loading drought history…</p>';
    overview.hidden = true;
    areaEl.hidden = false;
    if (focus) $('area-title').focus();
    const summary = await renderArea(code);
    if (summary != null) live.announce(`${titleOf(a)} selected. ${summary}`);
  }

  // Fill the area panel for the selected week. Returns a one-line summary
  // for the live region, or null when superseded or failed.
  async function renderArea(code) {
    const a = byCode.get(code);
    const j = wk;
    $('area-date').textContent = validText();
    let recs;
    try {
      recs = await Promise.all(a.components.map((c) =>
        recCache.cached(c.geoid, () => MCO.fetchJSON(`${DATA}/${c.geoid}.json`))));
    } catch (e) {
      if (selected !== code) return null;
      compsEl.innerHTML = '<p>Could not load this area’s drought history. ' +
        '<button type="button" class="nav-btn" id="btn-area-retry">Retry</button></p>';
      $('btn-area-retry').addEventListener('click', () => selectArea(code, { focus: false }));
      return null;
    }
    if (selected !== code || wk !== j) return null;   // a newer selection won
    const multi = a.components.length > 1;
    compsEl.innerHTML = a.components.map((c, i) => componentCard(c, recs[i], multi, j)).join('');
    for (const [i, c] of a.components.entries()) {
      const link = compsEl.querySelector(`a[data-csv="${CSS.escape(c.geoid)}"]`);
      if (!link) continue;
      // Built on demand: the href is swapped in before the click's default
      // action reads it, and the blob is released once the download has
      // had time to start.
      link.addEventListener('click', () => {
        const url = URL.createObjectURL(new Blob([csvFor(c, recs[i])], { type: 'text/csv' }));
        link.href = url;
        setTimeout(() => { URL.revokeObjectURL(url); link.href = '#'; }, 10000);
      });
    }
    const summary = a.components.map((c, i) => {
      const d = recs[i].cumulative.D1[j];
      return d == null ? '' : `${multi ? (c.comptyp === 'R' ? 'Reservation' : 'Trust land') + ': ' : ''}${d.toFixed(1)} percent in drought`;
    }).filter(Boolean).join('; ');
    return summary ? `${summary}.` : 'Not in that week’s map.';
  }

  function closeArea() {
    if (!selected) return;
    selected = null;
    areaEl.hidden = true;
    overview.hidden = false;
    compsEl.innerHTML = '';
    if (map && map.getLayer('aiannh-selected')) {
      map.setFilter('aiannh-selected', ['==', ['get', 'AIANNHCE'], '']);
    }
    // Restore focus — §5.12; the map canvas when the opener was a click.
    const target = opener && opener.isConnected && opener !== document.body
      ? opener : (map ? map.getCanvas() : $('main'));
    if (target && target.focus) target.focus();
    opener = null;
    pushState();
  }
  $('area-close').addEventListener('click', closeArea);

  /* ── URL state (§4): mirror every mutation; clean URL at defaults ──────── */

  function pushState() {
    const p = {};
    if (selected) p.area = selected;
    if (view !== 'usdm') p.view = view;
    if (meta && wk !== lastWk) p.date = isoDate(weekDate(wk));
    const theme = MCO.getTheme();
    if (theme) p.theme = theme;
    if (map) Object.assign(p, MCO.map.cameraParams(map));
    MCO.replaceUrlState(p);
  }
  if (map) map.on('moveend', pushState);

  // ?area= is a 4-character Census AIANNHCE, validated against areas.json.
  function resolveAreaParam() {
    const raw = (params.get('area') || '').trim().toUpperCase();
    return /^[0-9A-Z]{4}$/.test(raw) && byCode.has(raw) ? raw : null;
  }

  /* ── USDM week (navbar stepper; ?date=) ────────────────────────────────── */

  const dateInput = $('date-input');
  const btnPrev = $('btn-date-prev');
  const btnNext = $('btn-date-next');
  const validText = () => `US Drought Monitor map valid ${fmtDate(weekDate(wk))}`;

  // Any calendar date resolves to the USDM week in effect: the Tuesday on or
  // before it, within the archive.
  function weekOf(iso) {
    const t = Date.parse(iso + 'T00:00:00Z');
    if (Number.isNaN(t)) return null;
    return Math.min(lastWk, Math.max(0, Math.floor((t - week0) / MS_PER_WEEK)));
  }

  // Worst class of every component at week j: areas.json carries the latest
  // week; earlier weeks come from the archive's web JSON, fetched once.
  async function classesAt(j) {
    if (j === lastWk) {
      return new Map(areas.flatMap((a) => a.components.map((c) => [c.geoid, c.class])));
    }
    if (!webIdx) {
      webIdx = MCO.fetchJSON(WEB_JSON, { timeoutMs: 60000 }).then((d) => {
        if (d.schema !== 'usdm-max-class-aiannh/1' || d.week0 !== meta.week0) throw new Error('unexpected schema');
        return new Map(d.geoids.map((g, i) => [g, d.series[i]]));
      });
      webIdx.catch(() => { webIdx = null; });
    }
    const idx = await webIdx;
    const out = new Map();
    for (const a of areas) {
      for (const c of a.components) {
        const ch = (idx.get(c.geoid) || '')[j];
        if (ch && ch !== '.') out.set(c.geoid, ch.charCodeAt(0) - 48);
      }
    }
    return out;
  }

  function syncDateControls() {
    dateInput.value = isoDate(weekDate(wk));
    btnPrev.disabled = wk <= 0;
    btnNext.disabled = wk >= lastWk;
  }

  async function setWeek(j, { announce = true } = {}) {
    if (j == null || j === wk) { syncDateControls(); return; }
    wk = j;
    syncDateControls();
    pushState();
    showUsdm(j);
    let cls;
    try {
      cls = await classesAt(j);
    } catch (e) {
      if (wk === j) MCO.showToast('Could not load that week’s drought classes.', 4000);
      return;
    }
    if (wk !== j) return;                        // stepped again meanwhile
    classByGeoid.clear();
    for (const [g, k] of cls) classByGeoid.set(g, k);
    if (map && map.getLayer('aiannh-fill')) map.setPaintProperty('aiannh-fill', 'fill-color', fillColor());
    renderOverview();
    renderLegend();
    renderSRTable();
    let summary = null;
    if (selected) summary = await renderArea(selected);
    if (announce && wk === j) {
      live.announce(`${validText()}.` + (summary ? ` ${titleOf(byCode.get(selected))}: ${summary}` : ''));
    }
  }

  // The native picker commits on change; typing commits on Enter or blur
  // (change), and a partial date leaves the value empty, which we ignore.
  dateInput.addEventListener('change', () => {
    if (dateInput.value) setWeek(weekOf(dateInput.value));
  });
  btnPrev.addEventListener('click', () => setWeek(Math.max(0, wk - 1)));
  btnNext.addEventListener('click', () => setWeek(Math.min(lastWk, wk + 1)));

  /* ── Search (keyboard path to every polygon — §5.8) ────────────────────── */

  const searchCtl = MCO.initSearchCollapse({
    wrap: $('search-wrap'),
    toggle: $('btn-search-toggle'),
    input,
  });

  const fold = (s) => s.normalize('NFD').replace(/[̀-ͯʻ‘’']/g, '').toLowerCase();

  function submitSearch({ exactOnly }) {
    const q = input.value.trim();
    if (!q) return;
    let a = byLabel.get(fold(q));
    if (!a && !exactOnly) {
      const f = fold(q);
      const hits = areas.filter((x) => fold(labelOf(x)).includes(f) || fold(x.name).includes(f));
      if (hits.length === 1) {
        a = hits[0];
      } else if (hits.length > 1) {
        MCO.showToast(`${hits.length} matches — keep typing or pick from the list.`);
        live.announce(`${hits.length} Tribal areas match ${q}.`);
        return;
      }
    }
    if (!a) {
      if (!exactOnly) {
        MCO.showToast(`No Tribal area matches “${q}”.`);
        live.announce(`No Tribal area matches ${q}.`);
      }
      return;
    }
    input.value = '';
    searchCtl.close({ restoreFocus: false });    // the panel takes focus next
    selectArea(a.aiannhce, { fly: true });
  }
  input.addEventListener('change', () => submitSearch({ exactOnly: true }));   // datalist pick
  input.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') { e.preventDefault(); submitSearch({ exactOnly: false }); }
  });

  // Esc precedence: search overlay first, then the area panel.
  document.addEventListener('keydown', (e) => {
    if (e.key !== 'Escape') return;
    if (searchCtl.isOpen()) { searchCtl.close(); return; }
    if (selected) closeArea();
  });

  /* ── Map pointer interactions ──────────────────────────────────────────── */

  if (map) {
    map.on('mousemove', 'aiannh-hit', (e) => {
      const f = e.features && e.features[0];
      if (!f) return;
      if (hoverId !== null && hoverId !== f.id) {
        map.setFeatureState({ source: SRC, id: hoverId }, { hover: false });
      }
      hoverId = f.id;
      map.setFeatureState({ source: SRC, id: hoverId }, { hover: true });
      map.getCanvas().style.cursor = 'pointer';
      const cls = classByGeoid.get(f.properties.GEOID);
      tooltip.innerHTML = `<span class="tooltip-name">${esc(f.properties.NameLSAD)}</span>` +
        `<span class="tooltip-cls">${cls == null ? 'Not in this week’s map'
          : cls === 0 ? 'No drought' : `Most severe: ${CLASSES[cls]} ${esc(CLASS_NAMES[cls])}`}</span>`;
      // .mco-tooltip is position:fixed — use viewport coordinates.
      tooltip.style.left = (e.originalEvent.clientX + 14) + 'px';
      tooltip.style.top = (e.originalEvent.clientY + 14) + 'px';
      tooltip.classList.add('visible');
    });
    map.on('mouseleave', 'aiannh-hit', () => {
      if (hoverId !== null) map.setFeatureState({ source: SRC, id: hoverId }, { hover: false });
      hoverId = null;
      map.getCanvas().style.cursor = '';
      tooltip.classList.remove('visible');
    });
    map.on('click', 'aiannh-hit', (e) => {
      const f = e.features && e.features[0];
      if (f && byCode.has(f.properties.AIANNHCE)) selectArea(f.properties.AIANNHCE);
    });
  }

  /* ── Theme (§4): swap style, then re-add everything setStyle wiped ─────── */

  MCO.initThemeToggle({
    button: $('btn-theme'),
    iconSun: $('icon-sun'),
    iconMoon: $('icon-moon'),
    onChange: () => {
      if (map) {
        hoverId = null;
        map.setStyle(MCO.map.cartoStyleUrl());
        map.once('style.load', addCustomLayers);
      }
      pushState();
    },
  });

  MCO.initInfoModal({ dialog: $('info-modal'), trigger: $('btn-info') });

  /* ── Screen-reader table twin (§5.2) — built once per load ─────────────── */

  function renderSRTable() {
    const clsText = (k) => (k == null ? 'Not in this week’s map'
      : k === 0 ? 'No drought' : `${CLASSES[k]} ${esc(CLASS_NAMES[k])}`);
    const body = areas.map((a) => a.components.map((c) =>
      `<tr><th scope="row">${esc(c.name_lsad)}</th>` +
      `<td>${clsText(classByGeoid.get(c.geoid))}</td>` +
      `<td><button type="button" class="link-btn" data-area="${esc(a.aiannhce)}">Show details</button></td></tr>`).join('')).join('');
    srTable.innerHTML =
      `<caption>Most severe US Drought Monitor class for the week of ${esc(isoDate(weekDate(wk)))}</caption>` +
      '<thead><tr><th scope="col">Area</th><th scope="col">Most severe class</th><th scope="col">Details</th></tr></thead>' +
      `<tbody>${body}</tbody>`;
  }
  srTable.addEventListener('click', (e) => {
    const b = e.target.closest('button[data-area]');
    if (b) selectArea(b.dataset.area, { fly: true });
  });

  function showFallback() {
    document.documentElement.classList.add('is-fallback');
    srSection.classList.remove('sr-only');
    srSection.classList.add('is-visible');
  }

  /* ── Boot ──────────────────────────────────────────────────────────────── */

  function loadAll() {
    note('Loading Tribal areas…');
    Promise.all([
      MCO.fetchJSON(`${DATA}/areas.json`),
      MCO.fetchJSON(BOUNDARIES, { timeoutMs: 60000 }).catch(() => null),
      // The fade mask is optional: without it the map falls back to
      // "Most severe by area".
      MCO.fetchJSON(`${DATA}/mask.geojson`, { timeoutMs: 60000 }).catch(() => null),
    ]).then(([idx, topo, mask]) => {
      if (idx.schema !== 'usdm-aiannh-areas/1') throw new Error('unexpected schema');
      meta = idx;
      areas = idx.areas;
      week0 = Date.parse(idx.week0 + 'T00:00:00Z');
      lastWk = idx.weeks - 1;
      wk = lastWk;
      dateInput.min = idx.week0;
      dateInput.max = isoDate(weekDate(lastWk));
      maskFC = mask;
      if (!maskFC) view = 'worst';
      for (const a of areas) {
        byCode.set(a.aiannhce, a);
        byLabel.set(fold(labelOf(a)), a);
        for (const c of a.components) classByGeoid.set(c.geoid, c.class);
      }
      datalist.replaceChildren(...areas
        .map(labelOf)
        .sort((p, q) => p.localeCompare(q))
        .map((l) => { const o = document.createElement('option'); o.value = l; return o; }));
      renderOverview();
      renderLegend();
      renderSRTable();
      syncDateControls();
      note('');

      const r = resolveAreaParam();
      if (r) selectArea(r, { fly: !params.has('lng'), focus: false });
      // ?date= resolves to its USDM week; the latest week needs no param.
      const d = params.get('date');
      const wd = d && /^\d{4}-\d{2}-\d{2}$/.test(d) ? weekOf(d) : null;
      if (wd != null && wd !== lastWk) setWeek(wd, { announce: false });
      else showUsdm(wk);

      if (!map || !topo) {
        showFallback();
        if (map && !topo) note('Boundaries failed to load; all areas are listed below.');
        return;
      }
      areaFC = topojson.feature(topo, topo.objects.aiannh);
      const start = () => {
        addCustomLayers();
        zoomFloor && zoomFloor.refresh();
        live.announce(`Map loaded with ${areas.length} Tribal areas.`);
      };
      if (map.isStyleLoaded()) start(); else map.once('load', start);
    }).catch(() => {
      note('Failed to load drought data. <button type="button" class="nav-btn" id="btn-retry">Retry</button>');
      $('btn-retry').addEventListener('click', loadAll);
      MCO.showToast('Failed to load data.', 4000);
    });
  }

  if (map) {
    map.on('error', (e) => {
      if (e && e.error && /style/i.test(String(e.error.message || ''))) {
        note('Basemap failed to load. The areas will still draw once data arrives.');
      } else if (e && e.error) {
        console.error(e.error);                  // a listener silences MapLibre's own log
      }
    });
  }
  loadAll();
})();
