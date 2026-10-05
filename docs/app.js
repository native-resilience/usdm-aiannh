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
                            from that week's boundary vintage
   Boundaries: census-aiannh's newest vintage, simplified (display only).
   ========================================================================== */
(function () {
  'use strict';

  /* ── Constants ─────────────────────────────────────────────────────────── */

  // The archive is served by CloudFront E2O1CP3LOBQEN8. Switch this to
  // https://data.native-resilience.com/usdm-aiannh once that hostname has
  // its DNS record and CloudFront alias. Local development (serving the repo
  // root) reads the freshly built ../dashboard/ instead.
  const ARCHIVE = 'https://d1kohdhusg35um.cloudfront.net/usdm-aiannh';
  const LOCAL = ['localhost', '127.0.0.1'].includes(location.hostname);
  const DATA = LOCAL ? '../dashboard' : ARCHIVE + '/dashboard';
  const BOUNDARIES =
    'https://data.sustainable-fsa.com/census-aiannh/census-aiannh_simple.topojson';
  const D3DROUGHT = 'https://d3drought.org/#spi/30d/rolling-30/';

  // Full extent of the areas: the Aleutians to Maine, Hawaiʻi to the North Slope.
  const BOUNDS = [[-174.24, 18.91], [-67.04, 71.34]];
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
  const worstOf = (a) => Math.max(...a.components.map((c) => c.class));

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
      ...MCO.map.initialCamera(params, { bounds: BOUNDS, fitOpts: FIT_OPTS }),
    });
    MCO.map.addNavigation(map);                  // house default: no compass
    MCO.map.addFitControl(map, {
      bounds: BOUNDS, fitOpts: FIT_OPTS,
      title: 'Zoom to all Tribal areas',
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

  // Everything map.setStyle() wipes gets re-added here (theme switch — §4).
  // Layer order (§7): basemap → hillshade → fill → basemap labels → line →
  // selection.
  function addCustomLayers() {
    if (!areaFC) return;
    MCO.map.addHillshade(map);
    if (!map.getSource(SRC)) {
      map.addSource(SRC, { type: 'geojson', data: areaFC, promoteId: 'GEOID' });
    }
    map.addLayer({
      id: 'aiannh-fill', type: 'fill', source: SRC,
      paint: {
        'fill-color': fillColor(),
        'fill-opacity': ['case',
          ['boolean', ['feature-state', 'hover'], false], 0.95, 0.8],
      },
    }, MCO.map.firstSymbolLayerId(map));
    // Outline in the theme's border token: thin, so the class fills — not
    // the boundaries — carry the read. Decorative (no text sits on it).
    map.addLayer({
      id: 'aiannh-line', type: 'line', source: SRC,
      paint: { 'line-color': token('--text-dim'), 'line-width': 0.5, 'line-opacity': 0.8 },
    });
    map.addLayer({
      id: 'aiannh-selected', type: 'line', source: SRC,
      filter: ['==', ['get', 'AIANNHCE'], selected ?? ''],
      paint: { 'line-color': token('--selection-ring'), 'line-width': 2.5 },
    });
  }

  /* ── Legend (kit panel; text labels carry the classes — §6) ────────────── */

  function renderLegend() {
    const counts = new Array(CLASSES.length).fill(0);
    for (const a of areas) counts[worstOf(a)] += 1;
    const rows = CLASSES.map((c, i) =>
      `<div class="legend-row"><span class="legend-swatch" data-cls="${i}" aria-hidden="true"></span>` +
      `<span>${c === 'None' ? 'None' : `${c} ${esc(CLASS_NAMES[i])}`}</span>` +
      `<span class="legend-count">${counts[i]}</span></div>`).join('');
    $('legend-body').innerHTML = rows +
      '<div class="legend-row"><span class="legend-swatch" data-cls="na" aria-hidden="true"></span>' +
      '<span>Not in this week’s map</span></div>' +
      '<p class="legend-note">Counts are Tribal areas, by their most severe part.</p>';
  }
  MCO.initCollapsible({ toggle: $('legend-toggle'), body: $('legend-body'),
                       autoCollapseOnCompact: true });

  /* ── Overview (no area selected) ───────────────────────────────────────── */

  function renderOverview() {
    const latest = new Date(meta.latest + 'T00:00:00Z');
    $('overview-date').textContent = `US Drought Monitor map valid ${fmtDate(latest)}`;
    const inDrought = areas.filter((a) => worstOf(a) >= 2).length;
    const severe = areas.filter((a) => worstOf(a) >= 4).length;
    $('overview-summary').innerHTML =
      `<strong>${inDrought} of ${areas.length}</strong> Tribal areas have land in drought (D1 or worse) this week` +
      (severe ? `, including <strong>${severe}</strong> with extreme or exceptional drought (D3–D4).` : '.');
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
  function historyChart(cum, weeks) {
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
    return `<svg class="history" viewBox="0 0 ${W} ${H}" aria-hidden="true" focusable="false">` +
      `${paths}${grid}<line class="axis" x1="${L}" x2="${W - R}" y1="${H - B}" y2="${H - B}"/>${xt}</svg>`;
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

  function csvFor(comp, rec) {
    const lines = ['date,geoid,census_year,' + CUM.map((k) => `${k}_D4_percent`).join(',')];
    const cy = rec.census_year;
    let ci = 0;
    for (let j = 0; j < rec.weeks; j++) {
      if (rec.cumulative.D0[j] == null) continue;
      while (ci + 1 < cy.length && cy[ci + 1].from_week <= j) ci++;
      lines.push([isoDate(weekDate(j)), comp.geoid, cy[ci].census_year,
        ...CUM.map((k) => rec.cumulative[k][j])].join(','));
    }
    return lines.join('\n') + '\n';
  }

  function componentCard(comp, rec, multi) {
    const L = rec.weeks - 1;
    const cum = rec.cumulative;
    const cat = categorical(cum, L);
    const id = `c-${comp.geoid}`;
    const heading = multi
      ? (comp.comptyp === 'R' ? 'Reservation' : 'Off-reservation trust land')
      : 'Current conditions';
    if (!cat) {
      return `<article class="comp" aria-labelledby="${id}-h"><h3 id="${id}-h">${esc(heading)}</h3>` +
        `<p class="comp-name">${esc(comp.name_lsad)}</p><p>Not in this week’s map.</p></article>`;
    }
    const drought = cum.D1[L];
    const rows = CLASSES.map((c, i) =>
      `<tr><th scope="row"><span class="swatch" data-cls="${i}" aria-hidden="true"></span>` +
      `${c === 'None' ? 'None' : c} <span class="cls-name">${esc(CLASS_NAMES[i])}</span></th>` +
      `<td>${pct(cat[i])}</td></tr>`).join('');
    const csvName = `usdm-${comp.geoid}-${isoDate(weekDate(L))}.csv`;
    return `<article class="comp" aria-labelledby="${id}-h">
      <h3 id="${id}-h">${esc(heading)}</h3>
      ${multi ? `<p class="comp-name">${esc(comp.name_lsad)}</p>` : ''}
      <div class="stat">
        <span class="stat-value">${pct(drought)}</span>
        <span class="stat-label">of the area in drought (D1–D4)</span>
      </div>
      <p class="stat-change">${esc(change(drought, cum.D1[L - 1]))} since last week ·
         ${esc(change(drought, cum.D1[L - 4]))} since last month</p>
      ${stackedBar(cat)}
      <table class="class-table"><caption class="sr-only">Share of ${esc(comp.name_lsad)} in each drought class</caption>
        <tbody>${rows}</tbody></table>
      <figure class="chart">
        <figcaption>Percent of area by drought class, ${weekDate(0).getUTCFullYear()}–present</figcaption>
        ${historyChart(cum, rec.weeks)}
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
    $('area-date').textContent = `US Drought Monitor map valid ${fmtDate(new Date(meta.latest + 'T00:00:00Z'))}`;
    compsEl.innerHTML = '<p class="loading">Loading drought history…</p>';
    overview.hidden = true;
    areaEl.hidden = false;
    if (focus) $('area-title').focus();

    let recs;
    try {
      recs = await Promise.all(a.components.map((c) =>
        recCache.cached(c.geoid, () => MCO.fetchJSON(`${DATA}/${c.geoid}.json`))));
    } catch (e) {
      if (selected !== code) return;
      compsEl.innerHTML = '<p>Could not load this area’s drought history. ' +
        '<button type="button" class="nav-btn" id="btn-area-retry">Retry</button></p>';
      $('btn-area-retry').addEventListener('click', () => selectArea(code, { focus: false }));
      return;
    }
    if (selected !== code) return;              // a newer selection won
    const multi = a.components.length > 1;
    compsEl.innerHTML = a.components.map((c, i) => componentCard(c, recs[i], multi)).join('');
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
      const L = recs[i].weeks - 1;
      const d = recs[i].cumulative.D1[L];
      return d == null ? '' : `${multi ? (c.comptyp === 'R' ? 'Reservation' : 'Trust land') + ': ' : ''}${d.toFixed(1)} percent in drought`;
    }).filter(Boolean).join('; ');
    live.announce(`${titleOf(a)} selected. ${summary}.`);
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
    map.on('mousemove', 'aiannh-fill', (e) => {
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
    map.on('mouseleave', 'aiannh-fill', () => {
      if (hoverId !== null) map.setFeatureState({ source: SRC, id: hoverId }, { hover: false });
      hoverId = null;
      map.getCanvas().style.cursor = '';
      tooltip.classList.remove('visible');
    });
    map.on('click', 'aiannh-fill', (e) => {
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
    const body = areas.map((a) => a.components.map((c) =>
      `<tr><th scope="row">${esc(c.name_lsad)}</th>` +
      `<td>${c.class === 0 ? 'No drought' : `${CLASSES[c.class]} ${esc(CLASS_NAMES[c.class])}`}</td>` +
      `<td><button type="button" class="link-btn" data-area="${esc(a.aiannhce)}">Show details</button></td></tr>`).join('')).join('');
    srTable.innerHTML =
      `<caption>Most severe US Drought Monitor class for the week of ${esc(meta.latest)}</caption>` +
      '<thead><tr><th scope="col">Area</th><th scope="col">Most severe class</th><th scope="col">Details</th></tr></thead>' +
      `<tbody>${body}</tbody>`;
    srTable.addEventListener('click', (e) => {
      const b = e.target.closest('button[data-area]');
      if (b) selectArea(b.dataset.area, { fly: true });
    });
  }

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
    ]).then(([idx, topo]) => {
      if (idx.schema !== 'usdm-aiannh-areas/1') throw new Error('unexpected schema');
      meta = idx;
      areas = idx.areas;
      week0 = Date.parse(idx.week0 + 'T00:00:00Z');
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
      note('');

      const r = resolveAreaParam();
      if (r) selectArea(r, { fly: !params.has('lng'), focus: false });

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
      }
    });
  }
  loadAll();
})();
