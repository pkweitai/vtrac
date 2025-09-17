(() => {
  const STATE = {
    rows: [],
    filtered: [],
    sortKey: 'symbol',
    sortDir: 'asc',
    activeFilter: null,
    meta: { interval: '1d', period: '—' }
  };

  const $asOf   = document.getElementById('asOf');
  const $intp   = document.getElementById('intp');
  const $count  = document.getElementById('count');
  const $tbody  = document.getElementById('rows');
  const $search = document.getElementById('search');
  const $reload = document.getElementById('reloadBtn');
  const $pills  = document.querySelectorAll('.pill');

  $reload.addEventListener('click', load);
  $search.addEventListener('input', applyFilters);
  $pills.forEach(p => p.addEventListener('click', onPill));

  document.querySelectorAll('#grid thead th[data-sort]').forEach(th => {
    th.addEventListener('click', () => {
      const key = th.dataset.sort;
      if (STATE.sortKey === key) {
        STATE.sortDir = STATE.sortDir === 'asc' ? 'desc' : 'asc';
      } else {
        STATE.sortKey = key; STATE.sortDir = 'asc';
      }
      render();
      markSort(th);
    });
  });

  function markSort(activeTh) {
    document.querySelectorAll('#grid thead th').forEach(th => th.classList.remove('sorted-asc','sorted-desc'));
    activeTh.classList.add(STATE.sortDir === 'asc' ? 'sorted-asc' : 'sorted-desc');
  }

  function onPill(e) {
    const key = e.currentTarget.dataset.filter;
    $pills.forEach(p => p.classList.remove('active'));
    if (key === 'clear') {
      STATE.activeFilter = null;
    } else {
      STATE.activeFilter = key;
      e.currentTarget.classList.add('active');
    }
    applyFilters();
  }

  function load() {
    fetch('data/snapshot.json', { cache: 'no-store' })
      .then(r => r.json())
      .then(json => {
        const rows = Array.isArray(json?.data) ? json.data : [];
        STATE.rows = rows.map(normalizeRow);
        STATE.meta.interval = json?.interval || '1d';
        STATE.meta.period = json?.period || '—';
        $asOf.textContent = `as of ${fmtAsOf(json.as_of_utc)}`;
        $intp.textContent = `${STATE.meta.interval} • ${STATE.meta.period}`;
        applyFilters();
      })
      .catch(err => {
        console.error('Failed to load snapshot.json', err);
        $asOf.textContent = 'as of — (failed to load)';
        $intp.textContent = '';
      });
  }

  function normalizeRow(r) {
    return {
      symbol: r.symbol || '',
      name: r.name || r.symbol || '',
      sector: r.sector ?? '—',
      price: toNum(r.price),
      ret1d: toNum(r.ret1d),
      ret5d: toNum(r.ret5d),
      rsi14: toNum(r.rsi14),
      vol_z: toNum(r.vol_z),
      sharpe: toNum(r.sharpe),
      mcap: toNum(r.mcap),
      pe_ttm: toNum(r.pe_ttm),
      pb: toNum(r.pb),
      div_yield: toNum(r.div_yield),
      beta: toNum(r.beta),
      news_24h: isFinite(+r.news_24h) ? +r.news_24h : 0,
      spark30: Array.isArray(r.spark30) ? r.spark30 : null,
    };
  }

  function applyFilters() {
    const q = ($search.value || '').trim().toLowerCase();
    let rows = STATE.rows.slice();

    if (q) {
      rows = rows.filter(r =>
        r.symbol.toLowerCase().includes(q) ||
        r.name.toLowerCase().includes(q)
      );
    }

    switch (STATE.activeFilter) {
      case 'rsiHigh': rows = rows.filter(r => r.rsi14 != null && r.rsi14 > 70); break;
      case 'rsiLow':  rows = rows.filter(r => r.rsi14 != null && r.rsi14 < 30); break;
      case 'volHigh': rows = rows.filter(r => r.vol_z != null && r.vol_z > 2); break;
      case 'volLow':  rows = rows.filter(r => r.vol_z != null && r.vol_z < -2); break;
      case 'news':    rows = rows.filter(r => r.news_24h && r.news_24h > 0); break;
    }

    STATE.filtered = rows;
    sort();
    render();
  }

  function sort() {
    const { sortKey, sortDir } = STATE;
    const dir = sortDir === 'asc' ? 1 : -1;
    STATE.filtered.sort((a, b) => {
      const va = a[sortKey], vb = b[sortKey];
      if (typeof va === 'string' || typeof vb === 'string') {
        return String(va).localeCompare(String(vb)) * dir;
      }
      if (va == null && vb == null) return 0;
      if (va == null) return 1;
      if (vb == null) return -1;
      return (va - vb) * dir;
    });
  }

  function render() {
    const rows = STATE.filtered;
    $tbody.innerHTML = rows.map(rowHTML).join('');
    $count.textContent = `${rows.length} shown`;
  }

  function rowHTML(r) {
    const ret1dCls = clsPn(r.ret1d);
    const ret5dCls = clsPn(r.ret5d);
    const rsiCls   = r.rsi14 != null ? (r.rsi14 > 70 ? 'neg' : (r.rsi14 < 30 ? 'pos' : '')) : '';
    const volCls   = r.vol_z != null ? (r.vol_z > 2 ? 'warn' : (r.vol_z < -2 ? 'info' : '')) : '';
    const shpCls   = r.sharpe != null ? (r.sharpe >= 1 ? 'pos' : (r.sharpe <= 0 ? 'neg' : '')) : '';
    const sparkSVG = sparkline(r.spark30);

    return `
      <tr>
        <td><span class="badge">${esc(r.symbol)}</span></td>
        <td class="hide-sm">${esc(r.name)}</td>
        <td class="num">${fmtPrice(r.price)}</td>
        <td class="num ${ret1dCls}">${fmtPct(r.ret1d)}</td>
        <td class="num hide-sm ${ret5dCls}">${fmtPct(r.ret5d)}</td>
        <td class="num ${rsiCls}">${fmtNum(r.rsi14, 2)}</td>
        <td class="num ${volCls}">${fmtNum(r.vol_z, 2)}</td>
        <td class="num ${shpCls}">${fmtNum(r.sharpe, 3)}</td>
        <td class="num hide-md">${fmtNum(r.pe_ttm, 2)}</td>
        <td class="num hide-md">${fmtNum(r.pb, 2)}</td>
        <td class="num hide-md">${fmtPct(r.div_yield)}</td>
        <td class="num hide-md">${fmtMcap(r.mcap)}</td>
        <td class="num hide-lg">${fmtNum(r.beta, 2)}</td>
        <td class="num">${sparkSVG}</td>
      </tr>
    `;
  }

  function sparkline(arr) {
    if (!arr || arr.length < 2) return '';
    const w = 100, h = 28, pad = 2;
    const xs = arr.map((_, i) => pad + (i * (w - pad*2)) / (arr.length - 1));
    const ys = arr.map(v => {
      const y = h - pad - (v * (h - pad*2));
      return Math.max(pad, Math.min(h - pad, y));
    });
    let d = `M ${xs[0].toFixed(2)} ${ys[0].toFixed(2)}`;
    for (let i = 1; i < xs.length; i++) d += ` L ${xs[i].toFixed(2)} ${ys[i].toFixed(2)}`;
    const baseY = (h - pad) - 0.5 * (h - pad*2);
    return `<svg class="sparkline" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">
      <path class="baseline" d="M ${pad} ${baseY.toFixed(2)} L ${w-pad} ${baseY.toFixed(2)}" />
      <path d="${d}"></path>
    </svg>`;
  }

  // ── utils ────────────────────────────────────────────
  function toNum(x) {
    const n = Number(x);
    return Number.isFinite(n) ? n : null;
  }
  function fmtAsOf(iso) {
    if (!iso) return '—';
    try { return new Date(iso).toLocaleString(); } catch { return iso; }
  }
  function fmtPrice(n) { return n == null ? '—' : n.toFixed(2); }
  function fmtPct(n) {
    if (n == null) return '—';
    const pct = (n * 100);
    return (pct >= 0 ? '+' : '') + pct.toFixed(2) + '%';
  }
  function fmtNum(n, p=2) { return n == null ? '—' : n.toFixed(p); }
  function fmtMcap(n) {
    if (n == null || !isFinite(n)) return '—';
    const abs = Math.abs(n);
    if (abs >= 1e12) return (n/1e12).toFixed(2) + 'T';
    if (abs >= 1e9)  return (n/1e9).toFixed(2)  + 'B';
    if (abs >= 1e6)  return (n/1e6).toFixed(2)  + 'M';
    return n.toFixed(0);
  }
  function esc(s) {
    return String(s ?? '').replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
  }
  function clsPn(x) {
    if (x == null) return '';
    return x > 0 ? 'pos' : (x < 0 ? 'neg' : '');
  }

  load();
})();
