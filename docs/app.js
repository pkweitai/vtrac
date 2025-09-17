// docs/app.js
(() => {
  'use strict';

  // DOM helpers
  const $ = (id) => document.getElementById(id);
  const esc = (s) => String(s ?? '').replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
  const fmtNum = (n, p=2) => (n==null || !Number.isFinite(+n)) ? '—' : (+n).toFixed(p);
  const fmtPct = (n) => (n==null || !Number.isFinite(+n)) ? '—' : ((+n)*100).toFixed(2)+'%';
  const fmtMcap = (n) => { n=+n; if(!Number.isFinite(n)) return '—'; const a=Math.abs(n);
    if(a>=1e12) return (n/1e12).toFixed(2)+'T'; if(a>=1e9) return (n/1e9).toFixed(2)+'B';
    if(a>=1e6) return (n/1e6).toFixed(2)+'M'; return n.toFixed(0); };
  const tvSym = (s) => String(s||'').replace('-', '.');

  const spark = (arr) => {
    if (!Array.isArray(arr) || arr.length < 2) return '';
    const w=110, h=28, pad=2;
    const xs = arr.map((_,i)=> pad + i*(w-2*pad)/(arr.length-1));
    const ys = arr.map(v => {
      const y = h - pad - (v * (h - 2*pad));
      return Math.max(pad, Math.min(h-pad, y));
    });
    let d = `M ${xs[0].toFixed(1)} ${ys[0].toFixed(1)}`;
    for(let i=1;i<xs.length;i++) d += ` L ${xs[i].toFixed(1)} ${ys[i].toFixed(1)}`;
    return `<svg class="sparkline" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">
      <path class="baseline" d="M ${pad} ${(h/2).toFixed(1)} L ${w-pad} ${(h/2).toFixed(1)}"></path>
      <path d="${d}"></path>
    </svg>`;
  };

  const state = { rows: [], filtered: [], asOf:'', interval:'', period:'' };

  function applyFilters(){
    const q = ($('q').value||'').trim().toLowerCase();
    const sec = $('sector').value;
    let rows = state.rows.slice();
    if (q) rows = rows.filter(r => r.symbol.toLowerCase().includes(q) || (r.name||'').toLowerCase().includes(q));
    if (sec) rows = rows.filter(r => r.sector === sec);

    const key = $('sort').value;
    const dir = $('dir').value === 'asc' ? 1 : -1;
    rows.sort((a,b) => {
      if (['symbol','name','sector'].includes(key)) {
        return String(a[key]||'').localeCompare(String(b[key]||'')) * dir;
      }
      const av = (a[key]==null ? -Infinity : +a[key]);
      const bv = (b[key]==null ? -Infinity : +b[key]);
      return (av - bv) * dir;
    });

    state.filtered = rows;
  }

  function render(){
    const tb = $('rows'); tb.innerHTML = '';
    const frag = document.createDocumentFragment();
    for (const r of state.filtered){
      const tr = document.createElement('tr');
      const d1 = +r.ret1d || 0;
      tr.innerHTML = `
        <td><a href="https://www.tradingview.com/symbols/${tvSym(r.symbol)}/" target="_blank" rel="noopener">${esc(r.symbol)}</a></td>
        <td class="hide-sm">${esc(r.name ?? '')}</td>
        <td class="hide-sm"><span class="badge">${esc(r.sector ?? '—')}</span></td>
        <td class="num">${fmtNum(r.price,2)}</td>
        <td class="num ${d1>=0?'pos':'neg'}">${fmtPct(r.ret1d)}</td>
        <td class="num hide-sm ${(+r.ret5d||0)>=0?'pos':'neg'}">${fmtPct(r.ret5d)}</td>
        <td class="num">${fmtNum(r.rsi14,1)}</td>
        <td class="num">${fmtNum(r.sharpe,3)}</td>
        <td class="num">${fmtNum(r.vol_z,2)}</td>
        <td class="num">${r.iv30==null?'—':fmtPct(r.iv30)}</td>
        <td class="num">${fmtNum(r.iv_rank,2)}</td>
        <td class="num">${fmtNum(r.iv_percentile,2)}</td>
        <td class="num hide-sm">${fmtNum(r.pe_ttm,2)}</td>
        <td class="num hide-sm">${fmtNum(r.pb,2)}</td>
        <td class="num hide-sm">${fmtPct(r.div_yield!=null ? r.div_yield/100.0 : null)}</td>
        <td class="num hide-sm">${fmtMcap(r.mcap)}</td>
        <td class="num hide-sm">${fmtNum(r.beta,2)}</td>
        <td class="num">${spark(r.spark30)}</td>
      `;
      frag.appendChild(tr);
    }
    tb.appendChild(frag);
    $('meta').textContent = `${state.filtered.length} shown • ${state.interval} ${state.period}`;
    $('asof').textContent = `As of ${state.asOf || '—'}`;
  }

  async function load(){
    const res = await fetch('data/snapshot.json', {cache:'no-store'});
    const js = await res.json();
    state.asOf = js.as_of_utc || '';
    state.interval = js.interval || '';
    state.period = js.period || '';
    state.rows = (js.data||[]).map(x => ({
      symbol:x.symbol, name:x.name??x.symbol, sector:x.sector??'—',
      price:+x.price, ret1d:x.ret1d, ret5d:x.ret5d, rsi14:x.rsi14, sharpe:x.sharpe,
      vol_z:x.vol_z, iv30:x.iv30, iv_rank:x.iv_rank, iv_percentile:x.iv_percentile,
      mcap:x.mcap, pe_ttm:x.pe_ttm, pb:x.pb, div_yield:x.div_yield, beta:x.beta,
      news_24h:x.news_24h, spark30:(Array.isArray(x.spark30)?x.spark30:null),
    }));
    const set = new Set(state.rows.map(r=>r.sector).filter(Boolean));
    const sel = $('sector'); sel.innerHTML = '<option value="">All sectors</option>';
    [...set].sort().forEach(s => { const o=document.createElement('option'); o.value=s; o.textContent=s; sel.appendChild(o); });
    applyFilters(); render();
  }

  window.addEventListener('DOMContentLoaded', () => {
    $('q').addEventListener('input', () => { applyFilters(); render(); });
    $('sector').addEventListener('change', () => { applyFilters(); render(); });
    $('sort').addEventListener('change', () => { applyFilters(); render(); });
    $('dir').addEventListener('change', () => { applyFilters(); render(); });
    $('themeBtn').addEventListener('click', () => {
      const root = document.documentElement;
      const cur = root.getAttribute('data-theme') || 'dark';
      root.setAttribute('data-theme', cur==='dark' ? 'light' : 'dark');
    });
    load();
  });
})();
