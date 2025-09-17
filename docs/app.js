(() => {
  'use strict';

  // ── helpers ──────────────────────────────────────────────────────────────
  const $  = id => document.getElementById(id);
  const esc = s => String(s ?? '').replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
  const n2 = (x,d=2) => (x==null || !Number.isFinite(+x)) ? '—' : (+x).toFixed(d);
  const pct = x => (x==null || !Number.isFinite(+x)) ? '—' : ((+x)*100).toFixed(2)+'%';
  const mcap = n => { n=+n; if(!Number.isFinite(n)) return '—'; const a=Math.abs(n);
    if(a>=1e12) return (n/1e12).toFixed(2)+'T'; if(a>=1e9) return (n/1e9).toFixed(2)+'B';
    if(a>=1e6) return (n/1e6).toFixed(2)+'M'; return n.toFixed(0); };
  const tv = s => String(s||'').replace('-', '.');

  // cache-buster for JSON fetches
  const bust = () => `ts=${Date.now()}`;

  // ── sparkline ────────────────────────────────────────────────────────────
  function sparkSVG(values, w=120, h=28, pad=2){
    if (!Array.isArray(values) || values.length < 2) return '';
    const xs = values.map((_,i)=> pad + i*(w-2*pad)/(values.length-1));
    const ys = values.map(v => {
      const clamped = Math.max(0, Math.min(1, +v));
      return Math.max(pad, Math.min(h-pad, h - pad - (clamped * (h - 2*pad))));
    });
    let d = `M ${xs[0].toFixed(1)} ${ys[0].toFixed(1)}`;
    for(let i=1;i<xs.length;i++) d += ` L ${xs[i].toFixed(1)} ${ys[i].toFixed(1)}`;
    return `<svg class="sparkline" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">
      <path class="baseline" d="M ${pad} ${(h/2).toFixed(1)} L ${w-pad} ${(h/2).toFixed(1)}"></path>
      <path d="${d}"></path>
    </svg>`;
  }

  // ── RSI (Wilder) ─────────────────────────────────────────────────────────
  function rsi(values, period=14){
    const v = values.filter(x => Number.isFinite(+x));
    if (v.length < period+1) return null;
    let gain=0, loss=0;
    for (let i=1;i<=period;i++){
      const ch = v[i]-v[i-1];
      if (ch>=0) gain+=ch; else loss-=ch;
    }
    gain/=period; loss/=period;
    let rs = loss===0 ? Infinity : gain/loss;
    let out = [100 - 100/(1+rs)];
    for (let i=period+1;i<v.length;i++){
      const ch = v[i]-v[i-1];
      const g = Math.max(ch,0), l = Math.max(-ch,0);
      gain = (gain*(period-1)+g)/period;
      loss = (loss*(period-1)+l)/period;
      rs = loss===0 ? Infinity : gain/loss;
      out.push(100 - 100/(1+rs));
    }
    return out.length ? out[out.length-1] : null;
  }

  // ── Sharpe (annualized) ──────────────────────────────────────────────────
  function sharpeFromCloses(closes, lookback, rf_annual, interval){
    const v = closes.filter(x => Number.isFinite(+x));
    if (v.length < Math.max(lookback, 2)) return null;
    const tail = v.slice(-lookback);
    const rets = [];
    for (let i=1;i<tail.length;i++){
      const r = tail[i]/tail[i-1]-1;
      if (Number.isFinite(r)) rets.push(r);
    }
    if (rets.length < 2) return null;

    function ppy(interval){
      const iv = (interval||'1d').toLowerCase();
      if (iv==='1d') return 252;
      if (iv==='1wk') return 52;
      if (iv==='1mo') return 12;
      if (iv==='1h' || iv==='60m') return 252*6.5;
      if (iv==='30m') return 252*13;
      if (iv==='15m') return 252*26;
      if (iv==='5m')  return 252*78;
      if (iv==='2m')  return 252*195;
      if (iv==='1m')  return 252*390;
      return 252;
    }
    const P = ppy(interval);
    const rf_per = (rf_annual||0)/P;
    const ex = rets.map(r => r - rf_per);
    const mean = ex.reduce((a,b)=>a+b,0)/ex.length;
    const sd = Math.sqrt(ex.reduce((a,b)=>a+(b-mean)*(b-mean),0)/(ex.length-1));
    if (!Number.isFinite(sd) || sd<=0) return null;
    return (mean/sd)*Math.sqrt(P);
  }

  // ── IV rank/percentile ───────────────────────────────────────────────────
  function ivRankPct(history, current, win){
    const vals = (history||[]).slice(-win).filter(x => Number.isFinite(+x));
    if (!vals.length || !Number.isFinite(+current)) return {rank:null, pct:null};
    const mn = Math.min(...vals), mx = Math.max(...vals);
    const rank = (mx>mn) ? 100*(current - mn)/(mx - mn) : null;
    const pct  = 100*(vals.filter(v => v <= current).length)/vals.length;
    return {rank, pct};
  }

  // ── state ────────────────────────────────────────────────────────────────
  const state = {
    rows:[], filtered:[], asOf:'', interval:'', period:'', rf:0,
    expanded:new Set(), ivHist:{},
    ui: { rsiWin:30, sharpeWin:120, ivWin:180 }
  };

  // recompute fields based on current UI windows
  function enrichForUI(row){
    const c = (row.hist && Array.isArray(row.hist.c)) ? row.hist.c.map(Number) : [];
    // RSI
    row.rsi_ui = (c.length >= state.ui.rsiWin + 1) ? rsi(c, state.ui.rsiWin) : null;
    // Sharpe
    row.sharpe_ui = (c.length >= state.ui.sharpeWin) ? sharpeFromCloses(c, state.ui.sharpeWin, state.rf, state.interval) : null;
    // IV
    const ivSeries = (state.ivHist[row.symbol] || []).map(Number);
    const curIV = Number.isFinite(+row.iv30) ? +row.iv30 : null;
    const win = state.ui.ivWin;
    const enoughIV = ivSeries.length >= Math.max(2, win);
    const {rank, pct} = enoughIV && curIV!=null ? ivRankPct(ivSeries, curIV, win) : {rank:null, pct:null};
    row.iv_rank_ui = rank == null ? null : +rank;
    row.iv_pct_ui  = pct  == null ? null : +pct;
    return row;
  }

  function applyFilters(){
    const q = ($('q').value || '').trim().toLowerCase();
    const sec = $('sector').value;
    const key = $('sort').value;
    const dir = $('dir').value === 'asc' ? 1 : -1;

    let rows = state.rows.slice().map(enrichForUI);
    if (q) rows = rows.filter(r => r.symbol.toLowerCase().includes(q) || (r.name||'').toLowerCase().includes(q));
    if (sec) rows = rows.filter(r => r.sector === sec);

    rows.sort((a,b) => {
      if (['symbol','name','sector'].includes(key)) return String(a[key]||'').localeCompare(String(b[key]||'')) * dir;
      const av = (a[key]==null ? -Infinity : +a[key]);
      const bv = (b[key]==null ? -Infinity : +b[key]);
      return (av - bv) * dir;
    });

    state.filtered = rows;
  }

  function detailPanel(r){
    const divPct = (r.div_yield!=null ? (r.div_yield/100.0) : null);
    return `
      <div class="panel">
        <div class="kv">
          <div class="k">Symbol</div><div>${esc(r.symbol)} (${esc(r.name||'')})</div>
          <div class="k">Sector</div><div>${esc(r.sector||'—')}</div>
          <div class="k">Price</div><div>${n2(r.price,2)}</div>
          <div class="k">1D / 5D</div><div>${pct(r.ret1d)} / ${pct(r.ret5d)}</div>
          <div class="k">RSI (${state.ui.rsiWin})</div><div>${n2(r.rsi_ui,1)}</div>
          <div class="k">Sharpe (${state.ui.sharpeWin})</div><div>${n2(r.sharpe_ui,3)}</div>
          <div class="k">Vol Z</div><div>${n2(r.vol_z,2)}</div>
          <div class="k">IV30</div><div>${r.iv30==null?'—':pct(r.iv30)}</div>
          <div class="k">IV Rank (${state.ui.ivWin})</div><div>${n2(r.iv_rank_ui,2)}</div>
          <div class="k">IV %ile (${state.ui.ivWin})</div><div>${n2(r.iv_pct_ui,2)}</div>
          <div class="k">P/E</div><div>${n2(r.pe_ttm,2)}</div>
          <div class="k">P/B</div><div>${n2(r.pb,2)}</div>
          <div class="k">Div%</div><div>${pct(divPct)}</div>
          <div class="k">MktCap</div><div>${mcap(r.mcap)}</div>
          <div class="k">Beta</div><div>${n2(r.beta,2)}</div>
          <div class="k">News (24h)</div><div>${r.news_24h==null?'—':r.news_24h}</div>
        </div>
        <div class="chart">
          <div class="big">${sparkSVG(r.spark30 || [], 260, 64, 3)}</div>
          <div class="links">
            <div><strong>Links</strong></div>
            <a href="https://www.tradingview.com/symbols/${tv(r.symbol)}/" target="_blank" rel="noopener">TradingView</a>
            <a href="https://finance.yahoo.com/quote/${encodeURIComponent(r.symbol)}" target="_blank" rel="noopener">Yahoo</a>
            <a href="https://www.google.com/finance/quote/${encodeURIComponent(r.symbol)}" target="_blank" rel="noopener">Google</a>
          </div>
        </div>
      </div>`;
  }

  function updateStatus(){
    const haveIV = Object.keys(state.ivHist||{}).length;
    const maxHist = Math.max(0, ...state.rows.map(r => (r.hist && r.hist.c ? r.hist.c.length : 0)));
    $('meta').textContent =
      `${state.filtered.length} shown • ${state.interval} ${state.period} • maxHist=${maxHist} • ivHist=${haveIV} syms`;
    $('asof').textContent = `As of ${state.asOf || '—'}`;
  }

  function render(){
    const tb = $('rows'); tb.innerHTML = '';
    const frag = document.createDocumentFragment();

    state.filtered.forEach(r => {
      const isOpen = state.expanded.has(r.symbol);
      const tr = document.createElement('tr');
      const d1 = +r.ret1d || 0;
      tr.innerHTML = `
        <td class="stick-l"><span class="chev ${isOpen?'open':''}" data-sym="${r.symbol}" title="Expand">▸</span></td>
        <td class="stick-l"><a href="https://www.tradingview.com/symbols/${tv(r.symbol)}/" target="_blank" rel="noopener">${esc(r.symbol)}</a></td>
        <td class="hide-sm">${esc(r.name ?? '')}</td>
        <td class="h
