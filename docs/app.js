(() => {
  'use strict';
  const $  = id => document.getElementById(id);
  const esc = s => String(s ?? '').replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
  const n2 = (x,d=2) => (x==null || !Number.isFinite(+x)) ? '—' : (+x).toFixed(d);
  const pct = x => (x==null || !Number.isFinite(+x)) ? '—' : ((+x)*100).toFixed(2)+'%';
  const mcap = n => { n=+n; if(!Number.isFinite(n)) return '—'; const a=Math.abs(n);
    if(a>=1e12) return (n/1e12).toFixed(2)+'T'; if(a>=1e9) return (n/1e9).toFixed(2)+'B';
    if(a>=1e6) return (n/1e6).toFixed(2)+'M'; return n.toFixed(0); };
  const tv = s => String(s||'').replace('-', '.');
  const bust = () => `ts=${Date.now()}`;

  // spark
  function sparkSVG(values, w=520, h=90, pad=8){
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

  // RSI (Wilder)
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

  function ivRankPct(history, current, win){
    const vals = (history||[]).slice(-win).filter(x => Number.isFinite(+x));
    if (!vals.length || !Number.isFinite(+current)) return {rank:null, pct:null};
    const mn = Math.min(...vals), mx = Math.max(...vals);
    const rank = (mx>mn) ? 100*(current - mn)/(mx - mn) : null;
    const pct  = 100*(vals.filter(v => v <= current).length)/vals.length;
    return {rank, pct};
  }

  const state = {
    rows:[], filtered:[], asOf:'', interval:'', period:'', rf:0,
    expanded:new Set(), ivHist:{},
    ui: { rsiWin:30, sharpeWin:120, ivWin:180 },
  };

  const ALERT_LABELS = {
    brk:"Breakout", pbuy:"Pullback Buy", osb:"Oversold Bounce", obf:"Overbought Fade",
    bullDiv:"Bullish Divergence", bearDiv:"Bearish Divergence", range:"Range Income",
    ivHigh:"High IV (Sell)", ivLow:"Low IV (Buy)", event:"Event Spike"
  };

  function enrichForUI(row){
    const c = (row.hist && Array.isArray(row.hist.c)) ? row.hist.c.map(Number) : [];
    row.rsi_ui = (c.length >= state.ui.rsiWin + 1) ? rsi(c, state.ui.rsiWin) : null;
    row.sharpe_ui = (c.length >= state.ui.sharpeWin) ? sharpeFromCloses(c, state.ui.sharpeWin, state.rf, state.interval) : null;

    // IV rank/%ile using history
    const ivSeries = (state.ivHist[row.symbol] || []).map(Number);
    const curIV = Number.isFinite(+row.iv30) ? +row.iv30 : null;
    const win = state.ui.ivWin;
    const enoughIV = ivSeries.length >= Math.max(2, win);
    const {rank, pct} = enoughIV && curIV!=null ? ivRankPct(ivSeries, curIV, win) : {rank:null, pct:null};
    row.iv_rank_ui = rank == null ? null : +rank;
    row.iv_pct_ui  = pct  == null ? null : +pct;

    // count alerts (server-provided)
    row.alertCount = Array.isArray(row.alerts) ? row.alerts.length : 0;
    return row;
  }

  function chip(a){
    const sev = a.sev || 'info';
    const lbl = a.label || ALERT_LABELS[a.code] || a.code;
    const why = a.why ? `<div class="why">${esc(a.why)}</div>` : '';
    const idea = a.opt ? `<div class="idea"><strong>Idea:</strong> ${esc(a.opt)}</div>` : '';
    return `<div class="chip ${sev}" title="${esc(lbl)}">${esc(lbl)}</div>${why}${idea}`;
  }

  function applyFilters(){
    const q = ($('q').value || '').trim().toLowerCase();
    const sec = $('sector').value;
    const key = $('sort').value;
    const dir = $('dir').value === 'asc' ? 1 : -1;
    const af = $('alertFilter').value;

    let rows = state.rows.slice().map(enrichForUI);
    if (q) rows = rows.filter(r => r.symbol.toLowerCase().includes(q) || (r.name||'').toLowerCase().includes(q));
    if (sec) rows = rows.filter(r => r.sector === sec);
    if (af) rows = rows.filter(r => (Array.isArray(r.alerts) && r.alerts.some(a => a.code === af)));

    rows.sort((a,b) => {
      if (['symbol','name','sector'].includes(key)) return String(a[key]||'').localeCompare(String(b[key]||'')) * dir;
      const av = (a[key]==null ? -Infinity : +a[key]);
      const bv = (b[key]==null ? -Infinity : +b[key]);
      return (av - bv) * dir;
    });

    state.filtered = rows;
  }

  function panelStats(r){
    const divPct = (r.div_yield!=null ? (r.div_yield/100.0) : null);
    const stats = [
      ["Sector", r.sector ?? "—"],
      ["Price", n2(r.price,2)],
      ["1D / 5D", `${pct(r.ret1d)} / ${pct(r.ret5d)}`],
      [`RSI (${state.ui.rsiWin})`, n2(r.rsi_ui,1)],
      [`Sharpe (${state.ui.sharpeWin})`, n2(r.sharpe_ui,3)],
      ["Vol Z", n2(r.vol_z,2)],
      ["IV30", r.iv30==null ? "—" : pct(r.iv30)],
      [`IV Rank (${state.ui.ivWin})`, n2(r.iv_rank_ui,2)],
      [`IV %ile (${state.ui.ivWin})`, n2(r.iv_pct_ui,2)],
      ["P/E", n2(r.pe_ttm,2)],
      ["P/B", n2(r.pb,2)],
      ["Div%", pct(divPct)],
      ["MktCap", mcap(r.mcap)],
      ["Beta", n2(r.beta,2)],
      ["News (24h)", r.news_24h==null?'—':r.news_24h]
    ];
    return stats.map(([k,v]) => `
      <div class="stat">
        <div class="k">${esc(k)}</div>
        <div class="v">${esc(v)}</div>
      </div>
    `).join('');
  }

  function detailPanel(r){
    const chips = (Array.isArray(r.alerts) ? r.alerts : [])
      .map(a => `<div>${chip(a)}</div>`).join('') || '<div class="why">No active alerts</div>';

    return `
      <div class="detail">
        <div>
          <div class="hdr-line">
            <div class="ticker">${esc(r.symbol)}</div>
            <div class="name">${esc(r.name || '')}</div>
            <div class="price-chip">$ ${n2(r.price,2)}</div>
          </div>
          <div class="spark-wrap">${sparkSVG(r.spark30 || [], 520, 90, 8)}</div>
          <div class="link-row">
            <a href="https://www.tradingview.com/symbols/${tv(r.symbol)}/" target="_blank" rel="noopener">TradingView</a>
            <a href="https://finance.yahoo.com/quote/${encodeURIComponent(r.symbol)}" target="_blank" rel="noopener">Yahoo Finance</a>
            <a href="https://www.google.com/finance/quote/${encodeURIComponent(r.symbol)}" target="_blank" rel="noopener">Google Finance</a>
          </div>
        </div>
        <div>
          <div class="stats">
            ${panelStats(r)}
          </div>
          <div class="alert-chips" style="margin-top:10px;">
            ${chips}
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
      const alertHTML = (Array.isArray(r.alerts) ? r.alerts.slice(0,3).map(a =>
        `<span class="chip ${a.sev||'info'}" title="${esc(a.why||'')}">${esc(a.label||a.code)}</span>`
      ).join(' ') : '');
      tr.innerHTML = `
        <td class="stick-l"><span class="chev ${isOpen?'open':''}" data-sym="${r.symbol}" title="Expand">▸</span></td>
        <td class="stick-l"><a href="https://www.tradingview.com/symbols/${tv(r.symbol)}/" target="_blank" rel="noopener">${esc(r.symbol)}</a></td>
        <td class="hide-sm">${esc(r.name ?? '')}</td>
        <td class="hide-sm"><span class="badge">${esc(r.sector ?? '—')}</span></td>
        <td class="num">${n2(r.price,2)}</td>
        <td class="num ${d1>=0?'pos':'neg'}">${pct(r.ret1d)}</td>
        <td class="num hide-sm ${(+r.ret5d||0)>=0?'pos':'neg'}">${pct(r.ret5d)}</td>
        <td class="num">${n2(r.rsi_ui,1)}</td>
        <td class="num">${n2(r.sharpe_ui,3)}</td>
        <td class="num">${n2(r.vol_z,2)}</td>
        <td class="num">${r.iv30==null?'—':pct(r.iv30)}</td>
        <td class="alerts-cell">${alertHTML}${(r.alerts?.length||0)>3?` <span class="more">+${r.alerts.length-3}</span>`:''}</td>
        <td class="num">${n2(r.iv_rank_ui,2)}</td>
        <td class="num">${n2(r.iv_pct_ui,2)}</td>
        <td class="num hide-sm">${n2(r.pe_ttm,2)}</td>
        <td class="num hide-sm">${n2(r.pb,2)}</td>
        <td class="num hide-sm">${pct(r.div_yield!=null ? r.div_yield/100.0 : null)}</td>
        <td class="num hide-sm">${mcap(r.mcap)}</td>
        <td class="num hide-sm">${n2(r.beta,2)}</td>
        <td class="num">${sparkSVG(r.spark30, 140, 36, 4)}</td>
      `;
      frag.appendChild(tr);

      if (isOpen){
        const tr2 = document.createElement('tr');
        tr2.className = 'expander';
        tr2.innerHTML = `<td colspan="20"><div class="panel-wrap">${detailPanel(r)}</div></td>`;
        frag.appendChild(tr2);
      }
    });

    tb.appendChild(frag);

    tb.querySelectorAll('.chev').forEach(el => {
      el.onclick = () => {
        const s = el.getAttribute('data-sym');
        if (state.expanded.has(s)) state.expanded.delete(s);
        else state.expanded.add(s);
        render();
      };
    });

    updateStatus();
  }

  async function load(){
    const ts = bust();
    // snapshot
    const snapRes = await fetch(`data/snapshot.json?${ts}`, {cache:'no-store'});
    const js = await snapRes.json();
    state.asOf = js.as_of_utc || '';
    state.interval = js.interval || '';
    state.period = js.period || '';
    state.rf = Number(js.risk_free || 0);

    state.rows = (js.data||[]).map(x => ({
      symbol:x.symbol, name:x.name??x.symbol, sector:x.sector??'—',
      price:+x.price, ret1d:x.ret1d, ret5d:x.ret5d, rsi14:x.rsi14, sharpe:x.sharpe,
      vol_z:x.vol_z, iv30:x.iv30, iv_rank:x.iv_rank, iv_percentile:x.iv_percentile,
      mcap:x.mcap, pe_ttm:x.pe_ttm, pb:x.pb, div_yield:x.div_yield, beta:x.beta,
      news_24h:x.news_24h, spark30:(Array.isArray(x.spark30)?x.spark30:null),
      hist:(x.hist||null), alerts:(Array.isArray(x.alerts)?x.alerts:[]),
      // UI-calculated
      rsi_ui:null, sharpe_ui:null, iv_rank_ui:null, iv_pct_ui:null, alertCount:0
    }));

    // sectors
    const sel = $('sector');
    const set = new Set(state.rows.map(r=>r.sector).filter(Boolean));
    sel.innerHTML = '<option value="">All sectors</option>';
    [...set].sort().forEach(s => { const o=document.createElement('option'); o.value=s; o.textContent=s; sel.appendChild(o); });

    // iv history
    try{
      const ivRes = await fetch(`data/iv_history.json?${ts}`, {cache:'no-store'});
      if (ivRes.ok){
        state.ivHist = await ivRes.json();
      }
    }catch(_){ state.ivHist = {}; }

    applyFilters(); render();
  }

  window.addEventListener('DOMContentLoaded', () => {
    $('q').addEventListener('input', () => { applyFilters(); render(); });
    $('sector').addEventListener('change', () => { applyFilters(); render(); });
    $('alertFilter').addEventListener('change', () => { applyFilters(); render(); });
    $('sort').addEventListener('change', () => { applyFilters(); render(); });
    $('dir').addEventListener('change', () => { applyFilters(); render(); });

    $('rsiWin').addEventListener('change', () => { state.ui.rsiWin = +$('rsiWin').value; applyFilters(); render(); });
    $('sharpeWin').addEventListener('change', () => { state.ui.sharpeWin = +$('sharpeWin').value; applyFilters(); render(); });
    $('ivWin').addEventListener('change', () => { state.ui.ivWin = +$('ivWin').value; applyFilters(); render(); });

    $('themeBtn').addEventListener('click', () => {
      const root = document.documentElement;
      const cur = root.getAttribute('data-theme') || 'dark';
      root.setAttribute('data-theme', cur==='dark' ? 'light' : 'dark');
    });
    $('scrollLeft').addEventListener('click', () => $('tableWrap').scrollBy({left:-300,behavior:'smooth'}));
    $('scrollRight').addEventListener('click', () => $('tableWrap').scrollBy({left:300,behavior:'smooth'}));

    load();
  });
})();
