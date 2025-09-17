const state = {
  all: [],
  filtered: [],
  sectors: new Set(),
  sortKey: 'surge',
  watchOnly: false,
  watch: new Set(JSON.parse(localStorage.getItem('watchlist') || '[]')),
};

const $ = s => document.querySelector(s);
const $$ = s => document.querySelectorAll(s);

function pct(x){ return (x==null||isNaN(x))? '—' : (x*100).toFixed(2)+'%'; }
function num(x, d=2){ return (x==null||isNaN(x))? '—' : Number(x).toFixed(d); }

function calcSurge(r){
  const ivz = r.iv_rank!=null ? r.iv_rank : 0;
  const volz = Math.max(r.vol_z ?? 0, 0);
  const news = Math.min(r.news_24h ?? 0, 20) / 10; // 0..2
  const ret = Math.max(Math.abs(r.ret1d ?? 0), Math.abs(r.ret5d ?? 0));
  const rsiKick = r.rsi14!=null ? Math.max(0, Math.abs((r.rsi14-50)/50))*0.5 : 0;
  return +(0.35*ivz + 0.25*ret*10 + 0.2*volz + 0.15*news + 0.05*rsiKick).toFixed(3);
}

function star(symbol){
  const on = state.watch.has(symbol);
  return `<span class="star ${on?'on':''}" data-sym="${symbol}" title="Toggle watchlist">★</span>`;
}

function sparkline(values){
  if(!values || values.length<2) return '';
  const w=120,h=28; const pts = values.map((v,i)=>[i*(w/(values.length-1)), (1-v)*(h-4)+2]);
  const d = pts.map((p,i)=> (i? 'L':'M')+p[0].toFixed(1)+','+p[1].toFixed(1)).join(' ');
  return `<svg class="spark" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">
    <path d="${d}" fill="none" stroke="var(--accent)" stroke-width="1.5"/>
  </svg>`;
}

function render(){
  const tbody = $('#rows'); tbody.innerHTML = '';
  let rows = [...state.all];

  const q = $('#q').value.trim().toLowerCase();
  if(q) rows = rows.filter(r => r.symbol.toLowerCase().includes(q) || r.name.toLowerCase().includes(q));

  const sec = $('#sector').value;
  if(sec) rows = rows.filter(r => r.sector===sec);

  if(state.watchOnly) rows = rows.filter(r => state.watch.has(r.symbol));

  rows.forEach(r => r.surge = calcSurge(r));
  const key = $('#sort').value;
  rows.sort((a,b) => {
    if(key==='symbol' || key==='sector' || key==='name') return String(a[key]).localeCompare(b[key]);
    const av = a[key]; const bv = b[key];
    return (bv??-Infinity) - (av??-Infinity);
  });

  const frag = document.createDocumentFragment();
  rows.forEach(r => {
    const tr = document.createElement('tr');
    const d1 = r.ret1d ?? 0;
    tr.innerHTML = `
      <td>${star(r.symbol)}</td>
      <td><a href="https://www.tradingview.com/symbols/${r.symbol}/" target="_blank">${r.symbol}</a></td>
      <td>${r.name}</td>
      <td><span class="badge">${r.sector}</span></td>
      <td class="num">${num(r.price,2)}</td>
      <td class="num delta ${d1>=0?'up':'down'}">${pct(r.ret1d)}</td>
      <td class="num ${ (r.ret5d??0)>=0?'delta up':'delta down' }">${pct(r.ret5d)}</td>
      <td class="num">${num(r.rsi14,1)}</td>
      <td class="num">${r.iv30==null?'—':pct(r.iv30)}</td>
      <td class="num">${num(r.vol_z,2)}</td>
      <td class="num">${r.news_24h ?? '—'}</td>
      <td>${sparkline(r.spark30)}</td>
    `;
    frag.appendChild(tr);
  });
  tbody.appendChild(frag);

  $$('.star').forEach(el => el.onclick = () => {
    const s = el.dataset.sym;
    if(state.watch.has(s)) state.watch.delete(s); else state.watch.add(s);
    localStorage.setItem('watchlist', JSON.stringify([...state.watch]));
    render();
  });
}

async function load(){
  const res = await fetch('data/snapshot.json', {cache:'no-store'});
  const js = await res.json();
  $('#asof').textContent = `As of ${js.as_of_utc}`;
  state.all = js.data;
  state.sectors = new Set(state.all.map(r=>r.sector).filter(Boolean));
  const sel = $('#sector'); state.sectors.forEach(s => {
    const opt = document.createElement('option'); opt.value=s; opt.textContent=s; sel.appendChild(opt);
  });
  render();
}

$('#q').oninput = render;
$('#sector').onchange = render;
$('#sort').onchange = render;
$('#watchOnly').onchange = (e)=>{ state.watchOnly = e.target.checked; render(); };
$('#themeBtn').onclick = ()=>{
  const cur = document.documentElement.getAttribute('data-theme');
  document.documentElement.setAttribute('data-theme', cur==='dark'?'light':'dark');
};

load();
