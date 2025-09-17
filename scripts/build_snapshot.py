#!/usr/bin/env python3
"""
build_snapshot.py — Snapshot builder with:
  • Expanded universe (SP500/NAS100/DOW30/EXTRA)
  • Intraday support (YF_INTERVAL/YF_PERIOD)
  • Technicals (RSI, short momentum, volume z-score)
  • NEW: Sharpe ratio (annualized)
  • NEW: Fundamentals (market cap, P/E ttm, P/B, dividend yield, beta)
  • GitHub upload (with PR-branch fallback on 403)

ENV (optional)
-------------
UNIVERSE=SP500,NAS100,DOW30,EXTRA
UPDATE_SYMBOLS_FROM_WEB=0|1
YF_INTERVAL=1d               # e.g., 15m, 30m, 60m, 1d, 1wk...
YF_PERIOD=120d               # e.g., 30d, 60d, 6mo, 1y...
RISK_FREE=0.02               # annual risk-free for Sharpe (default 0.00)
NEWSAPI_KEY=...              # optional

# GitHub upload
GH_TOKEN=...
GH_REPO=owner/repo
GH_BRANCH=main
GH_PATH=docs/data/snapshot.json
GH_COMMITTER_NAME=sp500-bot
GH_COMMITTER_EMAIL=actions@users.noreply.github.com
"""

import os, io, sys, json, time, base64, math, argparse, logging
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import requests
import yfinance as yf
from ta.momentum import RSIIndicator
from ta.trend import MACD

# ───────────────────────── Logger ─────────────────────────
def setup_logger(verbosity:int):
    level = logging.WARNING
    if verbosity == 1: level = logging.INFO
    if verbosity >= 2: level = logging.DEBUG
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )

TRUE_SET = {"1","true","yes","on"}
HDR = {"User-Agent": "Mozilla/5.0", "Accept-Language": "en-US,en;q=0.9"}

# ───────────────────── Yahoo interval helpers ─────────────────────
SUPPORTED_INTERVALS = {"1m","2m","5m","15m","30m","60m","90m","1h","1d","5d","1wk","1mo","3mo"}

def norm_interval(iv: str) -> str:
    iv = (iv or "1d").lower().strip()
    if iv == "10m":
        logging.warning("Yahoo does not support 10m. Using 15m instead.")
        return "15m"
    if iv not in SUPPORTED_INTERVALS:
        logging.warning("Unsupported interval '%s'. Falling back to 1d.", iv)
        return "1d"
    return iv

YF_INTERVAL = norm_interval(os.getenv("YF_INTERVAL", "1d"))
YF_PERIOD   = os.getenv("YF_PERIOD", "120d")
RISK_FREE   = float(os.getenv("RISK_FREE", "0.00"))

# Annualization factors (approx; equities trading ~252 days/year, 6.5h/day)
def periods_per_year(interval: str, symbol: str) -> float:
    interval = interval.lower()
    # Crypto trades 24/7; use simple approximations for intraday
    is_crypto = symbol in {"BTC-USD","ETH-USD"}
    if interval in {"1d"}:            return 365.0 if is_crypto else 252.0
    if interval in {"1wk"}:           return 52.0
    if interval in {"1mo","3mo"}:     return 12.0 if interval=="1mo" else 4.0
    if interval in {"1h","60m"}:      return (365*24) if is_crypto else (252*6.5)
    if interval in {"90m"}:           return (365*16) if is_crypto else (252*4.33)
    if interval in {"30m"}:           return (365*48) if is_crypto else (252*13)
    if interval in {"15m"}:           return (365*96) if is_crypto else (252*26)
    if interval in {"5m"}:            return (365*288) if is_crypto else (252*78)
    if interval in {"2m"}:            return (365*720) if is_crypto else (252*195)
    if interval in {"1m"}:            return (365*1440) if is_crypto else (252*390)
    return 252.0

# ───────────────────── Offline seeds (short) ─────────────────────
SP500_SEED = [
    "AAPL","MSFT","NVDA","AMZN","META","GOOGL","GOOG","AVGO","BRK-B","LLY","JPM","TSLA","V","WMT","XOM",
    "PG","UNH","MA","COST","ORCL","HD","JNJ","MRK","BAC","ADBE","PEP","KO","CSCO","CVX","CRM",
]
NAS100_SEED = [
    "AAPL","MSFT","NVDA","AMZN","META","GOOGL","GOOG","AVGO","ADBE","PEP","CSCO","NFLX","AMD","INTC","QCOM",
    "TXN","AMAT","PDD","BKNG","KDP","SBUX","PYPL","ADI","MDLZ","LRCX","MRVL","REGN","VRTX","CSGP","CRWD",
]
DOW30_SEED = [
    "AAPL","MSFT","NKE","V","WMT","JPM","DIS","HD","KO","PG","CRM","INTC","AMGN","CAT","MCD","TRV","HON",
    "CSCO","IBM","MRK","JNJ","MMM","AXP","BA","CVX","GS","UNH","WBA","DOW","RTX",
]
EXTRA_SYMBOLS = ["^VIX","BTC-USD","ETH-USD"]

def dedup_symbols(symbols):
    seen, out = set(), []
    for s in symbols:
        s = (s or "").strip().upper().replace(".", "-")
        if s and s not in seen:
            seen.add(s); out.append(s)
    return out

def _from_wiki(url: str, symbol_like=("symbol","ticker")):
    r = requests.get(url, headers=HDR, timeout=30)
    r.raise_for_status()
    tables = pd.read_html(io.StringIO(r.text), flavor="lxml")
    for df in tables:
        cols = {str(c).lower(): c for c in df.columns}
        for key in symbol_like:
            if key in cols:
                col = cols[key]
                syms = df[col].astype(str).str.replace(".", "-", regex=False).str.strip().tolist()
                syms = [s for s in syms if s and s.upper() != "N/A"]
                if len(syms) >= 25:
                    return dedup_symbols(syms)
    return []

def get_sp500_web():
    return _from_wiki("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", ("symbol","ticker"))

def get_nas100_web():
    return _from_wiki("https://en.wikipedia.org/wiki/NASDAQ-100", ("ticker","symbol"))

def get_dow30_web():
    return _from_wiki("https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average", ("symbol","ticker"))

def get_universe(update_from_web: bool, sources: list[str]):
    universe = []
    sources = [s.strip().upper() for s in sources if s.strip()]
    logging.info("Universe sources: %s", ",".join(sources))

    if update_from_web:
        try:
            if "SP500" in sources:
                syms = get_sp500_web(); universe += syms if syms else SP500_SEED
                logging.info("SP500: %d symbols.", len(syms) if syms else len(SP500_SEED))
        except Exception as e:
            logging.warning("SP500 web failed: %s", e); universe += SP500_SEED
        try:
            if "NAS100" in sources:
                syms = get_nas100_web(); universe += syms if syms else NAS100_SEED
                logging.info("NAS100: %d symbols.", len(syms) if syms else len(NAS100_SEED))
        except Exception as e:
            logging.warning("NAS100 web failed: %s", e); universe += NAS100_SEED
        try:
            if "DOW30" in sources:
                syms = get_dow30_web(); universe += syms if syms else DOW30_SEED
                logging.info("DOW30: %d symbols.", len(syms) if syms else len(DOW30_SEED))
        except Exception as e:
            logging.warning("DOW30 web failed: %s", e); universe += DOW30_SEED
    else:
        if "SP500" in sources: universe += SP500_SEED
        if "NAS100" in sources: universe += NAS100_SEED
        if "DOW30" in sources: universe += DOW30_SEED

    if "EXTRA" in sources: universe += EXTRA_SYMBOLS
    universe = dedup_symbols(universe)
    logging.info("Universe size after dedupe: %d", len(universe))
    return universe

# ───────────── Yahoo fetch (flat frame + query2 fallback) ─────────────
def _chart_to_df(j):
    r = j.get("chart",{}).get("result",[None])[0]
    if not r: return pd.DataFrame()
    ts = r.get("timestamp") or []
    qs = (r.get("indicators",{}) or {}).get("quote",[{}])
    ac = (r.get("indicators",{}) or {}).get("adjclose",[{}])
    q  = qs[0] if qs else {}
    if not ts or not q: return pd.DataFrame()
    idx = pd.to_datetime(ts, unit="s", utc=True).tz_convert(None)
    close = (ac[0].get("adjclose") if ac and ac[0].get("adjclose") is not None else q.get("close"))
    df = pd.DataFrame({
        "Open":   q.get("open"),
        "High":   q.get("high"),
        "Low":    q.get("low"),
        "Close":  close,
        "Volume": q.get("volume"),
    }, index=idx)
    return df.dropna(how="all")

def _fetch_chart(symbol, period: str, interval: str, endpoint="query2"):
    sess = requests.Session()
    sess.headers.update({"User-Agent":"Mozilla/5.0","Accept":"*/*","Accept-Language":"en-US,en;q=0.9"})
    url = f"https://{endpoint}.finance.yahoo.com/v8/finance/chart/{symbol}"
    params={"range": period, "interval": interval, "includeAdjustedClose":"true", "events":"div,splits,capitalGains"}
    r = sess.get(url, params=params, timeout=20)
    if r.status_code==200:
        return _chart_to_df(r.json())
    logging.debug("query2 %s %s → %s", symbol, interval, r.status_code)
    return pd.DataFrame()

def _flatten_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(-1)
    for c in ("Open","High","Low","Close","Adj Close","Volume"):
        if c in df.columns and isinstance(df[c], pd.DataFrame):
            df[c] = df[c].iloc[:,0]
    if "Adj Close" in df.columns and "Close" not in df.columns:
        df = df.rename(columns={"Adj Close":"Close"})
    keep = [c for c in ("Open","High","Low","Close","Volume") if c in df.columns]
    return df[keep].dropna(how="all")

def try_download(symbol, period: str, interval: str):
    df = yf.download(
        symbol,
        period=period,
        interval=interval,
        auto_adjust=True,
        group_by="column",
        multi_level_index=False,
        threads=False,
        progress=False,
    )
    if isinstance(df, pd.DataFrame) and not df.empty:
        df = _flatten_ohlc(df)
        if len(df) >= 2:
            return df

    df = _fetch_chart(symbol, period, interval, endpoint="query2")
    if not df.empty:
        return df

    if "-" in symbol:
        alt = symbol.replace("-", ".")
        df = _fetch_chart(alt, period, interval, endpoint="query2")
        if not df.empty:
            return df

    return pd.DataFrame()

# ───────────────────── Metrics ─────────────────────
def _series(df: pd.DataFrame, col: str) -> pd.Series:
    s = df[col] if col in df.columns else pd.Series(index=df.index, dtype=float)
    if isinstance(s, pd.DataFrame):
        s = s.iloc[:,0]
    s = pd.to_numeric(s, errors="coerce")
    return s

def sharpe_ratio(close: pd.Series, interval: str, symbol: str, risk_free_annual: float) -> float | None:
    try:
        rets = close.pct_change().dropna()
        if len(rets) < 30:
            return None
        ppy = periods_per_year(interval, symbol)
        rf_per = risk_free_annual / ppy
        excess = rets - rf_per
        mu = excess.mean()
        sig = excess.std(ddof=1)
        if not (np.isfinite(mu) and np.isfinite(sig)) or sig <= 0:
            return None
        return round(float((mu / sig) * math.sqrt(ppy)), 3)
    except Exception:
        return None

def fetch_fundamentals(symbol: str):
    """Lightweight fundamentals via yfinance; best-effort."""
    out = {"mcap": None, "pe_ttm": None, "pb": None, "div_yield": None, "beta": None}
    try:
        tk = yf.Ticker(symbol)
        # prefer fast_info; fallback to get_info/info
        finfo = getattr(tk, "fast_info", None)
        info = None
        try:
            info = tk.get_info() if hasattr(tk, "get_info") else tk.info
        except Exception:
            info = {}
        def pick(*keys):
            for k in keys:
                if isinstance(finfo, dict) and k in finfo and finfo[k] is not None:
                    return finfo[k]
                if info and k in info and info[k] is not None:
                    return info[k]
            return None

        out["mcap"]     = pick("market_cap","marketCap")
        out["pe_ttm"]   = pick("trailing_pe","trailingPE")
        out["pb"]       = pick("price_to_book","priceToBook")
        out["div_yield"]= pick("dividend_yield","dividendYield")  # often decimal (0.0123)
        out["beta"]     = pick("beta")
        # sanitize numbers
        for k in out:
            v = out[k]
            if v is None: continue
            try:
                out[k] = float(v)
            except Exception:
                out[k] = None
        return out
    except Exception:
        return out

def indicators_for(symbol, period: str, interval: str):
    try:
        df = try_download(symbol, period, interval)
        if df is None or df.empty or len(df) < 60:
            logging.debug("No/short data for %s (len=%s)", symbol, 0 if df is None else len(df))
            return None

        close = _series(df, "Close")
        volume= _series(df, "Volume")

        rsi = RSIIndicator(close=close, window=14).rsi()
        macd_ind = MACD(close=close)
        # macd_line   = macd_ind.macd(); macd_signal = macd_ind.macd_signal()  # (not stored; can add later)

        ret1 = close.pct_change(1)
        ret5 = close.pct_change(5)

        volz = None
        if volume.notna().sum() >= 60:
            logv = np.log(volume.replace(0, np.nan))
            volz = (logv - logv.rolling(60).mean()) / logv.rolling(60).std()

        spark_src = close.tail(30).dropna().to_list()
        spark_norm = None
        if len(spark_src) >= 2:
            mn, mx = float(min(spark_src)), float(max(spark_src))
            if mx > mn:
                spark_norm = [round((float(v) - mn) / (mx - mn), 4) for v in spark_src]

        rsi_last  = float(rsi.iloc[-1])  if np.isfinite(rsi.iloc[-1])  else None
        volz_last = float(volz.iloc[-1]) if isinstance(volz, pd.Series) and np.isfinite(volz.iloc[-1]) else None

        shp = sharpe_ratio(close, interval, symbol, RISK_FREE)

        return {
            "price": round(float(close.iloc[-1]), 4),
            "ret1d": round(float(ret1.iloc[-1]), 5) if np.isfinite(ret1.iloc[-1]) else None,
            "ret5d": round(float(ret5.iloc[-1]), 5) if np.isfinite(ret5.iloc[-1]) else None,
            "rsi14": None if rsi_last is None else round(rsi_last, 2),
            "vol_z": None if volz_last is None else round(volz_last, 2),
            "spark30": spark_norm,
            "sharpe": shp,
        }
    except Exception as e:
        logging.debug("Error indicators %s: %s", symbol, e)
        return None

def fetch_news_count(symbol, key):
    if not key:
        return None
    try:
        from_dt = (datetime.utcnow() - timedelta(days=1)).isoformat(timespec="seconds")+"Z"
        url = "https://newsapi.org/v2/everything"
        q = f'"{symbol}"'
        r = requests.get(
            url,
            params={"q": q, "from": from_dt, "language": "en", "sortBy": "publishedAt", "pageSize": 100},
            headers={"X-Api-Key": key},
            timeout=15
        )
        data = r.json()
        return min(int(data.get("totalResults", 0)), 100)
    except Exception as e:
        logging.debug("News fetch failed for %s: %s", symbol, e)
        return None

# ─────────────── GitHub upload (same pattern) ───────────────
def _gh(api, token, method="GET", **kwargs):
    h = {"Authorization": f"Bearer {token}", "Accept":"application/vnd.github+json"}
    return requests.request(method, api, headers=h, timeout=60, **kwargs)

def _gh_get_ref(repo, branch, token):
    api = f"https://api.github.com/repos/{repo}/git/refs/heads/{branch}"
    r = _gh(api, token)
    if r.status_code==200: return r.json()["object"]["sha"]
    raise RuntimeError(f"Cannot read ref {branch}: {r.status_code} {r.text}")

def _gh_create_ref(repo, new_branch, from_sha, token):
    api = f"https://api.github.com/repos/{repo}/git/refs"
    r = _gh(api, token, "POST", json={"ref": f"refs/heads/{new_branch}", "sha": from_sha})
    if r.status_code not in (200,201): raise RuntimeError(f"Create ref failed: {r.status_code} {r.text}")

def maybe_upload_to_github(local_path):
    token = os.getenv("GH_TOKEN"); repo = os.getenv("GH_REPO")
    if not token or not repo:
        logging.info("Upload: GH_TOKEN or GH_REPO not set; skipping."); return False, None
    branch = os.getenv("GH_BRANCH","main")
    dest   = os.getenv("GH_PATH","docs/data/snapshot.json")
    name   = os.getenv("GH_COMMITTER_NAME","sp500-bot")
    email  = os.getenv("GH_COMMITTER_EMAIL","actions@users.noreply.github.com")

    with open(local_path,"rb") as f:
        content_b64 = base64.b64encode(f.read()).decode("ascii")
    msg = f"snapshot {datetime.utcnow().isoformat(timespec='seconds')}Z"

    api_get = f"https://api.github.com/repos/{repo}/contents/{dest}"
    r = _gh(api_get, token, params={"ref": branch})
    sha = r.json().get("sha") if r.status_code==200 else None

    api_put = f"https://api.github.com/repos/{repo}/contents/{dest}"
    payload = {"message": msg, "content": content_b64, "branch": branch, "committer": {"name": name, "email": email}}
    if sha: payload["sha"] = sha
    r = _gh(api_put, token, "PUT", json=payload)
    if r.status_code in (200,201):
        commit_sha = (r.json().get("commit") or {}).get("sha")
        logging.info("Upload: wrote to %s:%s (commit %s).", repo, dest, commit_sha or "unknown")
        return True, commit_sha

    if r.status_code == 403:
        logging.warning("Upload: 403 on %s — falling back to PR branch.", branch)
        base_sha = _gh_get_ref(repo, branch, token)
        pr_branch = "bot-data"
        try: _gh_create_ref(repo, pr_branch, base_sha, token)
        except Exception: pass
        payload.pop("sha", None)
        payload["branch"] = pr_branch
        r2 = _gh(api_put, token, "PUT", json=payload)
        if r2.status_code in (200,201):
            commit_sha = (r2.json().get("commit") or {}).get("sha")
            logging.info("Upload: pushed to %s. PR → https://github.com/%s/compare/%s...%s",
                         pr_branch, repo, branch, pr_branch)
            return True, commit_sha
        raise RuntimeError(f"GitHub upload still failing [{r2.status_code}]: {r2.text}")

    raise RuntimeError(f"GitHub upload failed [{r.status_code}]: {r.text}")

# ───────────────────── Build + write ─────────────────────
def build_snapshot(symbols, news_key=None, period="120d", interval="1d", limit=None):
    if limit:
        symbols = symbols[:limit]
        logging.info("Limiting to first %d symbols.", limit)

    rows, n = [], len(symbols)
    t0 = time.time()
    logging.info("Start fetch (%s, %s): %d symbols.", period, interval, n)

    for i, sym in enumerate(symbols, start=1):
        t_sym = time.time()
        feat = indicators_for(sym, period, interval)
        if not feat:
            logging.warning("[%d/%d] %s: no data (skipped).", i, n, sym)
        else:
            news_ct = fetch_news_count(sym, news_key) if news_key else None
            # fundamentals (best-effort; skip for ^VIX & crypto)
            fund = {"mcap": None, "pe_ttm": None, "pb": None, "div_yield": None, "beta": None}
            if sym not in {"^VIX","BTC-USD","ETH-USD"}:
                fund = fetch_fundamentals(sym)

            rows.append({
                "symbol": sym, "name": sym, "sector": "—",
                "price": feat["price"], "ret1d": feat["ret1d"], "ret5d": feat["ret5d"],
                "rsi14": feat["rsi14"], "vol_z": feat["vol_z"], "sharpe": feat["sharpe"],
                "iv30": None, "iv_rank": None, "iv_percentile": None,  # placeholders; add later if needed
                "mcap": fund["mcap"], "pe_ttm": fund["pe_ttm"], "pb": fund["pb"],
                "div_yield": fund["div_yield"], "beta": fund["beta"],
                "news_24h": news_ct,
                "spark30": feat["spark30"],
            })
            logging.info("[%d/%d] %s: ok (%.2fs) price=%.4f rsi=%s volz=%s sharpe=%s",
                         i, n, sym, time.time()-t_sym, feat["price"], feat["rsi14"], feat["vol_z"], feat["sharpe"])

        if (i % 10 == 0) or (i == n):
            done = len(rows)
            elapsed = time.time() - t0
            avg = elapsed / max(i,1)
            eta = avg*(n - i)
            logging.info("Progress: %d/%d processed, %d kept | elapsed %.1fs, ETA %.1fs",
                         i, n, done, elapsed, max(0.0, eta))
        time.sleep(0.05)

    out = {"as_of_utc": datetime.utcnow().isoformat(timespec="seconds")+"Z",
           "interval": interval, "period": period,
           "count": len(rows), "data": rows}
    logging.info("Done build: %d rows in %.1fs.", len(rows), time.time()-t0)
    return out

def write_local_snapshot(snapshot, path="docs/data/snapshot.json"):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(snapshot, f, separators=(",",":"))
    logging.info("Saved → %s (%.1f KB)", path, os.path.getsize(path)/1024)
    return path

# ───────────────────────── CLI ─────────────────────────
def parse_args():
    ap = argparse.ArgumentParser(description="Build snapshot with techs, Sharpe, fundamentals.")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("-v","--verbose", action="count", default=0)
    ap.add_argument("--update-symbols-from-web", action="store_true",
        default=os.getenv("UPDATE_SYMBOLS_FROM_WEB","0").lower() in TRUE_SET)
    ap.add_argument("--universe", default=os.getenv("UNIVERSE","SP500,NAS100,DOW30,EXTRA"),
        help="Comma-separated: SP500,NAS100,DOW30,EXTRA")
    ap.add_argument("--interval", default=YF_INTERVAL)
    ap.add_argument("--period", default=YF_PERIOD)
    ap.add_argument("--output", default="docs/data/snapshot.json")
    return ap.parse_args()

def main():
    args = parse_args()
    setup_logger(args.verbose)

    interval = norm_interval(args.interval)
    period   = args.period

    logging.info("=== Snapshot builder ===")
    logging.info("Interval=%s Period=%s | RF=%s | update_symbols_from_web=%s | universe=%s",
                 interval, period, RISK_FREE, args.update_symbols_from_web, args.universe)

    sources = [s.strip() for s in args.universe.split(",") if s.strip()]
    symbols = get_universe(args.update_symbols_from_web, sources)
    logging.info("Universe: %d symbols.", len(symbols))

    news_key = os.getenv("NEWSAPI_KEY") or None
    logging.info("News: %s.", "enabled" if news_key else "disabled (no NEWSAPI_KEY)")

    snap = build_snapshot(symbols, news_key=news_key, period=period, interval=interval, limit=args.limit)
    local_path = write_local_snapshot(snap, path=args.output)

    if os.getenv("GH_TOKEN") and os.getenv("GH_REPO"):
        try:
            maybe_upload_to_github(local_path)
        except Exception as e:
            logging.error("Upload failed: %s", e)
    else:
        logging.info("Upload: skipped (GH_TOKEN or GH_REPO not set).")

    logging.info("All done. as_of=%s rows=%d", snap["as_of_utc"], snap["count"])

if __name__ == "__main__":
    main()
