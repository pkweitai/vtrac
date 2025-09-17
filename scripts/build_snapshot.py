import os, json, time, math, requests, pandas as pd, numpy as np
from datetime import datetime, timedelta, timezone
import pandas_ta as ta
import yfinance as yf

OUT = "docs/data/snapshot.json"

def get_sp500():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    tbl = pd.read_html(url)[0]
    df = tbl.rename(columns={"Symbol":"symbol","Security":"name","GICS Sector":"sector"})
    df["symbol"] = df["symbol"].str.replace(".", "-", regex=False)
    return df[["symbol","name","sector"]]

def indicators_for(symbol, days=120):
    try:
        df = yf.download(symbol, period=f"{days}d", interval="1d", auto_adjust=True, progress=False)
        if df is None or df.empty or len(df) < 60:
            return None
        df["rsi"] = ta.rsi(df["Close"], length=14)
        ret1d = df["Close"].pct_change(1)
        ret5d = df["Close"].pct_change(5)
        volz  = (np.log(df["Volume"]) - np.log(df["Volume"]).rolling(60).mean()) / np.log(df["Volume"]).rolling(60).std()
        spark_src = df["Close"].tail(30).to_list()
        spark_norm = None
        if len(spark_src) >= 2:
            mn, mx = min(spark_src), max(spark_src)
            if mx > mn:
                spark_norm = [round((v - mn) / (mx - mn), 4) for v in spark_src]
        last = df.iloc[-1]
        return {
            "price": round(float(last["Close"]), 4),
            "ret1d": round(float(ret1d.iloc[-1]), 5),
            "ret5d": round(float(ret5d.iloc[-1]), 5),
            "rsi14": None if math.isnan(last["rsi"]) else round(float(last["rsi"]), 2),
            "vol_z": None if math.isnan(volz.iloc[-1]) else round(float(volz.iloc[-1]), 2),
            "spark30": spark_norm
        }
    except Exception:
        return None

def fetch_news_count(symbol, company, key):
    if not key: return None
    try:
        from_dt = (datetime.utcnow() - timedelta(days=1)).isoformat(timespec="seconds")+"Z"
        url = "https://newsapi.org/v2/everything"
        q = f'"{company}" OR {symbol}'
        r = requests.get(url, params={"q":q, "from":from_dt, "language":"en", "sortBy":"publishedAt", "pageSize":100}, headers={"X-Api-Key":key}, timeout=15)
        data = r.json()
        return min(int(data.get("totalResults", 0)), 100)
    except Exception:
        return None

def fetch_iv_fields(symbol, price):
    iv30 = iv_rank = iv_percentile = None
    ORATS_KEY = os.getenv("ORATS_API_KEY", "")
    if ORATS_KEY:
        # Implement your ORATS fetch here
        pass
    POLY = os.getenv("POLYGON_API_KEY", "")
    if (not ORATS_KEY) and POLY:
        # Implement your Polygon options fetch & ATM IV calc here
        pass
    return {"iv30":iv30, "iv_rank":iv_rank, "iv_percentile":iv_percentile}

def main():
    base = get_sp500()
    rows = []
    NEWS_KEY = os.getenv("NEWSAPI_KEY","")
    for _, row in base.iterrows():
        sym, name, sector = row["symbol"], row["name"], row["sector"]
        feat = indicators_for(sym)
        if not feat: 
            continue
        ivf = fetch_iv_fields(sym, feat["price"])
        news = fetch_news_count(sym, name, NEWS_KEY)
        rows.append({
            "symbol": sym,
            "name": name,
            "sector": sector,
            "price": feat["price"],
            "ret1d": feat["ret1d"],
            "ret5d": feat["ret5d"],
            "rsi14": feat["rsi14"],
            "vol_z": feat["vol_z"],
            "iv30": ivf["iv30"],
            "iv_rank": ivf["iv_rank"],
            "iv_percentile": ivf["iv_percentile"],
            "news_24h": news,
            "spark30": feat["spark30"]
        })
        time.sleep(0.1)
    out = {
        "as_of_utc": datetime.utcnow().isoformat(timespec="seconds")+"Z",
        "count": len(rows),
        "data": rows
    }
    os.makedirs("docs/data", exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(out, f, separators=(",",":"))
    print(f"wrote {OUT} with {len(rows)} rows")

if __name__ == "__main__":
    main()
