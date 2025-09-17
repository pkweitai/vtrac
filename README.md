# S&P 500 Radar

A GitHub Pages dashboard that tracks all S&P 500 stocks daily: returns, RSI, volume spikes, news bursts, optional IV metrics, and 30-day sparklines. Built by a scheduled GitHub Action that writes `/docs/data/snapshot.json`.

- Data: Yahoo Finance for OHLCV; optional IV from ORATS/Polygon; optional NewsAPI counts.
- Membership: pulled daily from Wikipedia to stay current.

## Setup

1. Enable **Pages**: Settings → Pages → Deploy from Branch → `main` + `/docs`.
2. Add secrets (optional): `POLYGON_API_KEY`, `ORATS_API_KEY`, `NEWSAPI_KEY`.
3. The workflow runs daily ~**7:30 AM America/Phoenix** (14:30 UTC) and commits `docs/data/snapshot.json`.
4. Visit your site: `https://<you>.github.io/sp500-radar/`.

## Notes
- S&P 500 membership updates periodically (pulled from Wikipedia).
- GitHub Actions schedule uses UTC.

_Not investment advice._
