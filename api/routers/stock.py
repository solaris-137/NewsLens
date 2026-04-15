import asyncio
from datetime import datetime

import yfinance as yf
from fastapi import APIRouter, HTTPException, Request

from cache import cache_get, cache_set
from rate_limit import limiter

router = APIRouter()


class MarketClosedError(Exception):
    pass


def fetch_aapl_history():
    ticker = yf.Ticker("AAPL")
    dataframe = ticker.history(period="1d", interval="30m")
    if dataframe.empty:
        raise MarketClosedError("AAPL history is empty")

    dataframe = dataframe.reset_index()
    datetime_column = "Datetime" if "Datetime" in dataframe.columns else "Date"
    return [
        {
            "timestamp": row[datetime_column].isoformat(),
            "open": round(float(row["Open"]), 4),
            "close": round(float(row["Close"]), 4),
            "high": round(float(row["High"]), 4),
            "low": round(float(row["Low"]), 4),
            "volume": int(row["Volume"]),
        }
        for _, row in dataframe.iterrows()
    ]


def fetch_aapl_current():
    ticker = yf.Ticker("AAPL")
    info = ticker.fast_info

    last_price = getattr(info, "last_price", None)
    previous_close = getattr(info, "previous_close", None)
    if isinstance(info, dict):
        last_price = last_price or info.get("lastPrice") or info.get("last_price")
        previous_close = previous_close or info.get("previousClose") or info.get(
            "previous_close"
        )

    if last_price is None or previous_close in (None, 0):
        raise ValueError("AAPL fast_info missing price fields")

    return {
        "price": round(float(last_price), 2),
        "previous_close": round(float(previous_close), 2),
        "change_pct": round((float(last_price) - float(previous_close)) / float(previous_close) * 100, 2),
        "timestamp": datetime.utcnow().isoformat(),
    }


@router.get("/api/stock/history")
@limiter.limit("100/minute")
async def get_stock_history(request: Request):
    cache_key = "stock:aapl:history"
    cached = await cache_get(cache_key)

    try:
        history = await asyncio.to_thread(fetch_aapl_history)
        payload = {"history": history, "market_closed": False, "cached": False}
        await cache_set(cache_key, payload, 300)
        return payload
    except MarketClosedError:
        if cached is not None:
            return {**cached, "market_closed": True, "cached": True}
        return {"history": [], "market_closed": True, "cached": False}
    except Exception as exc:
        if cached is not None:
            return {**cached, "cached": True}
        raise HTTPException(status_code=503, detail="Unable to fetch AAPL history") from exc


@router.get("/api/stock/current")
@limiter.limit("100/minute")
async def get_stock_current(request: Request):
    cache_key = "stock:aapl:current"
    cached = await cache_get(cache_key)

    try:
        payload = await asyncio.to_thread(fetch_aapl_current)
        await cache_set(cache_key, payload, 60)
        return payload
    except Exception as exc:
        if cached is not None:
            return cached
        raise HTTPException(status_code=503, detail="Unable to fetch AAPL current price") from exc
