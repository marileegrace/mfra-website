"""Data fetching with yfinance, daily cache, and batch throttling."""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import yfinance as yf
import pandas as pd

from config.config import (
    BATCH_SIZE, BATCH_PAUSE_SECONDS, CACHE_DIR, CACHE_MAX_AGE_DAYS,
    LOOKBACK_CALENDAR_DAYS, MAX_RETRIES,
)

logger = logging.getLogger(__name__)


def _cache_path(ticker: str, date_str: str) -> str:
    return os.path.join(CACHE_DIR, f"{date_str}_{ticker}.json")


def _cleanup_old_cache():
    """Remove cache files older than CACHE_MAX_AGE_DAYS."""
    cutoff = datetime.now() - timedelta(days=CACHE_MAX_AGE_DAYS)
    cache = Path(CACHE_DIR)
    if not cache.exists():
        return
    for f in cache.glob("*.json"):
        if datetime.fromtimestamp(f.stat().st_mtime) < cutoff:
            f.unlink()
            logger.debug("Removed old cache file: %s", f.name)


def _load_from_cache(ticker: str, date_str: str) -> Optional[pd.Series]:
    """Load cached close prices for a ticker."""
    path = _cache_path(ticker, date_str)
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            data = json.load(f)
        series = pd.Series(data["close"], index=pd.DatetimeIndex(data["dates"]), name=ticker)
        return series
    except Exception:
        return None


def _save_to_cache(ticker: str, date_str: str, series: pd.Series):
    """Save close prices to cache."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    data = {
        "dates": [d.isoformat() for d in series.index],
        "close": series.tolist(),
    }
    with open(_cache_path(ticker, date_str), "w") as f:
        json.dump(data, f)


def fetch_prices(tickers: list[str], use_cache: bool = True) -> dict[str, pd.Series]:
    """
    Fetch adjusted close prices for all tickers.
    Returns dict of ticker -> pd.Series of close prices.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=LOOKBACK_CALENDAR_DAYS)).strftime("%Y-%m-%d")
    results = {}
    to_fetch = []

    _cleanup_old_cache()

    # Check cache first
    if use_cache:
        for t in tickers:
            cached = _load_from_cache(t, today)
            if cached is not None and len(cached) > 0:
                results[t] = cached
                logger.debug("Cache hit: %s", t)
            else:
                to_fetch.append(t)
    else:
        to_fetch = list(tickers)

    if not to_fetch:
        return results

    # Batch download
    for i in range(0, len(to_fetch), BATCH_SIZE):
        batch = to_fetch[i:i + BATCH_SIZE]
        logger.info("Fetching batch %d-%d: %s", i + 1, i + len(batch), ", ".join(batch))

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                df = yf.download(
                    batch,
                    start=start_date,
                    end=today,
                    auto_adjust=True,
                    progress=False,
                    threads=True,
                )
                break
            except Exception as e:
                logger.warning("Batch download attempt %d failed: %s", attempt, e)
                if attempt < MAX_RETRIES:
                    time.sleep(BATCH_PAUSE_SECONDS)
                else:
                    logger.error("Batch download failed after %d retries", MAX_RETRIES)
                    df = pd.DataFrame()

        if df.empty:
            for t in batch:
                logger.warning("No data returned for %s", t)
            continue

        # Extract close prices per ticker
        if len(batch) == 1:
            # Single ticker: df["Close"] is a Series
            t = batch[0]
            close = df["Close"].dropna()
            if len(close) > 0:
                results[t] = close.rename(t)
                _save_to_cache(t, today, results[t])
            else:
                logger.warning("No close data for %s", t)
        else:
            # Multiple tickers: df["Close"] is a DataFrame with ticker columns
            close_df = df["Close"]
            for t in batch:
                if t in close_df.columns:
                    series = close_df[t].dropna()
                    if len(series) > 0:
                        results[t] = series.rename(t)
                        _save_to_cache(t, today, results[t])
                    else:
                        logger.warning("No close data for %s", t)
                else:
                    logger.warning("Ticker %s not in download results", t)

        if i + BATCH_SIZE < len(to_fetch):
            time.sleep(BATCH_PAUSE_SECONDS)

    return results


def check_market_open(prices: dict[str, pd.Series]) -> bool:
    """Check if SPY has recent data (market was open)."""
    spy = prices.get("SPY")
    if spy is None or len(spy) == 0:
        return False
    last_date = spy.index[-1]
    # If last data point is more than 3 calendar days old, market likely closed
    return (datetime.now() - last_date.to_pydatetime().replace(tzinfo=None)).days <= 3
