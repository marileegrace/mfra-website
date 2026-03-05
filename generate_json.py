#!/usr/bin/env python3
"""MFRA Scanner — JSON output for website (replaces PDF generation)."""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.config import WATCHLIST_PATH, CACHE_DIR
from src.data_fetcher import fetch_prices, check_market_open
from src.mfra_engine import compute_mfra
from src.entry_criteria import evaluate


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def load_watchlist():
    with open(WATCHLIST_PATH) as f:
        data = json.load(f)
    return data["stocks"], data["market_etf"]


def collect_all_tickers(stocks, market_etf):
    tickers = set()
    tickers.add(market_etf)
    for s in stocks:
        tickers.add(s["ticker"])
        tickers.add(s["sector_etf"])
        tickers.add(s["sub_sector_etf"])
    return sorted(tickers)


def _native(v):
    """Convert numpy types to native Python types for JSON serialization."""
    if hasattr(v, 'item'):
        return v.item()
    return v


def scan_result_to_dict(r):
    """Convert a ScanResult to a JSON-serializable dict."""
    c = r.contributions_10d
    d = r.details
    return {
        "ticker": r.ticker,
        "sector_etf": r.sector_etf,
        "sub_sector_etf": r.sub_sector_etf,
        "quality": r.quality,
        "r_squared": round(float(r.r_squared), 4),
        "betas": {
            "mkt": round(float(r.betas.get("mkt", 0)), 3),
            "sec": round(float(r.betas.get("sec", 0)), 3),
            "sub": round(float(r.betas.get("sub", 0)), 3),
        },
        "contributions_10d": {
            "mkt": round(float(c.get("mkt", 0)), 3),
            "sec": round(float(c.get("sec", 0)), 3),
            "sub": round(float(c.get("sub", 0)), 3),
            "resid": round(float(c.get("resid", 0)), 3),
        },
        "criteria": {
            "purple_positive_count": int(d.get("purple_positive_count", 0)),
            "purple_dominance": round(float(d.get("purple_dominance", 0)), 4),
            "has_tailwind": bool(d.get("has_tailwind", False)),
            "cyan_share": round(float(d.get("cyan_share", 0)), 4),
        },
        "failed_rules": r.failed_rules,
    }


def main():
    setup_logging()
    logger = logging.getLogger("mfra_json")
    logger.info("MFRA JSON generator starting")

    os.makedirs(CACHE_DIR, exist_ok=True)

    stocks, market_etf = load_watchlist()
    all_tickers = collect_all_tickers(stocks, market_etf)
    logger.info("Fetching prices for %d tickers", len(all_tickers))

    prices = fetch_prices(all_tickers, use_cache=True)
    logger.info("Got prices for %d/%d tickers", len(prices), len(all_tickers))

    if not check_market_open(prices):
        logger.info("Market appears closed. Skipping scan.")
        return

    opportunities = []
    near_misses = []
    skipped = []

    for stock_info in stocks:
        ticker = stock_info["ticker"]
        sec_etf = stock_info["sector_etf"]
        sub_etf = stock_info["sub_sector_etf"]

        missing = [t for t in [ticker, market_etf, sec_etf, sub_etf] if t not in prices]
        if missing:
            logger.warning("Skipping %s — missing: %s", ticker, ", ".join(missing))
            skipped.append(ticker)
            continue

        mfra_result = compute_mfra(
            prices[ticker], prices[market_etf], prices[sec_etf], prices[sub_etf], ticker
        )

        if not mfra_result.success:
            logger.warning("MFRA failed for %s: %s", ticker, mfra_result.error)
            skipped.append(ticker)
            continue

        scan_result = evaluate(mfra_result)
        scan_result.sector_etf = sec_etf
        scan_result.sub_sector_etf = sub_etf

        if scan_result.passed:
            opportunities.append(scan_result)
            logger.info("PASS: %s (%s)", ticker, scan_result.quality)
        elif len(scan_result.failed_rules) == 1:
            near_misses.append(scan_result)
            logger.info("NEAR MISS: %s (failed: %s)", ticker, scan_result.failed_rules[0])

    # Sort: Best first, then by purple descending
    opportunities.sort(
        key=lambda r: (0 if r.quality == "Best" else 1, -r.contributions_10d.get("resid", 0))
    )
    near_misses.sort(key=lambda r: r.ticker)

    scan_date = datetime.now().strftime("%Y-%m-%d")
    total_scanned = len(stocks) - len(skipped)

    output = {
        "meta": {
            "scan_date": scan_date,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M ET"),
            "total_scanned": total_scanned,
            "opportunity_count": len(opportunities),
            "near_miss_count": len(near_misses),
            "skipped_count": len(skipped),
            "skipped_tickers": skipped,
        },
        "opportunities": [scan_result_to_dict(r) for r in opportunities],
        "near_misses": [scan_result_to_dict(r) for r in near_misses],
    }

    # Write JSON
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    os.makedirs(data_dir, exist_ok=True)
    out_path = os.path.join(data_dir, "scan_results.json")

    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    logger.info(
        "Done: %d scanned, %d opportunities, %d near misses → %s",
        total_scanned, len(opportunities), len(near_misses), out_path,
    )


if __name__ == "__main__":
    main()
