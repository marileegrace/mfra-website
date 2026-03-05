"""MFRA math engine — replicates the Pine Script Multi-Factor Return Attribution."""
import logging
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from config.config import OLS_WINDOW, ROLLING_SUM_WINDOW

logger = logging.getLogger(__name__)


@dataclass
class MFRAResult:
    ticker: str
    success: bool = False
    error: str = ""
    r_squared: float = 0.0
    r_squared_series: pd.Series = field(default_factory=lambda: pd.Series(dtype=float))
    betas: dict = field(default_factory=dict)        # mkt, sec, sub
    contributions_10d: dict = field(default_factory=dict)  # mkt, sec, sub, resid
    daily_residuals_10d: list = field(default_factory=list)
    bars_10d: dict = field(default_factory=dict)     # rolling sum series (last 10 values)


def _compute_returns(prices: pd.Series) -> pd.Series:
    """Simple daily percentage returns."""
    return prices.pct_change().dropna()


def _orthogonalize(mkt_ret: pd.Series, sec_ret: pd.Series, sub_ret: pd.Series):
    """
    Gram-Schmidt style orthogonalization matching Pine Script:
    f_mkt = mkt_ret
    f_sec = sec_ret - mkt_ret
    f_sub = sub_ret - sec_ret
    """
    f_mkt = mkt_ret
    f_sec = sec_ret - mkt_ret
    f_sub = sub_ret - sec_ret
    return f_mkt, f_sec, f_sub


def _rolling_ols(y: np.ndarray, X: np.ndarray, window: int):
    """
    Rolling OLS regression.
    Returns arrays of betas, r_squared for each valid window.
    X should include intercept column.
    """
    n = len(y)
    n_params = X.shape[1]
    betas = np.full((n, n_params), np.nan)
    r_squared = np.full(n, np.nan)

    for i in range(window - 1, n):
        y_w = y[i - window + 1:i + 1]
        X_w = X[i - window + 1:i + 1]

        try:
            # Normal equations: (X'X) * beta = X'y
            XtX = X_w.T @ X_w
            Xty = X_w.T @ y_w
            b = np.linalg.solve(XtX, Xty)
            betas[i] = b

            # R-squared
            y_hat = X_w @ b
            ss_res = np.sum((y_w - y_hat) ** 2)
            ss_tot = np.sum((y_w - np.mean(y_w)) ** 2)
            if ss_tot > 1e-20:
                r_squared[i] = 1.0 - ss_res / ss_tot
            else:
                r_squared[i] = 0.0
        except np.linalg.LinAlgError:
            # Singular matrix — skip this window
            continue

    return betas, r_squared


def compute_mfra(
    stock_prices: pd.Series,
    mkt_prices: pd.Series,
    sec_prices: pd.Series,
    sub_prices: pd.Series,
    ticker: str,
) -> MFRAResult:
    """
    Run the full MFRA decomposition for one stock.
    Returns MFRAResult with latest values and rolling series.
    """
    result = MFRAResult(ticker=ticker)

    try:
        # Compute returns
        stock_ret = _compute_returns(stock_prices)
        mkt_ret = _compute_returns(mkt_prices)
        sec_ret = _compute_returns(sec_prices)
        sub_ret = _compute_returns(sub_prices)

        # Align all series on common dates
        common_idx = stock_ret.index.intersection(mkt_ret.index)\
                                     .intersection(sec_ret.index)\
                                     .intersection(sub_ret.index)
        if len(common_idx) < OLS_WINDOW + ROLLING_SUM_WINDOW:
            result.error = f"Insufficient data: {len(common_idx)} days (need {OLS_WINDOW + ROLLING_SUM_WINDOW})"
            return result

        stock_ret = stock_ret.loc[common_idx]
        mkt_ret = mkt_ret.loc[common_idx]
        sec_ret = sec_ret.loc[common_idx]
        sub_ret = sub_ret.loc[common_idx]

        # Orthogonalize factors
        f_mkt, f_sec, f_sub = _orthogonalize(mkt_ret, sec_ret, sub_ret)

        # Check if sub-sector degenerates (same ETF as sector)
        sub_is_degenerate = (f_sub.abs().sum() < 1e-12)

        n = len(common_idx)
        y = stock_ret.values

        if sub_is_degenerate:
            # 2-factor model: [intercept, f_mkt, f_sec]
            X = np.column_stack([np.ones(n), f_mkt.values, f_sec.values])
            betas_arr, r2_arr = _rolling_ols(y, X, OLS_WINDOW)
            mkt_contrib = betas_arr[:, 1] * f_mkt.values * 100.0
            sec_contrib = betas_arr[:, 2] * f_sec.values * 100.0
            sub_contrib = np.zeros(n)
        else:
            # Full 3-factor model: [intercept, f_mkt, f_sec, f_sub]
            X = np.column_stack([np.ones(n), f_mkt.values, f_sec.values, f_sub.values])
            betas_arr, r2_arr = _rolling_ols(y, X, OLS_WINDOW)
            mkt_contrib = betas_arr[:, 1] * f_mkt.values * 100.0
            sec_contrib = betas_arr[:, 2] * f_sec.values * 100.0
            sub_contrib = betas_arr[:, 3] * f_sub.values * 100.0

        total_daily = stock_ret.values * 100.0
        resid_contrib = total_daily - mkt_contrib - sec_contrib - sub_contrib

        # Build Series with DatetimeIndex
        idx = common_idx
        mkt_s = pd.Series(mkt_contrib, index=idx, name="mkt")
        sec_s = pd.Series(sec_contrib, index=idx, name="sec")
        sub_s = pd.Series(sub_contrib, index=idx, name="sub")
        resid_s = pd.Series(resid_contrib, index=idx, name="resid")
        r2_s = pd.Series(r2_arr, index=idx, name="r2")

        # Rolling sums (the "bars")
        mkt_roll = mkt_s.rolling(ROLLING_SUM_WINDOW).sum()
        sec_roll = sec_s.rolling(ROLLING_SUM_WINDOW).sum()
        sub_roll = sub_s.rolling(ROLLING_SUM_WINDOW).sum()
        resid_roll = resid_s.rolling(ROLLING_SUM_WINDOW).sum()

        # Latest valid values
        last_valid = r2_s.last_valid_index()
        if last_valid is None:
            result.error = "No valid OLS results"
            return result

        loc = idx.get_loc(last_valid)

        result.r_squared = r2_arr[loc]
        result.r_squared_series = r2_s.dropna()
        result.betas = {
            "mkt": betas_arr[loc, 1],
            "sec": betas_arr[loc, 2],
            "sub": betas_arr[loc, 3] if not sub_is_degenerate else 0.0,
        }
        result.contributions_10d = {
            "mkt": mkt_roll.iloc[loc] if not np.isnan(mkt_roll.iloc[loc]) else 0.0,
            "sec": sec_roll.iloc[loc] if not np.isnan(sec_roll.iloc[loc]) else 0.0,
            "sub": sub_roll.iloc[loc] if not np.isnan(sub_roll.iloc[loc]) else 0.0,
            "resid": resid_roll.iloc[loc] if not np.isnan(resid_roll.iloc[loc]) else 0.0,
        }

        # Last 10 individual daily residual contributions
        start_i = max(0, loc - ROLLING_SUM_WINDOW + 1)
        result.daily_residuals_10d = resid_s.iloc[start_i:loc + 1].tolist()

        # Last 10 bars for each component
        result.bars_10d = {
            "mkt": mkt_s.iloc[start_i:loc + 1].tolist(),
            "sec": sec_s.iloc[start_i:loc + 1].tolist(),
            "sub": sub_s.iloc[start_i:loc + 1].tolist(),
            "resid": resid_s.iloc[start_i:loc + 1].tolist(),
        }

        result.success = True

    except Exception as e:
        result.error = str(e)
        logger.exception("MFRA computation failed for %s", ticker)

    return result
