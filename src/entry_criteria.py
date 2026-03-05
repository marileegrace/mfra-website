"""Apply the 5-rule entry criteria to MFRA results."""
from __future__ import annotations

import logging
from dataclasses import dataclass

from config.config import (
    R2_MIN, R2_MAX, PURPLE_POSITIVE_MIN_DAYS, PURPLE_DOMINANCE_MAX,
    CYAN_DOMINANCE_MAX, SECTOR_TAILWIND_TOLERANCE,
    BEST_R2_MIN, BEST_R2_MAX, BEST_PURPLE_MIN_DAYS,
)
from src.mfra_engine import MFRAResult

logger = logging.getLogger(__name__)


@dataclass
class ScanResult:
    ticker: str
    passed: bool = False
    quality: str = ""           # "Best" or "Acceptable"
    failed_rules: list = None   # List of rule names that failed
    r_squared: float = 0.0
    betas: dict = None
    contributions_10d: dict = None
    details: dict = None        # Extra info for the report
    sector_etf: str = ""
    sub_sector_etf: str = ""

    def __post_init__(self):
        if self.failed_rules is None:
            self.failed_rules = []
        if self.betas is None:
            self.betas = {}
        if self.contributions_10d is None:
            self.contributions_10d = {}
        if self.details is None:
            self.details = {}


def evaluate(mfra: MFRAResult) -> ScanResult:
    """Apply all 5 entry criteria rules. Returns ScanResult."""
    result = ScanResult(
        ticker=mfra.ticker,
        r_squared=mfra.r_squared,
        betas=mfra.betas,
        contributions_10d=mfra.contributions_10d,
    )

    if not mfra.success:
        result.failed_rules = ["data_error"]
        result.details["error"] = mfra.error
        return result

    residuals = mfra.daily_residuals_10d
    contribs = mfra.contributions_10d

    # --- Rule 1: R² range ---
    if not (R2_MIN <= mfra.r_squared <= R2_MAX):
        result.failed_rules.append("r_squared")
        result.details["r2_note"] = f"R²={mfra.r_squared:.3f} outside [{R2_MIN}, {R2_MAX}]"

    # --- Rule 2: Sustained purple momentum ---
    purple_positive_count = sum(1 for r in residuals if r > 0)
    result.details["purple_positive_count"] = purple_positive_count
    if purple_positive_count < PURPLE_POSITIVE_MIN_DAYS:
        result.failed_rules.append("purple_sustained")
        result.details["purple_note"] = (
            f"Purple positive {purple_positive_count}/{len(residuals)} "
            f"(need {PURPLE_POSITIVE_MIN_DAYS})"
        )

    # --- Rule 3: No single purple spike dominates ---
    positive_residuals = [r for r in residuals if r > 0]
    if positive_residuals:
        total_positive_purple = sum(positive_residuals)
        max_single = max(abs(r) for r in residuals)
        if total_positive_purple > 0:
            dominance = max_single / total_positive_purple
            result.details["purple_dominance"] = dominance
            if dominance > PURPLE_DOMINANCE_MAX:
                result.failed_rules.append("purple_spike")
                result.details["spike_note"] = (
                    f"Single bar dominance {dominance:.1%} > {PURPLE_DOMINANCE_MAX:.0%}"
                )
    else:
        result.details["purple_dominance"] = 0.0

    # --- Rule 4: Sector tailwind ---
    orange_10d = contribs.get("sec", 0.0)
    green_10d = contribs.get("sub", 0.0)
    result.details["has_tailwind"] = (
        orange_10d >= SECTOR_TAILWIND_TOLERANCE
        or green_10d >= SECTOR_TAILWIND_TOLERANCE
    )
    if not result.details["has_tailwind"]:
        result.failed_rules.append("sector_tailwind")
        result.details["sector_note"] = (
            f"Orange={orange_10d:+.2f}, Green={green_10d:+.2f} -- both negative"
        )

    # --- Rule 5: Cyan doesn't dominate ---
    mkt_10d = contribs.get("mkt", 0.0)
    # Total positive contributions across all components
    all_contribs = [contribs.get(k, 0.0) for k in ("mkt", "sec", "sub", "resid")]
    total_positive = sum(c for c in all_contribs if c > 0)
    cyan_share = (mkt_10d / total_positive) if (total_positive > 0 and mkt_10d > 0) else 0.0
    result.details["cyan_share"] = cyan_share
    if total_positive > 0 and mkt_10d > 0:
        if cyan_share > CYAN_DOMINANCE_MAX:
            result.failed_rules.append("cyan_dominated")
            result.details["cyan_note"] = (
                f"Cyan share {cyan_share:.1%} > {CYAN_DOMINANCE_MAX:.0%}"
            )

    # --- Evaluate pass/quality ---
    result.passed = len(result.failed_rules) == 0

    if result.passed:
        # Check "Best" vs "Acceptable"
        is_best = (
            BEST_R2_MIN <= mfra.r_squared <= BEST_R2_MAX
            and purple_positive_count >= BEST_PURPLE_MIN_DAYS
            and (orange_10d > 0 or green_10d > 0)
        )
        result.quality = "Best" if is_best else "Acceptable"

    return result
