"""MFRA Scanner configuration."""
import os

# --- MFRA Model Parameters ---
OLS_WINDOW = 42           # Rolling OLS regression window (bars)
ROLLING_SUM_WINDOW = 10   # Rolling sum window for contribution bars

# --- Entry Criteria Thresholds ---
R2_MIN = 0.4
R2_MAX = 0.7
PURPLE_POSITIVE_MIN_DAYS = 5       # Out of last 10
PURPLE_DOMINANCE_MAX = 0.60        # No single bar > 60% of total purple
CYAN_DOMINANCE_MAX = 0.70          # Market can't dominate > 70% of positive
SECTOR_TAILWIND_TOLERANCE = -0.001 # Small noise tolerance for orange/green

# --- "Best" Quality Thresholds ---
BEST_R2_MIN = 0.50
BEST_R2_MAX = 0.65
BEST_PURPLE_MIN_DAYS = 7  # Out of last 10

# --- Data Fetching ---
LOOKBACK_CALENDAR_DAYS = 120
BATCH_SIZE = 20
BATCH_PAUSE_SECONDS = 2
MAX_RETRIES = 3
CACHE_MAX_AGE_DAYS = 7

# --- Email (via macOS Mail.app) ---
EMAIL_TO = "marilee@foundationaltrading.com"

# --- Paths ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(BASE_DIR, ".cache")
LOG_DIR = os.path.join(BASE_DIR, "logs")
WATCHLIST_PATH = os.path.join(BASE_DIR, "config", "watchlist.json")

# --- Indicator Colors (for email) ---
COLORS = {
    "cyan": "#00BCD4",     # Market
    "orange": "#FF7F0E",   # Sector
    "green": "#4CAF50",    # Sub-Sector
    "purple": "#E040FB",   # Idiosyncratic/Residual
    "white": "#FFFFFF",    # Total return
    "r2_blue": "#5DADE2",  # R-squared
}
