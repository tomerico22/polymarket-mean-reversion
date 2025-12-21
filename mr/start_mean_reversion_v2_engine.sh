#!/bin/bash
set -euo pipefail

# =============================================================================
# Mean Reversion V2 - Optimized Configuration
# =============================================================================
# Based on 7-day analysis of ~900 trades
#
# Core insights applied:
# - Target mild dislocations only (10-40%)
# - Avoid crashes, crypto, fdv, pre-market, recurring structures
# - Favor cheap contracts (<20c, sweet spot <10c)
# - Winners resolve fast -> shorter max hold
# - Faster loss-streak bans, tighter daily loss cap
# - Dedicated keyword blacklist (v2-only)
# =============================================================================

# ------------------------------------------------------------
# Required
# ------------------------------------------------------------
export DB_URL="${DB_URL:?DB_URL not set}"
export MR_STRATEGY="mean_reversion_v2"

# ------------------------------------------------------------
# Position sizing
# ------------------------------------------------------------
export MR_BASE_POSITION_USD="${MR_BASE_POSITION_USD:-100}"

# ------------------------------------------------------------
# Entry filters (OPTIMIZED)
# ------------------------------------------------------------
# Mild dips only
export MR_DISLOCATION_THRESHOLD="${MR_DISLOCATION_THRESHOLD:-0.10}"   # >=10% dip
export MR_MAX_DISLOCATION="${MR_MAX_DISLOCATION:-0.40}"               # <=40% dip

# Price targeting
export MR_MIN_PRICE="${MR_MIN_PRICE:-0.02}"                           # 2c floor
export MR_MAX_PRICE="${MR_MAX_PRICE:-0.20}"                           # 20c cap

# Mean window
export MR_AVG_WINDOW_HOURS="${MR_AVG_WINDOW_HOURS:-18}"

# ------------------------------------------------------------
# Exit logic
# ------------------------------------------------------------
export MR_TAKE_PROFIT_PCT="${MR_TAKE_PROFIT_PCT:-0.15}"
export MR_STOP_LOSS_PCT="${MR_STOP_LOSS_PCT:-0.15}"
export MR_MAX_STOP_LOSS_PCT="${MR_MAX_STOP_LOSS_PCT:-0.20}"
export MR_MAX_HOLD_HOURS="${MR_MAX_HOLD_HOURS:-8}"

# ------------------------------------------------------------
# Risk management
# ------------------------------------------------------------
export MR_MAX_OPEN_POSITIONS="${MR_MAX_OPEN_POSITIONS:-30}"
export MR_MAX_POSITIONS_PER_MARKET="${MR_MAX_POSITIONS_PER_MARKET:-1}"

# Market-level drawdown (fraction of base position)
export MR_MARKET_DD_FRACTION="${MR_MARKET_DD_FRACTION:-0.75}"

# Daily circuit breaker
export MR_DAILY_LOSS_LIMIT="${MR_DAILY_LOSS_LIMIT:-500}"

# Loss streak control
export MR_MAX_LOSS_STREAK="${MR_MAX_LOSS_STREAK:-3}"
export MR_LOSS_STREAK_BAN_HOURS="${MR_LOSS_STREAK_BAN_HOURS:-24}"

# ------------------------------------------------------------
# Market selection
# ------------------------------------------------------------
export MR_TOP_MARKETS="${MR_TOP_MARKETS:-500}"
export MR_MIN_VOLUME_24H="${MR_MIN_VOLUME_24H:-5000}"
export MR_MIN_VOLUME_1H="${MR_MIN_VOLUME_1H:-500}"

# ------------------------------------------------------------
# Execution / loop
# ------------------------------------------------------------
export MR_SLIPPAGE="${MR_SLIPPAGE:-0.01}"
export MR_LOOP_SLEEP="${MR_LOOP_SLEEP:-10}"

# ------------------------------------------------------------
# Price staleness protection
# ------------------------------------------------------------
export MR_PRICE_STALE_SECS="${MR_PRICE_STALE_SECS:-300}"
export MR_STALE_BAN_SECS="${MR_STALE_BAN_SECS:-900}"

export PYTHONUNBUFFERED=1

# ------------------------------------------------------------
# Tag exclusions (STRUCTURAL LOSERS)
# ------------------------------------------------------------
export MR_EXCLUDED_TAGS="${MR_EXCLUDED_TAGS:-\
sports,nfl,nba,soccer,mlb,hockey,tennis,mma,boxing,\
crypto,crypto prices,\
weekly,recurring,pre-market,\
fdv,public sales,hit price,\
elections,global elections,chile election,time poty\
}"

# ------------------------------------------------------------
# Keyword blacklist (V2 ONLY)
# ------------------------------------------------------------
# File-based blacklist (hot-reloaded by executor)
export MR_KEYWORD_BLACKLIST_PATH="${MR_KEYWORD_BLACKLIST_PATH:-/root/polymarket-mean-reversion/mr/config/keyword_blacklist_v2.txt}"

# Require question text? (0 = allow missing)
export MR_REQUIRE_QUESTION="${MR_REQUIRE_QUESTION:-0}"

# Optional env keywords (merged with file)
export MR_EXCLUDE_KEYWORDS="${MR_EXCLUDE_KEYWORDS:-}"

# ------------------------------------------------------------
# Startup banner (logs only)
# ------------------------------------------------------------
echo "============================================================"
echo "Mean Reversion V2 - OPTIMIZED"
echo "============================================================"
echo "Strategy: $MR_STRATEGY"
echo "Price: ${MR_MIN_PRICE} - ${MR_MAX_PRICE}"
echo "Dislocation: ${MR_DISLOCATION_THRESHOLD} - ${MR_MAX_DISLOCATION}"
echo "Avg window: ${MR_AVG_WINDOW_HOURS}h"
echo "TP/SL: ${MR_TAKE_PROFIT_PCT} / ${MR_STOP_LOSS_PCT}"
echo "Max hold: ${MR_MAX_HOLD_HOURS}h"
echo "Daily loss limit: ${MR_DAILY_LOSS_LIMIT}"
echo "Loss streak: ${MR_MAX_LOSS_STREAK}"
echo "Keyword blacklist: ${MR_KEYWORD_BLACKLIST_PATH}"
echo "============================================================"
echo ""

# ------------------------------------------------------------
# Sanity check + exec (single-instance lock)
# ------------------------------------------------------------
PY="/root/polymarket-mean-reversion/.venv/bin/python"
"$PY" -c "import psycopg" >/dev/null

exec flock -n /tmp/mr_v2.lock "$PY" mr/mean_reversion_executor.py