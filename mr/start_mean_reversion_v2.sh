#!/bin/bash
set -euo pipefail

# Mean Reversion Executor v2 Startup Script

# DB connection (must be set in environment or systemd EnvironmentFile)
export DB_URL="${DB_URL}"

# Use v2 strategy
export MR_STRATEGY="mean_reversion_v2"

# ------------------------------------------------------------
# Your requested bands
# Entry price band: 0.05 - 0.20
# Dislocation band (px vs avg): -0.40 to -0.20
#
# NOTE: your executor enforces:
#   dislo = (px-avg)/avg
#   requires dislo < 0
#   and DISLOCATION_THRESHOLD <= abs(dislo) <= MAX_DISLOCATION
# So -0.40..-0.20 maps to:
#   MR_DISLOCATION_THRESHOLD=0.20
#   MR_MAX_DISLOCATION=0.40
# ------------------------------------------------------------

# Dislocation band
export MR_DISLOCATION_THRESHOLD="${MR_DISLOCATION_THRESHOLD:-0.20}"
export MR_MAX_DISLOCATION="${MR_MAX_DISLOCATION:-0.40}"

# Entry price band (bounds are checked on raw px, before slippage)
export MR_MIN_PRICE="${MR_MIN_PRICE:-0.05}"
export MR_MAX_PRICE="${MR_MAX_PRICE:-0.20}"

# Rolling average window (your executor currently hardcodes 18 hours in SQL)
export MR_AVG_WINDOW_HOURS="${MR_AVG_WINDOW_HOURS:-18}"

# Exits
export MR_TAKE_PROFIT_PCT="${MR_TAKE_PROFIT_PCT:-0.15}"
export MR_STOP_LOSS_PCT="${MR_STOP_LOSS_PCT:-0.15}"
export MR_MAX_HOLD_HOURS="${MR_MAX_HOLD_HOURS:-12}"

# Position sizing
export MR_BASE_POSITION_USD="${MR_BASE_POSITION_USD:-100}"
export MR_MAX_POSITION_USD="${MR_MAX_POSITION_USD:-200}"

# Risk management
export MR_MAX_OPEN_POSITIONS="${MR_MAX_OPEN_POSITIONS:-30}"
export MR_MAX_POSITIONS_PER_MARKET="${MR_MAX_POSITIONS_PER_MARKET:-1}"
export MR_MARKET_COOLDOWN_SECS="${MR_MARKET_COOLDOWN_SECS:-600}"
export MR_DAILY_LOSS_LIMIT="${MR_DAILY_LOSS_LIMIT:-1000}"
export MR_MAX_STOP_LOSS_PCT="${MR_MAX_STOP_LOSS_PCT:-0.20}"
export MR_MAX_LOSS_STREAK="${MR_MAX_LOSS_STREAK:-4}"

# Market selection
export MR_TOP_MARKETS="${MR_TOP_MARKETS:-500}"
export MR_MIN_VOLUME_24H="${MR_MIN_VOLUME_24H:-5000}"

# Execution
export MR_SLIPPAGE="${MR_SLIPPAGE:-0.01}"
export MR_LOOP_SLEEP="${MR_LOOP_SLEEP:-10}"

# V2-specific filters (only applied if your executor code supports MR2_*)
# If supported, keep these aligned with your intent:
export MR2_MAX_ENTRY_PX="${MR2_MAX_ENTRY_PX:-0.20}"
export MR2_MIN_DISLOCATION="${MR2_MIN_DISLOCATION:--0.40}"
export MR2_EXCLUDED_TAGS="${MR2_EXCLUDED_TAGS:-Neg Risk,Tweet Markets,Ethereum,Solana,elections,global elections,world elections,politics,Geopolitics,geopolitics,Pre-Market,Recurring,Almanak}"
export MR2_MARKET_MAX_LOSS_USD="${MR2_MARKET_MAX_LOSS_USD:-75}"

# Keyword blacklist
export MR_EXCLUDE_KEYWORDS="${MR_EXCLUDE_KEYWORDS:-}"

export PYTHONUNBUFFERED=1

echo "=========================================="
echo "Mean Reversion Executor v2 (Paper)"
echo "Strategy: $MR_STRATEGY"
echo "Entry px band: ${MR_MIN_PRICE} - ${MR_MAX_PRICE} (raw px, before slippage)"
echo "Dislocation band: -${MR_MAX_DISLOCATION} to -${MR_DISLOCATION_THRESHOLD}"
echo "Exits: TP ${MR_TAKE_PROFIT_PCT}, SL ${MR_STOP_LOSS_PCT}, hard cap ${MR_MAX_STOP_LOSS_PCT}, max hold ${MR_MAX_HOLD_HOURS}h"
echo "Universe: top ${MR_TOP_MARKETS} markets, min vol \$${MR_MIN_VOLUME_24H}"
echo "V2 tag blacklist: ${MR2_EXCLUDED_TAGS}"
echo "Per-market PnL cap: -\$${MR2_MARKET_MAX_LOSS_USD}"
echo "=========================================="
echo ""

PY="/root/polymarket-mean-reversion/.venv/bin/python"

"$PY" -c "import psycopg" 2>/dev/null || {
    echo "ERROR: psycopg not installed in venv. Run: pip install psycopg[binary]"
    exit 1
}

"$PY" mr/mean_reversion_executor.py