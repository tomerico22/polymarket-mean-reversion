#!/bin/bash
set -euo pipefail

# DB connection (same as v1)
export DB_URL="${DB_URL}"

# Use v2 strategy
export MR_STRATEGY="mean_reversion_v2"

# Core MR parameters (same as your local script)
export MR_DISLOCATION_THRESHOLD="${MR_DISLOCATION_THRESHOLD:-0.20}"
export MR_MAX_DISLOCATION="${MR_MAX_DISLOCATION:-0.45}"
export MR_MIN_PRICE="${MR_MIN_PRICE:-0.05}"
export MR_MAX_PRICE="${MR_MAX_PRICE:-0.95}"
export MR_AVG_WINDOW_HOURS="${MR_AVG_WINDOW_HOURS:-18}"

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

# V2-specific filters
export MR2_MAX_ENTRY_PX="${MR2_MAX_ENTRY_PX:-0.15}"
export MR2_MIN_DISLOCATION="${MR2_MIN_DISLOCATION:--0.45}"
export MR2_EXCLUDED_TAGS="${MR2_EXCLUDED_TAGS:-Neg Risk,Tweet Markets,Ethereum,Solana,elections,global elections,world elections,politics,Geopolitics,geopolitics,Pre-Market,Recurring,Almanak}"
export MR2_MARKET_MAX_LOSS_USD="${MR2_MARKET_MAX_LOSS_USD:-75}"

# NOTE: keyword blacklist now comes from mr/config/keyword_blacklist.txt
# MR_EXCLUDE_KEYWORDS env is only a fallback if the file is missing
export MR_EXCLUDE_KEYWORDS="${MR_EXCLUDE_KEYWORDS:-}"

export PYTHONUNBUFFERED=1

echo "=========================================="
echo "Mean Reversion Executor v2 (Paper)"
echo "Strategy: $MR_STRATEGY"
echo "Entry caps: dislo >= ${MR_DISLOCATION_THRESHOLD}, max abs dislo ${MR_MAX_DISLOCATION}, max entry px ${MR2_MAX_ENTRY_PX}"
echo "Exits: TP ${MR_TAKE_PROFIT_PCT}, SL ${MR_STOP_LOSS_PCT}, hard cap ${MR_MAX_STOP_LOSS_PCT}, max hold ${MR_MAX_HOLD_HOURS}h"
echo "Universe: top ${MR_TOP_MARKETS} markets, min vol \$${MR_MIN_VOLUME_24H}"
echo "V2 tag blacklist: ${MR2_EXCLUDED_TAGS}"
echo "Per-market PnL cap: -\$${MR2_MARKET_MAX_LOSS_USD}"
echo "=========================================="
echo ""

python3 -c "import psycopg" 2>/dev/null || {
    echo "ERROR: psycopg not installed. Run: pip install psycopg[binary]"
    exit 1
}

python3 mr/mean_reversion_executor.py