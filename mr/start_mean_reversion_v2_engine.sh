#!/bin/bash
set -euo pipefail

export DB_URL="${DB_URL:?DB_URL not set}"

export MR_STRATEGY="mean_reversion_v2"

export MR_BASE_POSITION_USD="${MR_BASE_POSITION_USD:-100}"

# Safety (shared with v1)
export MR_DAILY_LOSS_LIMIT="${MR_DAILY_LOSS_LIMIT:-1000}"
export MR_MAX_LOSS_STREAK="${MR_MAX_LOSS_STREAK:-4}"
export MR_LOSS_STREAK_BAN_HOURS="${MR_LOSS_STREAK_BAN_HOURS:-12}"
# 1h volume gate (0 disables)
export MR_MIN_VOLUME_1H="${MR_MIN_VOLUME_1H:-500}"


export MR_MARKET_DD_FRACTION="${MR_MARKET_DD_FRACTION:-0.75}"
export MR_SLIPPAGE="${MR_SLIPPAGE:-0.01}"

export MR_DISLOCATION_THRESHOLD="${MR_DISLOCATION_THRESHOLD:-0.20}"
export MR_MAX_DISLOCATION="${MR_MAX_DISLOCATION:-0.45}"

export MR_TAKE_PROFIT_PCT="${MR_TAKE_PROFIT_PCT:-0.15}"
export MR_STOP_LOSS_PCT="${MR_STOP_LOSS_PCT:-0.15}"
export MR_MAX_STOP_LOSS_PCT="${MR_MAX_STOP_LOSS_PCT:-0.20}"
export MR_MAX_HOLD_HOURS="${MR_MAX_HOLD_HOURS:-12}"

export MR_MAX_OPEN_POSITIONS="${MR_MAX_OPEN_POSITIONS:-30}"
export MR_MAX_POSITIONS_PER_MARKET="${MR_MAX_POSITIONS_PER_MARKET:-1}"

export MR_TOP_MARKETS="${MR_TOP_MARKETS:-500}"
export MR_MIN_VOLUME_24H="${MR_MIN_VOLUME_24H:-5000}"

export MR_EXCLUDED_TAGS="${MR_EXCLUDED_TAGS:-sports,nfl,nba,soccer,mlb,hockey}"

export MR_PRICE_STALE_SECS="${MR_PRICE_STALE_SECS:-300}"
export MR_STALE_BAN_SECS="${MR_STALE_BAN_SECS:-900}"

export MR_LOOP_SLEEP="${MR_LOOP_SLEEP:-10}"
export PYTHONUNBUFFERED=1

PY="/root/polymarket-mean-reversion/.venv/bin/python"
"$PY" -c "import psycopg" >/dev/null

exec "$PY" mr/mean_reversion_executor.py
