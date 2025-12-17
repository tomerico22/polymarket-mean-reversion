#!/bin/bash
set -euo pipefail

# Mean Reversion Executor Startup Script
# Based on validated backtest: 85.4% WR, +9.40% avg P&L

export DB_URL="${DB_URL}"

# Strategy name
export MR_STRATEGY="mean_reversion_v1"

# ------------------------------------------------------------
# Price staleness + cooldown ban
# ------------------------------------------------------------
export MR_PRICE_STALE_SECS="${MR_PRICE_STALE_SECS:-10800}"
export MR_STALE_BAN_SECS="${MR_STALE_BAN_SECS:-10800}"

# ------------------------------------------------------------
# Entry filters (validated from backtest)
# ------------------------------------------------------------
export MR_DISLOCATION_THRESHOLD="${MR_DISLOCATION_THRESHOLD:-0.20}"   # 20% below avg
export MR_MAX_DISLOCATION="${MR_MAX_DISLOCATION:-0.45}"               # cap extreme dislocations
export MR_MIN_PRICE="${MR_MIN_PRICE:-0.05}"                           # don't trade below 5%
export MR_MAX_PRICE="${MR_MAX_PRICE:-0.95}"                           # don't trade above 95%
export MR_AVG_WINDOW_HOURS="${MR_AVG_WINDOW_HOURS:-18}"               # rolling average window

# ------------------------------------------------------------
# Exit parameters (validated from backtest)
# ------------------------------------------------------------
export MR_TAKE_PROFIT_PCT="${MR_TAKE_PROFIT_PCT:-0.15}"
export MR_STOP_LOSS_PCT="${MR_STOP_LOSS_PCT:-0.15}"
export MR_MAX_STOP_LOSS_PCT="${MR_MAX_STOP_LOSS_PCT:-0.20}"           # hard max loss cap (realized)
export MR_MAX_HOLD_HOURS="${MR_MAX_HOLD_HOURS:-12}"

# ------------------------------------------------------------
# Position sizing
# ------------------------------------------------------------
export MR_BASE_POSITION_USD="${MR_BASE_POSITION_USD:-100}"
export MR_MAX_POSITION_USD="${MR_MAX_POSITION_USD:-200}"

# ------------------------------------------------------------
# Risk management
# ------------------------------------------------------------
export MR_MAX_OPEN_POSITIONS="${MR_MAX_OPEN_POSITIONS:-30}"
export MR_MAX_POSITIONS_PER_MARKET="${MR_MAX_POSITIONS_PER_MARKET:-1}"
export MR_MARKET_COOLDOWN_SECS="${MR_MARKET_COOLDOWN_SECS:-600}"      # if you later re-enable market cooldown logic

# Daily stop (circuit breaker)
export MR_DAILY_LOSS_LIMIT="${MR_DAILY_LOSS_LIMIT:-1000}"

# Loss streak ban (per market/outcome)
export MR_MAX_LOSS_STREAK="${MR_MAX_LOSS_STREAK:-4}"
export MR_LOSS_STREAK_BAN_HOURS="${MR_LOSS_STREAK_BAN_HOURS:-12}"

# ------------------------------------------------------------
# Market selection
# ------------------------------------------------------------
export MR_TOP_MARKETS="${MR_TOP_MARKETS:-1000}"

# 24h volume gate
export MR_MIN_VOLUME_24H="${MR_MIN_VOLUME_24H:-5000}"

# 1h volume gate (0 disables in executor)
export MR_MIN_VOLUME_1H="${MR_MIN_VOLUME_1H:-500}"

# ------------------------------------------------------------
# Execution
# ------------------------------------------------------------
export MR_SLIPPAGE="${MR_SLIPPAGE:-0.01}"
export MR_LOOP_SLEEP="${MR_LOOP_SLEEP:-10}"
export PYTHONUNBUFFERED=1

# ------------------------------------------------------------
# Exclusions
# ------------------------------------------------------------
export MR_EXCLUDED_TAGS="${MR_EXCLUDED_TAGS:-${MR_EXCLUDE_TAGS:-sports,nfl,nba,soccer,mlb,hockey,tennis,mma,boxing,15m,1h,up or down}}"
export MR_EXCLUDED_CATEGORIES="${MR_EXCLUDED_CATEGORIES:-Sports}"

export MR_EXCLUDE_KEYWORDS="${MR_EXCLUDE_KEYWORDS:-election,mayor,president next,governor,senator,win the,next president,next mayor,next governor,presidential election,mayoral election,up or down,15m,1h,15 minute,1 hour,minute,tweets from,posts from,elon musk post,tweet count,post count,tweet between,views on day 1,opening weekend,box office,first week,top grossing,highest grossing,mrbeast,youtube video,day 1 views,between and,win on 202,match on 202,game on 202,vs on 202,vs. on 202,bitcoin reach,ethereum hit,btc reach,eth hit,hit $,reach $,price of bitcoin,price of btc,bitcoin be above,ethereum be above,top 5 searched,top 10 searched,rank in google,temperature,weather,between $ and,price between,ufo,declassif,strike on,attack on,bomb on,invade on,on december,on january,on february,on march,fdv,fdv above,market cap above,fdv >,valuation above}"

export MR_REQUIRE_QUESTION="${MR_REQUIRE_QUESTION:-0}"

echo "=========================================="
echo "Mean Reversion Trading Executor"
echo "=========================================="
echo "Strategy: $MR_STRATEGY"
echo ""
echo "Entry: Price < ${MR_AVG_WINDOW_HOURS}h avg by ${MR_DISLOCATION_THRESHOLD}"
echo "Exit: TP ${MR_TAKE_PROFIT_PCT}, SL ${MR_STOP_LOSS_PCT}, Max ${MR_MAX_HOLD_HOURS}h"
echo "Position: \$${MR_BASE_POSITION_USD} per trade"
echo "Limits: max positions ${MR_MAX_OPEN_POSITIONS}, daily loss \$${MR_DAILY_LOSS_LIMIT}"
echo "Loss streak: ${MR_MAX_LOSS_STREAK} losses -> ban ${MR_LOSS_STREAK_BAN_HOURS}h"
echo "Volume: 24h min ${MR_MIN_VOLUME_24H}, 1h min ${MR_MIN_VOLUME_1H}"
echo "Stale: ${MR_PRICE_STALE_SECS}s, stale ban ${MR_STALE_BAN_SECS}s"
echo ""
echo "EXCLUDED TAGS: ${MR_EXCLUDED_TAGS}"
echo "EXCLUDED KEYWORDS: ${MR_EXCLUDE_KEYWORDS:-'(none)'}"
echo "Require question: ${MR_REQUIRE_QUESTION}"
echo "=========================================="
echo ""

PY="/root/polymarket-mean-reversion/.venv/bin/python"

"$PY" -c "import psycopg" 2>/dev/null || {
  echo "ERROR: psycopg not installed. Run: pip install psycopg[binary]"
  exit 1
}

exec flock -n /tmp/mr_v1.lock "$PY" mr/mean_reversion_executor.py
