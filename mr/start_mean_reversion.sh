#!/bin/bash

# Mean Reversion Executor Startup Script
# Based on validated backtest: 85.4% WR, +9.40% avg P&L

export DB_URL="${DB_URL}"

# Strategy name
export MR_STRATEGY="mean_reversion_v1"

# Entry filters (VALIDATED from backtest)
export MR_DISLOCATION_THRESHOLD="0.20"     # 20% below 18h avg
export MR_MAX_DISLOCATION="0.45"           # Cap extreme dislocations at 45%
export MR_MIN_PRICE="0.05"                 # Don't trade below 5%
export MR_MAX_PRICE="0.95"                 # Don't trade above 95%
export MR_AVG_WINDOW_HOURS="18"            # 18-hour rolling average

# Exit parameters (VALIDATED from backtest)
export MR_TAKE_PROFIT_PCT="0.15"           # 15% take profit
export MR_STOP_LOSS_PCT="0.15"             # 15% stop loss
export MR_MAX_HOLD_HOURS="12"              # 12 hour maximum hold

# Position sizing
export MR_BASE_POSITION_USD="100"          # $100 per trade
export MR_MAX_POSITION_USD="200"           # Max $200 per trade

# Risk management
export MR_MAX_OPEN_POSITIONS="${MR_MAX_OPEN_POSITIONS:-30}"          # Max 30 concurrent positions (wider universe)
export MR_MAX_POSITIONS_PER_MARKET="1"     # Max 1 per market/outcome
export MR_MARKET_COOLDOWN_SECS="600"       # 10 min cooldown after close
export MR_DAILY_LOSS_LIMIT="${MR_DAILY_LOSS_LIMIT:-1000}"           # Circuit breaker (default $1000/day)
# Hard maximum stop loss cap (realized P&L), default 20%
export MR_MAX_STOP_LOSS_PCT="${MR_MAX_STOP_LOSS_PCT:-0.20}"
# Max consecutive losses per market before blocking new entries
export MR_MAX_LOSS_STREAK="${MR_MAX_LOSS_STREAK:-4}"

# Market selection
export MR_TOP_MARKETS="${MR_TOP_MARKETS:-500}"                 # Top 500 by volume (wider universe)
export MR_MIN_VOLUME_24H="${MR_MIN_VOLUME_24H:-5000}"          # Min $5k 24h volume (looser)

# Execution
export MR_SLIPPAGE="0.01"                  # 1% slippage
export MR_LOOP_SLEEP="10"                  # Check every 10 seconds

# Targeted exclusions to avoid directional/timed markets
export MR_EXCLUDED_TAGS="${MR_EXCLUDE_TAGS:-sports,nfl,nba,soccer,mlb,hockey,tennis,mma,boxing,15m,1h,up or down}"
export MR_EXCLUDED_CATEGORIES="${MR_EXCLUDED_CATEGORIES:-Sports}"
# Default keyword blocklist (directional/timed patterns + FDV/valuation)
export MR_EXCLUDE_KEYWORDS="${MR_EXCLUDE_KEYWORDS:-election,mayor,president next,governor,senator,win the,next president,next mayor,next governor,presidential election,mayoral election,up or down,15m,1h,15 minute,1 hour,minute,tweets from,posts from,elon musk post,tweet count,post count,tweet between,views on day 1,opening weekend,box office,first week,top grossing,highest grossing,mrbeast,youtube video,day 1 views,between and,win on 202,match on 202,game on 202,vs on 202,vs. on 202,bitcoin reach,ethereum hit,btc reach,eth hit,hit $,reach $,price of bitcoin,price of btc,bitcoin be above,ethereum be above,top 5 searched,top 10 searched,rank in google,temperature,weather,between $ and,price between,ufo,declassif,strike on,attack on,bomb on,invade on,on december,on january,on february,on march,fdv,fdv above,market cap above,fdv >,valuation above}"
export MR_REQUIRE_QUESTION="${MR_REQUIRE_QUESTION:-0}"

export PYTHONUNBUFFERED=1

echo "=========================================="
echo "Mean Reversion Trading Executor"
echo "=========================================="
echo "Strategy: $MR_STRATEGY"
echo "Backtest Results: 85.4% WR, +9.40% avg P&L"
echo ""
echo "Entry: Price < 18h avg by 20%+"
echo "Exit: TP +15%, SL -15%, Max 12h"
echo "Position: \$${MR_BASE_POSITION_USD} per trade"
echo "Limits: ${MR_MAX_OPEN_POSITIONS} positions max, daily loss \$${MR_DAILY_LOSS_LIMIT}"
echo ""
echo "EXCLUDED TAGS: ${MR_EXCLUDED_TAGS}"
echo "EXCLUDED KEYWORDS: ${MR_EXCLUDE_KEYWORDS:-'(none)'}"
echo "Require question: ${MR_REQUIRE_QUESTION}"
echo "=========================================="
echo ""

# Check dependencies
python3 -c "import psycopg" 2>/dev/null || {
    echo "ERROR: psycopg not installed. Run: pip install psycopg[binary]"
    exit 1
}

# Run the executor
python3 mean_reversion_executor.py
