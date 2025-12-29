#!/usr/bin/env bash
set -euo pipefail

cd /root/polymarket-mean-reversion

set -a
source .env
set +a

need_file() { [[ -f "$1" ]] || { echo "FATAL: missing file: $1" >&2; exit 1; }; }
need_cmd()  { command -v "$1" >/dev/null 2>&1 || { echo "FATAL: missing cmd: $1" >&2; exit 1; }; }

need_cmd tmux
need_cmd psql
need_cmd rg

VENV_PY=/root/polymarket-mean-reversion/.venv/bin/python
[[ -x "$VENV_PY" ]] || { echo "FATAL: venv python not found: $VENV_PY" >&2; exit 1; }

: "${DB_URL:?FATAL: DB_URL not set (source .env failed?)}"
: "${POLY_CLOB_HTTP_BASE:?FATAL: POLY_CLOB_HTTP_BASE not set}"
: "${FUNDER_ADDRESS:?FATAL: FUNDER_ADDRESS not set}"
: "${POLY_PRIVATE_KEY:?FATAL: POLY_PRIVATE_KEY not set}"

need_file mr/intent_to_strategy_orders_live.py
need_file mr/positions_to_strategy_orders_sell_live.py
need_file mr/settle_sells_to_positions.py
need_file mr/strategy_orders_submit_live.py
need_file mr/worker_heartbeat_daemon.py
need_file mr/run_strategy_orders_submit_live_loop.sh

# basic sanity that submitter supports SELL
rg -n "def norm_side|sell" mr/strategy_orders_submit_live.py >/dev/null || {
  echo "FATAL: submitter doesn't look patched for sell: mr/strategy_orders_submit_live.py" >&2
  exit 1
}

$VENV_PY -m py_compile \
  mr/intent_to_strategy_orders_live.py \
  mr/positions_to_strategy_orders_sell_live.py \
  mr/settle_sells_to_positions.py \
  mr/strategy_orders_submit_live.py \
  mr/worker_heartbeat_daemon.py

psql "$DB_URL" -v ON_ERROR_STOP=1 -c "SELECT 1;" >/dev/null

export PYTHONUNBUFFERED=1
export MR_LIVE_EXECUTION=1
export MR_STRATEGY=mean_reversion_v1

export MR_SUBMIT_DRY_RUN="${MR_SUBMIT_DRY_RUN:-0}"
export MR_SUBMIT_MAX_USD="${MR_SUBMIT_MAX_USD:-2}"
export MR_SUBMIT_SLEEP_SECS="${MR_SUBMIT_SLEEP_SECS:-2}"
export MR_SUBMIT_LIVE_OK="${MR_SUBMIT_LIVE_OK:-YES}"

export MR_TMUX_SESSION="${MR_TMUX_SESSION:-mr_exits}"
export MR_TMUX_SUBMIT_SESSION="${MR_TMUX_SUBMIT_SESSION:-mr_submit_live}"

tmux kill-session -t "$MR_TMUX_SESSION" 2>/dev/null || true
tmux new-session -d -s "$MR_TMUX_SESSION" -n intent \
"cd /root/polymarket-mean-reversion && source .venv/bin/activate && set -a; source .env; set +a; PYTHONUNBUFFERED=1 MR_LIVE_EXECUTION=1 MR_STRATEGY=mean_reversion_v1 $VENV_PY -B mr/intent_to_strategy_orders_live.py"

tmux new-window -t "$MR_TMUX_SESSION" -n sell \
"cd /root/polymarket-mean-reversion && source .venv/bin/activate && set -a; source .env; set +a; PYTHONUNBUFFERED=1 MR_LIVE_EXECUTION=1 MR_STRATEGY=mean_reversion_v1 $VENV_PY -B mr/positions_to_strategy_orders_sell_live.py"

tmux new-window -t "$MR_TMUX_SESSION" -n settle \
"cd /root/polymarket-mean-reversion && source .venv/bin/activate && set -a; source .env; set +a; PYTHONUNBUFFERED=1 $VENV_PY -B mr/settle_sells_to_positions.py"

tmux new-window -t "$MR_TMUX_SESSION" -n heartbeat \
"cd /root/polymarket-mean-reversion && source .venv/bin/activate && set -a; source .env; set +a; PYTHONUNBUFFERED=1 $VENV_PY -B mr/worker_heartbeat_daemon.py"

tmux kill-session -t "$MR_TMUX_SUBMIT_SESSION" 2>/dev/null || true
tmux new-session -d -s "$MR_TMUX_SUBMIT_SESSION" \
"cd /root/polymarket-mean-reversion && source .venv/bin/activate && set -a; source .env; set +a; export MR_SUBMIT_DRY_RUN=$MR_SUBMIT_DRY_RUN; export MR_SUBMIT_MAX_USD=$MR_SUBMIT_MAX_USD; export MR_SUBMIT_SLEEP_SECS=$MR_SUBMIT_SLEEP_SECS; export MR_SUBMIT_LIVE_OK=$MR_SUBMIT_LIVE_OK; /root/polymarket-mean-reversion/mr/run_strategy_orders_submit_live_loop.sh"

echo "OK: started"
echo "tmux attach -t $MR_TMUX_SESSION"
echo "tmux attach -t $MR_TMUX_SUBMIT_SESSION"
