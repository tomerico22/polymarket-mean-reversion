#!/usr/bin/env bash
set -euo pipefail
MR_TMUX_SESSION="${MR_TMUX_SESSION:-mr_exits}"
MR_TMUX_SUBMIT_SESSION="${MR_TMUX_SUBMIT_SESSION:-mr_submit_live}"
tmux kill-session -t "$MR_TMUX_SESSION" 2>/dev/null || true
tmux kill-session -t "$MR_TMUX_SUBMIT_SESSION" 2>/dev/null || true
pkill -f "mr/intent_to_strategy_orders_live.py" 2>/dev/null || true
pkill -f "mr/positions_to_strategy_orders_sell_live.py" 2>/dev/null || true
pkill -f "mr/settle_sells_to_positions.py" 2>/dev/null || true
pkill -f "mr/strategy_orders_submit_live.py" 2>/dev/null || true
pkill -f "mr/worker_heartbeat_daemon.py" 2>/dev/null || true
echo "OK: stopped"
