#!/bin/bash
cd /root/polymarket-mean-reversion

check_and_restart() {
    local name=$1
    local script=$2
    local log=$3
    
    if ! pgrep -f "$script" > /dev/null; then
        echo "[$(date)] $name crashed, restarting..."
        nohup .venv/bin/python -B $script >> $log 2>&1 &
    fi
}

while true; do
    check_and_restart "sell_worker" "mr/positions_to_strategy_orders_sell_live.py" "logs/live_sell.log"
    check_and_restart "exit_monitor" "mr/exit_monitor.py" "logs/exit_monitor.log"
    check_and_restart "fill_checker" "mr/check_order_fills.py" "logs/fill_checker.log"
    check_and_restart "submit_worker" "mr/strategy_orders_submit_live.py" "logs/live_submit.log"
    check_and_restart "settle_worker" "mr/settle_sells_to_positions.py" "logs/live_settle.log"
    check_and_restart "reconciler" "mr/position_reconciler.py" "logs/reconciler.log"
    check_and_restart "executor" "mr/mean_reversion_executor.py" "logs/live_exec.log"
    check_and_restart "intent_worker" "mr/intent_to_strategy_orders_live.py" "logs/live_intent.log"
    sleep 30
done
