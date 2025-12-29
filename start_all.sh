#!/bin/bash
cd /root/polymarket-mean-reversion
set -a; source ./.env; set +a

echo "Starting all MR workers..."

# Kill any existing first
./kill_all.sh

sleep 2

# Start workers
echo "Starting position_reconciler..."
nohup .venv/bin/python -B mr/position_reconciler.py >> logs/reconciler.log 2>&1 &

echo "Starting exit_monitor..."
nohup .venv/bin/python -B mr/exit_monitor.py >> logs/exit_monitor.log 2>&1 &

echo "Starting positions_to_strategy_orders_sell_live..."
nohup .venv/bin/python -B mr/positions_to_strategy_orders_sell_live.py >> logs/live_sell.log 2>&1 &

echo "Starting strategy_orders_submit_live..."
nohup bash -c 'while true; do .venv/bin/python -B mr/strategy_orders_submit_live.py 2>&1; sleep 2; done' >> logs/live_submit.log 2>&1 &

echo "Starting settle_sells_to_positions..."
nohup .venv/bin/python -B mr/settle_sells_to_positions.py >> logs/live_settle.log 2>&1 &

echo "Starting mean_reversion_executor..."
nohup .venv/bin/python -B mr/mean_reversion_executor.py >> logs/live_exec.log 2>&1 &

echo "Starting intent_to_strategy_orders_live..."
nohup .venv/bin/python -B mr/intent_to_strategy_orders_live.py >> logs/live_intents.log 2>&1 &

sleep 5

echo ""
echo "=== WORKERS RUNNING ==="
ps aux | grep -E "mr/.*\.py" | grep -v grep | awk '{print $NF}'

echo ""
echo "=== CURRENT POSITIONS ==="
psql "$DB_URL" -c "SELECT id, LEFT(market_id, 16) as market, entry_price, size, status, exit_reason FROM mr_positions WHERE paper = false AND strategy = 'mean_reversion_v1' AND status IN ('open', 'closing') ORDER BY id;"

echo "Starting check_order_fills..."
nohup .venv/bin/python -B mr/check_order_fills.py >> logs/fill_checker.log 2>&1 &


echo "Starting watchdog..."
nohup ./watchdog.sh >> logs/watchdog.log 2>&1 &

