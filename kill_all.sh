#!/bin/bash
echo "Killing all MR workers..."
pkill -f "mean_reversion_executor" 2>/dev/null
pkill -f "intent_to_strategy_orders" 2>/dev/null
pkill -f "positions_to_strategy_orders_sell" 2>/dev/null
pkill -f "strategy_orders_submit" 2>/dev/null
pkill -f "settle_sells_to_positions" 2>/dev/null
pkill -f "exit_monitor" 2>/dev/null
pkill -f "position_reconciler" 2>/dev/null
pkill -f "check_order_fills" 2>/dev/null
sleep 2
echo "Remaining python processes:"
ps aux | grep -E "mr/.*\.py" | grep -v grep || echo "  None - all killed!"
