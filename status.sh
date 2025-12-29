#!/bin/bash
cd /root/polymarket-mean-reversion
echo "=== WORKERS ==="
for script in "exit_monitor" "sell_live" "submit_live" "check_order_fills" "settle_sells" "reconciler" "executor" "intent_to_strategy"; do
    if pgrep -f "$script" > /dev/null; then
        echo "✅ $script"
    else
        echo "❌ $script"
    fi
done
echo ""
echo "=== POSITIONS ==="
curl -s "https://data-api.polymarket.com/positions?user=0x356a7bc9C5AA7553f5A32F54Fe616f0639821354" 2>/dev/null | python3 -c "
import sys, json
data = json.load(sys.stdin)
active = [p for p in data if not (abs(p.get('curPrice', 0)) < 0.0001 and p.get('percentPnl', 0) < -99)]
print(f'{len(active)} active')
for p in active:
    print(f'  {p[\"percentPnl\"]:+.1f}% - {p[\"title\"][:35]}')
" 2>/dev/null || echo "API error"
