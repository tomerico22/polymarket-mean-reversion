#!/usr/bin/env bash
set -euo pipefail

cd /root/polymarket-mean-reversion
set -a; source .env; set +a

PY="/root/polymarket-mean-reversion/.venv/bin/python"
: "${MR_SUBMIT_SLEEP_SECS:=2}"

while true; do
  "$PY" mr/strategy_orders_submit_live.py || true
  sleep "$MR_SUBMIT_SLEEP_SECS"
done
