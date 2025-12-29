#!/usr/bin/env bash
set -euo pipefail
cd /root/polymarket-mean-reversion

mkdir -p logs run
set -a; source ./.env; set +a

# Guard - do not start if already running
PAT='pipeline\.ingestors\.poll_trades_http|mr/mean_reversion_executor\.py|mr/intent_to_strategy_orders_live\.py|mr/strategy_orders_submit_live\.py'
if ps -ef | egrep -q "$PAT"; then
  echo "ERROR: one or more processes already running:"
  ps -ef | egrep "$PAT" | grep -v egrep || true
  exit 1
fi

echo "[start] ingest"
nohup .venv/bin/python -m pipeline.ingestors.poll_trades_http \
  >> logs/live_ingest.log 2>&1 &
echo $! > run/live_ingest.pid

echo "[start] executor (intents)"
nohup env MR_LIVE_EXECUTION=1 MR_STRATEGY=mean_reversion_v1 \
  flock -n /tmp/mr_v1.lock .venv/bin/python -B mr/mean_reversion_executor.py \
  >> logs/live_exec.log 2>&1 &
echo $! > run/live_exec.pid

echo "[start] intents -> strategy_orders"
nohup env MR_LIVE_EXECUTION=1 MR_STRATEGY=mean_reversion_v1 \
  .venv/bin/python -B mr/intent_to_strategy_orders_live.py \
  >> logs/live_intents.log 2>&1 &
echo $! > run/live_intents.pid

echo "[start] submitter"
nohup env MR_SUBMIT_DRY_RUN=0 MR_SUBMIT_LIVE_OK=1 MR_STRATEGY=mean_reversion_v1 \
  .venv/bin/python -B mr/strategy_orders_submit_live.py \
  >> logs/live_submit.log 2>&1 &
echo $! > run/live_submit.pid

echo
echo "[start] running:"
ps -ef | egrep "poll_trades_http|mean_reversion_executor|intent_to_strategy_orders_live|strategy_orders_submit_live" | grep -v egrep || true
