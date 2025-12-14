#!/bin/bash
set -euo pipefail

cd /root/polymarket-mean-reversion

# Ensure log directory exists
mkdir -p /root/polymarket-mean-reversion/logs

# Activate venv + env once
source .venv/bin/activate
set -a; source .env; set +a

echo ">>> Starting / checking Smartflow tmux session..."
if ! tmux has-session -t elwa_smartflow_full 2>/dev/null; then
  ./pipeline/scripts/run_smartflow_full_tmux.sh
else
  echo "    elwa_smartflow_full already running"
fi

echo ">>> Starting / checking MR v1 tmux session..."
if ! tmux has-session -t mr_v1 2>/dev/null; then
  tmux new-session -d -s mr_v1 \
    'cd /root/polymarket-mean-reversion && \
     source .venv/bin/activate && \
     set -a; source .env; set +a; \
     ./mr/start_mean_reversion.sh \
     >> /root/polymarket-mean-reversion/logs/mr_v1.log 2>&1'
  echo "    mr_v1 started in tmux session mr_v1"
else
  echo "    mr_v1 already running"
fi

echo ">>> Starting / checking MR v2 tmux session..."
if ! tmux has-session -t mr_v2 2>/dev/null; then
  tmux new-session -d -s mr_v2 \
    'cd /root/polymarket-mean-reversion && \
     source .venv/bin/activate && \
     set -a; source .env; set +a; \
     ./mr/start_mean_reversion_v2.sh \
     >> /root/polymarket-mean-reversion/logs/mr_v2.log 2>&1'
  echo "    mr_v2 started in tmux session mr_v2"
else
  echo "    mr_v2 already running"
fi

echo
echo "Current tmux sessions:"
tmux ls || echo "  (none)"