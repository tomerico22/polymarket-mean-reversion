#!/usr/bin/env bash
set -euo pipefail

SESSION_NAME=${SESSION_NAME:-elwa_smartflow_full}
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ORIG_DB_URL="${DB_URL:-}"

cd "$PROJECT_ROOT"
export PYTHONPATH="$PROJECT_ROOT:$PROJECT_ROOT/pipeline"

# Load envs, but preserve a DB_URL passed in.
set -a
source "$PROJECT_ROOT/.venv/bin/activate"
[[ -f ".env" ]] && source .env
[[ -f ".env.smartflow" ]] && source .env.smartflow
set +a
if [[ -n "$ORIG_DB_URL" ]]; then
  export DB_URL="$ORIG_DB_URL"
fi
: "${DB_URL:?DB_URL must be set}"

# Kill existing session if present.
tmux kill-session -t "$SESSION_NAME" 2>/dev/null || true

# Create base session/window
tmux new-session -d -s "$SESSION_NAME" -n "flow"

# Pane 0: HTTP trades poller
tmux send-keys -t "${SESSION_NAME}:0.0" "cd '$PROJECT_ROOT' && source $PROJECT_ROOT/.venv/bin/activate && set -a; source '$PROJECT_ROOT/.env'; if [ -f '$PROJECT_ROOT/.env.smartflow' ]; then source '$PROJECT_ROOT/.env.smartflow'; fi; set +a; PYTHONUNBUFFERED=1 python -m pipeline.ingestors.poll_trades_http" C-m

# Pane 1: flow snapshots runner
tmux split-window -v -t "${SESSION_NAME}:0"
tmux send-keys -t "${SESSION_NAME}:0.1" "cd '$PROJECT_ROOT' && source $PROJECT_ROOT/.venv/bin/activate && set -a; source '$PROJECT_ROOT/.env'; if [ -f '$PROJECT_ROOT/.env.smartflow' ]; then source '$PROJECT_ROOT/.env.smartflow'; fi; set +a; PYTHONUNBUFFERED=1 python -m pipeline.bots.flow_snapshots_runner" C-m

# Pane 2: smartflow runner
tmux split-window -h -t "${SESSION_NAME}:0.1"
tmux send-keys -t "${SESSION_NAME}:0.2" "cd '$PROJECT_ROOT' && source $PROJECT_ROOT/.venv/bin/activate && set -a; source '$PROJECT_ROOT/.env'; if [ -f '$PROJECT_ROOT/.env.smartflow' ]; then source '$PROJECT_ROOT/.env.smartflow'; fi; set +a; STRATEGY=sm_smartflow_v1 PYTHONUNBUFFERED=1 python -m pipeline.bots.smartflow_runner" C-m

# Pane 3: smartflow executor (paper)
tmux split-window -h -t "${SESSION_NAME}:0"
tmux send-keys -t "${SESSION_NAME}:0.3" "cd '$PROJECT_ROOT' && source $PROJECT_ROOT/.venv/bin/activate && set -a; source '$PROJECT_ROOT/.env'; if [ -f '$PROJECT_ROOT/.env.smartflow' ]; then source '$PROJECT_ROOT/.env.smartflow'; fi; set +a; SMARTFLOW_SIGNAL_STRATEGY=sm_smartflow_v1 PYTHONUNBUFFERED=1 python -m pipeline.bots.smartflow_executor" C-m

# Window 1: wallet labeler (manual trigger)
tmux new-window -t "$SESSION_NAME" -n "labeler"
tmux send-keys -t "${SESSION_NAME}:1.0" "cd '$PROJECT_ROOT' && source $PROJECT_ROOT/.venv/bin/activate && PYTHONUNBUFFERED=1 python -m pipeline.bots.wallet_labeler" C-m

# Window 2: stats builder loop (hourly by default)
tmux new-window -t "$SESSION_NAME" -n "stats_builder"
tmux send-keys -t "${SESSION_NAME}:2.0" "cd '$PROJECT_ROOT' && source $PROJECT_ROOT/.venv/bin/activate && while true; do PYTHONUNBUFFERED=1 python -m pipeline.bots.wallet_stats_daily_builder; sleep \${WALLET_STATS_REFRESH_SECS:-3600}; done" C-m

# Focus flow window
tmux select-window -t "${SESSION_NAME}:0"
tmux select-pane -t "${SESSION_NAME}:0.0"

echo "Smartflow full tmux session started: tmux attach -t $SESSION_NAME"
