#!/usr/bin/env bash
set -euo pipefail

cd /root/polymarket-mean-reversion

echo "== Preflight =="
date -u
echo "host=$(hostname) user=$(whoami) pwd=$(pwd)"
echo

echo "== Env file =="
if [[ ! -f ./.env ]]; then
  echo "ERROR: ./.env not found in $(pwd)"
  exit 2
fi
set -a; source ./.env; set +a
echo "DB_URL set? $([[ -n "${DB_URL:-}" ]] && echo yes || echo no)"
echo

echo "== Credentials (presence only) =="
python3 - <<'PY'
import os
keys=["PRIVATE_KEY","FUNDER_ADDRESS","POLY_API_KEY","POLY_API_SECRET","POLY_API_PASSPHRASE"]
for k in keys:
    v=(os.getenv(k) or "").strip()
    print(f"{k}: set={bool(v)} len={len(v)}")
PY
echo

echo "== Kill duplicates (safety) =="
PAT='pipeline\.ingestors\.poll_trades_http|mr/mean_reversion_executor\.py|mr/intent_to_strategy_orders_live\.py|mr/strategy_orders_submit_live\.py|mr/positions_to_strategy_orders_sell_live\.py|mr/settle_sells_to_positions\.py'
ps -ef | egrep -i "$PAT" | grep -v egrep || true
echo

echo "== Database sanity =="
psql "$DB_URL" -v ON_ERROR_STOP=1 -c "SELECT now() as db_now;" >/dev/null
echo "DB connect OK"
echo

echo "== Outstanding orders in the pipe (paper=false) =="
psql "$DB_URL" -v ON_ERROR_STOP=1 -c "
SELECT status, COUNT(*)
FROM strategy_orders
WHERE paper=false
  AND strategy='mean_reversion_v1'
  AND status IN ('submitted','live','error')
GROUP BY 1
ORDER BY 1;"
echo

echo "== Recent intent summary =="
psql "$DB_URL" -v ON_ERROR_STOP=1 -c "
SELECT status, COUNT(*)
FROM mr_trade_intents
WHERE strategy='mean_reversion_v1'
GROUP BY 1
ORDER BY 1;"
echo

echo "== Done =="
