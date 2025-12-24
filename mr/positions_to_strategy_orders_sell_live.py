#!/usr/bin/env python3
import os, time, json
from decimal import Decimal
import psycopg
from psycopg.rows import dict_row

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise SystemExit("DB_URL not set")

EXEC_STRATEGY = os.getenv("MR_STRATEGY", "mean_reversion_v1")
POLL_SECS = float(os.getenv("MR_SELL_POLL_SECS", "2.0"))
BATCH = int(os.getenv("MR_SELL_BATCH", "50"))

LIVE_FLAG = os.getenv("MR_LIVE_EXECUTION", "").strip().lower()
LIVE_ENABLED = LIVE_FLAG in ("1", "true", "yes", "y", "on")
PAPER = False

def dec(x):
    try:
        return Decimal(str(x))
    except Exception:
        return Decimal("0")

def norm_outcome(out_raw):
    if out_raw is None:
        return None
    s = str(out_raw).strip().lower()
    if s in ("0", "1"):
        return int(s)
    if s in ("yes", "y", "buy"):
        return 0
    if s in ("no", "n", "sell"):
        return 1
    try:
        return int(s)
    except Exception:
        return None

SQL_PICK = """
WITH picked AS (
  SELECT p.id, p.strategy, p.market_id, p.outcome, p.size, p.exit_reason,
         p.exit_signal_price, p.entry_price
  FROM mr_positions p
  WHERE p.paper=false
    AND p.strategy = %s
    AND COALESCE(p.status,'open') IN ('open','closing')
    AND p.exit_reason IS NOT NULL
    AND (p.exit_signal_price IS NOT NULL OR p.entry_price IS NOT NULL)
    AND NOT EXISTS (
      SELECT 1 FROM strategy_orders o
      WHERE o.paper=false
        AND o.strategy = p.strategy
        AND o.side ILIKE 'sell%%'
        AND (o.metadata->>'position_id') = p.id::text
        AND o.status IN ('submitted','live','matched')
    )
  ORDER BY p.id ASC
  LIMIT %s
  FOR UPDATE SKIP LOCKED
)
SELECT * FROM picked;
"""

SQL_INTENT_GET = """
SELECT id
FROM mr_trade_intents
WHERE strategy=%s
  AND market_id=%s
  AND outcome=%s
  AND side=%s
  AND entry_price=%s
LIMIT 1;
"""

SQL_INTENT_INS = """
INSERT INTO mr_trade_intents (
  strategy, market_id, outcome, side, entry_price, size_usd, dislocation, avg_price_18h,
  source, note, status
)
VALUES (%s,%s,%s,%s,%s,%s,NULL,NULL,%s,%s,%s)
RETURNING id;
"""

SQL_ORDER_INS = """
INSERT INTO strategy_orders (
  strategy, market_id, side, qty, limit_px, status, paper,
  intent_id, outcome, metadata
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
RETURNING id;
"""

def main():
    if not LIVE_ENABLED:
        raise SystemExit("Refusing to start: MR_LIVE_EXECUTION is not enabled. Set MR_LIVE_EXECUTION=1")

    print(f"[pos2sell_live] started strategy={EXEC_STRATEGY} paper={PAPER} poll={POLL_SECS}s batch={BATCH}")

    with psycopg.connect(DB_URL, row_factory=dict_row) as conn:
        conn.autocommit = True
        while True:
            with conn.cursor() as cur:
                cur.execute(SQL_PICK, (EXEC_STRATEGY, BATCH))
                rows = cur.fetchall()

                for p in rows:
                    pid = int(p["id"])
                    market_id = p["market_id"]
                    outcome = norm_outcome(p["outcome"])
                    if outcome is None:
                        print(f"[pos2sell_live] skip pos={pid} bad outcome={p.get('outcome')}")
                        continue

                    qty = dec(p["size"])
                    limit_px = dec(p.get("exit_signal_price") or p.get("entry_price"))
                    if qty <= 0 or limit_px <= 0:
                        continue

                    # Make a unique "side" for the exit intent so it never conflicts with normal buys
                    intent_side = f"exit_{pid}"
                    intent_outcome_txt = str(outcome)

                    # Create or fetch an intent_id (strategy_orders.intent_id is NOT NULL)
                    cur.execute(SQL_INTENT_GET, (EXEC_STRATEGY, market_id, intent_outcome_txt, intent_side, limit_px))
                    r = cur.fetchone()
                    if r:
                        intent_id = int(r["id"])
                    else:
                        note = f"exit_from_position id={pid} reason={p.get('exit_reason')} px={limit_px}"
                        cur.execute(
                            SQL_INTENT_INS,
                            (
                                EXEC_STRATEGY,
                                market_id,
                                intent_outcome_txt,
                                intent_side,
                                limit_px,
                                Decimal("0"),                 # size_usd not meaningful for exits; keep 0
                                "mr_positions_exit",
                                note,
                                "queued",
                            ),
                        )
                        intent_id = int(cur.fetchone()["id"])

                    meta = {
                        "source": "mr_positions_exit",
                        "position_id": pid,
                        "exit_reason": p.get("exit_reason"),
                        "limit_px": str(limit_px),
                        "intent_id": intent_id,
                        "live_execution": True,
                    }

                    cur.execute(
                        SQL_ORDER_INS,
                        (
                            EXEC_STRATEGY,
                            market_id,
                            "sell",
                            qty,
                            limit_px,
                            "submitted",
                            PAPER,
                            intent_id,
                            outcome,
                            json.dumps(meta),
                        ),
                    )
                    oid = int(cur.fetchone()["id"])

                    cur.execute("UPDATE mr_positions SET status='closing' WHERE id=%s", (pid,))
                    print(f"[pos2sell_live] pos={pid} -> sell order_id={oid} intent_id={intent_id} market={market_id[:12]} out={outcome} qty={qty} px={limit_px}")

            time.sleep(POLL_SECS)

if __name__ == "__main__":
    main()
