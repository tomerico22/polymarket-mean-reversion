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

# How far from entry_ts we allow matching a buy order if position_id link is missing
ENTRY_MATCH_WINDOW_SECS = int(os.getenv("MR_SELL_ENTRY_MATCH_WINDOW_SECS", "3600"))  # 1 hour

def dec(x) -> Decimal:
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
  SELECT
    p.id, p.strategy, p.market_id, p.outcome, p.size, p.exit_reason,
    p.exit_signal_price, p.entry_price, p.entry_ts
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

# 1) Preferred: buy orders explicitly linked to this position_id
SQL_FILLED_BUY_QTY_LINKED = """
SELECT COALESCE(SUM(f.qty), 0) AS filled_qty,
       MAX(o.id) FILTER (WHERE o.status='matched') AS matched_buy_order_id
FROM strategy_orders o
JOIN strategy_fills f ON f.order_id = o.id
WHERE o.paper=false
  AND o.strategy = %s
  AND o.side = 'buy'
  AND (o.metadata->>'position_id') = %s
"""

# 2) Fallback: closest matched buy order around entry_ts (same market/outcome)
SQL_FIND_MATCHED_BUY_ORDER_FALLBACK = """
SELECT o.id
FROM strategy_orders o
WHERE o.paper=false
  AND o.strategy = %s
  AND o.side='buy'
  AND o.status='matched'
  AND o.market_id=%s
  AND o.outcome=%s
  AND o.created_at BETWEEN (%s::timestamptz - (%s || ' seconds')::interval)
                       AND (%s::timestamptz + (%s || ' seconds')::interval)
ORDER BY ABS(EXTRACT(EPOCH FROM (o.created_at - %s::timestamptz))) ASC
LIMIT 1
"""

SQL_FILLED_QTY_FOR_ORDER = """
SELECT COALESCE(SUM(qty), 0) AS filled_qty
FROM strategy_fills
WHERE order_id=%s AND paper=false
"""

SQL_INTENT_GET = """
SELECT id
FROM mr_trade_intents
WHERE strategy=%s
  AND market_id=%s
  AND outcome=%s
  AND side=%s
  AND entry_price=%s
  AND status NOT IN ('canceled', 'skipped', 'error')
LIMIT 1;
"""

SQL_INTENT_INS = """
INSERT INTO mr_trade_intents (
  strategy, market_id, outcome, side, entry_price, size_usd, dislocation, avg_price_18h,
  source, note, status
)
VALUES (%s,%s,%s,%s,%s,%s,NULL,NULL,%s,%s,%s)
ON CONFLICT (strategy, market_id, outcome, side, entry_price) DO UPDATE SET status = 'queued'
RETURNING id;
"""

SQL_ORDER_INS = """
INSERT INTO strategy_orders (
  strategy, market_id, side, qty, limit_px, status, paper,
  intent_id, outcome, metadata
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
ON CONFLICT (intent_id) DO NOTHING
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

                    entry_ts = p.get("entry_ts")
                    exit_reason = p.get("exit_reason", "")
                    entry_price = dec(p.get("entry_price") or 0)
                    
                    # For SL: sell at current price (slightly below to ensure fill)
                    # For TP: sell at exit_signal_price or entry * 1.15
                    if exit_reason == "sl":
                        # Get current price from raw_trades
                        cur.execute("""
                            SELECT price FROM raw_trades 
                            WHERE market_id = %s AND outcome = %s 
                            ORDER BY ts DESC LIMIT 1
                        """, (market_id, str(outcome)))
                        price_row = cur.fetchone()
                        if price_row and price_row.get("price"):
                            current_price = dec(price_row["price"])
                            # Sell slightly below current to ensure fill
                            limit_px = current_price * dec("0.97")
                        else:
                            # Fallback: use entry price * 0.85 (SL threshold)
                            limit_px = entry_price * dec("0.85")
                        print(f"[pos2sell_live] SL pos={pid} using limit_px={limit_px}")
                    else:
                        # TP: use exit_signal_price or entry * 1.15
                        limit_px = dec(p.get("exit_signal_price") or entry_price * dec("1.15"))
                    
                    if limit_px <= 0:
                        print(f"[pos2sell_live] skip pos={pid} bad limit_px={limit_px}")
                        continue

                    pos_size = dec(p.get("size", 0))

                    # 1) Prefer linked buy fills by position_id
                    cur.execute(SQL_FILLED_BUY_QTY_LINKED, (EXEC_STRATEGY, str(pid)))
                    r = cur.fetchone() or {}
                    filled_buy_qty = dec(r.get("filled_qty", 0))
                    matched_buy_order_id = r.get("matched_buy_order_id")

                    # 2) Fallback: find matched buy order around entry_ts
                    if filled_buy_qty <= 0 and entry_ts is not None:
                        cur.execute(
                            SQL_FIND_MATCHED_BUY_ORDER_FALLBACK,
                            (
                                EXEC_STRATEGY,
                                market_id,
                                outcome,
                                entry_ts,
                                ENTRY_MATCH_WINDOW_SECS,
                                entry_ts,
                                ENTRY_MATCH_WINDOW_SECS,
                                entry_ts,
                            ),
                        )
                        rr = cur.fetchone()
                        if rr and rr.get("id"):
                            matched_buy_order_id = int(rr["id"])
                            cur.execute(SQL_FILLED_QTY_FOR_ORDER, (matched_buy_order_id,))
                            fr = cur.fetchone() or {}
                            filled_buy_qty = dec(fr.get("filled_qty", 0))

                    # NEW: fallback to mr_positions.size when fills are missing
                    if filled_buy_qty > 0:
                        sell_qty = filled_buy_qty
                        sell_qty_source = "fills"
                    else:
                        sell_qty = pos_size
                        sell_qty_source = "position_size"

                    if sell_qty <= 0:
                        print(f"[pos2sell_live] pos={pid} sell_qty<=0 (fills={filled_buy_qty} size={pos_size}) -> skip")
                        continue

                    intent_side = f"exit_{pid}"
                    intent_outcome_txt = str(outcome)

                    cur.execute(SQL_INTENT_GET, (EXEC_STRATEGY, market_id, intent_outcome_txt, intent_side, limit_px))
                    ir = cur.fetchone()
                    if ir:
                        intent_id = int(ir["id"])
                    else:
                        note = f"exit_from_position id={pid} reason={p.get('exit_reason')} px={limit_px} buy_order={matched_buy_order_id}"
                        try:
                            cur.execute(
                                SQL_INTENT_INS,
                                (
                                    EXEC_STRATEGY,
                                    market_id,
                                    intent_outcome_txt,
                                    intent_side,
                                    limit_px,
                                    Decimal("0"),
                                    "mr_positions_exit",
                                    note,
                                    "queued",
                                ),
                            )
                            result = cur.fetchone()
                            if result is None:
                                print(f"[pos2sell_live] pos={pid} -> intent insert returned None, skipping")
                                continue
                            intent_id = int(result["id"])
                        except Exception as e:
                            print(f"[pos2sell_live] pos={pid} -> intent error: {e}, skipping")
                            continue

                    meta = {
                        "source": "mr_positions_exit",
                        "position_id": pid,
                        "exit_reason": p.get("exit_reason"),
                        "limit_px": str(limit_px),
                        "intent_id": intent_id,
                        "live_execution": True,
                        "buy_order_id": matched_buy_order_id,
                        "sell_qty_source": sell_qty_source,
                        "sell_qty_from_fills": str(filled_buy_qty),
                        "sell_qty_from_position_size": str(pos_size),
                    }

                    cur.execute(
                        SQL_ORDER_INS,
                        (
                            EXEC_STRATEGY,
                            market_id,
                            "sell",
                            sell_qty,
                            limit_px,
                            "submitted",
                            PAPER,
                            intent_id,
                            outcome,
                            json.dumps(meta),
                        ),
                    )
                    result = cur.fetchone()
                    if result is None:
                        # Order already exists for this intent (ON CONFLICT DO NOTHING)
                        print(f"[pos2sell_live] pos={pid} -> skipped (intent already has order)")
                        continue
                    oid = int(result["id"])

                    cur.execute("UPDATE mr_positions SET status='closing' WHERE id=%s", (pid,))
                    print(f"[pos2sell_live] pos={pid} -> sell order_id={oid} qty={sell_qty} src={sell_qty_source} px={limit_px}")

            time.sleep(POLL_SECS)

if __name__ == "__main__":
    main()