#!/usr/bin/env python3
import os, time
from decimal import Decimal
import psycopg
from psycopg.rows import dict_row

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise SystemExit("DB_URL not set")

POLL_SECS = float(os.getenv("MR_SETTLE_POLL_SECS", "2.0"))
BATCH = int(os.getenv("MR_SETTLE_BATCH", "50"))

def dec(x):
    try:
        return Decimal(str(x))
    except Exception:
        return Decimal("0")

def main():
    print(f"[settle_sells] started poll={POLL_SECS}s batch={BATCH}")
    with psycopg.connect(DB_URL, row_factory=dict_row) as conn:
        conn.autocommit = True
        while True:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT o.id AS order_id,
                           (o.metadata->>'position_id')::int AS position_id
                    FROM strategy_orders o
                    WHERE o.paper=false
                      AND o.side='sell'
                      AND o.status='matched'
                      AND (o.metadata->>'position_id') IS NOT NULL
                    ORDER BY o.created_at ASC
                    LIMIT {int(BATCH)};
                """)
                orders = cur.fetchall()

                for o in orders:
                    order_id = int(o["order_id"])
                    pid = int(o["position_id"])

                    cur.execute(f"""
                        SELECT SUM(f.qty) AS q,
                               SUM(f.qty * f.price) AS qp,
                               MAX(f.ts) AS exit_ts
                        FROM strategy_fills f
                        WHERE f.order_id = {order_id};
                    """)
                    agg = cur.fetchone()
                    q = dec(agg["q"])
                    qp = dec(agg["qp"])
                    exit_ts = agg["exit_ts"]
                    if q <= 0 or qp <= 0 or exit_ts is None:
                        continue

                    exit_px = qp / q

                    cur.execute(f"SELECT entry_price, size, status FROM mr_positions WHERE id={pid};")
                    p = cur.fetchone()
                    if not p:
                        continue
                    if (p.get("status") or "open") == "closed":
                        continue

                    entry_px = dec(p["entry_price"])
                    size = dec(p["size"])
                    pnl = (exit_px - entry_px) * size

                    # use SQL literals - safe here since values come from DB + our own math
                    cur.execute(f"""
                        UPDATE mr_positions
                        SET status='closed',
                            exit_price={exit_px},
                            exit_ts='{exit_ts}',
                            exit_price_ts='{exit_ts}',
                            exit_value_usd={exit_px * size},
                            pnl={pnl}
                        WHERE id={pid};
                    """)

                    cur.execute(f"UPDATE strategy_orders SET status='settled' WHERE id={order_id};")

                    print(f"[settle_sells] pos={pid} closed via order={order_id} exit_px={exit_px} pnl={pnl}")

            time.sleep(POLL_SECS)

if __name__ == "__main__":
    main()
