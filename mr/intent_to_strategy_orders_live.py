#!/usr/bin/env python3
import os
import time
import json
from decimal import Decimal, InvalidOperation

import psycopg
from psycopg.rows import dict_row

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise SystemExit("DB_URL not set")

EXEC_STRATEGY = os.getenv("MR_STRATEGY", "mean_reversion_v1")
POLL_SECS = float(os.getenv("MR_INTENT_POLL_SECS", "2.0"))
BATCH = int(os.getenv("MR_INTENT_BATCH", "25"))

# Live safety: hard cap per order notional (USD)
LIVE_MAX_ORDER_USD = Decimal(os.getenv("MR_LIVE_MAX_ORDER_USD", "2"))

# Safety gate: refuse unless explicitly enabled
LIVE_FLAG = os.getenv("MR_LIVE_EXECUTION", "").strip().lower()
LIVE_ENABLED = LIVE_FLAG in ("1", "true", "yes", "y", "on")

# Live worker writes paper=false
PAPER = False

# Which intent "source" values should this worker consume?
# Default: mr1 (matches what you're seeing in DB)
INTENT_SOURCE = os.getenv("MR_INTENT_SOURCE", "mr1").strip() or "mr1"


def dec(x) -> Decimal:
    try:
        return Decimal(str(x))
    except (InvalidOperation, TypeError):
        return Decimal("0")


def main():
    if not LIVE_ENABLED:
        raise SystemExit(
            "Refusing to start: MR_LIVE_EXECUTION is not enabled. "
            "Set MR_LIVE_EXECUTION=1 to run live."
        )

    print(
        f"[intent2orders_live] started strategy={EXEC_STRATEGY} "
        f"paper={PAPER} live_enabled={LIVE_ENABLED} source={INTENT_SOURCE} "
        f"poll={POLL_SECS}s batch={BATCH}"
    )

    with psycopg.connect(DB_URL, row_factory=dict_row) as conn:
        conn.autocommit = True

        while True:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH picked AS (
                      SELECT id, created_ts, market_id, outcome, entry_price, size_usd, dislocation
                      FROM mr_trade_intents
                      WHERE source = %s
                        AND status IN ('new','queued')
                        AND order_id IS NULL
                      ORDER BY id ASC
                      LIMIT %s
                      FOR UPDATE SKIP LOCKED
                    )
                    SELECT * FROM picked;
                    """,
                    (INTENT_SOURCE, BATCH),
                )
                intents = cur.fetchall()

                if not intents:
                    time.sleep(POLL_SECS)
                    continue

                for it in intents:
                    intent_id = it["id"]
                    market_id = it["market_id"]
                    outcome = it["outcome"]
                    limit_px = dec(it["entry_price"])
                    size_usd = dec(it["size_usd"])
                    if LIVE_MAX_ORDER_USD > 0 and size_usd > LIVE_MAX_ORDER_USD:
                        size_usd = LIVE_MAX_ORDER_USD

                    dislo = it.get("dislocation")

                    if limit_px <= 0:
                        cur.execute(
                            "UPDATE mr_trade_intents "
                            "SET status='error', note='bad_entry_price' "
                            "WHERE id=%s",
                            (intent_id,),
                        )
                        print(f"[intent2orders_live] intent={intent_id} -> error bad_entry_price")
                        continue

                    qty = (size_usd / limit_px) if size_usd > 0 else Decimal("0")
                    if qty <= 0:
                        cur.execute(
                            "UPDATE mr_trade_intents "
                            "SET status='error', note='bad_size' "
                            "WHERE id=%s",
                            (intent_id,),
                        )
                        print(f"[intent2orders_live] intent={intent_id} -> error bad_size")
                        continue

                    # --------------------------------------------------
                    # DEDUPE GUARD (live buys)
                    # Prevent multiple live/submitted buys for same
                    # market+outcome.
                    # --------------------------------------------------
                    cur.execute(
                        """
                        SELECT 1
                        FROM strategy_orders
                        WHERE paper=false
                          AND strategy=%s
                          AND market_id=%s
                          AND outcome=%s
                          AND side='buy'
                          AND status IN ('submitted','live','matched')
                        LIMIT 1;
                        """,
                        (EXEC_STRATEGY, market_id, int(outcome)),
                    )
                    if cur.fetchone() is not None:
                        cur.execute(
                            """
                            UPDATE mr_trade_intents
                            SET status='skipped',
                                note='dedupe_existing_order'
                            WHERE id=%s
                            """,
                            (intent_id,),
                        )
                        print(
                            f"[intent2orders_live] intent={intent_id} -> skipped dedupe_existing_order "
                            f"market={market_id} outcome={outcome}"
                        )
                        continue

                    metadata = {
                        "source": "mr_trade_intents",
                        "intent_id": int(intent_id),
                        "outcome": int(outcome),
                        "dislocation": str(dislo) if dislo is not None else None,
                        "entry_price": str(limit_px),
                        "size_usd": str(size_usd),
                        "live_execution": True,
                        "intent_source": INTENT_SOURCE,
                    }

                    cur.execute(
                        """
                        INSERT INTO strategy_orders (
                          strategy, market_id, side, qty, limit_px, status, paper,
                          intent_id, outcome, metadata
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                        ON CONFLICT ON CONSTRAINT uq_strategy_orders_intent_id DO NOTHING
                        RETURNING id;
                        """,
                        (
                            EXEC_STRATEGY,
                            market_id,
                            "buy",
                            qty,
                            limit_px,
                            "submitted",
                            PAPER,
                            int(intent_id),
                            int(outcome),
                            json.dumps(metadata),
                        ),
                    )

                    row = cur.fetchone()
                    if row is None:
                        cur.execute(
                            "SELECT id FROM strategy_orders WHERE intent_id=%s",
                            (int(intent_id),),
                        )
                        row2 = cur.fetchone()
                        if row2 is None:
                            cur.execute(
                                "UPDATE mr_trade_intents "
                                "SET status='error', note='order_missing_after_conflict' "
                                "WHERE id=%s",
                                (intent_id,),
                            )
                            print(f"[intent2orders_live] intent={intent_id} -> error order_missing_after_conflict")
                            continue
                        order_id = row2["id"]
                    else:
                        order_id = row["id"]

                    cur.execute(
                        "UPDATE mr_trade_intents SET status='queued', order_id=%s WHERE id=%s",
                        (order_id, intent_id),
                    )

                    print(
                        f"[intent2orders_live] intent={intent_id} -> strategy_orders.id={order_id} "
                        f"market={market_id} outcome={outcome} px={limit_px} qty={qty}"
                    )

            time.sleep(0.05)


if __name__ == "__main__":
    main()