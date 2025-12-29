#!/usr/bin/env python3
"""
Exit Monitor - ONLY monitors open positions for exit signals.
Does NOT create positions from orders.
"""
import os
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import psycopg
from psycopg.rows import dict_row

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise SystemExit("DB_URL not set")

STRATEGY = os.getenv("MR_STRATEGY", "mean_reversion_v1")
POLL_SECS = float(os.getenv("MR_MONITOR_POLL_SECS", "5"))

TAKE_PROFIT_PCT = Decimal(os.getenv("MR_TAKE_PROFIT_PCT", "0.15"))
STOP_LOSS_PCT = Decimal(os.getenv("MR_STOP_LOSS_PCT", "0.15"))
MAX_HOLD_HOURS = int(os.getenv("MR_MAX_HOLD_HOURS", "12"))
PRICE_STALE_SECS = int(os.getenv("MR_PRICE_STALE_SECS", "3600"))


def dec(x) -> Decimal:
    try:
        return Decimal(str(x))
    except:
        return Decimal("0")


def main():
    print(f"[exit_monitor] started strategy={STRATEGY} TP={TAKE_PROFIT_PCT*100}% SL={STOP_LOSS_PCT*100}%")
    
    with psycopg.connect(DB_URL, row_factory=dict_row) as conn:
        conn.autocommit = True
        
        while True:
            now = datetime.now(timezone.utc)
            
            with conn.cursor() as cur:
                # Get open positions without exit_reason
                cur.execute("""
                    SELECT id, market_id, outcome, entry_price, entry_ts, size
                    FROM mr_positions
                    WHERE paper = false 
                      AND strategy = %s 
                      AND status = 'open' 
                      AND exit_reason IS NULL
                """, (STRATEGY,))
                
                for p in cur.fetchall():
                    pos_id = p['id']
                    market_id = p['market_id']
                    outcome = str(p['outcome'])
                    entry_price = dec(p['entry_price'])
                    entry_ts = p['entry_ts']
                    
                    # Get current price
                    cur.execute("""
                        SELECT price, ts FROM raw_trades 
                        WHERE market_id = %s AND outcome = %s 
                        ORDER BY ts DESC LIMIT 1
                    """, (market_id, outcome))
                    
                    pr = cur.fetchone()
                    if not pr:
                        continue
                    
                    current = dec(pr['price'])
                    price_age = (now - pr['ts']).total_seconds()
                    
                    if price_age > PRICE_STALE_SECS:
                        continue
                    
                    if entry_price <= 0:
                        continue
                    
                    pnl_pct = (current - entry_price) / entry_price
                    exit_price = current * Decimal("0.99")  # 1% slippage
                    
                    exit_reason = None
                    if pnl_pct >= TAKE_PROFIT_PCT:
                        exit_reason = "tp"
                    elif pnl_pct <= -STOP_LOSS_PCT:
                        exit_reason = "sl"
                    elif entry_ts and (now - entry_ts).total_seconds() / 3600 >= MAX_HOLD_HOURS:
                        exit_reason = "time"
                    
                    if exit_reason:
                        cur.execute("""
                            UPDATE mr_positions 
                            SET exit_reason = %s, exit_signal_price = %s
                            WHERE id = %s
                        """, (exit_reason, float(exit_price), pos_id))
                        
                        print(f"[exit_monitor] EXIT pos={pos_id} reason={exit_reason} "
                              f"entry={entry_price} current={current} pnl={float(pnl_pct)*100:.1f}%")
            
            time.sleep(POLL_SECS)


if __name__ == "__main__":
    main()
