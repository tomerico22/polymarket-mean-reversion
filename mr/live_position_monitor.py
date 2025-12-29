#!/usr/bin/env python3
"""
Live Position Monitor - bridges matched buys to positions and monitors exits.
Fixed: Only creates positions for orders that have NEVER had a position created.
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
LOG_PREFIX = "[live_monitor]"

TAKE_PROFIT_PCT = Decimal(os.getenv("MR_TAKE_PROFIT_PCT", "0.15"))
STOP_LOSS_PCT = Decimal(os.getenv("MR_STOP_LOSS_PCT", "0.15"))
MAX_STOP_LOSS_PCT = Decimal(os.getenv("MR_MAX_STOP_LOSS_PCT", "0.20"))
MAX_HOLD_HOURS = int(os.getenv("MR_MAX_HOLD_HOURS", "12"))
SLIPPAGE = Decimal(os.getenv("MR_SLIPPAGE", "0.01"))
PRICE_STALE_SECS = int(os.getenv("MR_PRICE_STALE_SECS", "3600"))


def dec(x) -> Decimal:
    try:
        return Decimal(str(x))
    except (InvalidOperation, TypeError):
        return Decimal("0")


def create_positions_from_matched_buys(cur, now_ts):
    """
    Create mr_positions from matched buy orders.
    Only processes orders that:
    1. Are 'matched' status
    2. Have NEVER had a position created (check by buy_order_id link)
    3. Were created in the last 24 hours (avoid processing ancient orders)
    """
    cur.execute("""
        INSERT INTO mr_positions (
            strategy, market_id, outcome, side, 
            entry_price, entry_ts, size, status, paper,
            dislocation, buy_order_id
        )
        SELECT 
            %s,
            o.market_id,
            o.outcome::text,
            'long',
            COALESCE(
                (SELECT SUM(f.qty * f.price) / NULLIF(SUM(f.qty), 0) FROM strategy_fills f WHERE f.order_id = o.id),
                o.limit_px
            ),
            COALESCE(
                (SELECT MAX(f.ts) FROM strategy_fills f WHERE f.order_id = o.id),
                o.created_at
            ),
            COALESCE(
                (SELECT SUM(f.qty) FROM strategy_fills f WHERE f.order_id = o.id),
                o.qty
            ),
            'open',
            false,
            (o.metadata->>'dislocation')::numeric,
            o.id
        FROM strategy_orders o
        WHERE o.paper = false 
          AND o.strategy = %s 
          AND o.side = 'buy' 
          AND o.status = 'matched'
          AND o.created_at >= NOW() - INTERVAL '24 hours'
          AND NOT EXISTS (
              SELECT 1 FROM mr_positions p 
              WHERE p.buy_order_id = o.id
          )
        RETURNING id, market_id, outcome, entry_price, size, buy_order_id
    """, (STRATEGY, STRATEGY))
    
    for r in cur.fetchall():
        print(f"{LOG_PREFIX} CREATED pos={r['id']} market={r['market_id'][:16]} "
              f"outcome={r['outcome']} px={r['entry_price']} size={r['size']} order={r['buy_order_id']}")


def check_exit_signals(cur, now_ts):
    """Check open positions for exit conditions and set exit_reason."""
    cur.execute("""
        SELECT p.id, p.market_id, p.outcome, p.entry_price, p.entry_ts, p.size
        FROM mr_positions p
        WHERE p.paper = false 
          AND p.strategy = %s 
          AND COALESCE(p.status, 'open') = 'open' 
          AND p.exit_reason IS NULL
    """, (STRATEGY,))
    
    for p in cur.fetchall():
        pos_id = p['id']
        market_id = p['market_id']
        outcome = str(p['outcome'])
        entry_price = dec(p['entry_price'])
        entry_ts = p['entry_ts']
        size = dec(p['size'])
        
        cur.execute("""
            SELECT price, ts FROM raw_trades 
            WHERE market_id = %s AND outcome = %s 
            ORDER BY ts DESC LIMIT 1
        """, (market_id, outcome))
        
        price_row = cur.fetchone()
        if not price_row:
            continue
        
        current_price = dec(price_row['price'])
        price_ts = price_row['ts']
        
        if (now_ts - price_ts).total_seconds() > PRICE_STALE_SECS:
            continue
        
        if entry_price <= 0:
            continue
            
        pnl_pct = (current_price - entry_price) / entry_price
        exit_price = current_price * (Decimal("1") - SLIPPAGE)
        realized_pnl_pct = (exit_price - entry_price) / entry_price
        
        exit_reason = None
        
        if realized_pnl_pct <= -MAX_STOP_LOSS_PCT:
            exit_reason = "max_sl"
        elif pnl_pct >= TAKE_PROFIT_PCT:
            exit_reason = "tp"
        elif pnl_pct <= -STOP_LOSS_PCT:
            exit_reason = "sl"
        elif entry_ts:
            elapsed_hours = (now_ts - entry_ts).total_seconds() / 3600
            if elapsed_hours >= MAX_HOLD_HOURS:
                exit_reason = "time"
        
        if exit_reason:
            cur.execute("""
                UPDATE mr_positions 
                SET exit_reason = %s, exit_signal_price = %s
                WHERE id = %s
            """, (exit_reason, exit_price, pos_id))
            
            est_pnl = (exit_price - entry_price) * size
            print(f"{LOG_PREFIX} EXIT_SIGNAL pos={pos_id} market={market_id[:16]} "
                  f"reason={exit_reason} pnl_pct={float(pnl_pct)*100:.1f}% est_pnl=${float(est_pnl):.2f}")


def print_status(cur):
    cur.execute("""
        SELECT 
            COUNT(*) FILTER (WHERE status = 'open' AND exit_reason IS NULL) as open_monitoring,
            COUNT(*) FILTER (WHERE status = 'open' AND exit_reason IS NOT NULL) as pending_sell,
            COUNT(*) FILTER (WHERE status = 'closing') as closing,
            COUNT(*) FILTER (WHERE status = 'closed') as closed,
            COALESCE(SUM(pnl) FILTER (WHERE status = 'closed'), 0) as total_pnl
        FROM mr_positions
        WHERE paper = false AND strategy = %s
    """, (STRATEGY,))
    
    r = cur.fetchone()
    print(f"{LOG_PREFIX} STATUS open={r['open_monitoring']} pending_sell={r['pending_sell']} "
          f"closing={r['closing']} closed={r['closed']} pnl=${float(r['total_pnl']):.2f}")


def main():
    print(f"{LOG_PREFIX} started strategy={STRATEGY} poll={POLL_SECS}s")
    print(f"{LOG_PREFIX} TP={TAKE_PROFIT_PCT*100}% SL={STOP_LOSS_PCT*100}% MaxHold={MAX_HOLD_HOURS}h")
    
    with psycopg.connect(DB_URL, row_factory=dict_row) as conn:
        conn.autocommit = True
        last_status = datetime.now(timezone.utc)
        
        while True:
            try:
                now_ts = datetime.now(timezone.utc)
                
                with conn.cursor() as cur:
                    create_positions_from_matched_buys(cur, now_ts)
                    check_exit_signals(cur, now_ts)
                    
                    if (now_ts - last_status).total_seconds() >= 60:
                        print_status(cur)
                        last_status = now_ts
                
                time.sleep(POLL_SECS)
                
            except Exception as e:
                print(f"{LOG_PREFIX} ERROR: {e}")
                import traceback
                traceback.print_exc()
                time.sleep(10)


if __name__ == "__main__":
    main()
