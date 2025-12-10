import os
from datetime import datetime, timedelta, timezone, date
from collections import defaultdict, deque
from decimal import Decimal

from psycopg import connect
from psycopg.rows import dict_row

DB_URL = os.getenv("DB_URL")

# Window we want to SCORE (e.g., last 24h)
STATS_LOOKBACK_HOURS = int(os.getenv("WALLET_STATS_LOOKBACK_HOURS", "24"))
# History for inventory warmup (so PnL for sells inside window sees earlier buys)
INVENTORY_LOOKBACK_DAYS = int(os.getenv("WALLET_INVENTORY_LOOKBACK_DAYS", "14"))


def get_conn():
    if not DB_URL:
        raise SystemExit("DB_URL not set")
    return connect(DB_URL, row_factory=dict_row)


def fetch_activity_with_inventory(conn, stats_start_ts, end_ts):
    """
    Fetch trades starting from INVENTORY_LOOKBACK_DAYS ago up to now.
    We need older trades to match SELLS occurring in the stats window.
    """
    inventory_start = end_ts - timedelta(days=INVENTORY_LOOKBACK_DAYS)

    # Ensure inventory_start is not after stats_start_ts
    if inventory_start > stats_start_ts:
        inventory_start = stats_start_ts - timedelta(days=1)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT wallet, market_id, side, price, size, value_usd, ts
            FROM wallet_activity
            WHERE ts >= %s AND ts <= %s
            ORDER BY wallet, ts ASC
            """,
            (inventory_start, end_ts),
        )
        return cur.fetchall()


def _to_dec(v):
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


def compute_wallet_stats(rows, stats_start_ts):
    """
    Processes a stream of trades using FIFO per market_id.
    Only aggregates stats (PnL, trades_count, win_rate, holds) if the trade
    occurred inside the active window (ts >= stats_start_ts).
    """
    markets_traded = set()
    trades_count = 0
    trades_won = 0
    trades_lost = 0
    gross_volume = Decimal("0")
    realized_pnl = Decimal("0")

    # FIFO inventory across entire history window
    # lots[market_id] = deque of [qty, price, ts]
    lots = defaultdict(deque)

    holds = []

    for r in rows:
        market_id = r["market_id"]
        side = (r["side"] or "").lower()
        price = _to_dec(r["price"])
        size = _to_dec(r["size"])
        ts = r["ts"]

        if not market_id or size is None or price is None or ts is None:
            continue

        value = _to_dec(r["value_usd"]) or (price * size)
        in_stats_window = ts >= stats_start_ts

        if in_stats_window:
            markets_traded.add(market_id)
            trades_count += 1
            gross_volume += abs(value)

        if side == "buy":
            lots[market_id].append([size, price, ts])
        elif side == "sell":
            qty_to_close = size
            dq = lots[market_id]
            trade_pnl = Decimal("0")
            while qty_to_close > 0 and dq:
                lot_qty, lot_price, lot_ts = dq[0]
                use_qty = min(lot_qty, qty_to_close)

                chunk_pnl = (price - lot_price) * use_qty

                if in_stats_window:
                    realized_pnl += chunk_pnl
                    trade_pnl += chunk_pnl
                    holds.append((ts - lot_ts).total_seconds())

                lot_qty -= use_qty
                qty_to_close -= use_qty

                if lot_qty <= 0:
                    dq.popleft()
                else:
                    dq[0][0] = lot_qty

            # Count win/loss once per sell trade in the stats window
            if in_stats_window:
                if trade_pnl > 0:
                    trades_won += 1
                elif trade_pnl < 0:
                    trades_lost += 1

    total_closed = trades_won + trades_lost
    win_rate = float(trades_won / total_closed) if total_closed > 0 else 0.0

    median_hold_secs = 0
    if holds:
        srt = sorted(holds)
        m = len(srt)
        if m % 2 == 1:
            median_hold_secs = srt[m // 2]
        else:
            median_hold_secs = (srt[m // 2 - 1] + srt[m // 2]) / 2

    return {
        "wallet": rows[0]["wallet"],
        "markets_traded": len(markets_traded),
        "trades_count": trades_count,
        "trades_won": trades_won,
        "trades_lost": trades_lost,
        "gross_volume": gross_volume,
        "realized_pnl": realized_pnl,
        "win_rate": win_rate,
        "median_hold_secs": median_hold_secs,
    }


def compute_stats(conn, stats_start_ts, now_ts):
    activity = fetch_activity_with_inventory(conn, stats_start_ts, now_ts)
    if not activity:
        return []

    by_wallet = defaultdict(list)
    for r in activity:
        if not r.get("wallet"):
            continue
        by_wallet[r["wallet"]].append(r)

    results = []
    for wallet, rows in by_wallet.items():
        # Ignore wallets whose last trade is before the stats window
        if rows[-1]["ts"] < stats_start_ts:
            continue
        if wallet is None:
            continue
        stats = compute_wallet_stats(rows, stats_start_ts)
        results.append(stats)
    return results


def upsert_stats(conn, day_value, rows):
    if not rows:
        return 0
    with conn.cursor() as cur:
        for r in rows:
            cur.execute(
                """
                INSERT INTO wallet_stats_daily (
                    day,
                    wallet,
                    markets_traded,
                    trades_count,
                    trades_won,
                    trades_lost,
                    gross_volume,
                    realized_pnl,
                    win_rate,
                    median_hold_secs,
                    last_updated
                )
                VALUES (
                    %(day)s,
                    %(wallet)s,
                    %(markets_traded)s,
                    %(trades_count)s,
                    %(trades_won)s,
                    %(trades_lost)s,
                    %(gross_volume)s,
                    %(realized_pnl)s,
                    %(win_rate)s,
                    %(median_hold_secs)s,
                    now()
                )
                ON CONFLICT (day, wallet) DO UPDATE
                SET markets_traded   = EXCLUDED.markets_traded,
                    trades_count     = EXCLUDED.trades_count,
                    trades_won       = EXCLUDED.trades_won,
                    trades_lost      = EXCLUDED.trades_lost,
                    gross_volume     = EXCLUDED.gross_volume,
                    realized_pnl     = EXCLUDED.realized_pnl,
                    win_rate         = EXCLUDED.win_rate,
                    median_hold_secs = EXCLUDED.median_hold_secs,
                    last_updated     = now();
                """,
                {
                    "day": day_value,
                    "wallet": r["wallet"],
                    "markets_traded": r["markets_traded"],
                    "trades_count": r["trades_count"],
                    "trades_won": r["trades_won"],
                    "trades_lost": r["trades_lost"],
                    "gross_volume": r["gross_volume"],
                    "realized_pnl": r["realized_pnl"],
                    "win_rate": r["win_rate"],
                    "median_hold_secs": r["median_hold_secs"],
                },
            )
        conn.commit()
    return len(rows)


def main():
    now_ts = datetime.now(timezone.utc)
    stats_start_ts = now_ts - timedelta(hours=STATS_LOOKBACK_HOURS)
    day_value = date.today()

    conn = get_conn()
    try:
        stats = compute_stats(conn, stats_start_ts, now_ts)
        n = upsert_stats(conn, day_value, stats)
        print(f"[wallet_stats] Processed {n} wallets for {day_value} (Window: {STATS_LOOKBACK_HOURS}h)")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
