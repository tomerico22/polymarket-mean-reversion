import os
import time
import requests
from datetime import datetime, timezone, timedelta

from psycopg import connect
from psycopg.rows import tuple_row

from . import live_trades_ws as trades_ws
from pipeline.ingestors.fetch_markets import ensure_market_exists


DB_URL = os.getenv("DB_URL")
DATA_BASE = os.getenv("POLY_TRADES_HTTP_BASE", "https://data-api.polymarket.com").rstrip("/")
TRADES_URL = f"{DATA_BASE}/trades"

POLL_INTERVAL = int(os.getenv("TRADES_POLL_INTERVAL", "60"))
POLL_LIMIT = int(os.getenv("TRADES_POLL_LIMIT", "500"))
POLL_MAX_BATCHES = int(os.getenv("TRADES_POLL_MAX_BATCHES", "5"))

if not DB_URL:
    raise SystemExit("DB_URL not set; cannot poll trades.")


def fetch_trades(limit=500, offset=0):
    params = {"limit": limit, "offset": offset}
    r = requests.get(TRADES_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict):
        data = data.get("data", [])
    return data if isinstance(data, list) else []


def initial_last_seen(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT max(ts) FROM trades;")
        row = cur.fetchone()
        if row and row[0]:
            return row[0]
    return datetime.now(timezone.utc) - timedelta(minutes=10)


def main():
    print(
        f"[poll_trades] starting HTTP poller {TRADES_URL} "
        f"(interval={POLL_INTERVAL}s, limit={POLL_LIMIT})"
    )
    known_markets = set()
    missing_markets = set()
    last_seen = None
    while True:
        try:
            with connect(DB_URL, row_factory=tuple_row) as conn:
                if last_seen is None:
                    last_seen = initial_last_seen(conn)

                threshold = last_seen - timedelta(seconds=5) if last_seen else None
                max_seen = last_seen
                total_inserted = 0

                offset = 0
                batches = 0
                keep_fetching = True

                while keep_fetching and batches < POLL_MAX_BATCHES:
                    trades = fetch_trades(limit=POLL_LIMIT, offset=offset)
                    if not trades:
                        break

                    batches += 1
                    batch_new = False

                    with conn.cursor() as cur:
                        for entry in reversed(trades):
                            mapped = trades_ws.map_trade(entry)
                            if not mapped:
                                continue
                            trade_row, wallet_rows = mapped
                            trade_ts = trade_row[8]
                            if threshold and trade_ts <= threshold:
                                continue
                            (
                                trade_id,
                                market_id,
                                taker,
                                maker,
                                side,
                                price,
                                size,
                                value_usd,
                                ts,
                                asset_id,
                                outcome,
                            ) = trade_row

                            # Ensure market metadata exists so dashboards can show names/tags.
                            if market_id not in known_markets and market_id not in missing_markets:
                                cur.execute(
                                    "SELECT question FROM markets WHERE market_id = %s",
                                    (market_id,),
                                )
                                row = cur.fetchone()
                                if row and row[0]:
                                    known_markets.add(market_id)
                                else:
                                    ok = ensure_market_exists(market_id, timeout=3)
                                    if ok:
                                        known_markets.add(market_id)
                                    else:
                                        missing_markets.add(market_id)

                            raw_trade_row = (
                                ts,
                                market_id,
                                str(outcome) if outcome is not None else None,
                                taker,
                                maker or taker,
                                (side or "").lower() if side else None,
                                size,
                                price,
                                trade_id,
                            )
                            cur.execute(trades_ws.INSERT_TRADE_SQL, trade_row)
                            cur.execute(trades_ws.INSERT_RAW_TRADE_SQL, raw_trade_row)
                            for wrow in wallet_rows:
                                cur.execute(trades_ws.INSERT_WALLET_SQL, wrow)
                            total_inserted += 1
                            batch_new = True
                            if not max_seen or trade_ts > max_seen:
                                max_seen = trade_ts

                    conn.commit()

                    if not batch_new:
                        keep_fetching = False
                    offset += POLL_LIMIT

                if max_seen:
                    last_seen = max_seen

                if total_inserted:
                    print(
                        f"[poll_trades] inserted {total_inserted} new rows "
                        f"(last_ts={last_seen.isoformat()})"
                    )

        except Exception as exc:
            print(f"[poll_trades] error: {exc}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
