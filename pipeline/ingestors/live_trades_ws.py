import os, asyncio, orjson, signal, sys
from datetime import datetime, timezone
import websockets
from psycopg import connect
from psycopg.rows import tuple_row

DB_URL = os.getenv("DB_URL")
# Preferred working endpoint (probe: ws-subscriptions-clob.polymarket.com/ws/market)
TRADES_WSS = os.getenv("POLY_TRADES_WSS", "wss://ws-subscriptions-clob.polymarket.com/ws/market")
DEBUG_PRINT_FIRST = 3  # print first N raw frames to inspect payload shape

INSERT_TRADE_SQL = """
INSERT INTO trades (trade_id, market_id, taker, maker, side, price, size, value_usd, ts, asset_id, outcome_index)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (trade_id) DO UPDATE
SET
  asset_id      = COALESCE(trades.asset_id, EXCLUDED.asset_id),
  outcome_index = COALESCE(trades.outcome_index, EXCLUDED.outcome_index),
  taker         = COALESCE(trades.taker, EXCLUDED.taker),
  maker         = COALESCE(trades.maker, EXCLUDED.maker),
  side          = COALESCE(trades.side, EXCLUDED.side),
  price         = COALESCE(trades.price, EXCLUDED.price),
  size          = COALESCE(trades.size, EXCLUDED.size),
  value_usd     = COALESCE(trades.value_usd, EXCLUDED.value_usd),
  ts            = COALESCE(trades.ts, EXCLUDED.ts)
"""

INSERT_WALLET_SQL = """
INSERT INTO wallet_activity (wallet, market_id, side, price, size, value_usd, role, tx_hash, ts)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

INSERT_RAW_TRADE_SQL = """
INSERT INTO raw_trades (
    ts,
    market_id,
    outcome,
    taker,
    maker,
    side,
    qty,
    price,
    tx_hash
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

def to_ts(x):
    if x is None: return None
    try:
        xv = float(x)
        if xv > 1e12: xv /= 1000.0  # ms -> s
        return datetime.fromtimestamp(xv, tz=timezone.utc)
    except Exception:
        try: return datetime.fromisoformat(str(x).replace("Z","+00:00")).astimezone(timezone.utc)
        except Exception: return None

def num(v):
    try: return float(v)
    except: return None

def map_trade(t):
    # Typical payload keys from Polymarket trade stream:
    # transactionHash, conditionId, timestamp, price, size, side, proxyWallet, maker (optional)
    trade_id  = t.get("transactionHash") or t.get("id")
    market_id = t.get("conditionId") or t.get("market")
    ts        = to_ts(t.get("timestamp") or t.get("time"))
    if not trade_id or not market_id or ts is None:
        return None
    side      = (t.get("side") or "").upper() or None
    price     = num(t.get("price"))
    size      = num(t.get("size"))
    value_usd = (price * size) if (price is not None and size is not None) else None
    taker     = t.get("proxyWallet") or t.get("taker")
    maker     = t.get("maker")
    asset_id  = t.get("asset")
    outcome   = t.get("outcomeIndex")
    try:
        outcome = int(outcome) if outcome is not None else None
    except Exception:
        outcome = None

    trade_row = (
        trade_id,
        market_id,
        taker,
        maker,
        side,
        price,
        size,
        value_usd,
        ts,
        str(asset_id) if asset_id else None,
        outcome,
    )

    wallet_rows = []
    if taker:
        wallet_rows.append(
            (
                taker,
                market_id,
                side,
                price,
                size,
                value_usd,
                "taker",
                trade_id,
                ts,
            )
        )
    if maker:
        maker_side = None
        if side == "BUY":
            maker_side = "SELL"
        elif side == "SELL":
            maker_side = "BUY"
        wallet_rows.append(
            (
                maker,
                market_id,
                maker_side or side,
                price,
                size,
                value_usd,
                "maker",
                trade_id,
                ts,
            )
        )

    return trade_row, wallet_rows

async def subscribe_trades(ws):
    """
    Try a couple of common subscribe formats:
    - {"type":"subscribe","channel":"trades"}
    - {"channel":"trades","type":"subscribe"}
    If the server replies with an ack, great; if not, we still proceed and parse incoming msgs.
    """
    frames = [
        {"type": "subscribe", "channel": "trades"},
        {"channel": "trades", "type": "subscribe"},
    ]
    for f in frames:
        try:
            await ws.send(orjson.dumps(f).decode())
        except Exception:
            pass

async def run():
    if not DB_URL:
        print("ERROR: DB_URL not set"); sys.exit(1)

    stop = asyncio.Event()
    try:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
            except NotImplementedError:
                pass
    except RuntimeError:
        pass

    while not stop.is_set():
        conn = None
        try:
            conn = connect(DB_URL, row_factory=tuple_row)
            conn.autocommit = True
            async with websockets.connect(TRADES_WSS, ping_interval=20, ping_timeout=20) as ws:
                print(f"[ws] connected to {TRADES_WSS}")
                await subscribe_trades(ws)

                seen = 0
                async for raw in ws:
                    try:
                        if seen < DEBUG_PRINT_FIRST:
                            print(f"[ws raw] {raw}")
                        seen += 1

                        try:
                            data = orjson.loads(raw)
                        except Exception as e:
                            if seen <= DEBUG_PRINT_FIRST + 2:
                                print(f"[ws] json decode error: {e} (raw type={type(raw)})")
                            continue

                        # Some gateways wrap as {"channel":"trades","data":[...]} or just a list of trades.
                        if isinstance(data, dict) and "data" in data:
                            events = data["data"]
                        else:
                            events = data if isinstance(data, list) else [data]

                        if not events:
                            continue

                        with conn.cursor() as cur:
                            for ev in events:
                                mapped = map_trade(ev)
                                if not mapped:
                                    continue
                                trade_row, wallet_rows = mapped

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

                                # insert into main trades table
                                cur.execute(INSERT_TRADE_SQL, trade_row)

                                # insert into raw_trades for smartflow
                                cur.execute(INSERT_RAW_TRADE_SQL, raw_trade_row)

                                # existing wallet_activity inserts
                                for wrow in wallet_rows:
                                    cur.execute(INSERT_WALLET_SQL, wrow)
                    except Exception as e:
                        print(f"[ws] parse/insert error: {e}")
            await asyncio.sleep(1.0)  # brief backoff before reconnect
        except Exception as e:
            print(f"[ws] connection error: {e}; retrying in 3s")
            await asyncio.sleep(3.0)
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

if __name__ == "__main__":
    asyncio.run(run())
