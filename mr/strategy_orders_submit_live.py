import os, time, json, requests
from decimal import Decimal, ROUND_UP
from psycopg import connect
from psycopg.rows import dict_row

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, OrderArgs, PartialCreateOrderOptions

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise SystemExit("DB_URL not set")

CLOB_HTTP = os.getenv("POLY_CLOB_HTTP_BASE", "https://clob.polymarket.com").rstrip("/")
CHAIN_ID = int((os.getenv("POLY_CHAIN_ID") or "137").strip())

PK     = (os.getenv("PRIVATE_KEY") or "").strip()       # MetaMask private key
FUNDER = (os.getenv("FUNDER_ADDRESS") or "").strip()    # Polymarket proxy/safe address

DRY_RUN = (os.getenv("MR_SUBMIT_DRY_RUN", "1").strip() != "0")
LIVE_OK = (os.getenv("MR_SUBMIT_LIVE_OK", "").strip().upper() in ("YES","Y","TRUE","1"))
# Safety: require explicit LIVE_OK to actually post
if (not DRY_RUN) and (not LIVE_OK):
    DRY_RUN = True
    print("[submit_live] SAFETY: MR_SUBMIT_DRY_RUN=0 but MR_SUBMIT_LIVE_OK not set -> forcing DRY_RUN")


# Optional safety cap (USD notional). Example: MR_SUBMIT_MAX_USD=2
MAX_USD = (os.getenv("MR_SUBMIT_MAX_USD") or "").strip()
MAX_USD = Decimal(MAX_USD) if MAX_USD else None

# Force minimum marketable notional buffer
MIN_NOTIONAL_USD = Decimal((os.getenv("MR_MIN_NOTIONAL_USD") or "1.05").strip())

# Cooldown per market between POSTs (seconds)
MARKET_COOLDOWN_SECS = int((os.getenv("MR_SUBMIT_MARKET_COOLDOWN_SECS") or "30").strip())

# Enforce at most 1 active live order per market_id (based on statuses)
ENFORCE_ONE_PER_MARKET = (os.getenv("MR_SUBMIT_ENFORCE_ONE_PER_MARKET", "1").strip() != "0")

def fetch_market(mid: str) -> dict:
    r = requests.get(f"{CLOB_HTTP}/markets/{mid}", timeout=30)
    r.raise_for_status()
    return r.json()

def extract_token_id(market: dict, outcome_idx: int) -> str:
    toks = market.get("tokens")
    if not (isinstance(toks, list) and len(toks) > outcome_idx):
        raise RuntimeError("market.tokens missing/unexpected")
    item = toks[outcome_idx]
    tid = item.get("token_id") or item.get("tokenId")
    if not tid:
        raise RuntimeError("token id not found in market.tokens")
    return str(tid)

def norm_side(s: str) -> str:
    s0 = (s or "").strip()
    su = s0.upper()
    if su in ("BUY", "B", "YES"):
        return "BUY"
    if su in ("SELL", "S", "NO"):
        return "SELL"
    if s0.lower() == "buy":
        return "BUY"
    if s0.lower() == "sell":
        return "SELL"
    raise RuntimeError(f"bad side={s}")

def ceil_decimal(x: Decimal) -> Decimal:
    return x.to_integral_value(rounding=ROUND_UP)

def now_epoch() -> Decimal:
    return Decimal(str(time.time()))

def main():
    # Pick ONE eligible order:
    # - paper=false AND status='submitted'
    # - optionally enforce one active (live/matched) per market
    # - apply market cooldown based on metadata.post_ts of any live/matched order in same market
    with connect(DB_URL, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH candidates AS (
                  SELECT *
                  FROM strategy_orders so
                  WHERE so.paper = false
                    AND so.status = 'submitted'
                    AND (
                      %s = false OR NOT EXISTS (
                        SELECT 1
                        FROM strategy_orders so2
                        WHERE so2.paper = false
                          AND so2.market_id = so.market_id
                          AND so2.side = so.side
                          AND so2.status IN ('live')
                      )
                    )
                    AND NOT EXISTS (
                      SELECT 1
                      FROM strategy_orders so3
                      WHERE so3.paper = false
                        AND so3.market_id = so.market_id
                        AND so3.side = so.side
                        AND so3.status IN ('live')
                        AND (
                          (so3.metadata ? 'post_ts')
                          AND (extract(epoch from now()) - (so3.metadata->>'post_ts')::double precision) < %s
                        )
                    )
                  ORDER BY so.created_at ASC
                  LIMIT 1
                )
                SELECT * FROM candidates;
                """,
                (ENFORCE_ONE_PER_MARKET, MARKET_COOLDOWN_SECS),
            )
            row = cur.fetchone()

    if not row:
        print("[submit_live] no eligible paper=false submitted orders found (cooldown/one-per-market may be blocking)")
        return

    db_order_id = int(row["id"])
    market_id   = row["market_id"]
    outcome     = int(row["outcome"])
    side        = norm_side(row["side"])
    price       = Decimal(str(row["limit_px"]))

    m = fetch_market(market_id)

    # hard skip closed/non-orderable markets before signing/posting
    if bool(m.get("closed")) or (not bool(m.get("accepting_orders", True))) or (not bool(m.get("enable_order_book", True))):
        reason = {
            "skip_reason": "market_not_orderable",
            "closed": bool(m.get("closed")),
            "accepting_orders": bool(m.get("accepting_orders", False)),
            "enable_order_book": bool(m.get("enable_order_book", False)),
        }
        print("[submit_live] SKIP:", reason)
        with connect(DB_URL, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE strategy_orders
                    SET status = %s,
                        metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb
                    WHERE id = %s
                    """,
                    ("skipped", json.dumps(reason), db_order_id),
                )
            conn.commit()
        return

    token_id   = extract_token_id(m, outcome)
    tick_size  = str(m.get("minimum_tick_size", "0.01"))
    neg_risk   = bool(m.get("neg_risk", False))
    min_sz     = Decimal(str(m.get("minimum_order_size", "1")))

    desired_qty = Decimal(str(row["qty"]))
    min_qty_for_notional = ceil_decimal(MIN_NOTIONAL_USD / price)

    final_qty = max(desired_qty, min_sz, min_qty_for_notional)

    capped = False
    if MAX_USD is not None:
        cap_qty = ceil_decimal(MAX_USD / price)
        final_qty = max(min_sz, min_qty_for_notional, min(final_qty, cap_qty))
        capped = True

    final_notional = (final_qty * price)

    print("[submit_live] DRY_RUN:", DRY_RUN)
    print("[submit_live] db_order_id:", db_order_id)
    print("[submit_live] market_id :", market_id)
    print("[submit_live] token_id  :", token_id)
    print("[submit_live] side/px   :", side, str(price))
    print("[submit_live] qty       :", str(final_qty))
    print("[submit_live] notional  :", str(final_notional))
    if capped:
        print("[submit_live] NOTE: capped via MR_SUBMIT_MAX_USD =", str(MAX_USD))
    print("[submit_live] market tick/min_sz/neg_risk:", tick_size, str(min_sz), neg_risk)

    if DRY_RUN:
        return

    creds = ApiCreds(
        api_key=(os.getenv("POLY_API_KEY") or "").strip(),
        api_secret=(os.getenv("POLY_API_SECRET") or "").strip(),
        api_passphrase=(os.getenv("POLY_API_PASSPHRASE") or "").strip(),
    )

    client = ClobClient(
        CLOB_HTTP,
        chain_id=CHAIN_ID,
        key=PK,
        creds=creds,
        funder=FUNDER,
        signature_type=2,  # proxy wallet
    )

    order_args = OrderArgs(
        token_id=token_id,
        side=side,
        price=float(price),
        size=float(final_qty),
        fee_rate_bps=0,
    )
    options = PartialCreateOrderOptions(tick_size=tick_size, neg_risk=neg_risk)
    signed = client.create_order(order_args, options)

    # POST with orderbook-missing skip
    try:
        resp = client.post_order(signed, orderType="GTC")
    except Exception as e:
        msg_l = str(e).lower()
        if "orderbook" in msg_l and ("does not exist" in msg_l or "no orderbook exists" in msg_l):
            reason = {"skip_reason": "orderbook_missing", "error": f"{type(e).__name__}: {str(e)}"}
            print("[submit_live] SKIP:", reason)
            with connect(DB_URL, row_factory=dict_row) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE strategy_orders
                        SET status = %s,
                            metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb
                        WHERE id = %s
                        """,
                        ("skipped", json.dumps(reason), db_order_id),
                    )
                conn.commit()
            return
        raise

    print("[submit_live] POST RESP:", resp)

    clob_order_id = resp.get("orderID") or resp.get("orderId") or resp.get("id")
    post_status = resp.get("status") or "posted"
    txs = resp.get("transactionsHashes") or resp.get("transactionHashes") or []

    # Update order metadata + status
    post_ts = float(time.time())
    with connect(DB_URL, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT metadata FROM strategy_orders WHERE id=%s FOR UPDATE", (db_order_id,))
            md = cur.fetchone()["metadata"] or {}
            md = dict(md)
            md.update({
                "clob_order_id": clob_order_id,
                "post_status": post_status,
                "post_txs": txs,
                "post_qty": str(final_qty),
                "post_px": str(price),
                "post_notional": str(final_notional),
                "post_ts": post_ts,
                "post_capped": bool(capped),
            })
            cur.execute(
                "UPDATE strategy_orders SET status=%s, metadata=%s WHERE id=%s",
                (post_status, json.dumps(md), db_order_id)
            )

            # CRITICAL: write fills so dashboard/SQL can see live results.
            # For now, if Polymarket says 'matched', we insert a single fill row.
            if str(post_status).lower() == "matched":
                # qty/price columns are your DB fill schema
                cur.execute(
                    """
                    INSERT INTO strategy_fills (order_id, qty, price, ts, paper)
                    VALUES (%s, %s, %s, now(), false)
                    """,
                    (db_order_id, str(final_qty), str(price)),
                )

        conn.commit()

    print("[submit_live] DB updated: strategy_orders.id =", db_order_id, "status =", post_status)

if __name__ == "__main__":
    main()
