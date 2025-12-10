import os
import json
import time
import math
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

from psycopg import connect
from psycopg.rows import dict_row

DB_URL = os.getenv("DB_URL")

# Strategies
EXEC_STRATEGY = os.getenv("SMARTFLOW_EXEC_STRATEGY", "sm_smartflow_paper_v1")
SIGNAL_STRATEGY = os.getenv("SMARTFLOW_SIGNAL_STRATEGY", "sm_smartflow_v1")

# Entry filters
MIN_SCORE = Decimal(os.getenv("SMARTFLOW_MIN_SCORE", "0.6"))
MIN_SMART_WALLETS = int(os.getenv("SMARTFLOW_MIN_SMART_WALLETS", "3"))
MIN_SMART_NET_FLOW = Decimal(os.getenv("SMARTFLOW_MIN_SMART_NET_FLOW", "150"))
MIN_PRICE = Decimal(os.getenv("SMARTFLOW_MIN_PRICE", "0.02"))
MAX_PRICE = Decimal(os.getenv("SMARTFLOW_MAX_PRICE", "0.98"))
MIN_ABS_FLOW_USD = Decimal(os.getenv("SMARTFLOW_MIN_ABS_FLOW_USD", "400"))
MIN_WALLETS = int(os.getenv("SMARTFLOW_MIN_WALLETS", "8"))
MIN_SMART_WALLETS_FLOW = int(os.getenv("SMARTFLOW_MIN_SMART_WALLETS", "2"))

# Dynamic sizing
BASE_RISK_USD = Decimal(os.getenv("SMARTFLOW_BASE_RISK_USD", "50"))
MAX_RISK_USD = Decimal(os.getenv("SMARTFLOW_MAX_RISK_USD", "200"))

# Exits (percent-based)
TP_PCT = Decimal(os.getenv("SMARTFLOW_TP_PCT", "0.15"))
SL_PCT = Decimal(os.getenv("SMARTFLOW_SL_PCT", "0.10"))
MAX_HOLD_HOURS = float(os.getenv("SMARTFLOW_MAX_HOLD_HOURS", "4"))

# Paper slippage
SLIPPAGE = Decimal(os.getenv("SMARTFLOW_PAPER_SLIPPAGE", "0.01"))

# Flow reversal stop
FLOW_REVERSAL_SCORE = Decimal(os.getenv("SMARTFLOW_REVERSAL_SCORE", "0.4"))
FLOW_REVERSAL_NET_FLOW = Decimal(os.getenv("SMARTFLOW_REVERSAL_NET_FLOW", "0"))

# Multi timeframe filter
SHORT_WINDOW_SECS = int(os.getenv("SMARTFLOW_SHORT_WINDOW_SECS", "300"))
MID_WINDOW_SECS = int(os.getenv("SMARTFLOW_MID_WINDOW_SECS", "1800"))
MIN_SHORT_SMART_FLOW = Decimal(os.getenv("SMARTFLOW_MIN_SHORT_SMART_FLOW", "100"))
MIN_MID_SMART_FLOW = Decimal(os.getenv("SMARTFLOW_MIN_MID_SMART_FLOW", "-50"))

# Cooldown per market after close
MARKET_COOLDOWN_SECS = int(os.getenv("SMARTFLOW_MARKET_COOLDOWN_SECS", "600"))

# Smart wallet consensus guards
MIN_SMART_CONSENSUS = int(os.getenv("SMARTFLOW_MIN_SMART_CONSENSUS", "2"))
MIN_CONSENSUS_SCORE = float(os.getenv("SMARTFLOW_MIN_CONSENSUS_SCORE", "60"))
CONSENSUS_LOOKBACK_MIN = int(os.getenv("SMARTFLOW_CONSENSUS_LOOKBACK_MIN", "10"))

LOOP_SLEEP = int(os.getenv("SMARTFLOW_EXEC_LOOP_SLEEP", "5"))

# Signal watermark
LAST_PROCESSED_SIGNAL_ID = 0
INITIALIZED_WATERMARK = False


def get_conn():
    if not DB_URL:
        raise SystemExit("DB_URL not set")
    return connect(DB_URL, row_factory=dict_row)


def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_positions (
                id             serial PRIMARY KEY,
                strategy       text,
                market_id      text,
                outcome        text,
                side           text,
                entry_price    numeric,
                entry_ts       timestamptz,
                size           numeric,
                score          numeric,
                smart_wallets  integer,
                smart_net_flow numeric,
                status         text DEFAULT 'open',
                exit_price     numeric,
                exit_ts        timestamptz,
                pnl            numeric
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_fills (
                id           serial PRIMARY KEY,
                position_id  integer REFERENCES paper_positions(id),
                price        numeric,
                ts           timestamptz,
                reason       text
            );
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_paper_positions_open ON paper_positions(status);"
        )
    conn.commit()


def parse_reason(reason_val):
    if isinstance(reason_val, dict):
        return reason_val
    if isinstance(reason_val, str):
        try:
            return json.loads(reason_val)
        except Exception:
            return {}
    return {}


def to_dec(val, default=Decimal("0")):
    try:
        return Decimal(str(val))
    except (InvalidOperation, TypeError):
        return default


def fetch_new_signals(cur, limit=50):
    global LAST_PROCESSED_SIGNAL_ID, INITIALIZED_WATERMARK
    if not INITIALIZED_WATERMARK:
        cur.execute(
            "SELECT MAX(id) FROM strategy_signals WHERE strategy=%s",
            (SIGNAL_STRATEGY,),
        )
        row = cur.fetchone()
        LAST_PROCESSED_SIGNAL_ID = row["max"] or 0 if row else 0
        INITIALIZED_WATERMARK = True
        return []

    cur.execute(
        """
        SELECT *
        FROM strategy_signals
        WHERE strategy = %s
          AND id > %s
        ORDER BY id ASC
        LIMIT %s
        """,
        (SIGNAL_STRATEGY, LAST_PROCESSED_SIGNAL_ID, limit),
    )
    rows = cur.fetchall()
    if rows:
        LAST_PROCESSED_SIGNAL_ID = rows[-1]["id"]
    return rows


def risk_usd_for_signal(score, smart_net_flow):
    boost_from_score = max(Decimal("0"), (score - MIN_SCORE) * Decimal("200"))
    boost_from_flow = max(Decimal("0"), smart_net_flow / Decimal("300"))
    risk = BASE_RISK_USD + boost_from_score + boost_from_flow
    if risk > MAX_RISK_USD:
        risk = MAX_RISK_USD
    if risk < BASE_RISK_USD:
        risk = BASE_RISK_USD
    return risk


def get_last_price(cur, market_id, outcome):
    cur.execute(
        """
        SELECT price
        FROM raw_trades
        WHERE market_id = %s AND outcome = %s
        ORDER BY ts DESC
        LIMIT 1
        """,
        (market_id, outcome),
    )
    row = cur.fetchone()
    if not row or row["price"] is None:
        return None
    return to_dec(row["price"], None)


def get_market_category_and_tags(cur, market_id):
    try:
        cur.execute(
            """
            SELECT category, tags
            FROM markets
            WHERE market_id = %s
            LIMIT 1
            """,
            (market_id,),
        )
        row = cur.fetchone()
        if not row:
            return None, set()
        category = row.get("category")
        tags_val = row.get("tags")
        tags_set = set()
        if isinstance(tags_val, list):
            tags_set = {str(t).lower() for t in tags_val}
        elif isinstance(tags_val, str):
            tags_set = {t.strip().lower() for t in tags_val.split(",") if t.strip()}
        return category, tags_set
    except Exception:
        return None, set()


ALLOWED_CATEGORIES = set(
    [c.strip() for c in os.getenv("SMARTFLOW_ALLOWED_CATEGORIES", "").split(",") if c.strip()]
)
EXCLUDED_TAGS = set(
    [t.strip() for t in os.getenv("SMARTFLOW_EXCLUDED_TAGS", "").split(",") if t.strip()]
)


def category_allowed(cur, market_id):
    if not ALLOWED_CATEGORIES and not EXCLUDED_TAGS:
        return True
    category, tags = get_market_category_and_tags(cur, market_id)
    if ALLOWED_CATEGORIES and category not in ALLOWED_CATEGORIES:
        return False
    if EXCLUDED_TAGS and tags:
        if any(t in EXCLUDED_TAGS for t in tags):
            return False
    return True


def multi_timeframe_ok(cur, market_id, outcome):
    # Disabled: allow all snapshots without multi-timeframe gating
    return True


def can_open_position(cur, market_id, outcome, side):
    cur.execute(
        """
        SELECT 1
        FROM paper_positions
        WHERE strategy = %s
          AND market_id = %s
          AND outcome = %s
          AND side = %s
          AND status = 'open'
        LIMIT 1
        """,
        (EXEC_STRATEGY, market_id, outcome, side),
    )
    if cur.fetchone():
        return False

    cur.execute(
        """
        SELECT 1
        FROM paper_positions
        WHERE strategy = %s
          AND market_id = %s
          AND outcome = %s
          AND side = %s
          AND status = 'closed'
          AND exit_ts >= now() - (%s || ' seconds')::interval
        LIMIT 1
        """,
        (EXEC_STRATEGY, market_id, outcome, side, MARKET_COOLDOWN_SECS),
    )
    if cur.fetchone():
        return False

    return True


def open_paper_position(cur, sig, price, side, reason_dict):
    smart_wallets = int(reason_dict.get("smart_wallets", 0))
    smart_net_flow = to_dec(reason_dict.get("smart_net_flow", 0))
    score = to_dec(sig["score"])

    risk_usd = risk_usd_for_signal(score, smart_net_flow)
    if price is None or price <= 0:
        return None
    size = risk_usd / price

    cur.execute(
        """
        INSERT INTO paper_positions (
            strategy,
            market_id,
            outcome,
            side,
            entry_price,
            entry_ts,
            size,
            score,
            smart_wallets,
            smart_net_flow,
            status
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'open')
        RETURNING id
        """,
        (
            EXEC_STRATEGY,
            sig["market_id"],
            sig["outcome"],
            side,
            price,
            datetime.now(timezone.utc),
            size,
            score,
            smart_wallets,
            smart_net_flow,
        ),
    )
    row = cur.fetchone()
    return row["id"] if row else None


def fetch_open_positions(cur):
    cur.execute(
        """
        SELECT *
        FROM paper_positions
        WHERE status = 'open'
        """
    )
    return cur.fetchall()


def get_latest_signal(cur, market_id, outcome):
    cur.execute(
        """
        SELECT *
        FROM strategy_signals
        WHERE strategy = %s
          AND market_id = %s
          AND outcome = %s
        ORDER BY id DESC
        LIMIT 1
        """,
        (SIGNAL_STRATEGY, market_id, outcome),
    )
    return cur.fetchone()

def check_smart_wallet_consensus(cur, market_id, minutes=10):
    """
    Consensus metrics for validated smart wallets in this market/outcome
    over the last `minutes`.
    Uses wallet_activity + wallet_labels.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)

    cur.execute(
        """
        SELECT 
            COUNT(DISTINCT wa.wallet)      AS consensus_count,
            AVG(wl.smart_score)::numeric   AS avg_score,
            SUM(wa.value_usd)::numeric     AS total_value
        FROM wallet_activity wa
        JOIN wallet_labels wl
          ON wl.wallet = wa.wallet
        WHERE wa.market_id = %s
          AND wa.side      = 'buy'
          AND wa.ts        >= %s
          AND wl.is_smart  = TRUE
          AND wl.smart_score >= %s
          AND wl.last_updated >= NOW() - INTERVAL '14 days'
        """,
        (market_id, cutoff, MIN_CONSENSUS_SCORE),
    )
    row = cur.fetchone() or {}

    return {
        "consensus_count": int(row.get("consensus_count") or 0),
        "avg_score": float(row.get("avg_score") or 0.0),
        "total_value": float(row.get("total_value") or 0.0),
    }


def close_position(cur, pos, price, reason):
    side = pos["side"]
    entry_price = Decimal(pos["entry_price"])
    size = Decimal(pos["size"])

    if side == "buy":
        pnl = (price - entry_price) * size
    else:
        pnl = (entry_price - price) * size

    now = datetime.now(timezone.utc)

    cur.execute(
        """
        UPDATE paper_positions
        SET
            status = 'closed',
            exit_price = %s,
            exit_ts = %s,
            pnl = %s
        WHERE id = %s
        """,
        (price, now, pnl, pos["id"]),
    )

    cur.execute(
        """
        INSERT INTO paper_fills (position_id, price, ts, reason)
        VALUES (%s,%s,%s,%s)
        """,
        (pos["id"], price, now, reason),
    )


def process_signals(cur):
    signals = fetch_new_signals(cur)
    counts = {
        "wallets": 0,
        "flow": 0,
        "abs_flow": 0,
        "wallet_count": 0,
        "smart_wallets_flow": 0,
        "consensus": 0,
        "price": 0,
        "cooldown": 0,
        "side": 0,
        "other": 0,
        "ok": 0,
    }
    for sig in signals:
        reason_dict = parse_reason(sig.get("reason"))
        smart_wallets = int(reason_dict.get("smart_wallets", 0))
        smart_net_flow = to_dec(reason_dict.get("smart_net_flow", 0))
        top_a_swing_wallets = int(reason_dict.get("top_a_swing_wallets", 0))
        has_top_a_swing = top_a_swing_wallets >= 1

        if smart_wallets < MIN_SMART_WALLETS and not has_top_a_swing:
            counts["wallets"] += 1
            continue
        if abs(smart_net_flow) < MIN_SMART_NET_FLOW:
            counts["flow"] += 1
            continue

        side = (sig["side"] or "").lower()
        # Disable shorts for now: only act on buys
        if side != "buy":
            counts["side"] += 1
            continue

        # Fetch latest snapshot at/before signal ts for additional guards
        cur.execute(
            """
            SELECT fs.*
            FROM flow_snapshots fs
            WHERE fs.market_id = %s
              AND fs.outcome   = %s
              AND fs.ts <= %s
            ORDER BY fs.ts DESC
            LIMIT 1
            """,
            (sig["market_id"], sig["outcome"], sig["ts"]),
        )
        snap = cur.fetchone()
        if not snap:
            counts["other"] += 1
            continue

        abs_flow = abs(to_dec(snap.get("smart_net_flow"), Decimal("0")))
        if abs_flow < MIN_ABS_FLOW_USD:
            counts["abs_flow"] += 1
            continue
        if snap.get("wallet_count") is None or snap["wallet_count"] < MIN_WALLETS:
            counts["wallet_count"] += 1
            continue
        snap_smart_wallets = snap.get("smart_wallets")
        if (snap_smart_wallets is None or snap_smart_wallets < MIN_SMART_WALLETS_FLOW) and not has_top_a_swing:
            counts["smart_wallets_flow"] += 1
            continue

        market_id = sig["market_id"]
        outcome = sig["outcome"]

        # Smart wallet consensus guard (recent buys from validated smart wallets)
        consensus = check_smart_wallet_consensus(
            cur, market_id, minutes=CONSENSUS_LOOKBACK_MIN
        )
        if consensus["consensus_count"] < MIN_SMART_CONSENSUS:
            counts["consensus"] += 1
            continue
        if consensus["avg_score"] < MIN_CONSENSUS_SCORE:
            counts["consensus"] += 1
            continue

        if not category_allowed(cur, market_id):
            counts["other"] += 1
            continue

        if not multi_timeframe_ok(cur, market_id, outcome):
            counts["other"] += 1
            continue

        if not can_open_position(cur, market_id, outcome, side):
            counts["cooldown"] += 1
            continue

        last_price = get_last_price(cur, market_id, outcome)
        if last_price is None:
            counts["other"] += 1
            continue

        if side == "buy":
            entry_price = last_price * (Decimal("1") + SLIPPAGE)
        else:
            entry_price = last_price * (Decimal("1") - SLIPPAGE)

        if entry_price <= 0 or entry_price >= 1:
            counts["price"] += 1
            continue
        if entry_price < MIN_PRICE or entry_price > MAX_PRICE:
            counts["price"] += 1
            continue

        open_paper_position(cur, sig, entry_price, side, reason_dict)
        counts["ok"] += 1
    if signals:
        print(f"[smartflow_exec] signal filter counts: {counts}")


def process_exits(cur):
    open_positions = fetch_open_positions(cur)
    now = datetime.now(timezone.utc)

    for pos in open_positions:
        market_id = pos["market_id"]
        outcome = pos["outcome"]
        entry_price = Decimal(pos["entry_price"])
        side = pos["side"]

        last_price = get_last_price(cur, market_id, outcome)
        if last_price is None:
            continue

        if side == "buy":
            tp_level = entry_price * (Decimal("1") + TP_PCT)
            sl_level = entry_price * (Decimal("1") - SL_PCT)
            exit_price_for_tp = last_price * (Decimal("1") - SLIPPAGE)
            exit_price_for_sl = last_price * (Decimal("1") - SLIPPAGE)

            if last_price >= tp_level:
                close_position(cur, pos, exit_price_for_tp, "tp")
                continue
            if last_price <= sl_level:
                close_position(cur, pos, exit_price_for_sl, "sl")
                continue
        else:
            tp_level = entry_price * (Decimal("1") - TP_PCT)
            sl_level = entry_price * (Decimal("1") + SL_PCT)
            exit_price_for_tp = last_price * (Decimal("1") + SLIPPAGE)
            exit_price_for_sl = last_price * (Decimal("1") + SLIPPAGE)

            if last_price <= tp_level:
                close_position(cur, pos, exit_price_for_tp, "tp")
                continue
            if last_price >= sl_level:
                close_position(cur, pos, exit_price_for_sl, "sl")
                continue

        elapsed = now - pos["entry_ts"]
        if elapsed.total_seconds() >= MAX_HOLD_HOURS * 3600:
            exit_price = last_price * (Decimal("1") - SLIPPAGE) if side == "buy" else last_price * (Decimal("1") + SLIPPAGE)
            close_position(cur, pos, exit_price, "time")
            continue

        latest = get_latest_signal(cur, market_id, outcome)
        if latest:
            lr = parse_reason(latest.get("reason"))
            latest_score = to_dec(latest["score"])
            latest_smart_flow = to_dec(lr.get("smart_net_flow", 0))
            if latest_score < FLOW_REVERSAL_SCORE or latest_smart_flow < FLOW_REVERSAL_NET_FLOW:
                exit_price = last_price * (Decimal("1") - SLIPPAGE) if side == "buy" else last_price * (Decimal("1") + SLIPPAGE)
                close_position(cur, pos, exit_price, "flow_reversal")
                continue


def main():
    conn = get_conn()
    ensure_tables(conn)
    conn.autocommit = True
    print(f"[smartflow_exec] started strategy={EXEC_STRATEGY} signals_from={SIGNAL_STRATEGY}")

    while True:
        try:
            with conn.cursor() as cur:
                process_signals(cur)
                process_exits(cur)
        except Exception as e:
            print("[smartflow_exec] error:", e)
            try:
                conn.close()
            except Exception:
                pass
            time.sleep(3)
            conn = get_conn()
            ensure_tables(conn)
            conn.autocommit = True
        time.sleep(LOOP_SLEEP)


if __name__ == "__main__":
    main()
