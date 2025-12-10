"""
Mean Reversion Trading Executor - Longs Only (Paper Trading)

Validated settings from backtest:
- 85.4% win rate
- +9.40% avg P&L per trade
- 18h rolling average window
- Excludes sports markets
- Longs only (shorts were unprofitable)
"""

import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from collections import defaultdict

from psycopg import connect
from psycopg.rows import dict_row

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise SystemExit("DB_URL not set")

# Strategy identifiers
STRATEGY_MEAN_REV_V1 = "mean_reversion_v1"
STRATEGY_MEAN_REV_STRICT_V1 = "mean_reversion_strict_v1"
STRATEGY_MEAN_REV_V2 = "mean_reversion_v2"

# Strategy name (selected via env)
STRATEGY = os.getenv("MR_STRATEGY", STRATEGY_MEAN_REV_V1)
# Log prefix: use [MRS] for strict variants, else [MR]
LOG_PREFIX = "[MRS]" if "strict" in STRATEGY else "[MR]"

# Entry filters (validated from backtest)
DISLOCATION_THRESHOLD = Decimal(os.getenv("MR_DISLOCATION_THRESHOLD", "0.20"))
MAX_DISLOCATION = Decimal(os.getenv("MR_MAX_DISLOCATION", "0.45"))
MIN_PRICE = Decimal(os.getenv("MR_MIN_PRICE", "0.05"))
MAX_PRICE = Decimal(os.getenv("MR_MAX_PRICE", "0.95"))
AVG_WINDOW_HOURS = int(os.getenv("MR_AVG_WINDOW_HOURS", "18"))

# Exit parameters (validated from backtest)
TAKE_PROFIT_PCT = Decimal(os.getenv("MR_TAKE_PROFIT_PCT", "0.15"))
STOP_LOSS_PCT = Decimal(os.getenv("MR_STOP_LOSS_PCT", "0.15"))
MAX_HOLD_HOURS = int(os.getenv("MR_MAX_HOLD_HOURS", "12"))
# Hard maximum stop loss as a safety net (realized P&L based)
MAX_STOP_LOSS_PCT = Decimal(os.getenv("MR_MAX_STOP_LOSS_PCT", "0.20"))
# Optional positive tag whitelist
_inc_tags_env = os.getenv("MR_INCLUDED_TAGS", "")
INCLUDED_TAGS = set([t.strip().lower() for t in _inc_tags_env.split(",") if t.strip()])
# Per-market PnL kill switch
MARKET_MAX_DRAWDOWN_USD = Decimal(os.getenv("MR_MARKET_MAX_DRAWDOWN_USD", "0"))
# Per market consecutive loss streak limit
MAX_LOSS_STREAK = int(os.getenv("MR_MAX_LOSS_STREAK", "4"))
# Track consecutive losses per market and outcome
MARKET_LOSS_STREAK = defaultdict(int)  # key: (strategy, market_id, outcome)
# --- MR V2 specific filters ---
MR2_MAX_ENTRY_PX = Decimal(os.getenv("MR2_MAX_ENTRY_PX", "0.15"))
MR2_MIN_DISLOCATION = Decimal(os.getenv("MR2_MIN_DISLOCATION", "-0.45"))
MR2_MARKET_MAX_LOSS_USD = Decimal(os.getenv("MR2_MARKET_MAX_LOSS_USD", "75"))
MR2_EXCLUDED_TAGS_SET = {t.strip() for t in (os.getenv("MR2_EXCLUDED_TAGS", "")).split(",") if t.strip()}
# market_id -> cumulative realized pnl for v2
MARKET_REALIZED_PNL_V2 = defaultdict(lambda: Decimal("0"))

# Position sizing
BASE_POSITION_USD = Decimal(os.getenv("MR_BASE_POSITION_USD", "100"))
MAX_POSITION_USD = Decimal(os.getenv("MR_MAX_POSITION_USD", "200"))

# Risk management
MAX_OPEN_POSITIONS = int(os.getenv("MR_MAX_OPEN_POSITIONS", "10"))
MAX_POSITIONS_PER_MARKET = int(os.getenv("MR_MAX_POSITIONS_PER_MARKET", "1"))
MARKET_COOLDOWN_SECS = int(os.getenv("MR_MARKET_COOLDOWN_SECS", "600"))  # 10 min

# Market selection
TOP_MARKETS = int(os.getenv("MR_TOP_MARKETS", "50"))
MIN_VOLUME_24H = Decimal(os.getenv("MR_MIN_VOLUME_24H", "10000"))

# Execution
SLIPPAGE = Decimal(os.getenv("MR_SLIPPAGE", "0.01"))
LOOP_SLEEP = int(os.getenv("MR_LOOP_SLEEP", "10"))

# Market filters - CRITICAL: exclude sports and unwanted keywords
_tags_env = os.getenv("MR_EXCLUDED_TAGS") or os.getenv("MR_EXCLUDE_TAGS") or "sports,nfl,nba,soccer,mlb,hockey"
EXCLUDED_TAGS = set([t.strip().lower() for t in _tags_env.split(",") if t.strip()])
EXCLUDED_CATEGORIES = set()  # categories not present in polymarket schema; tags only
EXCLUDED_KEYWORDS = set([k.strip().lower() for k in os.getenv("MR_EXCLUDE_KEYWORDS", "").split(",") if k.strip()])

# Require markets to have a question/title
REQUIRE_QUESTION = os.getenv("MR_REQUIRE_QUESTION", "0") == "1"

# Circuit breaker
DAILY_LOSS_LIMIT = Decimal(os.getenv("MR_DAILY_LOSS_LIMIT", "1000"))

# State tracking
LAST_MARKET_CLOSE = defaultdict(lambda: datetime.min.replace(tzinfo=timezone.utc))
DAILY_PNL_RESET = datetime.now(timezone.utc).date()
DAILY_PNL = Decimal("0")


def to_dec(val, default=None):
    try:
        return Decimal(str(val))
    except (InvalidOperation, TypeError):
        return default


def get_conn():
    return connect(DB_URL, row_factory=dict_row)


def ensure_tables(conn):
    """Create positions + shadow fills tables if not exists"""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mr_positions (
                id              serial PRIMARY KEY,
                strategy        text NOT NULL,
                market_id       text NOT NULL,
                outcome         text NOT NULL,
                side            text NOT NULL,
                entry_price     numeric NOT NULL,
                entry_ts        timestamptz NOT NULL,
                size            numeric NOT NULL,
                avg_price_18h   numeric NOT NULL,
                dislocation     numeric NOT NULL,
                status          text DEFAULT 'open',
                exit_price      numeric,
                exit_ts         timestamptz,
                exit_reason     text,
                pnl             numeric,
                market_class    text
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_mr_positions_open 
            ON mr_positions(strategy, status, market_id, outcome);
            """
        )

        # NEW: shadow fills table (execution simulation)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mr_shadow_fills (
                id              serial PRIMARY KEY,
                strategy        text NOT NULL,
                market_id       text NOT NULL,
                outcome         text NOT NULL,
                side            text NOT NULL,
                ts              timestamptz NOT NULL,
                size            numeric NOT NULL,
                signal_price    numeric NOT NULL,
                sim_entry_price numeric NOT NULL,
                avg_price_18h   numeric NOT NULL,
                dislocation     numeric NOT NULL,
                notes           text
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_mr_shadow_fills_main
            ON mr_shadow_fills(strategy, market_id, outcome, ts);
            """
        )
    conn.commit()


def load_market_pnls_for_mr_v2(conn):
    """
    Preload cumulative PnL per market for mean_reversion_v2 from mr_positions.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT market_id, COALESCE(SUM(pnl), 0) AS pnl
            FROM mr_positions
            WHERE strategy = %s AND status = 'closed'
            GROUP BY market_id
            """,
            (STRATEGY_MEAN_REV_V2,),
        )
        for market_id, pnl in cur.fetchall():
            MARKET_REALIZED_PNL_V2[str(market_id)] = to_dec(pnl, Decimal("0"))


def is_market_excluded(cur, market_id):
    """Check if market should be excluded (sports, etc)"""
    if not EXCLUDED_TAGS:
        return False
    cur.execute(
        """
        SELECT tags
        FROM markets
        WHERE market_id = %s
        LIMIT 1
        """,
        (market_id,),
    )
    row = cur.fetchone()
    if not row:
        return False

    tags_val = row.get("tags")
    if not tags_val:
        return False

    if isinstance(tags_val, list):
        tags_set = {str(t).lower() for t in tags_val}
    elif isinstance(tags_val, str):
        tags_set = {t.strip().lower() for t in tags_val.split(",") if t.strip()}
    else:
        tags_set = set()

    return any(t in EXCLUDED_TAGS for t in tags_set)


def is_market_included(cur, market_id):
    """If INCLUDED_TAGS is non-empty, require at least one overlapping tag."""
    if not INCLUDED_TAGS:
        return True

    cur.execute(
        "SELECT tags FROM markets WHERE market_id = %s LIMIT 1",
        (market_id,),
    )
    row = cur.fetchone()
    if not row:
        return False

    tags_val = row.get("tags")
    if isinstance(tags_val, list):
        tags_set = {str(t).lower() for t in tags_val}
    elif isinstance(tags_val, str):
        tags_set = {t.strip().lower() for t in tags_val.split(",") if t.strip()}
    else:
        tags_set = set()

    return bool(tags_set & INCLUDED_TAGS)


def has_market_pnl_capacity(cur, market_id):
    """
    Returns (ok: bool, total_pnl: Decimal).
    If cumulative closed PnL for this strategy+market is below -MARKET_MAX_DRAWDOWN_USD,
    we stop opening new positions in this market.
    """
    if MARKET_MAX_DRAWDOWN_USD <= 0:
        return True, Decimal("0")

    cur.execute(
        """
        SELECT COALESCE(SUM(pnl), 0) AS total_pnl
        FROM mr_positions
        WHERE strategy = %s
          AND market_id = %s
          AND status = 'closed'
        """,
        (STRATEGY, market_id),
    )
    row = cur.fetchone() or {}
    total_pnl = to_dec(row.get("total_pnl"), Decimal("0"))

    if total_pnl <= -MARKET_MAX_DRAWDOWN_USD:
        return False, total_pnl
    return True, total_pnl


def market_has_excluded_tag_v2(cur, market_id):
    """
    Checks v2-specific excluded tags (case-sensitive matches).
    """
    if not MR2_EXCLUDED_TAGS_SET:
        return False
    cur.execute(
        "SELECT tags FROM markets WHERE market_id = %s LIMIT 1",
        (market_id,),
    )
    row = cur.fetchone()
    if not row:
        return False
    tags_val = row.get("tags")
    if isinstance(tags_val, list):
        tags = [str(t).strip() for t in tags_val]
    elif isinstance(tags_val, str):
        tags = [t.strip() for t in tags_val.split(",")]
    else:
        tags = []
    return any(t in MR2_EXCLUDED_TAGS_SET for t in tags if t)


def is_market_valid(cur, market_id):
    """
    Validate market before trading:
    - Must exist
    - Must have a reasonable question if required
    - Must not match excluded keywords
    - Must not be resolving within 6 hours (if resolve_ts available)
    """
    cur.execute(
        """
        SELECT question, tags, resolve_ts
        FROM markets
        WHERE market_id = %s
        LIMIT 1
        """,
        (market_id,),
    )
    row = cur.fetchone()
    if not row:
        return False, "not_in_db"

    question = (row.get("question") or "").strip()

    if REQUIRE_QUESTION and len(question) < 10:
        return False, "no_question"

    if question and EXCLUDED_KEYWORDS:
        qlow = question.lower()
        for kw in EXCLUDED_KEYWORDS:
            if kw in qlow:
                return False, f"keyword_{kw}"

    # Check resolve_ts if present
    end_ts = row.get("resolve_ts")
    if end_ts:
        try:
            if isinstance(end_ts, str):
                end_ts = datetime.fromisoformat(end_ts)
            hours_left = (end_ts - datetime.now(timezone.utc)).total_seconds() / 3600
            if hours_left < 6:
                return False, "ending_soon"
        except Exception:
            pass

    return True, None


def detect_volatility_collapse(cur, market_id, outcome):
    """
    Leading indicator: stddev collapses from prior 3h to recent 1h.
    Returns (collapsed: bool, drop_pct: float)
    """
    cur.execute(
        """
        SELECT 
            STDDEV(price) FILTER (WHERE ts >= NOW() - INTERVAL '1 hour') AS vol_1h,
            STDDEV(price) FILTER (WHERE ts >= NOW() - INTERVAL '4 hours' AND ts < NOW() - INTERVAL '1 hour') AS vol_3h_prior,
            COUNT(*) FILTER (WHERE ts >= NOW() - INTERVAL '1 hour') AS trades_1h,
            COUNT(*) FILTER (WHERE ts >= NOW() - INTERVAL '4 hours' AND ts < NOW() - INTERVAL '1 hour') AS trades_3h
        FROM raw_trades
        WHERE market_id = %s AND outcome = %s
          AND ts >= NOW() - INTERVAL '4 hours'
        """,
        (market_id, outcome),
    )
    row = cur.fetchone() or {}
    vol_recent = float(row.get("vol_1h") or 0)
    vol_prior = float(row.get("vol_3h_prior") or 0)
    trades_1h = int(row.get("trades_1h") or 0)
    trades_3h = int(row.get("trades_3h") or 0)

    # Require minimum sample size to avoid noise on illiquid markets
    if trades_1h < 10 or trades_3h < 30:
        return False, 0.0

    if vol_prior <= 0 or vol_recent <= 0:
        return False, 0.0
    drop_pct = (vol_prior - vol_recent) / vol_prior
    return (drop_pct > 0.60), drop_pct


def detect_volume_spike(cur, market_id, outcome):
    """
    Concurrent indicator: 1h trade count vs prior 24h hourly average (excluding last hour).
    Returns (spike: bool, ratio: float)
    """
    cur.execute(
        """
        WITH recent AS (
            SELECT COUNT(*) AS trades_1h
            FROM raw_trades
            WHERE market_id = %s AND outcome = %s
              AND ts >= NOW() - INTERVAL '1 hour'
        ),
        hist AS (
            SELECT AVG(cnt) AS avg_trades
            FROM (
                SELECT COUNT(*) AS cnt
                FROM raw_trades
                WHERE market_id = %s AND outcome = %s
                  AND ts >= NOW() - INTERVAL '25 hours'
                  AND ts < NOW() - INTERVAL '1 hour'
                GROUP BY DATE_TRUNC('hour', ts)
            ) x
        )
        SELECT trades_1h, avg_trades FROM recent, hist;
        """,
        (market_id, outcome, market_id, outcome),
    )
    row = cur.fetchone() or {}
    trades_1h = float(row.get("trades_1h") or 0)
    avg_trades = float(row.get("avg_trades") or 0)
    if avg_trades <= 0:
        return False, 0.0
    ratio = trades_1h / avg_trades
    return (ratio > 4.0), ratio


def get_top_markets(cur):
    """Get top markets by 24h volume, excluding sports/invalid/keywords"""
    query = """
        SELECT 
            rt.market_id,
            SUM(COALESCE(rt.value_usd, rt.price * rt.qty, 0)) AS volume_24h
        FROM raw_trades rt
        JOIN markets m ON m.market_id = rt.market_id
        WHERE rt.ts >= NOW() - INTERVAL '24 hours'
    """

    filters = []
    params = []

    if REQUIRE_QUESTION:
        filters.append("m.question IS NOT NULL")
        filters.append("LENGTH(TRIM(m.question)) >= 10")

    if filters:
        query += " AND " + " AND ".join(filters)

    query += """
        GROUP BY rt.market_id
        HAVING SUM(COALESCE(rt.value_usd, rt.price * rt.qty, 0)) >= %s
        ORDER BY volume_24h DESC
        LIMIT %s
    """
    params.extend([MIN_VOLUME_24H, TOP_MARKETS * 3])  # over-fetch, then filter

    cur.execute(query, params)

    markets = []
    for row in cur.fetchall():
        mid = row["market_id"]

        if is_market_excluded(cur, mid):
            continue

        if not is_market_included(cur, mid):
            continue

        ok, reason = is_market_valid(cur, mid)
        if not ok:
            continue

        markets.append(mid)
        if len(markets) >= TOP_MARKETS:
            break

    return markets


def get_market_stats(cur, market_id, outcome, now_ts):
    """Get current price and rolling average for a market/outcome.

    Fallbacks:
      - If 18h average is missing, retry with a 72h window.
      - If still missing but we have a current price, use that as the avg proxy.
    This reduces no_price rejections on sparsely traded markets when ingestion
    had gaps.
    """
    start_18h = now_ts - timedelta(hours=AVG_WINDOW_HOURS)

    def avg_in_window(start_ts):
        cur.execute(
            """
            SELECT AVG(price) as avg_price
            FROM raw_trades
            WHERE market_id = %s 
              AND outcome = %s
              AND ts >= %s 
              AND ts < %s
            """,
            (market_id, outcome, start_ts, now_ts),
        )
        row = cur.fetchone()
        return to_dec(row["avg_price"]) if row and row["avg_price"] else None

    avg_price = avg_in_window(start_18h)
    if avg_price is None:
        # Try a wider lookback to handle sparse markets / ingest gaps
        avg_price = avg_in_window(now_ts - timedelta(hours=72))

    # Get current price (latest trade)
    cur.execute(
        """
        SELECT price
        FROM raw_trades
        WHERE market_id = %s 
          AND outcome = %s
        ORDER BY ts DESC
        LIMIT 1
        """,
        (market_id, outcome),
    )
    row = cur.fetchone()
    current_price = to_dec(row["price"]) if row else None

    # If we have a current price but no avg, use current as proxy
    if current_price is not None and avg_price is None:
        avg_price = current_price

    return current_price, avg_price


def can_open_position(cur, market_id, outcome, now_ts):
    """Check if we can open a new position"""
    global LAST_MARKET_CLOSE

    # Check per market loss streak
    streak_key = (STRATEGY, market_id, outcome)
    if MARKET_LOSS_STREAK[streak_key] >= MAX_LOSS_STREAK:
        return False, "loss_streak"

    ok_pnl, total_pnl = has_market_pnl_capacity(cur, market_id)
    if not ok_pnl:
        return False, "market_dd"
    
    # Check cooldown
    key = (market_id, outcome)
    last_close = LAST_MARKET_CLOSE[key]
    if (now_ts - last_close).total_seconds() < MARKET_COOLDOWN_SECS:
        return False, "cooldown"
    
    # Check total open positions
    cur.execute(
        """
        SELECT COUNT(*) as count
        FROM mr_positions
        WHERE strategy = %s AND status = 'open'
        """,
        (STRATEGY,),
    )
    row = cur.fetchone()
    if row and row["count"] >= MAX_OPEN_POSITIONS:
        return False, "max_global"
    
    # Check positions in this market/outcome
    cur.execute(
        """
        SELECT COUNT(*) as count
        FROM mr_positions
        WHERE strategy = %s 
          AND status = 'open'
          AND market_id = %s
          AND outcome = %s
        """,
        (STRATEGY, market_id, outcome),
    )
    row = cur.fetchone()
    if row and row["count"] >= MAX_POSITIONS_PER_MARKET:
        return False, "max_market"
    
    return True, None


def open_position(cur, market_id, outcome, entry_price, avg_price, dislocation, now_ts):
    """Open a new long position"""
    size = BASE_POSITION_USD / entry_price
    
    cur.execute(
        """
        INSERT INTO mr_positions (
            strategy,
            market_id,
            outcome,
            side,
            entry_price,
            entry_ts,
            size,
            avg_price_18h,
            dislocation,
            status
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'open')
        RETURNING id
        """,
        (
            STRATEGY,
            market_id,
            outcome,
            "long",
            entry_price,
            now_ts,
            size,
            avg_price,
            dislocation,
        ),
    )
    row = cur.fetchone()
    return row["id"] if row else None


def close_position(cur, pos, exit_price, exit_reason, now_ts):
    """Close an open position"""
    global DAILY_PNL, LAST_MARKET_CLOSE
    
    entry_price = to_dec(pos["entry_price"])
    size = to_dec(pos["size"])
    
    # Long only: profit when price goes up
    pnl = (exit_price - entry_price) * size
    
    cur.execute(
        """
        UPDATE mr_positions
        SET
            status = 'closed',
            exit_price = %s,
            exit_ts = %s,
            exit_reason = %s,
            pnl = %s
        WHERE id = %s
        """,
        (exit_price, now_ts, exit_reason, pnl, pos["id"]),
    )
    
    # Update tracking
    DAILY_PNL += pnl
    key = (pos["market_id"], pos["outcome"])
    LAST_MARKET_CLOSE[key] = now_ts

    # Update per market loss streak
    streak_key = (pos["strategy"], pos["market_id"], pos["outcome"])
    if pnl < 0:
        MARKET_LOSS_STREAK[streak_key] += 1
        if MARKET_LOSS_STREAK[streak_key] >= MAX_LOSS_STREAK:
            print(
                f"{LOG_PREFIX} LOSS STREAK: {pos['market_id'][:16]}.../{pos['outcome']} "
                f"banned after {MARKET_LOSS_STREAK[streak_key]} consecutive losses"
            )
    else:
        # Any non negative result resets the streak
        MARKET_LOSS_STREAK[streak_key] = 0

    # Track v2 per-market PnL
    if pos.get("strategy") == STRATEGY_MEAN_REV_V2:
        MARKET_REALIZED_PNL_V2[pos["market_id"]] += pnl
    
    return float(pnl)


def log_shadow_fill(cur, market_id, outcome, side, size, signal_price, sim_entry_price, avg_price, dislocation, now_ts, notes=None):
    """
    Log a 'shadow' execution for mean_reversion_v1:
    - This does NOT affect positions or PnL.
    - It just records what we *would* have done at this moment.
    """
    cur.execute(
        """
        INSERT INTO mr_shadow_fills (
            strategy,
            market_id,
            outcome,
            side,
            ts,
            size,
            signal_price,
            sim_entry_price,
            avg_price_18h,
            dislocation,
            notes
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            STRATEGY,
            market_id,
            outcome,
            side,
            now_ts,
            size,
            signal_price,
            sim_entry_price,
            avg_price,
            dislocation,
            notes,
        ),
    )


def scan_for_entries(cur, markets, now_ts):
    """Scan markets for entry opportunities"""
    entries = 0
    filter_counts = defaultdict(int)
    
    for market_id in markets:
        valid, reason = is_market_valid(cur, market_id)
        if not valid:
            filter_counts[f"invalid_{reason}"] += 1
            continue

        # Get both outcomes
        for outcome in ["0", "1"]:
            # Collapse / spike filters (avoid collapsing markets)
            vol_collapsed, vol_drop = detect_volatility_collapse(cur, market_id, outcome)
            if vol_collapsed:
                filter_counts["avoid_vol_collapse"] += 1
                # lightweight logging for first few hits
                if filter_counts["avoid_vol_collapse"] <= 3:
                    print(f"{LOG_PREFIX} FILTER: {market_id[:16]}.../{outcome} volatility dropped {vol_drop*100:.0f}%")
                continue

            vol_spike, vol_ratio = detect_volume_spike(cur, market_id, outcome)
            if vol_spike:
                filter_counts["avoid_volume_spike"] += 1
                if filter_counts["avoid_volume_spike"] <= 3:
                    print(f"{LOG_PREFIX} FILTER: {market_id[:16]}.../{outcome} volume spike {vol_ratio:.1f}x")
                continue

            current_price, avg_price = get_market_stats(cur, market_id, outcome, now_ts)
            
            if current_price is None or avg_price is None:
                filter_counts["no_price"] += 1
                continue
            
            if avg_price <= 0:
                filter_counts["bad_avg"] += 1
                continue
            
            # Check price range
            if current_price < MIN_PRICE or current_price > MAX_PRICE:
                filter_counts["price_range"] += 1
                continue
            
            # Calculate dislocation
            dislocation = (current_price - avg_price) / avg_price
            
            # LONGS ONLY: price must be BELOW average
            if dislocation >= 0:
                filter_counts["not_dislocation"] += 1
                continue
            
            # Check dislocation threshold
            if abs(dislocation) < DISLOCATION_THRESHOLD:
                filter_counts["too_small"] += 1
                continue
            
            if abs(dislocation) > MAX_DISLOCATION:
                filter_counts["too_extreme"] += 1
                continue

            # Additional v2-only filters
            if STRATEGY == STRATEGY_MEAN_REV_V2:
                # Per-market PnL cap
                if MARKET_REALIZED_PNL_V2.get(market_id, Decimal("0")) <= -MR2_MARKET_MAX_LOSS_USD:
                    filter_counts["mr2_market_pnl_cap"] += 1
                    continue

                # Entry price cap (apply to slippage-adjusted price)
                projected_entry = current_price * (Decimal("1") + SLIPPAGE)
                if projected_entry > MR2_MAX_ENTRY_PX:
                    filter_counts["mr2_price_cap"] += 1
                    continue

                # Dislocation floor (skip ultra-deep dips)
                if dislocation <= MR2_MIN_DISLOCATION:
                    filter_counts["mr2_dislocation_too_deep"] += 1
                    continue

                # Tag blacklist
                if market_has_excluded_tag_v2(cur, market_id):
                    filter_counts["mr2_excluded_tag"] += 1
                    continue
            
            # Check if we can open
            can_open, reason = can_open_position(cur, market_id, outcome, now_ts)
            if not can_open:
                filter_counts[f"cant_open_{reason}"] += 1
                continue
            
            # Apply entry slippage (buying = pay higher)
            entry_price = current_price * (Decimal("1") + SLIPPAGE)
            size = BASE_POSITION_USD / entry_price

            # NEW: log a shadow fill for this potential execution
            try:
                log_shadow_fill(
                    cur=cur,
                    market_id=market_id,
                    outcome=outcome,
                    side="long",
                    size=size,
                    signal_price=current_price,
                    sim_entry_price=entry_price,
                    avg_price=avg_price,
                    dislocation=dislocation,
                    now_ts=now_ts,
                    notes="mr_v1_shadow"
                )
            except Exception as e:
                # Keep it non-fatal; shadow logging must not break trading loop
                print(f"{LOG_PREFIX} Shadow fill logging error for {market_id[:16]}.../{outcome}: {e}")

            # Open position (paper, as before)
            pos_id = open_position(cur, market_id, outcome, entry_price, avg_price, dislocation, now_ts)
            if pos_id:
                entries += 1
                print(
                    f"{LOG_PREFIX} ENTRY #{pos_id}: {market_id[:16]}.../{outcome} @ {float(entry_price):.4f} "
                    f"(avg={float(avg_price):.4f}, dislo={float(dislocation)*100:.1f}%)"
                )
    
    if entries > 0 or sum(filter_counts.values()) > 0:
        print(f"{LOG_PREFIX} Scan: {entries} entries, filters: {dict(filter_counts)}")
    
    return entries


def process_exits(cur, now_ts):
    """Check and execute exits for open positions"""
    cur.execute(
        """
        SELECT *
        FROM mr_positions
        WHERE strategy = %s AND status = 'open'
        """,
        (STRATEGY,),
    )
    
    positions = cur.fetchall()
    exit_counts = defaultdict(int)
    
    for pos in positions:
        market_id = pos["market_id"]
        outcome = pos["outcome"]
        entry_price = to_dec(pos["entry_price"])
        
        # Get current price
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
        if not row:
            continue
        
        current_price = to_dec(row["price"])
        if not current_price:
            continue
        
        # Calculate P&L percentage based on current mid/last
        pnl_pct = (current_price - entry_price) / entry_price
        
        # Apply exit slippage (selling = receive lower)
        exit_price = current_price * (Decimal("1") - SLIPPAGE)

        # Hard maximum stop loss cap based on realized exit_price
        realized_pnl_pct = (exit_price - entry_price) / entry_price
        if realized_pnl_pct <= -MAX_STOP_LOSS_PCT:
            pnl = close_position(cur, pos, exit_price, "max_sl", now_ts)
            exit_counts["max_sl"] += 1
            print(
                f"{LOG_PREFIX} EXIT MAX_SL: #{pos['id']} {market_id[:16]}.../{outcome} "
                f"@ {float(exit_price):.4f} P&L: ${pnl:.2f} "
                f"({float(realized_pnl_pct)*100:.1f}%) - HARD CAP HIT"
            )
            continue

        # Early exit if market shows collapse after we're in and we're losing
        elapsed_hours = (now_ts - pos["entry_ts"]).total_seconds() / 3600
        if elapsed_hours >= 1 and pnl_pct < -Decimal("0.05"):
            vol_collapsed, vol_drop = detect_volatility_collapse(cur, market_id, outcome)
            if vol_collapsed:
                pnl = close_position(cur, pos, exit_price, "vol_collapse_exit", now_ts)
                exit_counts["vol_collapse_exit"] += 1
                print(f"{LOG_PREFIX} EXIT VOL_COLLAPSE: #{pos['id']} {market_id[:16]}.../{outcome} "
                      f"P&L: ${pnl:.2f} (vol drop {vol_drop*100:.0f}%)")
                continue
        
        # Check take profit
        if pnl_pct >= TAKE_PROFIT_PCT:
            pnl = close_position(cur, pos, exit_price, "tp", now_ts)
            exit_counts["tp"] += 1
            print(f"{LOG_PREFIX} EXIT TP: #{pos['id']} {market_id[:16]}.../{outcome} @ {float(exit_price):.4f} "
                  f"P&L: ${pnl:.2f} (+{float(pnl_pct)*100:.1f}%)")
            continue
        
        # Check stop loss
        if pnl_pct <= -STOP_LOSS_PCT:
            pnl = close_position(cur, pos, exit_price, "sl", now_ts)
            exit_counts["sl"] += 1
            print(f"{LOG_PREFIX} EXIT SL: #{pos['id']} {market_id[:16]}.../{outcome} @ {float(exit_price):.4f} "
                  f"P&L: ${pnl:.2f} ({float(pnl_pct)*100:.1f}%)")
            continue
        
        # Check time limit
        if elapsed_hours >= MAX_HOLD_HOURS:
            pnl = close_position(cur, pos, exit_price, "time", now_ts)
            exit_counts["time"] += 1
            print(f"{LOG_PREFIX} EXIT TIME: #{pos['id']} {market_id[:16]}.../{outcome} @ {float(exit_price):.4f} "
                  f"P&L: ${pnl:.2f} ({float(pnl_pct)*100:.1f}%) after {elapsed_hours:.1f}h")
            continue
    
    if sum(exit_counts.values()) > 0:
        print(f"{LOG_PREFIX} Exits: {dict(exit_counts)}")


def print_status(cur):
    """Print current status summary"""
    global DAILY_PNL, DAILY_PNL_RESET
    
    now = datetime.now(timezone.utc)
    
    # Reset daily P&L at midnight
    if now.date() > DAILY_PNL_RESET:
        DAILY_PNL = Decimal("0")
        DAILY_PNL_RESET = now.date()
    
    # Get position counts
    cur.execute(
        """
        SELECT 
            COUNT(*) FILTER (WHERE status = 'open') as open_count,
            COUNT(*) FILTER (WHERE status = 'closed') as closed_count,
            COUNT(*) FILTER (WHERE status = 'closed' AND pnl > 0) as winners,
            ROUND(AVG(pnl) FILTER (WHERE status = 'closed')::numeric, 2) as avg_pnl,
            ROUND(SUM(pnl) FILTER (WHERE status = 'closed')::numeric, 2) as total_pnl
        FROM mr_positions
        WHERE strategy = %s
        """,
        (STRATEGY,),
    )
    row = cur.fetchone()
    
    open_count = row["open_count"] or 0
    closed_count = row["closed_count"] or 0
    winners = row["winners"] or 0
    avg_pnl = float(row["avg_pnl"]) if row["avg_pnl"] else 0.0
    total_pnl = float(row["total_pnl"]) if row["total_pnl"] else 0.0
    
    win_rate = (winners / closed_count * 100) if closed_count > 0 else 0.0
    
    print(f"\n{LOG_PREFIX} === STATUS @ {now.strftime('%H:%M:%S')} ===")
    print(f"{LOG_PREFIX} Open: {open_count} | Closed: {closed_count} (WR: {win_rate:.1f}%)")
    print(f"{LOG_PREFIX} Avg P&L: ${avg_pnl:.2f} | Total: ${total_pnl:.2f} | Today: ${float(DAILY_PNL):.2f}")
    print(f"{LOG_PREFIX} =============================\n")


def main():
    global DAILY_PNL
    
    print(f"{LOG_PREFIX} Mean Reversion Executor Starting...")
    print(f"{LOG_PREFIX} Strategy: {STRATEGY}")
    print(f"{LOG_PREFIX} Settings: dislo={float(DISLOCATION_THRESHOLD)*100:.0f}%, tp={float(TAKE_PROFIT_PCT)*100:.0f}%, "
          f"sl={float(STOP_LOSS_PCT)*100:.0f}%, hold={MAX_HOLD_HOURS}h, window={AVG_WINDOW_HOURS}h")
    print(f"{LOG_PREFIX} Markets: top {TOP_MARKETS}, min vol ${float(MIN_VOLUME_24H):.0f}")
    print(f"{LOG_PREFIX} Excluded: tags={EXCLUDED_TAGS}, categories={EXCLUDED_CATEGORIES}")
    print(f"{LOG_PREFIX} Excluded keywords: {EXCLUDED_KEYWORDS}")
    print(f"{LOG_PREFIX} Require question: {REQUIRE_QUESTION}")
    print(f"{LOG_PREFIX} Limits: max {MAX_OPEN_POSITIONS} positions, ${float(DAILY_LOSS_LIMIT):.0f} daily loss limit")
    
    conn = get_conn()
    # Disable server-side prepared statements to avoid cached plan type errors after schema changes.
    conn.prepare_threshold = None
    # Enable autocommit up front to avoid in-transaction state when toggling.
    conn.autocommit = True
    ensure_tables(conn)
    if STRATEGY == STRATEGY_MEAN_REV_V2:
        load_market_pnls_for_mr_v2(conn)
    
    last_status_print = datetime.now(timezone.utc)
    
    try:
        while True:
            now_ts = datetime.now(timezone.utc)
            
            # Check circuit breaker
            if DAILY_PNL < -DAILY_LOSS_LIMIT:
                print(f"{LOG_PREFIX} CIRCUIT BREAKER: Daily loss ${float(DAILY_PNL):.2f} exceeds limit ${float(DAILY_LOSS_LIMIT):.2f}")
                print(f"{LOG_PREFIX} Skipping new entries, processing exits only")
                with conn.cursor() as cur:
                    process_exits(cur, now_ts)
                time.sleep(LOOP_SLEEP)
                continue
            
            with conn.cursor() as cur:
                # Get top markets
                markets = get_top_markets(cur)
                
                # Scan for entries
                scan_for_entries(cur, markets, now_ts)
                
                # Process exits
                process_exits(cur, now_ts)
                
                # Print status every 5 minutes
                if (now_ts - last_status_print).total_seconds() >= 300:
                    print_status(cur)
                    last_status_print = now_ts
            
            time.sleep(LOOP_SLEEP)
    
    except KeyboardInterrupt:
        print(f"\n{LOG_PREFIX} Shutting down...")
        with conn.cursor() as cur:
            print_status(cur)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
