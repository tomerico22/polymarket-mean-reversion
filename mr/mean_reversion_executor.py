import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from collections import defaultdict

from psycopg import connect
from psycopg.rows import dict_row

"""
Mean Reversion Executor (Paper)
- Longs only
- Peak-to-trough market drawdown (realized + unrealized, mark-to-market)
- Stale prices -> cooldown ban
- Drawdown -> permanent ban + forced liquidation
- Batch price snapshot (single query per loop)
- Entry dedup + position limits + time-based exits
- Entry COUNT caching to reduce DB load
"""

# =========================
# CONFIG
# =========================

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise SystemExit("DB_URL not set")

STRATEGY = os.getenv("MR_STRATEGY", "mean_reversion_v1")
LOG_PREFIX = "[MRS]"

# Position sizing
BASE_POSITION_USD = Decimal(os.getenv("MR_BASE_POSITION_USD", "100"))

# Risk - market drawdown (peak-to-trough)
MR_MARKET_DD_FRACTION = Decimal(os.getenv("MR_MARKET_DD_FRACTION", "0.75"))  # fraction of BASE_POSITION_USD
SLIPPAGE = Decimal(os.getenv("MR_SLIPPAGE", "0.01"))

# Price bounds
MIN_PRICE = Decimal(os.getenv("MR_MIN_PRICE", "0"))
MAX_PRICE = Decimal(os.getenv("MR_MAX_PRICE", "1"))

# ----------------------------
# Keyword blacklist (question-based)
# ----------------------------
KEYWORD_BLACKLIST_PATH = Path(os.getenv(
    "MR_KEYWORD_BLACKLIST_PATH",
    "/root/polymarket-mean-reversion/mr/config/keyword_blacklist.txt"
))
REQUIRE_QUESTION = os.getenv("MR_REQUIRE_QUESTION", "0").strip() == "1"

_kw_file_mtime = None
_kw_file_list = []

def _kw_norm(s: str) -> str:
    return " ".join(str(s).lower().strip().split())

def _load_keyword_blacklist():
    global _kw_file_mtime, _kw_file_list

    # env csv support (keeps MR_EXCLUDE_KEYWORDS behavior)
    env_csv = os.getenv("MR_EXCLUDE_KEYWORDS", "") or ""
    env_list = [_kw_norm(x) for x in env_csv.split(",") if _kw_norm(x)]

    # file list (auto-reload on mtime change)
    try:
        st = KEYWORD_BLACKLIST_PATH.stat()
        mtime = st.st_mtime
        if _kw_file_mtime != mtime:
            lines = KEYWORD_BLACKLIST_PATH.read_text().splitlines()
            cleaned = []
            for line in lines:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                cleaned.append(_kw_norm(line))
            _kw_file_list = [x for x in cleaned if x]
            _kw_file_mtime = mtime
    except Exception:
        _kw_file_list = []

    return sorted(set([x for x in (_kw_file_list + env_list) if x]))

def is_question_blocked(question: str) -> bool:
    q = _kw_norm(question or "")
    if REQUIRE_QUESTION and not q:
        return True
    for kw in _load_keyword_blacklist():
        if kw and kw in q:
            return True
    return False

def first_blocking_keyword(question: str) -> str:
    q = _kw_norm(question or "")
    if REQUIRE_QUESTION and not q:
        return "__missing_question__"
    for kw in _load_keyword_blacklist():
        if kw and kw in q:
            return kw
    return ""

# Strategy thresholds
DISLOCATION_THRESHOLD = Decimal(os.getenv("MR_DISLOCATION_THRESHOLD", "0.20"))
MAX_DISLOCATION = Decimal(os.getenv("MR_MAX_DISLOCATION", "0.45"))

# Exits
TAKE_PROFIT_PCT = Decimal(os.getenv("MR_TAKE_PROFIT_PCT", "0.15"))
STOP_LOSS_PCT = Decimal(os.getenv("MR_STOP_LOSS_PCT", "0.15"))
MAX_STOP_LOSS_PCT = Decimal(os.getenv("MR_MAX_STOP_LOSS_PCT", "0.20"))  # hard cap / worst-case fallback
MAX_HOLD_HOURS = int(os.getenv("MR_MAX_HOLD_HOURS", "12"))

# Limits
MAX_OPEN_POSITIONS = int(os.getenv("MR_MAX_OPEN_POSITIONS", "10"))
MAX_POSITIONS_PER_MARKET = int(os.getenv("MR_MAX_POSITIONS_PER_MARKET", "1"))

# Market selection
TOP_MARKETS = int(os.getenv("MR_TOP_MARKETS", "50"))
MIN_VOLUME_24H = Decimal(os.getenv("MR_MIN_VOLUME_24H", "10000"))

# Excluded tags (optional, requires markets.tags column)
_tags_env = os.getenv("MR_EXCLUDED_TAGS") or os.getenv("MR_EXCLUDE_TAGS") or "sports,nfl,nba,soccer,mlb,hockey"
EXCLUDED_TAGS = {t.strip().lower() for t in _tags_env.split(",") if t.strip()}

# Price staleness
MR_PRICE_STALE_SECS = int(os.getenv("MR_PRICE_STALE_SECS", "300"))
MR_STALE_BAN_SECS = int(os.getenv("MR_STALE_BAN_SECS", "900"))

# Loop
LOOP_SLEEP = int(os.getenv("MR_LOOP_SLEEP", "10"))

# =========================
# HELPERS
# =========================

def to_dec(val, default=None):
    try:
        if val is None:
            return default
        return Decimal(str(val))
    except (InvalidOperation, TypeError):
        return default

def norm_market_id(market_id):
    if market_id is None:
        raise ValueError("market_id cannot be None")
    mid = str(market_id).strip()
    if not mid:
        raise ValueError("market_id cannot be empty")
    return mid

def get_conn():
    conn = connect(DB_URL, row_factory=dict_row)
    conn.autocommit = True
    conn.prepare_threshold = None
    return conn

# =========================
# TABLES
# =========================

def ensure_tables(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS mr_positions (
        id SERIAL PRIMARY KEY,
        strategy TEXT NOT NULL,
        market_id TEXT NOT NULL,
        outcome TEXT NOT NULL,
        side TEXT NOT NULL,
        entry_price NUMERIC NOT NULL,
        entry_ts TIMESTAMPTZ NOT NULL,
        size NUMERIC NOT NULL,
        status TEXT DEFAULT 'open',
        exit_price NUMERIC,
        exit_ts TIMESTAMPTZ,
        exit_reason TEXT,
        pnl NUMERIC,
        avg_price_18h NUMERIC,
        dislocation NUMERIC
    );
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_mr_positions_open
    ON mr_positions(strategy, status, market_id, outcome);
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS mr_market_risk_state (
        strategy TEXT NOT NULL,
        market_id TEXT NOT NULL,
        peak_equity NUMERIC NOT NULL DEFAULT 0,
        last_equity NUMERIC NOT NULL DEFAULT 0,
        banned BOOLEAN NOT NULL DEFAULT FALSE,
        banned_until TIMESTAMPTZ,
        banned_reason TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (strategy, market_id)
    );
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_mr_market_risk_banned
    ON mr_market_risk_state(strategy, banned, banned_until)
    WHERE banned = true;
    """)

# =========================
# PRICE SNAPSHOT CACHE
# =========================

PRICE_SNAPSHOT = {}  # (market_id, outcome) -> (price: Decimal, ts: datetime)

def refresh_price_snapshot(cur, pairs):
    """
    Batch-load latest prices for (market_id, outcome) pairs into PRICE_SNAPSHOT.
    Uses zipped UNNEST of two arrays -> one row per pair.
    """
    global PRICE_SNAPSHOT
    PRICE_SNAPSHOT = {}

    pairs = list({(norm_market_id(m), str(o).strip()) for m, o in pairs if m is not None and o is not None})
    if not pairs:
        return

    market_ids = [m for m, _ in pairs]
    outcomes = [o for _, o in pairs]

    cur.execute("""
        WITH pairs(market_id, outcome) AS (
            SELECT * FROM UNNEST(%s::text[], %s::text[]) AS t(market_id, outcome)
        )
        SELECT DISTINCT ON (rt.market_id, rt.outcome)
            rt.market_id, rt.outcome, rt.price, rt.ts
        FROM raw_trades rt
        JOIN pairs p
          ON rt.market_id = p.market_id
         AND rt.outcome = p.outcome
        ORDER BY rt.market_id, rt.outcome, rt.ts DESC
    """, (market_ids, outcomes))

    for r in (cur.fetchall() or []):
        PRICE_SNAPSHOT[(norm_market_id(r["market_id"]), str(r["outcome"]).strip())] = (
            Decimal(str(r["price"])),
            r["ts"],
        )

def get_last_price_cached(market_id, outcome):
    return PRICE_SNAPSHOT.get((norm_market_id(market_id), str(outcome).strip()))

def is_price_stale(px_ts, now_ts):
    return (now_ts - px_ts).total_seconds() > MR_PRICE_STALE_SECS

def market_has_stale_price(market_id, now_ts):
    """
    Market-level stale check:
    If any required outcome price is missing OR stale, treat market as stale.
    """
    mid = norm_market_id(market_id)
    for outcome in ("0", "1"):
        cached = get_last_price_cached(mid, outcome)
        if not cached:
            return True
        _, ts = cached
        if is_price_stale(ts, now_ts):
            return True
    return False

# =========================
# MARKET TAG FILTER (optional)
# =========================

def is_market_excluded_by_tags(cur, market_id):
    """
    If markets.tags exists, exclude if any overlap with EXCLUDED_TAGS.
    If markets table or tags column doesn't exist, fail open (do not exclude).
    """
    if not EXCLUDED_TAGS:
        return False
    mid = norm_market_id(market_id)
    try:
        cur.execute("SELECT tags FROM markets WHERE market_id = %s LIMIT 1", (mid,))
        row = cur.fetchone()
        if not row:
            return False
        tags_val = row.get("tags")
        if not tags_val:
            return False

        if isinstance(tags_val, list):
            tags = {str(t).strip().lower() for t in tags_val if str(t).strip()}
        elif isinstance(tags_val, str):
            tags = {t.strip().lower() for t in tags_val.split(",") if t.strip()}
        else:
            tags = set()

        return bool(tags & EXCLUDED_TAGS)
    except Exception:
        return False

# =========================
# MARKET RISK STATE
# =========================

def load_market_state(cur, market_id):
    mid = norm_market_id(market_id)
    cur.execute("""
        SELECT * FROM mr_market_risk_state
        WHERE strategy = %s AND market_id = %s
    """, (STRATEGY, mid))
    return cur.fetchone()

def upsert_market_state(cur, market_id, **fields):
    mid = norm_market_id(market_id)

    cols = ", ".join(fields.keys())
    vals = list(fields.values())
    sets = ", ".join(f"{k}=EXCLUDED.{k}" for k in fields)

    cur.execute(f"""
        INSERT INTO mr_market_risk_state (strategy, market_id, {cols})
        VALUES (%s, %s, {",".join(["%s"] * len(vals))})
        ON CONFLICT (strategy, market_id)
        DO UPDATE SET {sets}, updated_at = NOW()
    """, [STRATEGY, mid] + vals)

def normalize_ban_state(cur, market_id, now_ts):
    row = load_market_state(cur, market_id)
    if row and row["banned"] and row["banned_until"]:
        if now_ts >= row["banned_until"]:
            upsert_market_state(
                cur,
                market_id,
                banned=False,
                banned_until=None,
                banned_reason=None,
            )

def ban_market(cur, market_id, now_ts, reason, permanent=True):
    mid = norm_market_id(market_id)

    # Step 2: idempotent ban (avoid repeated re-bans / log spam)
    try:
        row = load_market_state(cur, mid)
        if row and row.get("banned"):
            already_permanent = row.get("banned_until") is None
            same_reason = (row.get("banned_reason") or "") == (reason or "")
            if permanent and already_permanent and same_reason:
                return
    except Exception:
        pass

    banned_until = None
    if not permanent:
        banned_until = now_ts + timedelta(seconds=MR_STALE_BAN_SECS)

    upsert_market_state(
        cur,
        mid,
        banned=True,
        banned_until=banned_until,
        banned_reason=reason,
    )

    print(
        f"{LOG_PREFIX} MARKET BANNED {mid[:16]} "
        f"reason={reason} permanent={permanent}"
    )

def is_market_banned(cur, market_id, now_ts):
    mid = norm_market_id(market_id)
    normalize_ban_state(cur, mid, now_ts)
    row = load_market_state(cur, mid)
    return bool(row and row["banned"])

# =========================
# EQUITY / DRAW DOWN
# =========================

def compute_market_equity(cur, market_id, now_ts):
    """
    Equity = realized + unrealized (mark-to-market).
    Stale/missing prices are treated as missing and use conservative fallback.
    """
    mid = norm_market_id(market_id)
    cur.execute("""
        SELECT * FROM mr_positions
        WHERE strategy = %s AND market_id = %s
    """, (STRATEGY, mid))

    realized = Decimal("0")
    unreal = Decimal("0")

    for p in (cur.fetchall() or []):
        entry_px = Decimal(p["entry_price"])
        size = Decimal(p["size"])

        if p["status"] == "closed":
            realized += Decimal(p["pnl"] or 0)
            continue

        cached = get_last_price_cached(mid, p["outcome"])
        valid = False
        px = None
        if cached:
            px, ts = cached
            if not is_price_stale(ts, now_ts):
                valid = True

        if not valid:
            fallback_px = entry_px * (Decimal("1") - MAX_STOP_LOSS_PCT)
            exit_px = fallback_px * (Decimal("1") - SLIPPAGE)
        else:
            exit_px = px * (Decimal("1") - SLIPPAGE)

        unreal += (exit_px - entry_px) * size

    return realized + unreal

def update_market_dd_state(cur, market_id, now_ts):
    """
    If (peak_equity - equity) >= limit, permanently ban the market.
    limit is fraction of BASE_POSITION_USD (configurable).
    """
    mid = norm_market_id(market_id)
    equity = compute_market_equity(cur, mid, now_ts)
    row = load_market_state(cur, mid)

    peak = to_dec(row["peak_equity"], Decimal("0")) if row else Decimal("0")
    peak = max(peak, equity)

    limit = (BASE_POSITION_USD * MR_MARKET_DD_FRACTION)
    dd = peak - equity

    upsert_market_state(
        cur,
        mid,
        peak_equity=peak,
        last_equity=equity,
    )

    if dd >= limit:
        ban_market(cur, mid, now_ts, "drawdown", permanent=True)

# =========================
# FORCE CLOSE PERMA-KILLED MARKETS
# =========================

def force_close_killed_markets(cur, now_ts):
    """
    Force close all OPEN positions for permanently banned markets.
    If no fresh price, close at conservative fallback.
    """
    cur.execute("""
        SELECT * FROM mr_positions
        WHERE strategy = %s AND status = 'open'
    """, (STRATEGY,))

    for p in (cur.fetchall() or []):
        market_id = norm_market_id(p["market_id"])
        state = load_market_state(cur, market_id)

        # Only permanent bans: banned=True and banned_until is NULL
        if not state or not state["banned"] or state["banned_until"]:
            continue

        entry_px = Decimal(p["entry_price"])
        size = Decimal(p["size"])

        cached = get_last_price_cached(market_id, p["outcome"])
        if cached and not is_price_stale(cached[1], now_ts):
            exit_px = cached[0] * (Decimal("1") - SLIPPAGE)
        else:
            fallback_px = entry_px * (Decimal("1") - MAX_STOP_LOSS_PCT)
            exit_px = fallback_px * (Decimal("1") - SLIPPAGE)

        pnl = (exit_px - entry_px) * size

        cur.execute("""
            UPDATE mr_positions
            SET status='closed',
                exit_price=%s,
                exit_ts=%s,
                exit_reason='market_kill',
                pnl=%s
            WHERE id=%s
        """, (exit_px, now_ts, pnl, p["id"]))

        print(
            f"{LOG_PREFIX} FORCE EXIT #{p['id']} "
            f"{market_id[:16]} pnl=${float(pnl):.2f}"
        )

# =========================
# MARKET SELECTION
# =========================

def get_top_markets(cur, now_ts):
    """
    Top markets by 24h volume.
    Requires BOTH outcomes (0/1) to be present AND fresh.
    Uses COALESCE(value_usd, price*qty, 0) to be robust to schema differences.
    """
    cur.execute("""
        WITH per_outcome AS (
            SELECT
                rt.market_id,
                rt.outcome,
                SUM(COALESCE(rt.value_usd, rt.price * rt.qty, 0)) AS vol_24h,
                MAX(rt.ts) AS last_ts
            FROM raw_trades rt
            WHERE rt.ts >= %s - INTERVAL '24 hours'
              AND rt.ts < %s
              AND rt.outcome IN ('0','1')
            GROUP BY rt.market_id, rt.outcome
        ),
        per_market AS (
            SELECT
                market_id,
                SUM(vol_24h) AS volume_24h,
                MIN(last_ts) AS min_last_ts,
                COUNT(DISTINCT outcome) AS outcomes_seen
            FROM per_outcome
            GROUP BY market_id
        )
        SELECT
            market_id,
            volume_24h,
            min_last_ts AS last_trade_ts
        FROM per_market
        WHERE volume_24h >= %s
          AND outcomes_seen = 2
          AND min_last_ts >= %s - make_interval(secs => %s)
        ORDER BY volume_24h DESC
        LIMIT %s
    """, (now_ts, now_ts, MIN_VOLUME_24H, now_ts, MR_PRICE_STALE_SECS, TOP_MARKETS))

    markets = [norm_market_id(r["market_id"]) for r in (cur.fetchall() or [])]
    filtered = []

    # Pull questions in one query and filter by keyword blacklist
    qmap = {}
    try:
        cur.execute(
            "SELECT market_id, question FROM markets WHERE market_id = ANY(%s)",
            (markets,)
        )
        for r in (cur.fetchall() or []):
            qmap[norm_market_id(r.get("market_id"))] = (r.get("question") or "")
    except Exception:
        qmap = {}

    for mid in markets:
        if is_market_excluded_by_tags(cur, mid):
            continue
        q = qmap.get(mid, "")
        kw = first_blocking_keyword(q)
        if kw:
            print(f"{LOG_PREFIX} SKIP_KW {mid[:16]} kw={kw} q={q[:90]}")
            continue
        filtered.append(mid)

    print(f"{LOG_PREFIX} TOP_MARKETS raw={len(markets)} after_filters={len(filtered)}")
    return filtered

# =========================
# ENTRY LOGIC (COUNT CACHED)
# =========================

def scan_and_open(cur, markets, now_ts):
    """
    COUNT-cached entry scanning:
    - 1 query for global open count
    - 1 query for (market_id, outcome) open counts
    Then update local cache on each insert.

    Also:
    - Filters banned markets early
    - Cooldown bans stale markets early (market-level)
    """
    counters = defaultdict(int)
    entries = 0

    # Cache global count once
    cur.execute("""
        SELECT COUNT(*) AS c
        FROM mr_positions
        WHERE strategy = %s AND status = 'open'
    """, (STRATEGY,))
    global_count = int((cur.fetchone() or {}).get("c") or 0)
    if global_count >= MAX_OPEN_POSITIONS:
        counters["cap_global"] += 1
        print(f"{LOG_PREFIX} SCAN_SUMMARY markets_in={len(markets)} tradable=0 entries=0 cap_global=1")
        return

    # Cache per (market,outcome) open counts once
    cur.execute("""
        SELECT market_id, outcome, COUNT(*) AS c
        FROM mr_positions
        WHERE strategy = %s AND status = 'open'
        GROUP BY market_id, outcome
    """, (STRATEGY,))
    pos_counts = defaultdict(int)
    for r in (cur.fetchall() or []):
        pos_counts[(norm_market_id(r["market_id"]), str(r["outcome"]).strip())] = int(r["c"] or 0)

    # Filter banned markets before doing any work
    active_markets = []
    for m in markets:
        mid = norm_market_id(m)
        if is_market_banned(cur, mid, now_ts):
            counters["market_banned"] += 1
            continue
        active_markets.append(mid)

    # Market-level stale cooldown bans
    tradable_markets = []
    for mid in active_markets:
        if market_has_stale_price(mid, now_ts):
            counters["market_stale"] += 1
            ban_market(cur, mid, now_ts, "stale_price", permanent=False)
            continue
        tradable_markets.append(mid)

    for mid in tradable_markets:
        if global_count >= MAX_OPEN_POSITIONS:
            counters["cap_global_midloop"] += 1
            break

        # If DD perma-banned it earlier in this loop, skip
        if is_market_banned(cur, mid, now_ts):
            counters["market_banned_midloop"] += 1
            continue

        for outcome in ("0", "1"):
            if global_count >= MAX_OPEN_POSITIONS:
                counters["cap_global_outcome"] += 1
                break

            # Per market/outcome cap + dedup
            if pos_counts[(mid, outcome)] >= MAX_POSITIONS_PER_MARKET:
                counters["cap_per_market_outcome"] += 1
                continue

            cached = get_last_price_cached(mid, outcome)
            if not cached:
                counters["px_missing"] += 1
                ban_market(cur, mid, now_ts, "stale_price", permanent=False)
                continue
            px, ts = cached
            if is_price_stale(ts, now_ts):
                counters["px_stale"] += 1
                ban_market(cur, mid, now_ts, "stale_price", permanent=False)
                continue

            # Price guardrails
            if px < MIN_PRICE or px > MAX_PRICE:
                counters["px_oob"] += 1
                continue

            # 18h average aligned to now_ts
            cur.execute("""
                SELECT AVG(price) AS avg_price
                FROM raw_trades
                WHERE market_id=%s AND outcome=%s
                  AND ts >= %s - INTERVAL '18 hours'
                  AND ts < %s
            """, (mid, outcome, now_ts, now_ts))

            row = cur.fetchone() or {}
            avg = to_dec(row.get("avg_price"), None)
            if avg is None or avg <= 0:
                counters["avg_missing"] += 1
                continue

            dislo = (px - avg) / avg

            # Longs only: price must be below avg by threshold
            if dislo >= 0:
                counters["dislo_not_negative"] += 1
                continue
            if abs(dislo) < DISLOCATION_THRESHOLD:
                counters["dislo_too_small"] += 1
                continue
            if abs(dislo) > MAX_DISLOCATION:
                counters["dislo_too_big"] += 1
                continue

            entry_px = px * (Decimal("1") + SLIPPAGE)
            size = BASE_POSITION_USD / entry_px

            cur.execute("""
                INSERT INTO mr_positions (strategy, market_id, outcome, side, entry_price, entry_ts, size, avg_price_18h, dislocation)
                VALUES (%s,%s,%s,'long',%s,%s,%s,%s,%s)
            """, (STRATEGY, mid, outcome, entry_px, now_ts, size, avg, dislo))

            # Update caches
            global_count += 1
            pos_counts[(mid, outcome)] += 1
            entries += 1

            print(
                f"{LOG_PREFIX} ENTRY {mid[:16]} {outcome} "
                f"px={float(entry_px):.4f} dislo={float(dislo)*100:.1f}%"
            )

    # Print scan summary once per loop
    parts = [f"{k}={v}" for k, v in sorted(counters.items())]
    print(
        f"{LOG_PREFIX} SCAN_SUMMARY markets_in={len(markets)} "
        f"active={len(active_markets)} tradable={len(tradable_markets)} "
        f"entries={entries} " + " ".join(parts)
    )

# =========================
# EXIT LOGIC
# =========================

def close_position(cur, pos_id, exit_px, now_ts, reason, pnl):
    cur.execute("""
        UPDATE mr_positions
        SET status='closed',
            exit_price=%s,
            exit_ts=%s,
            exit_reason=%s,
            pnl=%s
        WHERE id=%s
    """, (exit_px, now_ts, reason, pnl, pos_id))

def process_exits(cur, now_ts):
    """
    Exit reasons:
    - max_sl: hard cap using realized exit price (slippage applied)
    - tp: take profit
    - sl: stop loss
    - time: time stop
    """
    cur.execute("""
        SELECT * FROM mr_positions
        WHERE strategy=%s AND status='open'
    """, (STRATEGY,))

    for p in (cur.fetchall() or []):
        market_id = norm_market_id(p["market_id"])
        outcome = str(p["outcome"]).strip()
        entry = Decimal(p["entry_price"])
        size = Decimal(p["size"])
        entry_ts = p["entry_ts"]

        cached = get_last_price_cached(market_id, outcome)
        if not cached or is_price_stale(cached[1], now_ts):
            # Do not close on stale quotes (risk layer handles stale via bans)
            continue

        mkt_px, _ = cached
        exit_px = mkt_px * (Decimal("1") - SLIPPAGE)

        pnl_pct_mid = (mkt_px - entry) / entry
        realized_pnl_pct = (exit_px - entry) / entry

        # Hard max stop loss based on realized price
        if realized_pnl_pct <= -MAX_STOP_LOSS_PCT:
            pnl = (exit_px - entry) * size
            close_position(cur, p["id"], exit_px, now_ts, "max_sl", pnl)
            print(f"{LOG_PREFIX} EXIT max_sl #{p['id']} pnl=${float(pnl):.2f}")
            continue

        # Time exit
        elapsed_hours = (now_ts - entry_ts).total_seconds() / 3600
        if elapsed_hours >= MAX_HOLD_HOURS:
            pnl = (exit_px - entry) * size
            close_position(cur, p["id"], exit_px, now_ts, "time", pnl)
            print(f"{LOG_PREFIX} EXIT time #{p['id']} pnl=${float(pnl):.2f}")
            continue

        # TP/SL exits
        if pnl_pct_mid >= TAKE_PROFIT_PCT:
            pnl = (exit_px - entry) * size
            close_position(cur, p["id"], exit_px, now_ts, "tp", pnl)
            print(f"{LOG_PREFIX} EXIT tp #{p['id']} pnl=${float(pnl):.2f}")
            continue

        if pnl_pct_mid <= -STOP_LOSS_PCT:
            pnl = (exit_px - entry) * size
            close_position(cur, p["id"], exit_px, now_ts, "sl", pnl)
            print(f"{LOG_PREFIX} EXIT sl #{p['id']} pnl=${float(pnl):.2f}")
            continue

# =========================
# STATUS
# =========================

def print_status(cur):
    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE status='open') AS open_count,
            COUNT(*) FILTER (WHERE status='closed') AS closed_count,
            COUNT(*) FILTER (WHERE status='closed' AND pnl > 0) AS winners,
            COALESCE(SUM(pnl) FILTER (WHERE status='closed'), 0) AS total_pnl
        FROM mr_positions
        WHERE strategy=%s
    """, (STRATEGY,))
    r = cur.fetchone() or {}
    open_count = int(r.get("open_count") or 0)
    closed_count = int(r.get("closed_count") or 0)
    winners = int(r.get("winners") or 0)
    total_pnl = float(r.get("total_pnl") or 0)
    wr = (100.0 * winners / closed_count) if closed_count > 0 else 0.0
    print(f"{LOG_PREFIX} STATUS open={open_count} closed={closed_count} WR={wr:.1f}% pnl=${total_pnl:.2f}")

# =========================
# MAIN LOOP
# =========================

def main():
    conn = get_conn()
    with conn.cursor() as cur:
        ensure_tables(cur)

    last_status = datetime.now(timezone.utc)

    while True:
        try:
            now_ts = datetime.now(timezone.utc)
            with conn.cursor() as cur:
                # 1) Select candidate markets
                markets = get_top_markets(cur, now_ts)

                # 2) Build snapshot pairs: open positions + scan universe
                cur.execute("""
                    SELECT market_id, outcome
                    FROM mr_positions
                    WHERE strategy=%s AND status='open'
                """, (STRATEGY,))
                open_pairs = [(norm_market_id(r["market_id"]), str(r["outcome"]).strip()) for r in (cur.fetchall() or [])]

                scan_pairs = []
                for m in markets:
                    scan_pairs.append((m, "0"))
                    scan_pairs.append((m, "1"))

                refresh_price_snapshot(cur, open_pairs + scan_pairs)

                # 3) Update DD for markets we have exposure to (positions history)
                cur.execute("""
                    SELECT DISTINCT market_id
                    FROM mr_positions
                    WHERE strategy=%s
                """, (STRATEGY,))
                risk_markets = [norm_market_id(r["market_id"]) for r in (cur.fetchall() or [])]
                for m in risk_markets:
                    update_market_dd_state(cur, m, now_ts)

                # 4) Force close permanently killed markets
                force_close_killed_markets(cur, now_ts)

                # 5) Entries
                scan_and_open(cur, markets, now_ts)

                # 6) Exits (tp/sl/time/max_sl)
                process_exits(cur, now_ts)

                # 7) Force close again (defensive: catches perma-bans that happened during this loop's activity)
                force_close_killed_markets(cur, now_ts)

                # Status every 5 minutes
                if (now_ts - last_status).total_seconds() >= 300:
                    print_status(cur)
                    last_status = now_ts

            time.sleep(LOOP_SLEEP)

        except Exception as e:
            print(f"{LOG_PREFIX} ERROR {e}")
            time.sleep(30)

if __name__ == "__main__":
    main()
PY