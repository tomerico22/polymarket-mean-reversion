# ingestors/fetch_markets.py
import os, sys, time, requests
from datetime import datetime, timezone
from psycopg import connect
from psycopg.rows import dict_row
from psycopg.types.json import Json
from config.settings import DB_URL

# Endpoints (robust to minor API shape differences)
GAMMA_API = os.getenv("POLY_MARKETS_HTTP_BASE", "https://gamma-api.polymarket.com").rstrip("/")
CLOB_HTTP = os.getenv("POLY_CLOB_HTTP_BASE", "https://clob.polymarket.com").rstrip("/")

def jget(obj, *path, default=None):
    cur = obj
    for p in path:
        if isinstance(cur, dict):
            cur = cur.get(p)
        else:
            return default
    return cur if cur is not None else default

def fetch_from_clob(limit=2000, page_size=500):
    """Primary: CLOB markets, which include conditionId and outcome assets."""
    out = []
    page = 1
    while True:
        r = requests.get(f"{CLOB_HTTP}/markets", params={"limit": page_size, "page": page}, timeout=30)
        if r.status_code == 404:
            break
        r.raise_for_status()
        data = r.json()
        rows = data if isinstance(data, list) else data.get("data", [])
        if not rows:
            break
        out.extend(rows)
        if len(out) >= limit or len(rows) < page_size:
            break
        page += 1
    return out

def fetch_from_gamma(limit=2000):
    """Fallback: gamma-api markets (some shapes differ)."""
    r = requests.get(f"{GAMMA_API}/markets", params={"limit": limit}, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else data.get("data", [])

def normalize_market(m):
    """
    Return a dict matching our DB columns using hex conditionId for market_id
    and yes/no token ids when determinable.
    """
    # Prefer conditionId (hex). If missing, skip.
    market_hex = jget(m, "conditionId") or jget(m, "condition_id")
    if not market_hex:
        return None

    # Question / title
    question = jget(m, "question") or jget(m, "title") or jget(m, "name") or ""

    # Collateral (string like 'USDC' sometimes); keep best-effort
    collateral = jget(m, "collateral") or jget(m, "collateralToken") or None

    # Created/resolve timestamps if present
    def to_ts(v):
        if not v: return None
        try:
            # seconds
            fv = float(v)
            if fv > 10_000_000_000:  # ms
                fv = fv / 1000.0
            return datetime.fromtimestamp(fv, tz=timezone.utc)
        except Exception:
            try:
                return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
            except Exception:
                return None

    created_ts = to_ts(jget(m, "createdTime") or jget(m, "created_at") or jget(m, "creationTime"))
    resolve_ts = to_ts(jget(m, "resolveTime") or jget(m, "resolvedTime"))
    resolution  = jget(m, "resolution") or jget(m, "status") or None
    event_id    = jget(m, "eventId") or jget(m, "event_id") or None

    raw_tags = jget(m, "tags")
    if isinstance(raw_tags, str):
        tags = [raw_tags.lower()]
    elif isinstance(raw_tags, list):
        tags = [str(t).lower() for t in raw_tags if t]
    else:
        tags = []

    vertical = jget(m, "vertical") or jget(m, "category") or jget(m, "eventCategory") or jget(m, "event", "category")
    if isinstance(vertical, list):
        vertical = vertical[0] if vertical else None
    if isinstance(vertical, dict):
        vertical = vertical.get("name") or vertical.get("category")
    vertical = str(vertical).lower() if vertical else None

    yes_token_id = None
    no_token_id  = None

    # Try common CLOB shape
    # outcomeAssets: [<yes_asset>, <no_asset>] or objects with .id
    outcome_assets = jget(m, "outcomeAssets") or jget(m, "assets") or jget(m, "outcomes")
    if isinstance(outcome_assets, list) and len(outcome_assets) == 2:
        a0 = outcome_assets[0]
        a1 = outcome_assets[1]
        # Items might be strings or dicts
        a0id = a0 if isinstance(a0, str) else (a0.get("id") or a0.get("asset") if isinstance(a0, dict) else None)
        a1id = a1 if isinstance(a1, str) else (a1.get("id") or a1.get("asset") if isinstance(a1, dict) else None)

        # If the API indicates outcome names, try to map by name/index
        # Otherwise assume index 0 = YES, 1 = NO (typical in CLOB)
        outcomes_meta = jget(m, "outcomeNames") or jget(m, "outcomes")
        if isinstance(outcomes_meta, list) and all(isinstance(x, str) for x in outcomes_meta):
            names = [x.lower() for x in outcomes_meta]
            if "yes" in names and "no" in names:
                yi = names.index("yes"); ni = names.index("no")
                pair = [a0id, a1id]
                yes_token_id = pair[yi] if yi < len(pair) else None
                no_token_id  = pair[ni] if ni < len(pair) else None
        if yes_token_id is None and a0id and a1id:
            yes_token_id, no_token_id = a0id, a1id

    # Another common shape: outcomes:[{name:'Yes', tokenId:'...'}, {name:'No', tokenId:'...'}]
    if not yes_token_id and isinstance(outcome_assets, list):
        yn = {"yes": None, "no": None}
        for o in outcome_assets:
            if isinstance(o, dict):
                name = str(o.get("name") or o.get("outcome") or "").lower()
                tok  = o.get("tokenId") or o.get("asset") or o.get("id")
                if name == "yes" and tok: yn["yes"] = tok
                if name == "no"  and tok: yn["no"]  = tok
        if yn["yes"] and yn["no"]:
            yes_token_id, no_token_id = yn["yes"], yn["no"]

    return {
        "market_id": market_hex,     # hex conditionId (string)
        "event_id": event_id,
        "question": question,
        "collateral": collateral,
        "yes_token_id": yes_token_id,
        "no_token_id": no_token_id,
        "created_ts": created_ts,
        "resolve_ts": resolve_ts,
        "resolution": resolution,
        "tags": tags,
        "vertical": vertical,
    }

UPSERT_SQL = """
INSERT INTO markets
(market_id, event_id, question, collateral, yes_token_id, no_token_id, created_ts, resolve_ts, resolution, tags, vertical)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (market_id) DO UPDATE SET
  event_id=EXCLUDED.event_id,
  question=EXCLUDED.question,
  collateral=EXCLUDED.collateral,
  yes_token_id=EXCLUDED.yes_token_id,
  no_token_id=EXCLUDED.no_token_id,
  created_ts=EXCLUDED.created_ts,
  resolve_ts=EXCLUDED.resolve_ts,
  resolution=EXCLUDED.resolution,
  tags = CASE WHEN EXCLUDED.tags IS NULL OR EXCLUDED.tags = '[]'::jsonb THEN markets.tags ELSE EXCLUDED.tags END,
  vertical = COALESCE(EXCLUDED.vertical, markets.vertical)
"""

# On-demand single-market upsert helpers
def fetch_single_market(mid: str, timeout: int = 5):
    """Fetch a single market by id (condition_id/market_id) from CLOB API."""
    r = requests.get(f"{CLOB_HTTP}/markets/{mid}", timeout=timeout)
    r.raise_for_status()
    return r.json()


def normalize_single_market(data: dict) -> dict:
    """Normalize a single market payload, tolerant to snake/camel keys."""
    mid = data.get("conditionId") or data.get("condition_id") or data.get("marketId") or data.get("market_id") or data.get("id")
    if not mid:
        raise KeyError("conditionId/marketId missing")
    return {
        "market_id": mid,
        # Avoid FK failures when events aren't present yet
        "event_id": None,
        "question": data.get("question") or data.get("title") or data.get("name"),
        "collateral": data.get("collateral") or "USDC",
        # Token ids: CLOB single-market payload uses tokens:[{token_id,outcome,...},...]
        "yes_token_id": (lambda d: ( 
            (d.get("yesToken") or d.get("yes_token_id") or d.get("yesTokenId")) or 
            ( (lambda toks: ( 
                ( (lambda names: ( 
                    (toks[names.index("yes")].get("token_id") or toks[names.index("yes")].get("tokenId") or toks[names.index("yes")].get("id")) if ("yes" in names and "no" in names) else 
                    (toks[0].get("token_id") or toks[0].get("tokenId") or toks[0].get("id"))
                ))([str(t.get("outcome") or t.get("name") or "").lower() for t in toks if isinstance(t, dict)]) )
            ))(d.get("tokens")) if isinstance(d.get("tokens"), list) and len(d.get("tokens")) >= 2 else None )
        ))(data),
        "no_token_id": (lambda d: ( 
            (d.get("noToken") or d.get("no_token_id") or d.get("noTokenId")) or 
            ( (lambda toks: ( 
                ( (lambda names: ( 
                    (toks[names.index("no")].get("token_id") or toks[names.index("no")].get("tokenId") or toks[names.index("no")].get("id")) if ("yes" in names and "no" in names) else 
                    (toks[1].get("token_id") or toks[1].get("tokenId") or toks[1].get("id"))
                ))([str(t.get("outcome") or t.get("name") or "").lower() for t in toks if isinstance(t, dict)]) )
            ))(d.get("tokens")) if isinstance(d.get("tokens"), list) and len(d.get("tokens")) >= 2 else None )
        ))(data),
        "created_ts": data.get("createdAt") or data.get("created_at") or data.get("created"),
        "resolve_ts": data.get("resolvedAt") or data.get("resolved_at"),
        "resolution": data.get("resolution"),
        "tags": data.get("tags") or [],
        "vertical": data.get("vertical"),
    }


def ensure_market_exists(market_id: str, timeout: int = 5) -> bool:
    """
    Fetch and store a single market if it doesn't exist or lacks a question.
    Returns True if market exists/was added, False on failure.
    """
    with connect(DB_URL, row_factory=dict_row) as conn, conn.cursor() as cur:
        cur.execute("SELECT question, yes_token_id, no_token_id FROM markets WHERE market_id = %s", (market_id,))
        row = cur.fetchone()
        if row:
            q  = row.get("question") if isinstance(row, dict) else row[0]
            yt = row.get("yes_token_id") if isinstance(row, dict) else row[1]
            nt = row.get("no_token_id")  if isinstance(row, dict) else row[2]
            if q and yt and nt:
                return True

        try:
            data = fetch_single_market(market_id, timeout=timeout)
            nm = normalize_single_market(data)
        except Exception as e:
            print(f"[fetch_markets] failed to fetch {market_id}: {e}")
            return False

        try:
            cur.execute(
                UPSERT_SQL,
                (
                    nm["market_id"],
                    nm.get("event_id"),
                    nm.get("question"),
                    nm.get("collateral"),
                    nm.get("yes_token_id"),
                    nm.get("no_token_id"),
                    nm.get("created_ts"),
                    nm.get("resolve_ts"),
                    nm.get("resolution"),
                    Json(nm.get("tags", [])),
                    nm.get("vertical"),
                ),
            )
            conn.commit()
            print(f"[fetch_markets] upserted {market_id} ({(nm.get('question') or '')[:60]})")
            return True
        except Exception as e:
            print(f"[fetch_markets] failed to upsert {market_id}: {e}")
            return False

def main():
    if not DB_URL:
        print("ERROR: DB_URL not set"); sys.exit(1)

    # 1) Try CLOB (best for conditionId + assets)
    rows = []
    try:
        rows = fetch_from_clob(limit=5000)
    except Exception as e:
        print("[warn] clob fetch failed:", e)

    # 2) Fallback to gamma-api
    if not rows:
        try:
            rows = fetch_from_gamma(limit=5000)
        except Exception as e:
            print("[error] gamma fetch failed:", e)

    if not rows:
        print("No markets fetched.")
        return

    normalized = []
    for m in rows:
        nm = normalize_market(m)
        if nm:
            # Avoid FK issues if events table is missing entries
            nm["event_id"] = None
            normalized.append(nm)

    if not normalized:
        print("Fetched data but could not normalize any markets.")
        return

    with connect(DB_URL) as conn, conn.cursor() as cur:
        inserted = 0
        for nm in normalized:
            cur.execute(UPSERT_SQL, (
                nm["market_id"], nm["event_id"], nm["question"], nm["collateral"],
                nm["yes_token_id"], nm["no_token_id"],
                nm["created_ts"], nm["resolve_ts"], nm["resolution"],
                Json(nm["tags"]), nm["vertical"]
            ))
            inserted += 1
        conn.commit()

    print(f"Upserted {inserted} markets (hex conditionId as market_id).")

if __name__ == "__main__":
    main()
