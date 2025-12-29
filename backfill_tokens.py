import time
from psycopg import connect
from psycopg.rows import dict_row
from pipeline.ingestors.fetch_markets import fetch_single_market
from config.settings import DB_URL

BATCH = 2000
SLEEP = 0.12

QUERY = """
WITH mids AS (
  SELECT
    t.market_id,
    max(t.ts) AS last_ts
  FROM trades t
  JOIN markets m ON m.market_id = t.market_id
  WHERE t.ts >= now() - interval '7 days'
    AND (m.yes_token_id IS NULL OR m.no_token_id IS NULL)
  GROUP BY t.market_id
  ORDER BY last_ts DESC
  LIMIT %s
)
SELECT market_id FROM mids;
"""

def extract_tokens(data):
    toks = data.get("tokens")
    if not isinstance(toks, list) or len(toks) < 2:
        return None, None

    def tid(x):
        return str(x.get("token_id") or x.get("tokenId") or x.get("id")) if isinstance(x, dict) else None

    names = [str(t.get("outcome") or t.get("name") or "").lower() for t in toks if isinstance(t, dict)]
    if "yes" in names and "no" in names:
        return tid(toks[names.index("yes")]), tid(toks[names.index("no")])

    return tid(toks[0]), tid(toks[1])

with connect(DB_URL, row_factory=dict_row) as conn, conn.cursor() as cur:
    cur.execute(QUERY, (BATCH,))
    mids = [r["market_id"] for r in cur.fetchall()]

print(f"found {len(mids)} markets to backfill")

ok = 0
for mid in mids:
    try:
        data = fetch_single_market(mid, timeout=10)
        yt, nt = extract_tokens(data)
        if yt and nt:
            with connect(DB_URL) as conn2, conn2.cursor() as cur2:
                cur2.execute(
                    "UPDATE markets SET yes_token_id=%s, no_token_id=%s WHERE market_id=%s",
                    (yt, nt, mid),
                )
                conn2.commit()
                ok += 1
        time.sleep(SLEEP)
    except Exception as e:
        print(f"skip {mid}: {e}")

print(f"backfilled {ok}/{len(mids)}")
