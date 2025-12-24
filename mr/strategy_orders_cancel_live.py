#!/usr/bin/env python3
import os, time, json
from psycopg import connect
from psycopg.rows import dict_row

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise SystemExit("DB_URL not set")

CLOB_HTTP = os.getenv("POLY_CLOB_HTTP_BASE", "https://clob.polymarket.com").rstrip("/")
CHAIN_ID = int((os.getenv("POLY_CHAIN_ID") or "137").strip())

PK     = (os.getenv("PRIVATE_KEY") or "").strip()
FUNDER = (os.getenv("FUNDER_ADDRESS") or "").strip()

POLL_SECS = float((os.getenv("MR_CANCEL_POLL_SECS") or "2").strip())
BATCH     = int((os.getenv("MR_CANCEL_BATCH") or "10").strip())
DRY_RUN   = (os.getenv("MR_CANCEL_DRY_RUN", "0").strip() != "0")

def make_client() -> ClobClient:
    creds = ApiCreds(
        api_key=(os.getenv("POLY_API_KEY") or "").strip(),
        api_secret=(os.getenv("POLY_API_SECRET") or "").strip(),
        api_passphrase=(os.getenv("POLY_API_PASSPHRASE") or "").strip(),
    )
    return ClobClient(
        CLOB_HTTP,
        chain_id=CHAIN_ID,
        key=PK,
        creds=creds,
        funder=FUNDER,
        signature_type=2,  # proxy wallet (same as submit)
    )

def _append_meta(cur, db_id: int, patch: dict, new_status: str | None = None):
    if new_status is None:
        cur.execute(
            "UPDATE strategy_orders SET metadata = COALESCE(metadata,'{}'::jsonb) || %s::jsonb WHERE id=%s",
            (json.dumps(patch), db_id),
        )
    else:
        cur.execute(
            "UPDATE strategy_orders SET status=%s, metadata = COALESCE(metadata,'{}'::jsonb) || %s::jsonb WHERE id=%s",
            (new_status, json.dumps(patch), db_id),
        )

def cancel_one(client: ClobClient, clob_order_id: str):
    # py_clob_client method names vary by version - try a few safely
    if hasattr(client, "cancel_order"):
        return client.cancel_order(clob_order_id)
    if hasattr(client, "cancel"):
        return client.cancel(clob_order_id)
    if hasattr(client, "cancel_orders"):
        return client.cancel_orders([clob_order_id])
    raise RuntimeError("No cancel method found on ClobClient")

def main():
    print(f"[cancel_live] start poll={POLL_SECS}s batch={BATCH} dry_run={DRY_RUN} clob={CLOB_HTTP} chain={CHAIN_ID}")

    client = None if DRY_RUN else make_client()

    while True:
        with connect(DB_URL, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, market_id, outcome, status, metadata
                    FROM strategy_orders
                    WHERE paper=false
                      AND status='cancel_requested'
                      AND COALESCE(metadata->>'clob_order_id','') <> ''
                    ORDER BY created_at ASC
                    LIMIT %s
                    """,
                    (BATCH,),
                )
                rows = cur.fetchall()

                if not rows:
                    pass
                else:
                    for r in rows:
                        db_id = int(r["id"])
                        md = r.get("metadata") or {}
                        clob_id = (md.get("clob_order_id") or "").strip()
                        if not clob_id:
                            _append_meta(cur, db_id, {"cancel_err": "missing_clob_order_id"}, new_status="cancel_failed")
                            continue

                        print(f"[cancel_live] cancel_requested id={db_id} clob_order_id={clob_id}")

                        if DRY_RUN:
                            _append_meta(cur, db_id, {"cancel_dry_run": True, "cancel_ts": time.time()}, new_status="cancel_skipped_dry_run")
                            continue

                        try:
                            resp = cancel_one(client, clob_id)
                            _append_meta(cur, db_id, {"cancel_ts": time.time(), "cancel_resp": resp}, new_status="canceled")
                            print(f"[cancel_live] OK id={db_id}")
                        except Exception as e:
                            _append_meta(cur, db_id, {"cancel_ts": time.time(), "cancel_err": f"{type(e).__name__}: {str(e)}"}, new_status="cancel_failed")
                            print(f"[cancel_live] FAIL id={db_id} err={type(e).__name__}: {e}")

            conn.commit()

        time.sleep(POLL_SECS)

if __name__ == "__main__":
    main()
