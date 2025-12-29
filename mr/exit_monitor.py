#!/usr/bin/env python3
"""
Exit Monitor V2 - Uses live order book prices instead of stale trade prices.
Fetches real-time bid prices for open positions to detect TP/SL accurately.
"""
import os
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import psycopg
from psycopg.rows import dict_row

# Only import CLOB client when needed
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise SystemExit("DB_URL not set")

STRATEGY = os.getenv("MR_STRATEGY", "mean_reversion_v1")
POLL_SECS = float(os.getenv("MR_MONITOR_POLL_SECS", "10"))  # Slightly slower due to API calls
TAKE_PROFIT_PCT = Decimal(os.getenv("MR_TAKE_PROFIT_PCT", "0.15"))
STOP_LOSS_PCT = Decimal(os.getenv("MR_STOP_LOSS_PCT", "0.15"))
MAX_HOLD_HOURS = int(os.getenv("MR_MAX_HOLD_HOURS", "12"))

def dec(x) -> Decimal:
    try:
        return Decimal(str(x))
    except:
        return Decimal("0")

def get_clob_client():
    """Create CLOB client for API calls."""
    from dotenv import load_dotenv
    load_dotenv()
    
    host = 'https://clob.polymarket.com'
    chain_id = 137
    creds = ApiCreds(
        api_key=os.environ['POLY_API_KEY'],
        api_secret=os.environ['POLY_API_SECRET'],
        api_passphrase=os.environ['POLY_API_PASSPHRASE']
    )
    return ClobClient(host, chain_id=chain_id, creds=creds)

def get_token_id(client, market_id: str, outcome: int) -> str:
    """Get token_id for a market/outcome pair."""
    try:
        market = client.get_market(market_id)
        tokens = market.get('tokens', [])
        if outcome < len(tokens):
            return tokens[outcome]['token_id']
    except Exception as e:
        print(f"[exit_monitor_v2] Error getting token_id for {market_id[:16]}: {e}")
    return None

def get_live_price(client, token_id: str) -> Decimal:
    """Get live sell price (bid) for a token."""
    try:
        result = client.get_price(token_id, 'sell')
        if result and 'price' in result:
            return dec(result['price'])
    except Exception as e:
        print(f"[exit_monitor_v2] Error getting price for token {token_id[:20]}: {e}")
    return Decimal("0")

def main():
    print(f"[exit_monitor_v2] started strategy={STRATEGY} TP={TAKE_PROFIT_PCT*100}% SL={STOP_LOSS_PCT*100}%")
    print(f"[exit_monitor_v2] Using LIVE order book prices")
    
    client = get_clob_client()
    
    # Cache token_ids to reduce API calls
    token_cache = {}  # (market_id, outcome) -> token_id
    
    with psycopg.connect(DB_URL, row_factory=dict_row) as conn:
        conn.autocommit = True
        
        while True:
            now = datetime.now(timezone.utc)
            
            with conn.cursor() as cur:
                # Get open positions without exit_reason
                cur.execute("""
                    SELECT id, market_id, outcome, entry_price, entry_ts, size
                    FROM mr_positions
                    WHERE paper = false 
                      AND strategy = %s 
                      AND status = 'open' 
                      AND exit_reason IS NULL
                """, (STRATEGY,))
                
                positions = cur.fetchall()
                
                for p in positions:
                    pos_id = p['id']
                    market_id = p['market_id']
                    outcome = int(p['outcome'])
                    entry_price = dec(p['entry_price'])
                    entry_ts = p['entry_ts']
                    
                    if entry_price <= 0:
                        continue
                    
                    # Get token_id (cached)
                    cache_key = (market_id, outcome)
                    if cache_key not in token_cache:
                        token_id = get_token_id(client, market_id, outcome)
                        if token_id:
                            token_cache[cache_key] = token_id
                        else:
                            continue
                    
                    token_id = token_cache[cache_key]
                    
                    # Get live price
                    current = get_live_price(client, token_id)
                    if current <= 0:
                        continue
                    
                    pnl_pct = (current - entry_price) / entry_price
                    exit_price = current * Decimal("0.97")  # 3% slippage buffer for limit order
                    
                    exit_reason = None
                    if pnl_pct >= TAKE_PROFIT_PCT:
                        exit_reason = "tp"
                    elif pnl_pct <= -STOP_LOSS_PCT:
                        exit_reason = "sl"
                    elif entry_ts and (now - entry_ts).total_seconds() / 3600 >= MAX_HOLD_HOURS:
                        exit_reason = "time"
                    
                    # Log current status
                    print(f"[exit_monitor_v2] pos={pos_id} entry={entry_price:.3f} current={current:.3f} pnl={float(pnl_pct)*100:+.1f}%")
                    
                    if exit_reason:
                        cur.execute("""
                            UPDATE mr_positions 
                            SET exit_reason = %s, exit_signal_price = %s
                            WHERE id = %s
                        """, (exit_reason, float(exit_price), pos_id))
                        
                        print(f"[exit_monitor_v2] EXIT pos={pos_id} reason={exit_reason} "
                              f"entry={entry_price} current={current} pnl={float(pnl_pct)*100:.1f}%")
            
            time.sleep(POLL_SECS)

if __name__ == "__main__":
    main()
