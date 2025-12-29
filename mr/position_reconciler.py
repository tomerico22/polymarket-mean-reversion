#!/usr/bin/env python3
"""
Position Reconciler - Syncs DB positions with Polymarket reality.
Runs every 60 seconds to ensure DB matches actual holdings.
"""
import os
import time
import requests
import psycopg
from decimal import Decimal

# Load env
with open('/root/polymarket-mean-reversion/.env') as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, v = line.split('=', 1)
            os.environ[k] = v

DB_URL = os.environ['DB_URL']
WALLET = os.environ.get('FUNDER_ADDRESS', '0x356a7bc9C5AA7553f5A32F54Fe616f0639821354')
STRATEGY = 'mean_reversion_v1'
POLL_INTERVAL = 60  # seconds

def get_polymarket_positions():
    """Fetch current positions from Polymarket API."""
    resp = requests.get(f'https://data-api.polymarket.com/positions?user={WALLET}', timeout=30)
    positions = resp.json()
    # Filter out resolved positions (curPrice near 0 and huge loss)
    active = []
    for p in positions:
        if abs(p.get('curPrice', 0)) < 0.0001 and p.get('percentPnl', 0) < -99:
            continue  # Skip resolved
        active.append(p)
    return active

def reconcile():
    """Sync DB with Polymarket positions."""
    poly_positions = get_polymarket_positions()
    
    conn = psycopg.connect(DB_URL)
    conn.autocommit = True
    cur = conn.cursor()
    
    # Get DB open positions
    cur.execute("""
        SELECT id, market_id, outcome, entry_price, size, status
        FROM mr_positions 
        WHERE paper = false AND strategy = %s AND status IN ('open', 'closing')
    """, (STRATEGY,))
    db_positions = {(row[1], row[2]): row for row in cur.fetchall()}
    
    poly_keys = set()
    
    for p in poly_positions:
        market_id = p['conditionId']
        outcome = str(p['outcomeIndex'])
        size = Decimal(str(p['size']))
        avg_price = Decimal(str(p['avgPrice']))
        title = p['title'][:40]
        
        key = (market_id, outcome)
        poly_keys.add(key)
        
        if key in db_positions:
            db_pos = db_positions[key]
            # Update size if significantly different
            if abs(float(db_pos[4]) - float(size)) > 0.5:
                cur.execute("UPDATE mr_positions SET size = %s WHERE id = %s", (size, db_pos[0]))
                print(f"[reconciler] Updated size for {db_pos[0]}: {db_pos[4]} -> {size}")
        else:
            # Create new position
            cur.execute("""
                INSERT INTO mr_positions (strategy, market_id, outcome, side, entry_price, entry_ts, size, status, paper)
                VALUES (%s, %s, %s, 'long', %s, NOW(), %s, 'open', false)
                RETURNING id
            """, (STRATEGY, market_id, outcome, avg_price, size))
            new_id = cur.fetchone()[0]
            print(f"[reconciler] Created position {new_id}: {title}")
    
    # Close DB positions not on Polymarket
    for key, db_pos in db_positions.items():
        if key not in poly_keys:
            cur.execute("UPDATE mr_positions SET status = 'closed', exit_reason = 'not_on_poly' WHERE id = %s", (db_pos[0],))
            print(f"[reconciler] Closed stale position {db_pos[0]}")
    
    conn.close()

def main():
    print(f"[reconciler] Started. Wallet={WALLET[:10]}... Poll={POLL_INTERVAL}s")
    while True:
        try:
            reconcile()
        except Exception as e:
            print(f"[reconciler] Error: {e}")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
