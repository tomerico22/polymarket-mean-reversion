#!/usr/bin/env python3
"""
Check Order Fills - Polls Polymarket for order status and creates fills.
Updates orders from 'live' to 'matched' when they fill.
"""
import os
import time
import json
from decimal import Decimal
from datetime import datetime, timezone
import psycopg
from psycopg.rows import dict_row

from dotenv import load_dotenv
load_dotenv()

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds

DB_URL = os.getenv("DB_URL")
POLL_SECS = float(os.getenv("MR_FILL_CHECK_POLL_SECS", "10"))

def get_client():
    return ClobClient(
        'https://clob.polymarket.com',
        chain_id=137,
        key=os.environ.get('POLY_PRIVATE_KEY'),
        creds=ApiCreds(
            api_key=os.environ['POLY_API_KEY'],
            api_secret=os.environ['POLY_API_SECRET'],
            api_passphrase=os.environ['POLY_API_PASSPHRASE']
        )
    )

def main():
    print(f"[fill_checker] started poll={POLL_SECS}s")
    client = get_client()
    
    with psycopg.connect(DB_URL, row_factory=dict_row) as conn:
        conn.autocommit = True
        
        while True:
            with conn.cursor() as cur:
                # Get live orders that might have filled
                cur.execute("""
                    SELECT id, metadata->>'clob_order_id' as clob_order_id, 
                           metadata->>'position_id' as position_id,
                           qty, limit_px
                    FROM strategy_orders 
                    WHERE paper = false 
                      AND status = 'live'
                      AND metadata->>'clob_order_id' IS NOT NULL
                    ORDER BY created_at ASC
                    LIMIT 20
                """)
                
                for row in cur.fetchall():
                    order_id = row['id']
                    clob_id = row['clob_order_id']
                    
                    try:
                        # Check order status on Polymarket
                        order = client.get_order(clob_id)
                        status = order.get('status', '').upper()
                        size_matched = Decimal(order.get('size_matched', '0'))
                        price = Decimal(order.get('price', '0'))
                        
                        if status == 'MATCHED' and size_matched > 0:
                            # Order filled! Update DB
                            cur.execute("""
                                UPDATE strategy_orders 
                                SET status = 'matched'
                                WHERE id = %s
                            """, (order_id,))
                            
                            # Create fill record
                            cur.execute("""
                                INSERT INTO strategy_fills (order_id, qty, price, ts, paper)
                                VALUES (%s, %s, %s, NOW(), false)
                                ON CONFLICT DO NOTHING
                            """, (order_id, str(size_matched), str(price)))
                            
                            print(f"[fill_checker] order={order_id} MATCHED qty={size_matched} px={price}")
                        
                        elif status == 'CANCELED':
                            cur.execute("""
                                UPDATE strategy_orders SET status = 'canceled' WHERE id = %s
                            """, (order_id,))
                            print(f"[fill_checker] order={order_id} CANCELED")
                            
                    except Exception as e:
                        print(f"[fill_checker] error checking order={order_id}: {e}")
            
            time.sleep(POLL_SECS)

if __name__ == "__main__":
    main()
