WITH params AS (
  SELECT
    NULLIF(:strategy, '') AS strategy_filter,
    NULLIF(:start_ts, '')::timestamptz AS start_ts,
    NULLIF(:end_ts, '')::timestamptz AS end_ts
),
orders AS (
  SELECT
    so.id                    AS order_id,
    so.created_at,
    so.strategy,
    so.market_id,
    LEFT(so.market_id, 12)   AS mkt,
    so.side,
    so.qty,
    so.limit_px,
    so.status,
    COALESCE(so.paper, false) AS paper,
    so.metadata,
    so.metadata->>'signal'                AS signal,
    so.metadata->>'reason'                AS reason,
    (so.metadata->>'usd_1h')::numeric     AS usd_1h_at_order,
    (so.metadata->>'net_flow')::numeric   AS flow_1h_at_order,
    (so.metadata->>'flow_ratio')::numeric AS flow_ratio_at_order
  FROM strategy_orders so
  CROSS JOIN params p
  WHERE (p.strategy_filter IS NULL OR so.strategy = p.strategy_filter)
    AND (p.start_ts IS NULL OR so.created_at >= p.start_ts)
    AND (p.end_ts IS NULL OR so.created_at < p.end_ts)
),
fills AS (
  SELECT
    o.strategy,
    o.market_id,
    o.side,
    f.order_id,
    COUNT(*)          AS fill_count,
    SUM(f.qty)::numeric AS fill_qty,
    CASE WHEN SUM(f.qty) <> 0
         THEN (SUM(f.qty * f.price) / SUM(f.qty))::numeric
    END               AS fill_avg_px,
    MIN(f.ts)         AS first_fill_ts,
    MAX(f.ts)         AS last_fill_ts,
    CASE WHEN o.side = 'YES'
         THEN SUM(f.qty * f.price)
         ELSE SUM(-f.qty * f.price)
    END               AS signed_notional
  FROM strategy_fills f
  JOIN strategy_orders o ON o.id = f.order_id
  CROSS JOIN params p
  WHERE (p.strategy_filter IS NULL OR o.strategy = p.strategy_filter)
    AND (p.start_ts IS NULL OR f.ts >= p.start_ts)
    AND (p.end_ts IS NULL OR f.ts < p.end_ts)
  GROUP BY 1,2,3,4
),
positions AS (
  SELECT
    strategy,
    market_id,
    side,
    qty::numeric      AS open_qty,
    avg_price::numeric AS open_avg
  FROM strategy_positions
),
marks AS (
  SELECT
    market_id,
    last_price_yes,
    last_price_no
  FROM market_spotlight_1h
)
SELECT
  o.order_id,
  o.created_at,
  o.strategy,
  o.market_id,
  o.mkt,
  o.side,
  o.qty,
  o.limit_px,
  COALESCE(f.fill_qty, 0)     AS fill_qty,
  f.fill_avg_px,
  o.status,
  o.paper,
  o.signal,
  o.reason,
  o.usd_1h_at_order,
  o.flow_1h_at_order,
  o.flow_ratio_at_order,
  mkt.tags,
  CASE
    WHEN o.side = 'YES' THEN mk.last_price_yes
    ELSE mk.last_price_no
  END AS mark_px_now,
  CASE
    WHEN f.fill_qty > 0 AND f.fill_avg_px IS NOT NULL THEN
      CASE WHEN o.side = 'YES'
           THEN (COALESCE(mk.last_price_yes, f.fill_avg_px) - f.fill_avg_px) * f.fill_qty
           ELSE (f.fill_avg_px - COALESCE(mk.last_price_no, f.fill_avg_px)) * f.fill_qty
      END
  END AS u_pnl_now,
  CASE
    WHEN f.fill_qty > 0 AND f.fill_avg_px IS NOT NULL THEN
      -(
        COALESCE(f.signed_notional, 0)
        - COALESCE(p.open_qty, 0) * COALESCE(p.open_avg, 0)
      )
  END AS realized_pnl
FROM orders o
LEFT JOIN fills f ON f.order_id = o.order_id
LEFT JOIN markets mkt ON mkt.market_id = o.market_id
LEFT JOIN positions p
  ON p.strategy = o.strategy
 AND p.market_id = o.market_id
 AND p.side = o.side
LEFT JOIN marks mk ON mk.market_id = o.market_id
ORDER BY o.created_at;
