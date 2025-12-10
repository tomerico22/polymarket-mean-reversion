WITH params AS (
  SELECT
    NULLIF(:strategy, '') AS strategy_filter,
    NULLIF(:start_ts, '')::timestamptz AS start_ts,
    NULLIF(:end_ts, '')::timestamptz AS end_ts
),
fills AS (
  SELECT
    o.strategy,
    o.market_id,
    o.side,
    f.qty::numeric AS qty,
    f.price::numeric AS price,
    f.ts
  FROM strategy_fills f
  JOIN strategy_orders o ON o.id = f.order_id
  CROSS JOIN params p
  WHERE (p.strategy_filter IS NULL OR o.strategy = p.strategy_filter)
    AND (p.start_ts IS NULL OR f.ts >= p.start_ts)
    AND (p.end_ts IS NULL OR f.ts < p.end_ts)
),
agg AS (
  SELECT
    strategy,
    market_id,
    side,
    SUM(qty)         AS signed_qty,
    SUM(qty * price) AS signed_notional
  FROM fills
  GROUP BY 1,2,3
),
pos AS (
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
    m.market_id,
    COALESCE(y.last_price, ms.last_price_yes) AS last_price_yes,
    COALESCE(n.last_price, ms.last_price_no)  AS last_price_no,
    GREATEST(y.last_price_ts, n.last_price_ts, ms.last_price_ts) AS last_price_ts
  FROM markets m
  LEFT JOIN asset_signals_1h y
    ON y.market_id = m.market_id AND y.asset_id::text = m.yes_token_id::text
  LEFT JOIN asset_signals_1h n
    ON n.market_id = m.market_id AND n.asset_id::text = m.no_token_id::text
  LEFT JOIN market_spotlight_1h ms
    ON ms.market_id = m.market_id
),
per_market AS (
  SELECT
    a.strategy,
    a.market_id,
    a.side,
    COALESCE(p.open_qty, 0) AS open_qty,
    COALESCE(p.open_avg, 0) AS open_avg,
    COALESCE(
      CASE WHEN a.side = 'YES' THEN m.last_price_yes ELSE m.last_price_no END,
      COALESCE(p.open_avg, 0)
    ) AS mark_px,
    -(COALESCE(a.signed_notional, 0) - COALESCE(p.open_qty, 0) * COALESCE(p.open_avg, 0)) AS realized_pnl
  FROM agg a
  LEFT JOIN pos p
    ON p.strategy = a.strategy
   AND p.market_id = a.market_id
   AND p.side = a.side
  LEFT JOIN marks m
    ON m.market_id = a.market_id
)
SELECT
  strategy,
  ROUND(SUM(realized_pnl), 6) AS realized_pnl,
  ROUND(SUM(open_qty * (mark_px - open_avg)), 6) AS unrealized_pnl,
  ROUND(SUM(realized_pnl + open_qty * (mark_px - open_avg)), 6) AS total_pnl,
  COUNT(DISTINCT market_id) AS markets_traded
FROM per_market
GROUP BY strategy
ORDER BY strategy;
