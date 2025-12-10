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
    f.ts,
    o.metadata->>'reason' AS reason
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
    SUM(qty)                    AS signed_qty,
    SUM(qty * price)            AS signed_notional,
    COUNT(*)                    AS fill_count,
    MIN(ts)                     AS first_fill_ts,
    MAX(ts)                     AS last_fill_ts,
    STRING_AGG(DISTINCT reason, ',') FILTER (WHERE COALESCE(reason, '') <> '') AS reasons
  FROM fills
  GROUP BY 1,2,3
),
pos AS (
  SELECT
    strategy,
    market_id,
    side,
    qty::numeric       AS open_qty,
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
)
SELECT
  a.strategy,
  a.market_id,
  LEFT(a.market_id, 12) AS mkt,
  a.side,
  a.fill_count,
  a.first_fill_ts,
  a.last_fill_ts,
  COALESCE(a.reasons, '') AS reason,
  ROUND(a.signed_qty, 4) AS net_qty_calc,
  ROUND(COALESCE(p.open_qty, 0), 4) AS open_qty,
  ROUND(COALESCE(p.open_avg, 0), 4) AS open_avg,
  ROUND(
    COALESCE(
      CASE WHEN a.side = 'YES' THEN m.last_price_yes ELSE m.last_price_no END,
      COALESCE(p.open_avg, 0)
    ),
    4
  ) AS mark_px,
  ROUND(
    -(COALESCE(a.signed_notional, 0) - COALESCE(p.open_qty, 0) * COALESCE(p.open_avg, 0)),
    6
  ) AS realized_pnl,
  ROUND(
    COALESCE(p.open_qty, 0)
    * (
      COALESCE(
        CASE WHEN a.side = 'YES' THEN m.last_price_yes ELSE m.last_price_no END,
        COALESCE(p.open_avg, 0)
      )
      - COALESCE(p.open_avg, 0)
    ),
    6
  ) AS unrealized_pnl,
  ROUND(
    -(COALESCE(a.signed_notional, 0) - COALESCE(p.open_qty, 0) * COALESCE(p.open_avg, 0))
    + COALESCE(p.open_qty, 0)
      * (
        COALESCE(
          CASE WHEN a.side = 'YES' THEN m.last_price_yes ELSE m.last_price_no END,
          COALESCE(p.open_avg, 0)
        )
        - COALESCE(p.open_avg, 0)
      ),
    6
  ) AS total_pnl
FROM agg a
LEFT JOIN pos p
  ON p.strategy = a.strategy
 AND p.market_id = a.market_id
 AND p.side = a.side
LEFT JOIN marks m
  ON m.market_id = a.market_id
ORDER BY a.strategy, mkt, a.side;
