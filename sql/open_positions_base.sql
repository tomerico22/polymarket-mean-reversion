WITH params AS (
  SELECT NULLIF(:strategy, '') AS strategy_filter
)
SELECT
  p.strategy,
  p.market_id,
  p.side,
  p.qty::numeric       AS open_qty,
  p.avg_price::numeric AS avg_price,
  ROUND(
    CASE WHEN p.side = 'YES' THEN m.last_price_yes ELSE m.last_price_no END,
    4
  ) AS mark_px,
  ROUND(
    p.qty::numeric * (
      COALESCE(CASE WHEN p.side = 'YES' THEN m.last_price_yes ELSE m.last_price_no END, p.avg_price)
      - p.avg_price::numeric
    ),
    6
  ) AS unrealized_pnl
FROM strategy_positions p
CROSS JOIN params pr
LEFT JOIN market_spotlight_1h m ON m.market_id = p.market_id
WHERE p.qty > 0
  AND (pr.strategy_filter IS NULL OR p.strategy = pr.strategy_filter)
ORDER BY p.strategy, p.market_id, p.side;
