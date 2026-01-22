WITH 
time_filter AS (
  SELECT 
    CASE 
      WHEN '{{Time Period}}' = 'Past Week'     THEN CURRENT_DATE - INTERVAL '7' day
      WHEN '{{Time Period}}' = 'Past Month'    THEN CURRENT_DATE - INTERVAL '1' month
      WHEN '{{Time Period}}' = 'Past 3 Months' THEN CURRENT_DATE - INTERVAL '3' month
      WHEN '{{Time Period}}' = 'Past Year'     THEN CURRENT_DATE - INTERVAL '1' year
      WHEN '{{Time Period}}' = 'All Time'      THEN CAST('2023-06-15' AS DATE)
      ELSE CURRENT_DATE - INTERVAL '30' day
    END AS start_date
),

address AS (
  SELECT FROM_HEX(REPLACE('{{wallet address:}}', '0x', '')) AS addr
),

eth_prices AS (
    SELECT 
        minute,
        price AS eth_usd_price
    FROM prices.usd
    WHERE blockchain = 'base'
    AND symbol = 'WETH'
    AND minute >= (SELECT start_date FROM time_filter)
),

trades_raw AS (
  SELECT 
    t.block_time AS time,
    t.block_date AS day,
    t.tx_hash,
    t.project AS marketplace,
    t.nft_contract_address,
    t.token_id,
    CASE WHEN t.seller = (SELECT addr FROM address) THEN 'SELL' ELSE 'BUY' END AS direction,
    (CASE 
        WHEN t.seller = (SELECT addr FROM address) THEN (t.amount_usd / NULLIF(ep.eth_usd_price, 0))
        ELSE -(t.amount_usd / NULLIF(ep.eth_usd_price, 0)) 
     END) AS amount_eth,
    t.amount_usd,
    COALESCE(p.symbol, 'ETH') AS original_currency
  FROM nft.trades t
  LEFT JOIN tokens.erc20 p ON p.contract_address = t.currency_contract AND p.blockchain = 'base'
  LEFT JOIN eth_prices ep ON ep.minute = DATE_TRUNC('minute', t.block_time)
  CROSS JOIN address
  CROSS JOIN time_filter
  WHERE t.blockchain = 'base'
    AND (t.seller = addr OR t.buyer = addr)
    AND t.block_time >= time_filter.start_date
),

tx_gas AS (
  SELECT 
    t.hash AS tx_hash,
    (t.gas_used * t.gas_price) / 1e18 AS gas_eth
  FROM base.transactions t
  INNER JOIN (SELECT DISTINCT tx_hash FROM trades_raw) tr ON tr.tx_hash = t.hash
  CROSS JOIN address
  CROSS JOIN time_filter
  WHERE t."from" = addr
    AND t.block_time >= time_filter.start_date
),

final_output AS (
  SELECT 
    tr.day,
    tr.time,
    tr.marketplace,
    tr.direction,
    tr.amount_eth,
    COALESCE(g.gas_eth, 0) AS gas_eth_spent,
    tr.amount_eth - COALESCE(g.gas_eth, 0) AS net_eth_flow,
    -- Totals
    SUM(tr.amount_eth - COALESCE(g.gas_eth, 0)) OVER (PARTITION BY tr.day) AS daily_eth_pnl,
    SUM(tr.amount_eth - COALESCE(g.gas_eth, 0)) OVER (ORDER BY tr.time) AS cumulative_eth_pnl,
    tr.nft_contract_address,
    tr.token_id,
    tr.original_currency,
    tr.tx_hash
  FROM trades_raw tr
  LEFT JOIN tx_gas g ON g.tx_hash = tr.tx_hash
)

SELECT * FROM final_output ORDER BY time DESC;
