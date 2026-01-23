WITH 
time_filter AS (
  SELECT 
    CASE 
      WHEN '{{Time Period}}' = 'Past Week'     THEN CURRENT_DATE - INTERVAL '7' day
      WHEN '{{Time Period}}' = 'Past Month'    THEN CURRENT_DATE - INTERVAL '30' day
      WHEN '{{Time Period}}' = 'Past 3 Months' THEN CURRENT_DATE - INTERVAL '90' day
      WHEN '{{Time Period}}' = 'Past Year'     THEN CURRENT_DATE - INTERVAL '365' day
      WHEN '{{Time Period}}' = 'All Time'      THEN CAST('2023-06-15' AS DATE)
      ELSE CURRENT_DATE - INTERVAL '30' day
    END AS start_date
),

address AS (
  SELECT FROM_HEX(REPLACE('{{wallet address:}}', '0x', '')) AS addr
),

trades_raw AS (
  SELECT 
    t.nft_contract_address,
    t.token_id,
    t.block_time,
    t.amount_original AS amount_eth,
    CASE WHEN t.seller = (SELECT addr FROM address) THEN 'SELL' ELSE 'BUY' END AS direction
  FROM nft.trades t
  CROSS JOIN address
  CROSS JOIN time_filter
  WHERE t.blockchain = 'base'
    AND (t.seller = addr OR t.buyer = addr)
    AND t.block_time >= time_filter.start_date
),

realized_pnl AS (
  SELECT 
    b.nft_contract_address,
    b.token_id,
    b.amount_eth AS buy_price_eth,
    s.amount_eth AS sell_price_eth,
    s.block_time AS sold_at,
    (s.amount_eth - b.amount_eth) AS profit_eth
  FROM trades_raw b
  INNER JOIN trades_raw s ON b.nft_contract_address = s.nft_contract_address 
    AND b.token_id = s.token_id
    AND b.direction = 'BUY' 
    AND s.direction = 'SELL'
    AND s.block_time > b.block_time
),

collection_summary AS (
  SELECT 
    p.nft_contract_address,
    COALESCE(m.name, 'Unknown Collection') AS collection_name,
    SUM(p.profit_eth) AS total_profit_eth,
    COUNT(p.token_id) AS items_sold,
    MAX(p.sold_at) AS last_sale_time
  FROM realized_pnl p
  LEFT JOIN tokens.nft m ON m.contract_address = p.nft_contract_address 
    AND m.blockchain = 'base'
  GROUP BY 1, 2
)

SELECT 
    collection_name,
    total_profit_eth,
    items_sold,
    last_sale_time,
    -- Clickable link to OpenSea Collection
    get_href(
        'https://opensea.io/assets/base/' || CAST(nft_contract_address AS VARCHAR), 
        'View on OpenSea'
    ) AS opensea_link,
    -- Clickable link to Basescan Contract
    get_href(
        get_chain_explorer_address('base', nft_contract_address), 
        CAST(nft_contract_address AS VARCHAR)
    ) AS basescan_link
FROM collection_summary
WHERE total_profit_eth > 0
ORDER BY total_profit_eth DESC
LIMIT {{Top N:}};
