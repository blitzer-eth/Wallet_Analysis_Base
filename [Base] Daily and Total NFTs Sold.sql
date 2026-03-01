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
  SELECT FROM_HEX(LOWER(REPLACE('{{wallet address:}}', '0x', ''))) AS addr
),

-- Combine ERC721 and ERC1155 (Single & Batch) transfers
all_transfers AS (
  -- ERC721 Sales
  SELECT 
    evt_block_time,
    evt_block_date,
    'ERC721' as standard
  FROM erc721_base.evt_Transfer
  CROSS JOIN address
  CROSS JOIN time_filter
  WHERE "from" = addr
    AND evt_block_time >= time_filter.start_date
    AND evt_block_date >= CAST(time_filter.start_date AS DATE)

  UNION ALL

  -- ERC1155 Sales (Single)
  SELECT 
    evt_block_time,
    evt_block_date,
    'ERC1155' as standard
  FROM erc1155_base.evt_TransferSingle
  CROSS JOIN address
  CROSS JOIN time_filter
  WHERE "from" = addr
    AND evt_block_time >= time_filter.start_date
    AND evt_block_date >= CAST(time_filter.start_date AS DATE)
    
  UNION ALL

  -- ERC1155 Sales (Batch)
  SELECT 
    evt_block_time,
    evt_block_date,
    'ERC1155' as standard
  FROM erc1155_base.evt_TransferBatch
  CROSS JOIN address
  CROSS JOIN time_filter
  WHERE "from" = addr
    AND evt_block_time >= time_filter.start_date
    AND evt_block_date >= CAST(time_filter.start_date AS DATE)
),

daily_stats AS (
  SELECT
    DATE_TRUNC('day', evt_block_time) AS day,
    COUNT(*) AS nfts_sold_daily
  FROM all_transfers
  GROUP BY 1
)

SELECT
  DATE_FORMAT(day, '%Y/%m/%d') AS date,
  nfts_sold_daily,
  SUM(nfts_sold_daily) OVER (ORDER BY day) AS nfts_sold_total
FROM daily_stats
ORDER BY day DESC;
