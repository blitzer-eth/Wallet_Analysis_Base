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

raw_activity AS (
    -- 1. Native Transactions
    SELECT DATE_TRUNC('day', block_time) as day, 1 as tx, 0 as internal, 0 as erc20, 0 as erc721, 0 as erc1155
    FROM base.transactions CROSS JOIN address CROSS JOIN time_filter
    WHERE ("from" = addr OR "to" = addr) AND block_time >= start_date AND block_date >= CAST(start_date AS DATE)

    UNION ALL
    -- 2. Internal Traces
    SELECT DATE_TRUNC('day', block_time) as day, 0, 1, 0, 0, 0
    FROM base.traces CROSS JOIN address CROSS JOIN time_filter
    WHERE ("from" = addr OR "to" = addr) AND block_time >= start_date AND block_date >= CAST(start_date AS DATE) AND success

    UNION ALL
    -- 3. ERC-20
    SELECT DATE_TRUNC('day', evt_block_time) as day, 0, 0, 1, 0, 0
    FROM erc20_base.evt_Transfer CROSS JOIN address CROSS JOIN time_filter
    WHERE ("from" = addr OR "to" = addr) AND evt_block_time >= start_date AND evt_block_date >= CAST(start_date AS DATE)

    UNION ALL
    -- 4. ERC-721
    SELECT DATE_TRUNC('day', evt_block_time) as day, 0, 0, 0, 1, 0
    FROM erc721_base.evt_Transfer CROSS JOIN address CROSS JOIN time_filter
    WHERE ("from" = addr OR "to" = addr) AND evt_block_time >= start_date AND evt_block_date >= CAST(start_date AS DATE)

    UNION ALL
    -- 5. ERC-1155 (Single + Batch)
    SELECT DATE_TRUNC('day', evt_block_time) as day, 0, 0, 0, 0, 1
    FROM (
        SELECT evt_block_time, evt_block_date, "from", "to" FROM erc1155_base.evt_TransferSingle
        UNION ALL
        SELECT evt_block_time, evt_block_date, "from", "to" FROM erc1155_base.evt_TransferBatch
    ) t CROSS JOIN address CROSS JOIN time_filter
    WHERE ("from" = addr OR "to" = addr) AND evt_block_time >= start_date AND evt_block_date >= CAST(start_date AS DATE)
),

daily_agg AS (
    SELECT 
        day,
        SUM(tx) as tx_count,
        SUM(internal) as internal_count,
        SUM(erc20) as erc20_transfer_count,
        SUM(erc721) as erc721_transfer_count,
        SUM(erc1155) as erc1155_transfer_count
    FROM raw_activity
    GROUP BY 1
)

SELECT
  DATE_FORMAT(day, '%Y/%m/%d') AS date,
  tx_count,
  internal_count,
  erc20_transfer_count,
  erc721_transfer_count,
  erc1155_transfer_count,
  -- Cumulative calculations
  SUM(tx_count) OVER (ORDER BY day) AS cum_tx,
  SUM(internal_count) OVER (ORDER BY day) AS cum_internal,
  SUM(erc20_transfer_count) OVER (ORDER BY day) AS cum_erc20,
  SUM(erc721_transfer_count) OVER (ORDER BY day) AS cum_erc721,
  SUM(erc1155_transfer_count) OVER (ORDER BY day) AS cum_erc1155,
  -- Total per day and total cumulative
  (tx_count + internal_count + erc20_transfer_count + erc721_transfer_count + erc1155_transfer_count) AS daily_total,
  SUM(tx_count + internal_count + erc20_transfer_count + erc721_transfer_count + erc1155_transfer_count) OVER (ORDER BY day) AS cum_total
FROM daily_agg
ORDER BY day DESC;
