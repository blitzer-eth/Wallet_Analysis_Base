WITH time_filter AS (
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

address_val AS (
  SELECT FROM_HEX(REPLACE('{{wallet address:}}', '0x', '')) AS addr
),

raw_interactions AS (
  SELECT 
    "to" AS counterparty, 
    gas_used * gas_price AS gas_spent, 
    block_time 
  FROM base.transactions
  CROSS JOIN address_val 
  CROSS JOIN time_filter
  WHERE "from" = addr
    AND block_time >= time_filter.start_date

  UNION ALL

  SELECT 
    "from" AS counterparty, 
    0 AS gas_spent,
    block_time 
  FROM base.transactions
  CROSS JOIN address_val 
  CROSS JOIN time_filter
  WHERE "to" = addr
    AND block_time >= time_filter.start_date
),

tx_stats AS (
  SELECT
    counterparty,
    COUNT(*) AS tx_count,
    SUM(gas_spent) / 1e18 AS total_gas_eth,
    MIN(block_time) AS first_interaction,
    MAX(block_time) AS last_interaction
  FROM raw_interactions
  WHERE counterparty IS NOT NULL
  GROUP BY 1
)

SELECT
  LOWER(
    CONCAT(
      '0x',
      SUBSTRING(TO_HEX(counterparty), 1, 2),
      '...',
      SUBSTRING(TO_HEX(counterparty), -2)
    )
  ) AS counterparty_short,
  tx_count,
  ROUND(total_gas_eth, 4) AS total_gas_spent_eth,
  first_interaction,
  last_interaction
FROM tx_stats
ORDER BY tx_count DESC
LIMIT 10;
