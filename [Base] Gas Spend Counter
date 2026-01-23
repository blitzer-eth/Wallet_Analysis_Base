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

input_wallet AS (
  SELECT FROM_HEX(LOWER(REPLACE('{{wallet address:}}', '0x', ''))) AS address
),

txns AS (
  SELECT
    DATE_TRUNC('day', block_time) AS day,
    (gas_used * gas_price) / 1e18 AS gas_eth
  FROM base.transactions
  CROSS JOIN input_wallet
  CROSS JOIN time_filter
  WHERE transactions."from" = input_wallet.address
    AND block_time >= time_filter.start_date
    AND block_date >= CAST(time_filter.start_date AS DATE)
),

daily_gas AS (
  SELECT
    day,
    SUM(gas_eth) AS daily_gas_eth
  FROM txns
  GROUP BY 1
),

cumulative_gas AS (
  SELECT
    day,
    daily_gas_eth,
    SUM(daily_gas_eth) OVER (ORDER BY day) AS cumulative_gas_eth
  FROM daily_gas
)

SELECT
  DATE_FORMAT(day, '%Y/%m/%d') AS date,
  CAST(daily_gas_eth AS DECIMAL(18,6)) AS daily_gas_eth,
  CAST(cumulative_gas_eth AS DECIMAL(18,6)) AS cumulative_gas_eth
FROM cumulative_gas
ORDER BY day DESC;
