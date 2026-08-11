/*
 * Query: [Base] Gas Spend Counter
 *
 * Purpose:
 *   Calculate the daily and cumulative transaction fees paid by one
 *   wallet on Base.
 *
 * Parameters:
 *   {{wallet address:}}
 *       EVM wallet address, configured as a typed free-form parameter.
 *
 *   {{Time Period}}
 *       Manual-list parameter with the following allowed values:
 *       - Past Week
 *       - Past Month
 *       - Past 3 Months
 *       - Past Year
 *       - All Time
 *
 * Performance:
 *   Filters all partition columns before aggregation to minimize the
 *   amount of transaction-level data scanned.
 */

WITH query_parameters AS (
    SELECT
        FROM_HEX(
            LOWER(
                REPLACE('{{wallet address:}}', '0x', '')
            )
        ) AS wallet_address,

        CAST(
            CASE '{{Time Period}}'
                WHEN 'Past Week'
                    THEN CURRENT_DATE - INTERVAL '7' DAY
                WHEN 'Past Month'
                    THEN CURRENT_DATE - INTERVAL '1' MONTH
                WHEN 'Past 3 Months'
                    THEN CURRENT_DATE - INTERVAL '3' MONTH
                WHEN 'Past Year'
                    THEN CURRENT_DATE - INTERVAL '1' YEAR
                WHEN 'All Time'
                    THEN DATE '2023-06-15'
                ELSE CURRENT_DATE - INTERVAL '30' DAY
            END AS DATE
        ) AS start_date
),

daily_gas_spend AS (
    SELECT
        fees.block_date AS day,
        SUM(fees.tx_fee) AS daily_gas_eth
    FROM gas.fees AS fees
    CROSS JOIN query_parameters AS parameters
    WHERE fees.blockchain = 'base'
        AND fees.block_month >= CAST(
            DATE_TRUNC('month', parameters.start_date) AS DATE
        )
        AND fees.block_date >= parameters.start_date
        AND fees.tx_from = parameters.wallet_address
    GROUP BY fees.block_date
),

cumulative_gas_spend AS (
    SELECT
        day,
        daily_gas_eth,
        SUM(daily_gas_eth) OVER (
            ORDER BY day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_gas_eth
    FROM daily_gas_spend
)

SELECT
    DATE_FORMAT(day, '%Y/%m/%d') AS date,
    CAST(daily_gas_eth AS DECIMAL(38, 6)) AS daily_gas_eth,
    CAST(cumulative_gas_eth AS DECIMAL(38, 6)) AS cumulative_gas_eth
FROM cumulative_gas_spend
ORDER BY day DESC;
