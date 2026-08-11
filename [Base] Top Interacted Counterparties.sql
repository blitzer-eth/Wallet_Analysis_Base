/*
 * Query: [Base] Top Wallet Counterparties
 *
 * Purpose:
 *   Identify the ten addresses that interacted most frequently with
 *   a selected wallet on Base.
 *
 * Metrics:
 *   - Number of transactions involving each counterparty
 *   - Gas paid by the selected wallet
 *   - First interaction
 *   - Most recent interaction
 *
 * Parameters:
 *   {{wallet address:}} - Base wallet address as a hexadecimal string.
 *   {{Time Period}}     - Dashboard time-period selection.
 *
 * Counting methodology:
 *   - Each blockchain transaction is counted once.
 *   - Incoming transactions contribute zero to the selected wallet's gas.
 *   - Self-transactions are counted once rather than twice.
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
                    THEN CURRENT_DATE - INTERVAL '30' DAY
                WHEN 'Past 3 Months'
                    THEN CURRENT_DATE - INTERVAL '90' DAY
                WHEN 'Past Year'
                    THEN CURRENT_DATE - INTERVAL '365' DAY
                WHEN 'All Time'
                    THEN DATE '2023-06-15'
                ELSE CURRENT_DATE - INTERVAL '30' DAY
            END AS DATE
        ) AS start_date
),

wallet_interactions AS (
    SELECT
        CASE
            WHEN fees.tx_from = parameters.wallet_address
                THEN fees.tx_to
            ELSE fees.tx_from
        END AS counterparty,

        CASE
            WHEN fees.tx_from = parameters.wallet_address
                THEN fees.tx_fee
            ELSE CAST(0 AS DOUBLE)
        END AS gas_spent_eth,

        fees.block_time
    FROM gas.fees AS fees
    CROSS JOIN query_parameters AS parameters
    WHERE fees.blockchain = 'base'
        AND fees.block_month >= CAST(
            DATE_TRUNC('month', parameters.start_date) AS DATE
        )
        AND fees.block_date >= parameters.start_date
        AND (
            fees.tx_from = parameters.wallet_address
            OR fees.tx_to = parameters.wallet_address
        )
),

top_counterparties AS (
    SELECT
        counterparty,
        COUNT(*) AS tx_count,
        SUM(gas_spent_eth) AS total_gas_spent_eth,
        MIN(block_time) AS first_interaction,
        MAX(block_time) AS last_interaction
    FROM wallet_interactions
    WHERE counterparty IS NOT NULL
    GROUP BY counterparty
    ORDER BY
        tx_count DESC,
        counterparty
    LIMIT 10
)

SELECT
    CONCAT(
        '0x',
        SUBSTRING(LOWER(TO_HEX(counterparty)), 1, 4),
        '...',
        SUBSTRING(LOWER(TO_HEX(counterparty)), -4)
    ) AS counterparty_short,

    tx_count,
    ROUND(total_gas_spent_eth, 4) AS total_gas_spent_eth,
    first_interaction,
    last_interaction
FROM top_counterparties
ORDER BY
    tx_count DESC,
    counterparty;
