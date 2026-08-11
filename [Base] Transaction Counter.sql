/*
 * Query: [Base] Wallet Activity
 *
 * Purpose:
 *   Calculate daily and cumulative activity for one wallet on Base,
 *   separated into native transactions, internal traces, ERC-20
 *   transfers, ERC-721 transfers, and ERC-1155 transfer events.
 *
 * Parameters:
 *   {{wallet address:}}
 *       Base wallet address entered as a hexadecimal string.
 *
 *   {{Time Period}}
 *       Manual-list parameter containing:
 *       - Past Week
 *       - Past Month
 *       - Past 3 Months
 *       - Past Year
 *       - All Time
 *
 * Counting methodology:
 *   - Each successful trace is counted as one internal activity.
 *   - Each ERC-1155 TransferBatch event is counted once, regardless
 *     of how many token IDs appear inside the event.
 *   - Self-transfers are counted once because each source uses a
 *     single OR condition.
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

source_daily_counts AS (
    /*
     * Native transactions
     */
    SELECT
        transactions.block_date AS day,
        'native_transaction' AS activity_type,
        COUNT(*) AS activity_count
    FROM base.transactions AS transactions
    CROSS JOIN query_parameters AS parameters
    WHERE transactions.block_date >= parameters.start_date
        AND (
            transactions."from" = parameters.wallet_address
            OR transactions."to" = parameters.wallet_address
        )
    GROUP BY transactions.block_date

    UNION ALL

    /*
     * Successful internal traces
     */
    SELECT
        traces.block_date AS day,
        'internal_trace' AS activity_type,
        COUNT(*) AS activity_count
    FROM base.traces AS traces
    CROSS JOIN query_parameters AS parameters
    WHERE traces.block_date >= parameters.start_date
        AND traces.success = TRUE
        AND (
            traces."from" = parameters.wallet_address
            OR traces."to" = parameters.wallet_address
        )
    GROUP BY traces.block_date

    UNION ALL

    /*
     * ERC-20 transfers
     */
    SELECT
        transfers.evt_block_date AS day,
        'erc20_transfer' AS activity_type,
        COUNT(*) AS activity_count
    FROM erc20_base.evt_Transfer AS transfers
    CROSS JOIN query_parameters AS parameters
    WHERE transfers.evt_block_date >= parameters.start_date
        AND (
            transfers."from" = parameters.wallet_address
            OR transfers."to" = parameters.wallet_address
        )
    GROUP BY transfers.evt_block_date

    UNION ALL

    /*
     * ERC-721 transfers
     */
    SELECT
        transfers.evt_block_date AS day,
        'erc721_transfer' AS activity_type,
        COUNT(*) AS activity_count
    FROM erc721_base.evt_Transfer AS transfers
    CROSS JOIN query_parameters AS parameters
    WHERE transfers.evt_block_date >= parameters.start_date
        AND (
            transfers."from" = parameters.wallet_address
            OR transfers."to" = parameters.wallet_address
        )
    GROUP BY transfers.evt_block_date

    UNION ALL

    /*
     * ERC-1155 single-transfer events
     */
    SELECT
        transfers.evt_block_date AS day,
        'erc1155_transfer' AS activity_type,
        COUNT(*) AS activity_count
    FROM erc1155_base.evt_TransferSingle AS transfers
    CROSS JOIN query_parameters AS parameters
    WHERE transfers.evt_block_date >= parameters.start_date
        AND (
            transfers."from" = parameters.wallet_address
            OR transfers."to" = parameters.wallet_address
        )
    GROUP BY transfers.evt_block_date

    UNION ALL

    /*
     * ERC-1155 batch-transfer events
     */
    SELECT
        transfers.evt_block_date AS day,
        'erc1155_transfer' AS activity_type,
        COUNT(*) AS activity_count
    FROM erc1155_base.evt_TransferBatch AS transfers
    CROSS JOIN query_parameters AS parameters
    WHERE transfers.evt_block_date >= parameters.start_date
        AND (
            transfers."from" = parameters.wallet_address
            OR transfers."to" = parameters.wallet_address
        )
    GROUP BY transfers.evt_block_date
),

daily_activity AS (
    SELECT
        day,

        SUM(
            CASE
                WHEN activity_type = 'native_transaction'
                    THEN activity_count
                ELSE 0
            END
        ) AS tx_count,

        SUM(
            CASE
                WHEN activity_type = 'internal_trace'
                    THEN activity_count
                ELSE 0
            END
        ) AS internal_count,

        SUM(
            CASE
                WHEN activity_type = 'erc20_transfer'
                    THEN activity_count
                ELSE 0
            END
        ) AS erc20_transfer_count,

        SUM(
            CASE
                WHEN activity_type = 'erc721_transfer'
                    THEN activity_count
                ELSE 0
            END
        ) AS erc721_transfer_count,

        SUM(
            CASE
                WHEN activity_type = 'erc1155_transfer'
                    THEN activity_count
                ELSE 0
            END
        ) AS erc1155_transfer_count
    FROM source_daily_counts
    GROUP BY day
),

daily_activity_with_totals AS (
    SELECT
        day,
        tx_count,
        internal_count,
        erc20_transfer_count,
        erc721_transfer_count,
        erc1155_transfer_count,
        (
            tx_count
            + internal_count
            + erc20_transfer_count
            + erc721_transfer_count
            + erc1155_transfer_count
        ) AS daily_total
    FROM daily_activity
)

SELECT
    DATE_FORMAT(CAST(day AS TIMESTAMP), '%Y/%m/%d') AS date,
    tx_count,
    internal_count,
    erc20_transfer_count,
    erc721_transfer_count,
    erc1155_transfer_count,

    SUM(tx_count) OVER (
        ORDER BY day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_tx,

    SUM(internal_count) OVER (
        ORDER BY day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_internal,

    SUM(erc20_transfer_count) OVER (
        ORDER BY day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_erc20,

    SUM(erc721_transfer_count) OVER (
        ORDER BY day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_erc721,

    SUM(erc1155_transfer_count) OVER (
        ORDER BY day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_erc1155,

    daily_total,

    SUM(daily_total) OVER (
        ORDER BY day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_total
FROM daily_activity_with_totals
ORDER BY day DESC;
