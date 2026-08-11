/*
 * Query: [Base] Wallet NFT Transfer Activity
 *
 * Purpose:
 *   Calculate daily and cumulative inbound and outbound NFT transfer
 *   events for one wallet on Base.
 *
 * Performance strategy:
 *   - Query Base-specific event tables.
 *   - Filter directly on each table's date partition.
 *   - Aggregate each source before UNION ALL.
 *   - Avoid reading evt_block_time.
 *   - Avoid DISTINCT and string-based direction fields.
 *
 * Counting methodology:
 *   - Each ERC-721 Transfer event counts once.
 *   - Each ERC-1155 TransferSingle event counts once.
 *   - Each ERC-1155 TransferBatch event counts once.
 *   - Inbound values are negative for visualization.
 *   - Self-transfers are classified as outbound.
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

source_daily_activity AS (
    /*
     * ERC-721 transfers
     */
    SELECT
        transfers.evt_block_date AS day,

        SUM(
            CASE
                WHEN transfers."from" = parameters.wallet_address
                    THEN 1
                ELSE 0
            END
        ) AS nfts_sold_daily,

        SUM(
            CASE
                WHEN transfers."from" = parameters.wallet_address
                    THEN 0
                ELSE -1
            END
        ) AS nfts_bought_daily
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
     * ERC-1155 single transfers
     */
    SELECT
        transfers.evt_block_date AS day,

        SUM(
            CASE
                WHEN transfers."from" = parameters.wallet_address
                    THEN 1
                ELSE 0
            END
        ) AS nfts_sold_daily,

        SUM(
            CASE
                WHEN transfers."from" = parameters.wallet_address
                    THEN 0
                ELSE -1
            END
        ) AS nfts_bought_daily
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
     * ERC-1155 batch transfers
     */
    SELECT
        transfers.evt_block_date AS day,

        SUM(
            CASE
                WHEN transfers."from" = parameters.wallet_address
                    THEN 1
                ELSE 0
            END
        ) AS nfts_sold_daily,

        SUM(
            CASE
                WHEN transfers."from" = parameters.wallet_address
                    THEN 0
                ELSE -1
            END
        ) AS nfts_bought_daily
    FROM erc1155_base.evt_TransferBatch AS transfers
    CROSS JOIN query_parameters AS parameters
    WHERE transfers.evt_block_date >= parameters.start_date
        AND (
            transfers."from" = parameters.wallet_address
            OR transfers."to" = parameters.wallet_address
        )
    GROUP BY transfers.evt_block_date
),

daily_nft_activity AS (
    SELECT
        day,
        SUM(nfts_sold_daily) AS nfts_sold_daily,
        SUM(nfts_bought_daily) AS nfts_bought_daily
    FROM source_daily_activity
    GROUP BY day
)

SELECT
    DATE_FORMAT(CAST(day AS TIMESTAMP), '%Y/%m/%d') AS date,
    nfts_sold_daily,
    nfts_bought_daily,

    SUM(nfts_sold_daily) OVER (
        ORDER BY day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS nfts_sold_total,

    SUM(nfts_bought_daily) OVER (
        ORDER BY day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS nfts_bought_total
FROM daily_nft_activity
ORDER BY day DESC;
