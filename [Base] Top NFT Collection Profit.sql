/*
 * Query: [Base] Most Profitable NFT Collections
 *
 * Purpose:
 *   Rank NFT collections by estimated realized ETH profit for a
 *   selected wallet on Base.
 *
 * Parameters:
 *   {{wallet address:}} - Wallet address to analyze.
 *   {{Time Period}}     - Historical period included in the analysis.
 *   {{Top N:}}          - Maximum number of collections returned.
 *
 * Methodology:
 *   - NFT purchases and sales are ordered separately for each token.
 *   - The first purchase is paired with the first subsequent sale,
 *     the second purchase with the second subsequent sale, and so on.
 *   - USD trade values are converted to ETH using minute-level WETH prices.
 *   - Collection profit equals matched sale value minus matched purchase value.
 *   - Only collections with positive total profit are returned.
 *
 * Interpretation:
 *   This is a simplified realized-profit estimate rather than complete
 *   accounting P&L. It excludes gas, marketplace fees, royalties, current
 *   holdings, transfers, and NFTs purchased before the selected period.
 *   Bundle trades and ERC-1155 quantities may not represent one independent
 *   token acquisition per row.
 *
 * Data sources:
 *   nft.trades, prices.usd, tokens.nft
 *
 * Performance note:
 *   The legacy prices.usd table is retained because it was empirically
 *   cheaper for this WETH-only lookup. Trade pairs are constructed with
 *   conditional aggregation to avoid scanning and joining numbered trades
 *   to themselves.
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

eth_minute_prices AS (
    SELECT
        prices.minute AS price_minute,
        prices.price AS eth_usd_price
    FROM prices.usd AS prices
    CROSS JOIN query_parameters AS parameters
    WHERE prices.blockchain = 'base'
        AND prices.contract_address =
            0x4200000000000000000000000000000000000006
        AND prices.minute >= CAST(parameters.start_date AS TIMESTAMP)
),

wallet_trades AS (
    SELECT
        trades.nft_contract_address,
        trades.token_id,
        trades.block_time,
        trades.unique_trade_id,

        trades.amount_usd
            / NULLIF(prices.eth_usd_price, 0) AS amount_eth,

        CASE
            WHEN trades.seller = parameters.wallet_address
                THEN 'SELL'
            ELSE 'BUY'
        END AS direction
    FROM nft.trades AS trades
    CROSS JOIN query_parameters AS parameters
    LEFT JOIN eth_minute_prices AS prices
        ON prices.price_minute =
            DATE_TRUNC('minute', trades.block_time)
    WHERE trades.blockchain = 'base'
        AND trades.block_month >= CAST(
            DATE_TRUNC('month', parameters.start_date) AS DATE
        )
        AND trades.block_date >= parameters.start_date
        AND (
            trades.seller = parameters.wallet_address
            OR trades.buyer = parameters.wallet_address
        )
),

numbered_trades AS (
    SELECT
        nft_contract_address,
        token_id,
        block_time,
        amount_eth,
        direction,

        ROW_NUMBER() OVER (
            PARTITION BY
                nft_contract_address,
                token_id,
                direction
            ORDER BY
                block_time,
                unique_trade_id
        ) AS trade_sequence
    FROM wallet_trades
    WHERE amount_eth IS NOT NULL
),

paired_trades AS (
    /*
     * Conditional aggregation produces one row per purchase-sale pair.
     * This avoids joining the complete numbered-trades dataset to itself.
     */
    SELECT
        nft_contract_address,
        token_id,
        trade_sequence,

        MAX(
            CASE
                WHEN direction = 'BUY' THEN amount_eth
            END
        ) AS buy_price_eth,

        MAX(
            CASE
                WHEN direction = 'SELL' THEN amount_eth
            END
        ) AS sell_price_eth,

        MAX(
            CASE
                WHEN direction = 'BUY' THEN block_time
            END
        ) AS bought_at,

        MAX(
            CASE
                WHEN direction = 'SELL' THEN block_time
            END
        ) AS sold_at
    FROM numbered_trades
    GROUP BY
        nft_contract_address,
        token_id,
        trade_sequence
),

realized_profit AS (
    SELECT
        nft_contract_address,
        token_id,
        buy_price_eth,
        sell_price_eth,
        sold_at,
        sell_price_eth - buy_price_eth AS profit_eth
    FROM paired_trades
    WHERE buy_price_eth IS NOT NULL
        AND sell_price_eth IS NOT NULL
        AND sold_at > bought_at
),

collection_metadata AS (
    /*
     * Grouping prevents duplicated metadata rows from multiplying
     * collection profit during the join.
     */
    SELECT
        contract_address,
        MAX(name) AS collection_name
    FROM tokens.nft
    WHERE blockchain = 'base'
    GROUP BY contract_address
),

collection_profit_summary AS (
    SELECT
        profit.nft_contract_address,
        COALESCE(
            metadata.collection_name,
            'Unknown Collection'
        ) AS collection_name,
        SUM(profit.profit_eth) AS total_profit_eth,
        COUNT(*) AS items_sold,
        MAX(profit.sold_at) AS last_sale_time
    FROM realized_profit AS profit
    LEFT JOIN collection_metadata AS metadata
        ON metadata.contract_address =
            profit.nft_contract_address
    GROUP BY
        profit.nft_contract_address,
        COALESCE(
            metadata.collection_name,
            'Unknown Collection'
        )
)

SELECT
    collection_name,
    total_profit_eth,
    items_sold,
    last_sale_time,

    get_href(
        'https://opensea.io/assets/base/'
            || CAST(nft_contract_address AS VARCHAR),
        'View on OpenSea'
    ) AS opensea_link,

    get_href(
        get_chain_explorer_address(
            'base',
            nft_contract_address
        ),
        CAST(nft_contract_address AS VARCHAR)
    ) AS basescan_link
FROM collection_profit_summary
WHERE total_profit_eth > 0
ORDER BY total_profit_eth DESC
LIMIT {{Top N:}};
