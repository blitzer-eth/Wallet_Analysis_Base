/*
 * Query: [Base] Wallet NFT Trade Cash Flow
 *
 * Purpose:
 *   Calculate ETH-denominated NFT trade cash flow for a selected wallet
 *   on Base.
 *
 * Parameters:
 *   {{wallet address:}} - Wallet address to analyze.
 *   {{Time Period}}     - Historical period included in the analysis.
 *
 * Methodology:
 *   - NFT sales are recorded as positive ETH cash flows.
 *   - NFT purchases are recorded as negative ETH cash flows.
 *   - USD trade values are converted to ETH using minute-level WETH prices.
 *   - Gas is included only when the selected wallet submitted the transaction.
 *   - Transaction gas is allocated equally across the wallet's NFT trade
 *     rows within that transaction.
 *
 * Interpretation:
 *   The result represents net NFT trading cash flow, not realized or
 *   mark-to-market P&L. It does not include the current value or cost basis
 *   of NFTs still held.
 *
 * Data sources:
 *   nft.trades, prices.usd, base.transactions
 *
 * Performance note:
 *   The legacy prices.usd table is retained because it was empirically
 *   cheaper for this WETH-only lookup than prices.minute.
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

filtered_trades AS (
    SELECT
        trades.block_time AS time,
        trades.block_date AS day,
        trades.tx_hash,
        trades.unique_trade_id,
        trades.project AS marketplace,
        trades.nft_contract_address,
        trades.token_id,
        trades.seller,
        trades.amount_usd,
        COALESCE(trades.currency_symbol, 'ETH') AS original_currency
    FROM nft.trades AS trades
    CROSS JOIN query_parameters AS parameters
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

counted_trades AS (
    SELECT
        *,
        COUNT(*) OVER (
            PARTITION BY tx_hash
        ) AS trades_in_transaction
    FROM filtered_trades
),

valued_trades AS (
    SELECT
        trades.day,
        trades.time,
        trades.tx_hash,
        trades.unique_trade_id,
        trades.marketplace,
        trades.nft_contract_address,
        trades.token_id,
        trades.original_currency,
        trades.trades_in_transaction,

        CASE
            WHEN trades.seller = parameters.wallet_address
                THEN 'SELL'
            ELSE 'BUY'
        END AS direction,

        CASE
            WHEN trades.seller = parameters.wallet_address
                THEN trades.amount_usd
                    / NULLIF(prices.eth_usd_price, 0)
            ELSE -trades.amount_usd
                    / NULLIF(prices.eth_usd_price, 0)
        END AS amount_eth
    FROM counted_trades AS trades
    CROSS JOIN query_parameters AS parameters
    LEFT JOIN eth_minute_prices AS prices
        ON prices.price_minute =
            DATE_TRUNC('minute', trades.time)
),

trade_transactions AS (
    SELECT
        day,
        tx_hash
    FROM filtered_trades
    GROUP BY
        day,
        tx_hash
),

transaction_gas AS (
    SELECT
        transactions.hash AS tx_hash,
        transactions.block_date AS day,

        (
            CAST(transactions.gas_used AS DOUBLE)
            * CAST(transactions.gas_price AS DOUBLE)
        ) / 1e18 AS transaction_gas_eth
    FROM base.transactions AS transactions
    INNER JOIN trade_transactions AS trade_tx
        ON trade_tx.day = transactions.block_date
        AND trade_tx.tx_hash = transactions.hash
    CROSS JOIN query_parameters AS parameters
    WHERE transactions.block_date >= parameters.start_date
        AND transactions."from" = parameters.wallet_address
),

allocated_trade_flows AS (
    SELECT
        trades.day,
        trades.time,
        trades.marketplace,
        trades.direction,
        trades.amount_eth,

        COALESCE(gas.transaction_gas_eth, 0)
            / trades.trades_in_transaction AS gas_eth_spent,

        trades.amount_eth
            - (
                COALESCE(gas.transaction_gas_eth, 0)
                / trades.trades_in_transaction
            ) AS net_eth_flow,

        trades.nft_contract_address,
        trades.token_id,
        trades.original_currency,
        trades.tx_hash,
        trades.unique_trade_id
    FROM valued_trades AS trades
    LEFT JOIN transaction_gas AS gas
        ON gas.day = trades.day
        AND gas.tx_hash = trades.tx_hash
),

trade_cash_flow AS (
    SELECT
        day,
        time,
        marketplace,
        direction,
        amount_eth,
        gas_eth_spent,
        net_eth_flow,

        SUM(net_eth_flow) OVER (
            PARTITION BY day
        ) AS daily_eth_pnl,

        SUM(net_eth_flow) OVER (
            ORDER BY
                time,
                tx_hash,
                unique_trade_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_eth_pnl,

        nft_contract_address,
        token_id,
        original_currency,
        tx_hash
    FROM allocated_trade_flows
)

SELECT
    day,
    time,
    marketplace,
    direction,
    amount_eth,
    gas_eth_spent,
    net_eth_flow,
    daily_eth_pnl,
    cumulative_eth_pnl,
    nft_contract_address,
    token_id,
    original_currency,
    tx_hash,

    CASE
        WHEN daily_eth_pnl > 0 THEN daily_eth_pnl
        ELSE 0
    END AS daily_profit_eth,

    CASE
        WHEN daily_eth_pnl < 0 THEN daily_eth_pnl
        ELSE 0
    END AS daily_loss_eth
FROM trade_cash_flow
ORDER BY time DESC;
