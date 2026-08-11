# 📊 Base Wallet Analysis

**[View Dashboard on Dune](https://dune.com/blitzer/wallet-analysis-base)**

This dashboard provides a technical and financial activity overview of any wallet on the **Base** network. It tracks transaction fees, on-chain interactions, NFT transfers, trading cash flow, estimated collection-level performance, and frequently contacted counterparties.

Users can select a wallet address and historical period to explore its activity through seven analytical modules.

---

## :camera_flash: Snapshot

<img width="3734" height="6764" alt="Base Wallet Analysis dashboard" src="https://github.com/user-attachments/assets/b7626065-8c62-4491-925f-4c2f105029ec" />

---

## 🔍 Query Breakdown

### 1. Gas Spend Counter

Tracks the transaction fees paid by the wallet on Base.

* **ETH Spend:** Calculates daily and cumulative transaction fees in ETH.
* **Cost Analysis:** Shows how the wallet’s network costs have changed over the selected period.
* **Base Fee Coverage:** Includes Base transaction-fee components available through Dune’s standardized gas dataset.

### 2. Transaction Counter

Measures the wallet’s on-chain activity across multiple interaction layers.

* **Activity Types:** Separates native transactions, successful internal traces, ERC-20 transfers, ERC-721 transfers, and ERC-1155 transfer events.
* **Daily Activity:** Identifies periods of increased or reduced network usage.
* **Cumulative Activity:** Tracks the wallet’s total recorded activity throughout the selected period.

Activity categories represent different on-chain events and should not be interpreted as a deduplicated count of unique transactions.

### 3. NFT Transfer Activity

Tracks inbound and outbound ERC-721 and ERC-1155 transfer events.

* **Outbound Transfers:** Counts NFT transfer events where the wallet is the sender.
* **Inbound Transfers:** Counts NFT transfer events where the wallet is the recipient.
* **Cumulative Movement:** Shows how NFT transfer activity develops over time.

An inbound or outbound transfer does not necessarily represent a purchase or sale. It may also represent a mint, gift, wallet-to-wallet transfer, or protocol interaction. Each ERC-1155 batch event is counted once, regardless of how many token IDs it contains.

### 4. NFT Trade Cash Flow

Tracks ETH-denominated NFT trading cash flow over time.

* **Signed Cash Flow:** NFT sales are positive, while NFT purchases are negative.
* **Price Normalization:** Converts supported trade values into ETH using historical WETH/USD prices.
* **Gas Allocation:** Distributes transaction gas across multiple NFT trade rows within the same transaction to prevent double counting.
* **Cumulative Cash Flow:** Shows the wallet’s running net NFT trading inflow or outflow.

This module measures net trading cash flow rather than realized or mark-to-market P/L. It does not include the current value or cost basis of NFTs still held.

### 5. Top NFT Collection Profit

Ranks collections with the highest estimated realized ETH profit.

* **Profit Ranking:** Compares estimated gains from sequentially matched NFT purchases and sales.
* **Matched Sales:** Shows how many matched sale records contribute to each collection’s result.
* **Recent Activity:** Displays the most recent matched sale time.
* **Direct Access:** Provides links to OpenSea and Basescan for additional verification.

The estimate excludes gas, marketplace fees, royalties, transfers, current holdings, and NFTs acquired before the selected period.

### 6. Top NFT Collection Loss

Ranks collections with the largest estimated realized ETH losses.

* **Loss Ranking:** Identifies collections whose matched purchases and sales generated the lowest estimated results.
* **Matched Sales:** Shows how many matched sale records contribute to each collection’s loss.
* **Recent Activity:** Displays the most recent matched sale time.
* **Direct Access:** Provides links to OpenSea and Basescan for additional verification.

The same simplified matching methodology and limitations used by the collection-profit module apply here.

### 7. Top Interacted Counterparties

Identifies the addresses most frequently involved in native transactions with the wallet.

* **Interaction Volume:** Counts native transactions sent to or received from each address or smart contract.
* **Gas Impact:** Calculates transaction fees paid by the selected wallet for outgoing interactions with each counterparty.
* **Timeline Tracking:** Records the first and most recent interaction timestamps within the selected period.

This module covers native Base transactions and does not treat every token-transfer participant as a transaction counterparty.
