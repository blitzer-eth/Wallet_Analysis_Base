# 📊 Base Wallet Analysis

**[View Dashboard on Dune](https://dune.com/blitzer/wallet-analysis-base)**

This dashboard provides a comprehensive technical and financial overview of any wallet on the **Base** network. It tracks asset movement, trading performance, and network interaction costs.

The dashboard is designed to transform raw on-chain data into actionable insights, specifically for NFT traders and active network users. It provides a real-time view of portfolio health and activity levels using five core modules.

---

## :camera_flash: Snapshot 
<img width="3734" height="5221" alt="dune-base-wallet" src="https://github.com/user-attachments/assets/1064d116-3423-48d6-9fd9-1f46e7c58be9" />

---

## 🔍 Query Breakdown

### 1. Gas Spend Counter

A dedicated financial log for network fees.

* **ETH Spend:** Tracks the total amount of ETH consumed by gas.
* **Cost Analysis:** Provides a clear view of how much it costs to maintain wallet operations over time.

### 2. Transaction Counter

Measures the wallet's "on-chain footprint" across different interaction layers.

* **Activity Type:** Breaks down volume by Native TXs, Internal traces, and Token transfers (ERC-20/721/1155).
* **Growth Tracking:** Shows both daily spikes in activity and long-term cumulative usage.

### 3. Daily and Total NFTs Sold

A volume-specific monitor for outbound digital assets.

* **Inventory Outflow:** Specifically counts how many NFTs (ERC-721 and ERC-1155) leave the wallet daily.
* **Sales Momentum:** Useful for tracking selling streaks and total lifetime inventory moved.

### 4. NFT Trade (Realized P/L)

Tracks the financial performance of NFT flips and acquisitions over time.

* **Net Profit:** Calculates actual ETH earned or spent per individual trade.
* **Multi-Asset:** Automatically handles trades made in **ETH, WETH, and USDC**.
* **Signed Flow:** Buy orders are displayed as negative values, while sales are positive.

### 5. Top NFT Collection Profits

A leaderboard of the wallet's most successful investments.

* **Profit Ranking:** Identifies which collections have generated the highest realized ETH returns.
* **Item Tracking:** Shows the total count of items sold within each collection to distinguish between "moonshots" and volume trading.
* **Direct Access:** Integrated deep links to **OpenSea** and **Basescan** for immediate collection verification.
