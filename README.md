# 📊 Crypto-Currency Insights — Real-Time Data Pipeline

A streaming-native, production-ready data pipeline delivering **real-time trading intelligence** from live crypto market data.

---

## 🚀 Project Purpose & Vision

### 🎯 Goal

To build a **cloud-based, real-time crypto analytics platform** using the full spectrum of data engineering tools and practices learned in a bootcamp.

- ✅ Real-time ingestion, transformation, and dashboarding  
- ✅ Streaming-native pipeline via **Delta Live Tables (DLT)**  
- ✅ Built on **Databricks**, using **Coinbase API**  
- ✅ Smart KPIs like VWAP, Buy/Sell Intent, and Whale Detection  

### 📈 What This Project Delivers

- 📊 Real-time KPIs from live Coinbase API for top 5 cryptocurrencies  
- 💡 Actionable trading signals:  
  - Buy vs. Sell Intent  
  - Whale Wall Detection  
  - VWAP by side  
  - Order Book Depth Analytics  
- 📺 Dashboards refreshing every 30 seconds for real-time monitoring  
- 🛠️ End-to-end pipeline with quality checks and modular DLT layers  

---

## 🧾 Dataset & API

### 📡 **Source:** Coinbase Public REST API

- `/products/{product_id}/book?level=2`: 100 top bids/asks  
- `/products/{product_id}/ticker`: Latest price and volume  

**Why this API?**

- No authentication needed  
- Rich financial signal content  
- Near real-time update rates  
- Great for simulating real-world trading environments  

---

## 🧰 Technology Stack

| Layer          | Technology                 | Why It Was Chosen                                                             |
| -------------- | -------------------------- | ----------------------------------------------------------------------------- |
| Ingestion      | Python + REST API          | Lightweight, scalable method for frequent data pulls                          |
| Landing Zone   | Parquet on DBFS            | Efficient columnar format for high-speed reads                                |
| Streaming      | Databricks + DLT           | Native support for Delta Lake, orchestration, and scale                       |
| Transformation | Delta Live Tables          | Modular design (Bronze → Silver → Gold), schema enforcement, & quality checks |
| Data Quality   | DLT Expectations           | Declarative checks for filtering & anomaly detection                          |
| Storage        | Delta Lake + Unity Catalog | Schema evolution & time travel                                                |
| Orchestration  | Databricks Jobs            | Auto-scheduling with monitoring and retry logic                               |
| Visualization  | Databricks SQL Dashboards  | Real-time dashboards with interactive UI                                      |

---

## ✅ Project Steps

### 1. Define Scope & KPIs

- Selected 5 coins: **BTC, ETH, ADA, SOL, DOGE**  
- Finalized KPIs: Buy/Sell Intent, VWAP, Whale Walls, etc.

### 2. Ingestion Layer

- Used `ThreadPoolExecutor` for parallel API calls  
- Saved raw JSON data as **Parquet** files every 10 seconds  

### 3. Bronze → Silver → Gold

- **Bronze**: Raw ingestion  
- **Silver**: Schema enforcement, enrichment (notional value), quality checks  
- **Gold**: Aggregations every 30s and 1d for dashboards  

### 4. Real-Time Dashboards

- Created per-symbol views with VWAP, Whale Walls, etc.  
- Dashboards auto-refresh every 30 seconds  

---

## ⚠️ Challenges & Solutions

| Challenge                        | Solution                                                          |
| -------------------------------- | ----------------------------------------------------------------- |
| Missed asks in initial ingest    | Appended both sides of order book with proper labeling            |
| File explosion from small writes | Used partitioning + `coalesce(1)` to optimize writes              |
| Schema drift from API changes    | Leveraged `_rescued_data` for error handling and schema evolution |
| Real-time aggregations lagging   | Designed DLT logic to always reprocess with checkpoints           |
| Cross-coin dashboard clutter     | Shifted to **symbol-specific dashboards**                         |

---

## 🧱 Architecture Diagram

```
+----------------------+
|  Coinbase API (REST) |
+----------+-----------+
           |
     (Every 10s)
           |
           v
+------------------------+
| Raw Ingestion Notebook |
| - Save as Parquet      |
+-----------+------------+
            |
            v
+---------------------------+
| Bronze Table (DLT)        |
+---------------------------+
            |
            v
+----------------------------+
| Silver Table (DLT)        |
| - Schema Checks           |
| - Enrichment              |
+----------------------------+
            |
            v
+-----------------------------------+
| Gold Tables (DLT)                  |
| - 30s & Daily Aggregates           |
+-----------------------------------+
            |
            v
+------------------------------+
| Dashboards (Databricks SQL)  |
+------------------------------+
```

---

## 🔍 Data Quality Checks

### ✅ Enforced via `@dlt.expect_or_drop`

- **Price**: Not null, > 0  
- **Size**: Not null, > 0  
- **Side**: Must be in `['buy', 'sell']`  
- **Symbol**: Must be in the set: BTC-USD, ETH-USD, ADA-USD, SOL-USD, DOGE-USD  

### ⚠️ Monitored via `@dlt.expect`

- **Price Ranges** (e.g., BTC: 10K–1M, DOGE: 0.01–1.5)  
- Helps detect anomalies without dropping rows  

---

## 🔢 Estimated Data Volume

| Layer          | Source             | Frequency        | Records/Fetch              | Est. Daily Records |
| -------------- | ------------------ | ---------------- | -------------------------- | ------------------ |
| **Bronze**     | Order Book Level 2 | Every 10 seconds | ~100–200 bids + asks/coin  | ~8.6M+             |
| **Bronze**     | Ticker             | Every 10 seconds | 1 per coin                 | ~43,200            |
| **Silver**     | Cleaned Streams    | Streaming        | Same as Bronze             | ~8.6M+             |
| **Gold (30s)** | Aggregated KPIs    | Every 30 seconds | ~5 rows per window         | ~14,400            |
| **Gold (1d)**  | Aggregated KPIs    | Daily            | 1 row per coin             | 5                  |

> ⚙️ Weekly total rows processed: **60M+**

---

## 📈 Value Delivered

| **Value Area**                    | **What This Project Enables**                                                                              |
| --------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| **Real-Time Market Sentiment**    | Instantly detect **buy vs. sell intent** via order book imbalance metrics.                                 |
| **Whale Order Detection**         | Identify and monitor **large, concentrated trades**—crucial for anticipating breakouts or dumps.           |
| **VWAP Analytics**                | Offers **Volume Weighted Average Price** by side to guide smarter order placements.                        |
| **Support & Resistance Insights** | Monitor shifts in order book depth to uncover **emerging support and resistance levels**.                  |
| **Symbol-Specific Dashboards**    | Track **coin-specific KPIs** in near real-time with dashboards refreshing every 30 seconds.                |
| **Scalability & Extensibility**   | Easily onboard new symbols, KPIs, and extend to broader crypto coverage.                                   |
| **Streaming Data Readiness**      | Built on a **streaming-native architecture**, handling millions of rows/day with real-time transformation. |

---

## Live Dashboard Screenshots:

![image](https://github.com/user-attachments/assets/3e9a12ba-e725-4559-a77a-fff8a349dd08)

![image](https://github.com/user-attachments/assets/73d6a5e8-1b16-4e37-949f-1d2c882aa906)

![Screenshot 2025-07-06 at 9 48 41 AM](https://github.com/user-attachments/assets/fbe6c484-f54b-4469-bf53-786aab93a565)

![image](https://github.com/user-attachments/assets/8444dd8e-0a13-4ba6-98d4-09d1cb4c0eaa)

![Screenshot 2025-07-06 at 9 49 53 AM](https://github.com/user-attachments/assets/b74d6977-6d6d-49f4-9a55-6a74bef32c97)








