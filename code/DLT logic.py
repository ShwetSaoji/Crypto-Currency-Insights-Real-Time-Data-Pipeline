# Databricks notebook source
import dlt
from pyspark.sql.functions import col, lit, expr, sum, when, count, window
from pyspark.sql.types import DecimalType, TimestampType, DateType

# COMMAND ----------

@dlt.table(comment="Raw ticker data ingested from Parquet files written by the ingestion job.")
def bronze_ticker():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", True)
        .load("/Volumes/tabular/dataexpert/shwet/capstone/ticker/")
    )

# COMMAND ----------

@dlt.table(comment="Raw order book data ingested from Parquet files written by the ingestion job.")
def bronze_orderbook():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes",True)
        .load("/Volumes/tabular/dataexpert/shwet/capstone/order_book")
    )

# COMMAND ----------

@dlt.table(comment="Cleaned and enriched order book data with notional value",
           partition_cols=["transaction_date", "symbol"])
@dlt.expect_or_drop("valid_price", "price IS NOT NULL AND price > 0")
@dlt.expect_or_drop("valid_size", "size IS NOT NULL AND size > 0")
@dlt.expect_or_drop("valid_side", "side IN ('buy', 'sell')")
@dlt.expect_or_drop(
    "valid_symbol_list",
    "symbol IN ('BTC-USD', 'ETH-USD', 'ADA-USD', 'SOL-USD', 'DOGE-USD')"
)
@dlt.expect("btc_price_reasonable", "symbol != 'BTC-USD' OR (price BETWEEN 10000 AND 1000000)")
@dlt.expect("eth_price_reasonable", "symbol != 'ETH-USD' OR (price BETWEEN 500 AND 10000)")
@dlt.expect("ada_price_reasonable", "symbol != 'ADA-USD' OR (price BETWEEN 0.1 AND 5)")
@dlt.expect("sol_price_reasonable", "symbol != 'SOL-USD' OR (price BETWEEN 5 AND 1000)")
@dlt.expect("doge_price_reasonable", "symbol != 'DOGE-USD' OR (price BETWEEN 0.01 AND 1.5)")
def silver_orderbook():
    df = dlt.read("bronze_orderbook")
    return df.select(
        col("symbol"),
        col("side"),
        col("price").cast(DecimalType(20, 8)),
        col("size").cast(DecimalType(20, 8)),
        col("total_orders").cast("int"),
        col("book_time").cast(TimestampType()),
        col("transaction_date").cast(DateType()),
        # 💡 Add notional_value = price * size
        (col("price").cast(DecimalType(20, 8)) * col("size").cast(DecimalType(20, 8))).alias("notional_value")
    )

# COMMAND ----------

@dlt.table(comment="Cleaned and enriched ticker data for real-time crypto analytics.",
           partition_cols=["transaction_date", "symbol"])
def silver_ticker():
    df = dlt.read("bronze_ticker")

    return df.select(
        col("symbol"),
        col("price").cast(DecimalType(20, 8)),
        col("bid").cast(DecimalType(20, 8)),
        col("ask").cast(DecimalType(20, 8)),
        col("size").cast(DecimalType(20, 8)),
        col("volume").cast(DecimalType(20, 8)),
        col("time").cast(TimestampType()),
        col("transaction_date").cast(DateType()),

        # KPI-Enabling Fields
        (col("ask") - col("bid")).alias("bid_ask_spread"),
        (col("price") * col("size")).alias("notional_value")
    )

# COMMAND ----------

@dlt.table(
  name="gold_orderbook_daily_metrics",
  comment="Gold-level KPIs for order book: VWAP, Imbalance, Whale Orders",
  partition_cols=["transaction_date", "symbol"],
  table_properties={"pipelines.reset.allowed": "true"}
)
def gold_orderbook_daily_metrics():
    df = dlt.read("silver_orderbook")

    # Calculate required aggregations per symbol and transaction_date
    agg_df = df.groupBy("transaction_date", "symbol").agg(
        # Total notional per side
        sum(when(col("side") == "buy", col("notional_value"))).alias("total_buy_notional"),
        sum(when(col("side") == "sell", col("notional_value"))).alias("total_sell_notional"),

        # VWAP per side
        (sum(when(col("side") == "buy", col("price") * col("size"))) /
         sum(when(col("side") == "buy", col("size")))).alias("vwap_buy"),
        (sum(when(col("side") == "sell", col("price") * col("size"))) /
         sum(when(col("side") == "sell", col("size")))).alias("vwap_sell"),

        # Whale order count per side
        count(when((col("side") == "buy") & (col("notional_value") > 100000), True)).alias("buy_whale_orders"),
        count(when((col("side") == "sell") & (col("notional_value") > 100000), True)).alias("sell_whale_orders")
    )

    # Buy Intent Percentage
    result = agg_df.withColumn(
        "buy_intent_percent",
        (col("total_buy_notional") / (col("total_buy_notional") + col("total_sell_notional"))) * 100
    )

    return result


# COMMAND ----------

@dlt.table(
  name="gold_orderbook_30s_metrics",
  comment="Gold KPIs at 30s cadence: VWAP, Imbalance, Whale Orders",
  partition_cols=["symbol"],
  table_properties={"pipelines.reset.allowed": "true"}
)
def gold_orderbook_30s_metrics():
    df = dlt.read_stream("silver_orderbook")

    agg_df = df.groupBy(
        window(col("book_time"), "30 seconds"),
        col("symbol")
    ).agg(
        # Total notional per side
        sum(when(col("side") == "buy", col("notional_value"))).alias("total_buy_notional"),
        sum(when(col("side") == "sell", col("notional_value"))).alias("total_sell_notional"),

        # VWAP per side
        (sum(when(col("side") == "buy", col("price") * col("size"))) /
         sum(when(col("side") == "buy", col("size")))).alias("vwap_buy"),

        (sum(when(col("side") == "sell", col("price") * col("size"))) /
         sum(when(col("side") == "sell", col("size")))).alias("vwap_sell"),

        # Whale orders
        sum(when((col("side") == "buy") & (col("notional_value") > 100000), 1).otherwise(0)).alias("buy_whale_orders"),
        sum(when((col("side") == "sell") & (col("notional_value") > 100000), 1).otherwise(0)).alias("sell_whale_orders")
    ).withColumn(
        "buy_intent_percent",
        (col("total_buy_notional") / (col("total_buy_notional") + col("total_sell_notional"))) * 100
    )

    return agg_df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "symbol",
        "total_buy_notional",
        "total_sell_notional",
        "vwap_buy",
        "vwap_sell",
        "buy_whale_orders",
        "sell_whale_orders",
        "buy_intent_percent"
    )