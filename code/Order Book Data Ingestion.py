# Databricks notebook source
import requests
from datetime import datetime, date
from decimal import Decimal
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType,DateType
from pyspark.sql import Row
from concurrent.futures import ThreadPoolExecutor
import time

symbols = ["BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD", "DOGE-USD"]
base_path = "/Volumes/tabular/dataexpert/shwet/capstone"



# COMMAND ----------

book_schema = StructType([
    StructField("symbol", StringType()),
    StructField("side", StringType()),
    StructField("price", DecimalType(20, 8)),
    StructField("size", DecimalType(20, 8)),
    StructField("total_orders", IntegerType()),
    StructField("book_time", TimestampType()),
    StructField("transaction_date", DateType(), True)
])


# COMMAND ----------

def get_coin_book(symbol):
  url = f"https://api.exchange.coinbase.com/products/{symbol}/book?level=2"
  response = requests.get(url)
  if response.status_code == 200:
    data = response.json()
    bids = data.get("bids", [])[:50]
    asks = data.get("asks", [])[:50]
    book_time = datetime.fromisoformat(data.get("time").replace("Z", "+00:00"))
    run_date = date.today()
    bid_rows = [
            Row(
                symbol=symbol,
                side="buy",
                price=Decimal(bid[0]),
                size=Decimal(bid[1]),
                total_orders=int(bid[2]),
                time=book_time,
                transaction_date=run_date
            )
            for bid in bids
        ]

    ask_rows = [
            Row(
                symbol=symbol,
                side="sell",
                price=Decimal(ask[0]),
                size=Decimal(ask[1]),
                total_orders=int(ask[2]),
                time=book_time,
                transaction_date=run_date
            )
            for ask in asks
        ]

    return bid_rows + ask_rows
  else:
      print(f"Error fetching {symbol}: {response.status_code}")
      return []


# order_book = []
# for symbol in symbols:
#     order_book.extend(get_coin_book(symbol,5))


while True:
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(get_coin_book, symbols))

    order_book = [row for result in results if result for row in result]
    if order_book:
        df = spark.createDataFrame(order_book, book_schema)
        # df.show()
        df.coalesce(1).write.mode("append").partitionBy("transaction_date", "symbol").parquet(f"{base_path}/order_book/")
        time.sleep(30)