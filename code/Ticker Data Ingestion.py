# Databricks notebook source
# MAGIC %md
# MAGIC # Importing required packages

# COMMAND ----------

import requests
import time
from datetime import datetime, date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, FloatType, DecimalType, DateType
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor

symbols = ["BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD", "DOGE-USD"]

#path to where raw data is being stored in parquet format. 
base_path = "/Volumes/tabular/dataexpert/shwet/capstone"


# COMMAND ----------

# MAGIC %md
# MAGIC # Defining Schema. 

# COMMAND ----------

# currency_schema = StructType([
#     StructField("symbol", StringType(), True),
#     StructField("ask", DecimalType(30,15), True),
#     StructField("bid", DecimalType(30,15), True),
#     StructField("volume", DecimalType(30,15), True),
#     StructField("trade_id", StringType(), True),
#     StructField("price", DecimalType(30,15), True),
#     StructField("size", DecimalType(30,15), True),
#     StructField("rfq_volume", DecimalType(30,15), True),
#     StructField("time", TimestampType(), True),
#     StructField("transaction_date", DateType(), True)
# ])

currency_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("ask", DecimalType(20,8), True),
    StructField("bid", DecimalType(20,8), True),
    StructField("volume", DecimalType(20,8), True),
    StructField("trade_id", StringType(), True),
    StructField("price", DecimalType(20,8), True),
    StructField("size", DecimalType(20,8), True),
    StructField("rfq_volume", DecimalType(20,8), True),
    StructField("time", TimestampType(), True),
    StructField("transaction_date", DateType(), True)
])



# COMMAND ----------

# MAGIC %md
# MAGIC # REST API Call and Schema Enforcement

# COMMAND ----------

def get_coinbase_ticker(symbol="BTC-USD"):
    url = f"https://api.exchange.coinbase.com/products/{symbol}/ticker"
    response = requests.get(url)
    data = response.json()
    data['symbol'] = symbol
    data['transaction_date'] = date.today()
    return data
    

def data_casting(data):
    cleaned = {
            "symbol": data["symbol"],
            "ask": Decimal(data["ask"]),
            "bid": Decimal(data["bid"]),
            "price": Decimal(data["price"]),
            "size": Decimal(data["size"]),
            "time": datetime.fromisoformat(data["time"].replace("Z", "+00:00")),  # to Python datetime
            "volume": Decimal(data["volume"]),
            "rfq_volume": Decimal(data["rfq_volume"]),
            "trade_id": int(data["trade_id"]),
            "transaction_date": data["transaction_date"]
        }

    return cleaned

def get_cleaned_ticker(symbol):
    record = get_coinbase_ticker(symbol)
    cleaned_record = data_casting(record)
    return cleaned_record if cleaned_record else None

# ticker = []
while True:
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(get_cleaned_ticker, symbols))

    ticker = [r for r in results if r is not None]
    if ticker:
        df = spark.createDataFrame(ticker, currency_schema)
        df.write.mode("append").partitionBy("transaction_date", "symbol").parquet(f"{base_path}/ticker/")
        time.sleep(5)