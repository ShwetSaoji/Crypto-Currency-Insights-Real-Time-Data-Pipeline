
#VWAP Logic: 
SELECT
  window_start,
  symbol,
  vwap_buy,
  vwap_sell
FROM tabular.dataexpert.gold_orderbook_30s_metrics
WHERE window_start >= now() - INTERVAL 1 DAY
ORDER BY window_start


# Buy/Bullish Intent Logic:
SELECT
  window_start,
  symbol,
  buy_intent_percent
FROM tabular.dataexpert.gold_orderbook_30s_metrics
WHERE window_start >= now() - INTERVAL 1 DAY
ORDER BY window_start

# Buy/Sell Notional Trend:
SELECT
  window_start,
  symbol,
  total_buy_notional,
  total_sell_notional
FROM tabular.dataexpert.gold_orderbook_30s_metrics 
where window_start >= now() - INTERVAL 1 DAY
ORDER BY window_start

# Whale Order Trend:
SELECT
  window_start,
  symbol,
  buy_whale_orders,
  sell_whale_orders
FROM tabular.dataexpert.gold_orderbook_30s_metrics
WHERE window_start >= now() - INTERVAL 1 DAY
ORDER BY window_start

#Market Overall Trend (Bullish/Bearish):
SELECT
  window_start,
  symbol,
  (total_buy_notional - total_sell_notional) AS notional_delta
FROM tabular.dataexpert.gold_orderbook_30s_metrics
WHERE window_start >= now() - INTERVAL 1 DAY
ORDER BY window_start
