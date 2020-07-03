# TvWebHook
Tradingview WebHook to MQ and Mongo
接收TradingView报警信息，并推送到RabbitMq和Mongo数据库

Tradingview报警信息格式
{"alert":"FRSI",
  "exchange":"{{exchange}}",
  "ticker":"{{ticker}}",
  "timeframe":"1m",
  "rsi": {{plot_0}}
  }
