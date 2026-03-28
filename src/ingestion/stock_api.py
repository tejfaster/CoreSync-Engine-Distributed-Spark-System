import yfinance as yf
from datetime import datetime

def fetch_stock_data(symbols,region):
    data = []

    try:
        stocks = yf.download(
            symbols,
            period="1d",
            interval="1m",
            group_by="ticker",
            progress=False
            )

        for symbol in symbols:
            try:
                latest = stocks[symbol].dropna().iloc[-1]

                data.append({
                    "symbol":symbol,
                    "price":float(latest["Close"]),
                    "volume":float(latest["Volume"]),
                    "region":region,
                    "timestamp":datetime.utcnow().isoformat()
                })
            except Exception:
                continue

    except Exception as e:    
        print(f"[ERROR] Fetch failed for {region}: {e}")

    return data    