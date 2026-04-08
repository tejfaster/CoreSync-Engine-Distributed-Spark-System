import random
import time
from src.ingestion.stock_api import fetch_stock_data
from src.queue.persistent_queue import write_batch

INTERVAL = 30
USA_STOCKS = ["AAPL","MSFT","GOOGL","TSLA","AMZW"] 
EU_STOCKS = ["SAP.DE","SIE.DE","BMW.DE","AIR.PA","OR.PA"]

if __name__ == "__main__":
    print("Starting CoreSync Persistent Streaming System...")

    last_usa_data = []
    last_eu_data =[]

    while True:
        print("\n=== NEW BATCH ===")
        
        # USA Data
        usa_data = fetch_stock_data(USA_STOCKS,"USA")

        if usa_data:
            last_usa_data = usa_data
        else:
            print("Using last USA data...")
            usa_data = last_usa_data    

        # EU Data
        eu_data = fetch_stock_data(EU_STOCKS,"EU")

        if eu_data:
            last_eu_data = eu_data
        else:
            print("Using last EU data...")
            eu_data = last_eu_data

        #Persistent Queue
        if usa_data:
            write_batch("USA",usa_data)

        if eu_data:
            write_batch("EU",eu_data)

        print(f"USA records: {len(usa_data)} | EU records: {len(eu_data)}")

        # Sleep (with jitter)
        sleep_time = INTERVAL + random.randint(1,5)
        print(f"Sleeping {sleep_time}s...\n")

        time.sleep(sleep_time)
  