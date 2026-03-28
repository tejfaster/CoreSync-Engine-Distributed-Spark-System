import random
import time
from multiprocessing import Process, cpu_count , Queue
from src.ingestion.stock_api import fetch_stock_data
from src.pipeline.region_pipeline import start_job
from src.queue.persistent_queue import write_batch, clear_queue

INTERVAL = 30
USA_STOCKS = ["AAPL","MSFT","GOOGL","TSLA","AMZW"] 
EU_STOCKS = ["SAP.DE","SIE.DE","BMW.DE","AIR.PA","OR.PA"]

# Initial Start of work
# def start_workers(eu_core,asia_core,eu_queue,asia_queue):
#     eu_process = Process(target=start_job,args=(eu_core,"EU",eu_queue))
#     asia_process = Process(target=start_job,args=(asia_core,"ASIA",asia_queue))

#     eu_process.start()
#     asia_process.start()

#     return eu_process,asia_process

# Restarting worker if failed
# def restart_worker(process, region, cores, queue):
#     print(f"[WARING] {region} worker crashed. Restarting...")

#     new_job = Process(target=start_job, args=(cores,region,queue))
#     new_job.start()

#     return new_job

# Safe fetch with Backoff 
def safe_fetch():
    try:
        return fetch_flight_data()
    except Exception as e:
        if "429" in str(e):
            print("Rate limit hit! Cooling down 60s...")
            time.sleep(60)
        else:
            print(f"Fetch error:{e}")
            time.sleep(10)

        return []    

if __name__ == "__main__":
    # cores = cpu_count()
    # usable_cores = cores - 2

    # eu_core = usable_cores // 2
    # asia_core = usable_cores - eu_core

    # eu_data = Queue()
    # asia_data = Queue()
    
    #start parallel jobs 
    # eu_job,asia_job = start_workers(eu_core,asia_core,eu_data,asia_data)
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
  