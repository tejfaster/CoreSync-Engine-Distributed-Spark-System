from multiprocessing import Process, cpu_count
# from src.pipeline.asia_pipeline import run_asia_pipeline
# from src.pipeline.eu_pipeline import run_eu_pipeline
from src.ingestion.flight_api import fetch_flight_data
from src.processing.region_split import split_by_region
from src.pipeline.region_pipeline import start_job

def clean_data(data):
    return{
        "icao24":str(data[0]),
        "callsign":str(data[1]).strip() if data[1] else None,
        "country":str(data[2]),
        "longitude":float(data[5]) if data[5] is not None else None,
        "latitude":float(data[6]) if data[6] is not None else None
    }

if __name__ == "__main__":

    data = fetch_flight_data()
    cleaned_data = [clean_data(item) for item in data]
    eu,asia = split_by_region(cleaned_data)
    total_data_length = len(eu) + len(asia)
    # eu_data_per = len(eu)/(total_data_length / 100)
   
    cores = cpu_count()
    MIN_CORES = 2
    usable_cores = cores - MIN_CORES
    MAX_CORES = usable_cores - MIN_CORES

    eu_core = int((len(eu) / total_data_length) * usable_cores)
    eu_core =  min(MAX_CORES,max(MIN_CORES,eu_core))
    asia_core = usable_cores - eu_core
    eu_process = Process(target=start_job,args=(eu_core,"EU",eu))
    asia_process = Process(target=start_job,args=(asia_core,"ASIA",asia))

    # eu_process = Process(target=run_eu_pipeline)
    # asia_process = Process(target=run_asia_pipeline)

    eu_process.start()
    asia_process.start()

    eu_process.join()
    asia_process.join()

    print("Both pipeline finished.")

    print("data length: ",total_data_length)
    # print("Eu data per length: ",eu_data_per)
    print("Cores EU: ",eu_core)
    print("Cores ASIA: ",asia_core)
    print("data of eu: ",len(eu))
    print("data of asia: ",len(asia))
    print("cores in system: ",cores)