import time
from src.ingestion.flight_api import fetch_flight_data
from src.processing.region_split import split_by_region

def stream_flights():
    while True:
        print("\nFetching new flight data...")

        data = fetch_flight_data()

        eu, asia = split_by_region(data)

        print(f"Eu Flights: {len(eu)}")
        print(f"Asia Flights: {len(asia)}")

        print(data[:3])

        print("-"*50)

        time.sleep(10)