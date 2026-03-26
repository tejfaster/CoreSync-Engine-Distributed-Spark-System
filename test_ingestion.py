from src.ingestion.flight_api import fetch_flight_data
from src.processing.region_split import split_by_region

data = fetch_flight_data()

eu, asia = split_by_region(data)

print("Number of Flights:",len(data))
print("Europe Flights:",len(eu))
print("Asia Flights:",len(asia))

# numbers of flight
for flight in data[:3]:
    # print(flight)