from src.ingestion.flight_api import fetch_flight_data

data = fetch_flight_data()

print("Number of Flights:",len(data))

# numbers of flight
for flight in data[:3]:
    print(flight)