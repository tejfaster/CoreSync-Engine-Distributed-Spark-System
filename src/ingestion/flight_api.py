import requests

def fetch_flight_data():
    url = "https://opensky-network.org/api/states/all"

    try:
        response = requests.get(url,timeout =10)
        response.raise_for_status()
        data = response.json()
        return data["states"]
    except Exception as e:
        print("Error fetching flight data:", e)
        return []

