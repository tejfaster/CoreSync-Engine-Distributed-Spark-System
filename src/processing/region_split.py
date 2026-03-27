def split_by_region(flights):
    europe = []
    asia = []

    for flight in flights:

        longitude = flight.get("longitude")
        latitude = flight.get("latitude")

        # skip if missing
        if longitude is None or latitude is None:
            continue

        # Europe
        if 35 <= latitude <= 70 and -10 <= longitude <= 40:
            europe.append(flight)

        # Asia
        elif 5 <= latitude <= 55 and 60 <= longitude <=150:
            asia.append(flight)

    return europe,asia        
