# config.py
FUEL_ECONOMY = {
    "BASE_URL": "https://fueleconomy.gov/ws/rest/vehicle",
    "HEADERS": {"Accept": "application/json"},
    "ENDPOINTS": {
        "get_years" : "/menu/year",
        "get_makes" : "/menu/make",
        "get_models" : "/menu/model",
        "get_vehicle_ids" : "/menu/options"
    }
}

# meter os outros enpoints aqui??
# definir aqui uma função para ir buscar os end poitns tipo
# get_endpo

# NHTSA = {
#     "BASE_URL": "https://api.nhtsa.gov/",
#     "HEADERS": {"Accept": "application/json"}
# }
