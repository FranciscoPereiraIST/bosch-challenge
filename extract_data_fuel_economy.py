import requests


def get_years(num_years):
    
    fuel_economy_base_url = "https://fueleconomy.gov/ws/rest/vehicle"
    headers = {
    "Accept": "application/json"
    }
    
    url_dict = {
        "years":  "/menu/year",
        "makes":  "/menu/make",
        "models": "/menu/model",
        "vehicle_ids": "/menu/options",
    }
    
    url_dict = {key: fuel_economy_base_url + value for key, value in url_dict.items()}
    
    # get a list of model years
    r_years = requests.get(url = url_dict["years"],  headers=headers)

    print(r_years.url, r_years.status_code, sep="\n")
    
    try:
        json_r = r_years.json()
    except ValueError:
        print(f'Response for {url_dict["years"]} not in JSON format')
    
    r_years_array = [elem['value'] for elem in json_r['menuItem']]
    r_years_array.sort(reverse=True)
    # Choose the most recent year
    recent_years = r_years_array[:num_years]
    return recent_years

def get_makes(year):
    fuel_economy_base_url = "https://fueleconomy.gov/ws/rest/vehicle"

    headers = {
    "Accept": "application/json"
    }
    
    url_dict = {
        "years":  "/menu/year",
        "makes":  "/menu/make",
        "models": "/menu/model",
        "vehicle_ids": "/menu/options",
    }
    
    url_dict = {key: fuel_economy_base_url + value for key, value in url_dict.items()}
    r_makes = requests.get(url = url_dict["makes"],  headers=headers, params={"year": year})
    
    print(r_makes.url, r_makes.status_code, sep="\n")
    
    try:
        json_makes = r_makes.json()
    except ValueError:
        print(f'Response for {url_dict["makes"]} not in JSON format')
        
    # print(f"MAKES:", json_makes)
    
    r_makes_array = [elem['value'] for elem in json_makes['menuItem']]
    
    return r_makes_array
    
    
def fetch_FE_data():
    
    years = get_years(4)
    
    print('YEARS:', years)
    
    makes_dict = {}
    
    for idx, v in enumerate(years):
        makes_dict[v] = get_makes(v)
    
    print(makes_dict)
    