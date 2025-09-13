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
    
    if isinstance(json_makes['menuItem'], list):
        r_makes_array = [elem['value'] for elem in json_makes['menuItem']]
    else:
        r_makes_array = [json_makes['menuItem']['value']]
            
    return r_makes_array

def get_models(year, make):
    fuel_economy_base_url = "https://fueleconomy.gov/ws/rest/vehicle"

    headers = {
    "Accept": "application/json"
    }
    
    url_dict = {
        "models": "/menu/model"}
    
    url_dict = {key: fuel_economy_base_url + value for key, value in url_dict.items()}
    r_models = requests.get(url = url_dict["models"],  headers=headers, params={"year": year, "make": make})
    
    print(r_models.url, r_models.status_code, sep="\n")
    
    try:
        json_models = r_models.json()
    except ValueError:
        print(f'Response for {url_dict["models"]} not in JSON format')
        
    # if year == '2025': # and make == 'Bugatti':
        # print(f"MODELS JSON:", json_models)
    
    if isinstance(json_models['menuItem'], list):
        r_models_array = [elem['value'] for elem in json_models['menuItem']]
    else:
        r_models_array = [json_models['menuItem']['value']]
    
    return r_models_array

def get_vehicle_ids(year, make, model):
    fuel_economy_base_url = "https://fueleconomy.gov/ws/rest/vehicle"

    headers = {
    "Accept": "application/json"
    }
    
    url_dict = {
       "vehicle_ids": "/menu/options"}
    
    url_dict = {key: fuel_economy_base_url + value for key, value in url_dict.items()}
    r_vids = requests.get(url = url_dict["vehicle_ids"],  headers=headers, params={"year": year, "make": make, "model": model})
    
    # print(r_vids.url, r_vids.status_code, sep="\n")
    
    try:
        json_vids = r_vids.json()
    except ValueError:
        print(f'Response for {url_dict["vehicle_ids"]} not in JSON format')
        
    # if year == '2025': # and make == 'Bugatti':
        # print(f"MODELS JSON:", json_models)
    
    if isinstance(json_vids['menuItem'], list):
        r_vids_array = [elem['value'] for elem in json_vids['menuItem']]
    else:
        r_vids_array = [json_vids['menuItem']['value']]
        
    # print(f"VIDS: {r_vids_array}")
    
    return r_vids_array
    
    
def fetch_FE_data():
    
    years = get_years(1)
    
    print('YEARS:', years)
    
    makes_dict = {}
    
    for idx, v in enumerate(years):
        makes_dict[v] = get_makes(v)
    
    print(makes_dict)
    
    print('MODELS api:')
    
    models = []
    for y, m in makes_dict.items():
        for make in m:
            print('\t', y, make, '\n')
            curr_models = get_models(y, make)
            print(curr_models)
            models.extend([Model(mdl, make, int(y)) for mdl in curr_models])
            # if y == '2025' and make == 'Bugatti':
                # break
        print(f"--------------\n")
    
    for idx, m in enumerate(models):
        print(f"\tModel {idx+1} - {m}")
        
    vids_array = []
    for mdl in models:
        mdl.fetch_vehicle_ids()
        print(mdl)
        vids_array.extend(mdl.get_vehicle_ids())
        # mdl.print_vehicle_ids()
        
    for idx, vehicle in enumerate(vids_array):
        print(f"\tVehicle {idx+1} - {vehicle}")
        vehicle.get_fuel_info()
        break
    
class Vehicle:
    def __init__(self, vehicle_id: str, year: int, make: str, model: str):
        self.id = vehicle_id
        self.year = year
        self.make = make
        self.model = model

    def __repr__(self):
        # return f"{self.id} {self.year} {self.make} {self.model} "
        return " | ".join(f"{k} = '{v}'" for k, v in self.__dict__.items())
    
    def get_fuel_info(self):
                
        fuel_economy_base_url = "https://fueleconomy.gov/ws/rest/vehicle"

        headers = {
        "Accept": "application/json"
        }
        
        details = requests.get(url = f"{fuel_economy_base_url}/{self.id}",  headers=headers)
        
        # print(r_vids.url, r_vids.status_code, sep="\n")
        
        try:
            details_json = details.json()
        except ValueError:
            print(f'Response for "{fuel_economy_base_url}/{self.id}" not in JSON format')
            
        print(f"JSON RESPONSE HAS {len(details_json)} fields!")
                
    
class Model:
    def __init__(self, name: str, make: str, year: int):
        self.name = name
        self.make = make
        self.year = year
        self.vehicles: list[Vehicle] = [] 
        
    def fetch_vehicle_ids(self):
        # API call to get vehicle ids for this year/make/model
        vids = get_vehicle_ids(self.year, self.make, self.name)
        self.vehicles = [
            Vehicle(vid, self.year, self.make, self.name) for vid in vids
        ]
        
    def get_vehicle_ids(self):
        return self.vehicles
    
    def print_vehicle_ids(self):
        for elem in self.vehicles:
            print(f"\t Vehicle {elem}")
    
    def get_year(self):
        return self.year
    
    def get_make(self):
        return self.make
    
    def get_model(self):
        return self.name
    
    def __repr__(self):
        return f"{self.year} - {self.make} - {self.name} with {len(self.vehicles)} vehicle_ids"