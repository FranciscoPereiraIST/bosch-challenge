import requests
from config import FUEL_ECONOMY
import pandas as pd

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
        
        endpoint = f"{FUEL_ECONOMY['BASE_URL']}"
        details = requests.get(url = f"{endpoint}/{self.id}",  headers=FUEL_ECONOMY["HEADERS"])
        
        # print(r_vids.url, r_vids.status_code, sep="\n")
        
        try:
            details_json = details.json()
        except ValueError:
            print(f'Response for "{endpoint}" not in JSON format')
        
        print(f"JSON RESPONSE HAS {len(details_json)} fields!")
        
        # print(type(details_json), details_json)
        # save emission List else where (its a list of an object Emission, cannot simply have it as a column in a df, must be processed separately)
        self.emissionsList = details_json["emissionsList"]
        # remove emission_list from dictionary
        details_json.pop('emissionsList', None)
        self.fuel_raw = details_json
        
    def process_fuel_info(self):
        
        vehicle_dict = {"vehicle_id" : self.id}
        vehicle_dict.update(self.fuel_raw)
        
        print(vehicle_dict)
        
        df = pd.DataFrame([vehicle_dict])
        
        print(f"DF HAS SHAPE: {df.shape}")
        print(df.head())
        
        self.processed_df = df
        
        # print(f" check if emissionsList is inside the df")
        # if 'emissionList' in df.columns:
        #     print('it is!!!')
        # else:
        #     print(f"ID OF THE VEHICLE IS {df['vehicle_id']}")

class Model:
    def __init__(self, name: str, make: str, year: int):
        self.name = name
        self.make = make
        self.year = year
        self.vehicles: list[Vehicle] = [] 
            
    def extract_vehicle_ids(self):
        
        endpoint = f"{FUEL_ECONOMY['BASE_URL']}{FUEL_ECONOMY['ENDPOINTS']['get_vehicle_ids']}"
        r_vids = requests.get(url = endpoint,  headers=FUEL_ECONOMY["HEADERS"]
                              , params={"year": self.year, "make": self.make, "model": self.name})
        
        # print(r_vids.url, r_vids.status_code, sep="\n")
        
        try:
            json_vids = r_vids.json()
        except ValueError:
            print(f'Response for {endpoint} not in JSON format')
            
        # if year == '2025': # and make == 'Bugatti':
            # print(f"MODELS JSON:", json_models)
        
        if isinstance(json_vids['menuItem'], list):
            r_vids_array = [elem['value'] for elem in json_vids['menuItem']]
        else:
            r_vids_array = [json_vids['menuItem']['value']]
            
        # print(f"VIDS: {r_vids_array}")
        
        return r_vids_array
        
    def fetch_vehicle_ids(self):
        # API call to get vehicle ids for this year/make/model
        vids = self.extract_vehicle_ids()
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

class FuelEconomy:
    BASE_URL = "https://fueleconomy.gov/ws/rest/vehicle"
    HEADERS = {"Accept": "application/json"}
    ENDPOINTS = {
        "get_years" : "/menu/year",
        "get_makes" : "/menu/make",
        "get_models" : "/menu/model",
        "get_vehicle_ids" : "/menu/options"
    }
    
    def __init__(self):
        self.data = []
              
    def get_years(self, num_years):
            
        # get a list of model years
        # endpoint = f"{FUEL_ECONOMY["BASE_URL"]}/menu/year"
        endpoint = f"{self.BASE_URL}{self.ENDPOINTS['get_years']}"
        r_years = requests.get(url = endpoint, headers=self.HEADERS)

        print(r_years.url, r_years.status_code, sep="\n")
        
        try:
            json_r = r_years.json()
        except ValueError:
            print(f'Response for {endpoint} not in JSON format')
            
        r_years_array = [elem['value'] for elem in json_r['menuItem']]
        r_years_array.sort(reverse=True)
        # Choose the most recent year
        recent_years = r_years_array[:num_years]
        return recent_years

    def get_makes(self, year):
        
        # endpoint = f"{FUEL_ECONOMY["BASE_URL"]}/menu/make"
        endpoint = f"{self.BASE_URL}{self.ENDPOINTS['get_makes']}"
        r_makes = requests.get(url = endpoint, headers=self.HEADERS, params={"year": year})
        
        print(r_makes.url, r_makes.status_code, sep="\n")
        
        try:
            json_makes = r_makes.json()
        except ValueError:
            print(f'Response for {endpoint} not in JSON format')
            
        # print(f"MAKES:", json_makes)
        
        if isinstance(json_makes['menuItem'], list):
            r_makes_array = [elem['value'] for elem in json_makes['menuItem']]
        else:
            r_makes_array = [json_makes['menuItem']['value']]
                
        return r_makes_array

    def get_models(self, year, make):
        
        # endpoint = f"{FUEL_ECONOMY["BASE_URL"]}/menu/model"
        endpoint = f"{self.BASE_URL}{self.ENDPOINTS['get_models']}"
        r_models = requests.get(url = endpoint,  headers=self.HEADERS, params={"year": year, "make": make})
        
        print(r_models.url, r_models.status_code, sep="\n")
        
        try:
            json_models = r_models.json()
        except ValueError:
            print(f'Response for {endpoint} not in JSON format')
            
        # if year == '2025': # and make == 'Bugatti':
            # print(f"MODELS JSON:", json_models)
        
        if isinstance(json_models['menuItem'], list):
            r_models_array = [elem['value'] for elem in json_models['menuItem']]
        else:
            r_models_array = [json_models['menuItem']['value']]
        
        return r_models_array

    def extract(self):
        years = self.get_years(1)

        print('YEARS:', years)

        makes_dict = {}

        for idx, v in enumerate(years):
            makes_dict[v] = self.get_makes(v)

        print(makes_dict)

        print('MODELS api:')

        models = []
        for y, m in makes_dict.items():
            for make in m:
                print('\t', y, make, '\n')
                curr_models = self.get_models(y, make)
                print(curr_models)
                models.extend([Model(mdl, make, int(y)) for mdl in curr_models])
                # break
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

        vids_array.append(Vehicle(31873, 2025, "test", "test model"))
        
        self.vehicles = vids_array
        
    def process(self):
          
        df_array = []
        for idx, vehicle in enumerate(self.vehicles):
            print(f"\tVehicle {idx+1} - {vehicle}")
            vehicle.get_fuel_info()
            # data[vehicle.id] = vehicle.fuel_raw
            vehicle.process_fuel_info()
            
            df_array.append(vehicle.processed_df)
            
        full_df = pd.concat(df_array)
        self.df_fuel = full_df
    
    def write_to_csv(self, filename):
        
        current_time = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        
        filename = f"raw_datasets/{filename}_{len(self.vehicles)}_vehicles_{current_time}.csv"
        try:
            self.df_fuel.to_csv(filename, index=False)
            print(f"Data written to file '{filename}' ({self.df_fuel.shape[0]} rows, {self.df_fuel.shape[1]} cols)")
        except Exception as e:
            print(f"Failed to write data to CSV: {e}")
            
    def run_all(self):
        self.extract()
        self.process()
        self.write_to_csv(filename="FuelEconomy")