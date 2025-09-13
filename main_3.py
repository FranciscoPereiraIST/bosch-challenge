import requests
import pandas as pd
import time

class FuelEconomyAPI:
    BASE_URL = "https://fueleconomy.gov/ws/rest/vehicle"
    HEADERS = {"Accept": "application/json"}
    ENDPOINTS = {
        "get_years": "/menu/year",
        "get_makes": "/menu/make",
        "get_models": "/menu/model",
        "get_vehicle_ids": "/menu/options"
    }

    def _fetch_menu_items(self, endpoint: str, params: dict = None) -> list:
        r = requests.get(url=endpoint, headers=self.HEADERS, params=params)
        try:
            data = r.json()
        except ValueError:
            print(f"Response for {endpoint} not in JSON format")
            return []

        if "menuItem" not in data:
            return []

        menu_item = data["menuItem"]
        if isinstance(menu_item, list):
            return [elem["value"] for elem in menu_item]
        else:
            return [menu_item["value"]]

    def get_years(self) -> list:
        endpoint = f"{self.BASE_URL}{self.ENDPOINTS['get_years']}"
        return self._fetch_menu_items(endpoint)

    def get_makes(self, year: int) -> list:
        endpoint = f"{self.BASE_URL}{self.ENDPOINTS['get_makes']}"
        return self._fetch_menu_items(endpoint, params={"year": year})

    def get_models(self, year: int, make: str) -> list:
        endpoint = f"{self.BASE_URL}{self.ENDPOINTS['get_models']}"
        return self._fetch_menu_items(endpoint, params={"year": year, "make": make})

    def get_vehicle_ids(self, year: int, make: str, model: str) -> list:
        endpoint = f"{self.BASE_URL}{self.ENDPOINTS['get_vehicle_ids']}"
        return self._fetch_menu_items(endpoint, params={"year": year, "make": make, "model": model})

    def get_vehicle_details(self, vehicle_id: str) -> dict:
        endpoint = f"{self.BASE_URL}/{vehicle_id}"
        r = requests.get(endpoint, headers=self.HEADERS)
        try:
            return r.json()
        except ValueError:
            print(f"Vehicle details for {vehicle_id} not in JSON format")
            return {}


class Vehicle:
    def __init__(self, vehicle_id: str, year: int, make: str, model: str, api_client: FuelEconomyAPI):
        self.id = vehicle_id
        self.year = year
        self.make = make
        self.model = model
        self.api = api_client

    def __repr__(self):
        attributes_to_ignore = ["emissionsList", "fuel_raw", "api", "processed_df"]
        return " | ".join(f"{k} = '{v}'" for k, v in self.__dict__.items() if k not in attributes_to_ignore)

    def get_fuel_info(self):
        details_json = self.api.get_vehicle_details(self.id)
        self.emissionsList = details_json.pop("emissionsList", [])
        self.fuel_raw = details_json

    def process_fuel_info(self):
        vehicle_dict = {"vehicle_id": self.id}
        vehicle_dict.update(self.fuel_raw)
        self.processed_df = pd.DataFrame([vehicle_dict])


class Model:
    def __init__(self, name: str, make: str, year: int, api_client: FuelEconomyAPI):
        self.name = name
        self.make = make
        self.year = year
        self.api = api_client
        self.vehicles: list[Vehicle] = []

    def fetch_vehicle_ids(self):
        vids = self.api.get_vehicle_ids(self.year, self.make, self.name)
        self.vehicles = [Vehicle(vid, self.year, self.make, self.name, self.api) for vid in vids]

    def get_vehicle_ids(self):
        return self.vehicles

    def __repr__(self):
        return f"{self.year} - {self.make} - {self.name} with {len(self.vehicles)} vehicle_ids"

class FuelEconomyETL:
    def __init__(self, num_years=1, filename="FuelEconomy"):
        self.api = FuelEconomyAPI()
        self.vehicles = []
        self.num_years =  num_years
        self.filename = filename

    def extract(self):
        
        print(f"Started Extracting.....")
        years = self.api.get_years()  # example: most recent year
        years.sort(reverse=True)
        years = years[:self.num_years]
        
        print(f"\t-Extracted {len(years)} years: {years}")
        
        models = []
        for y in years:
            makes = self.api.get_makes(y)
            limit_flag = False
            limit_val = 5
            if len(makes) > limit_val:
                max_l = limit_val
                limit_flag = True
            else:
                max_l = len(makes)
                
            filtered_makes = makes[:max_l]
            
            if limit_flag:
                print(f"\t-Extracted {len(makes)} makes for {y} but only processing {len(filtered_makes)} - {filtered_makes}")
            else:
                print(f"\t-Extracted {len(makes)} makes for {y} - {filtered_makes}")
                
            for make in filtered_makes:       
                for model_name in self.api.get_models(y, make):
                    models.append(Model(model_name, make, int(y), self.api))

        vids_array = []
        for mdl in models:
            mdl.fetch_vehicle_ids()
            vids_array.extend(mdl.get_vehicle_ids())
            
        # vids_array.append(Vehicle(31873, 2025, "test", "test model", self.api))

        self.vehicles = vids_array

    def process(self):
        
        print(f"Started Processing.....")
        df_array = []
        
        total_vehicles = len(self.vehicles) 
        
        print(f"Number of Vehicle_ids extracted = {total_vehicles}")
        milestone = 10  # percentage step
        next_print = milestone

        for idx, vehicle in enumerate(self.vehicles):
            # print(f"\t-Vehicle {idx+1} - {vehicle}")
            vehicle.get_fuel_info()
            vehicle.process_fuel_info()
            df_array.append(vehicle.processed_df)
            
            # Calculate percentage completed
            percent_complete = ((idx+1) / total_vehicles) * 100
            if percent_complete >= next_print:
                print(f"Processing: {int(percent_complete)}% complete ({idx+1}/{total_vehicles} vehicles)")
                print(f"\t-Vehicle {idx+1} - {vehicle}")
                next_print += milestone
            
        self.df_fuel = pd.concat(df_array)

    def write_to_csv(self):
        current_time = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        filename = f"raw_datasets/{self.filename}_{len(self.vehicles)}_vehicles_{current_time}.csv"
        self.df_fuel.to_csv(filename, index=False)
        print(f"Data written to file '{filename}' ({self.df_fuel.shape[0]} rows, {self.df_fuel.shape[1]} cols)")

    def run_all(self):
        self.extract()
        self.process()
        self.write_to_csv()

if __name__ == "__main__":
    
    start_time = time.time()  # start timer
    
    etl = FuelEconomyETL(num_years = 2)
    # etl.run_all(filename="TEST")
    etl.run_all()
    
    end_time = time.time()  # end timer
    elapsed_time = end_time - start_time
    print(f"\nTotal processing time for FuelEconomy source: {elapsed_time/60:.2f} minutes.")