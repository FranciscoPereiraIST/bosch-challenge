import asyncio
import aiohttp  # async replacement for requests
import pandas as pd
import os

def inspect_df(df: pd.DataFrame, name: str = "DataFrame", n: int = 5):
    
    if isinstance(df, pd.DataFrame):
        """Prints basic info about a DataFrame: its name, shape, and head rows."""
        print(f"\nInspecting {name}")
        print(f"Shape: {df.shape[0]} rows × {df.shape[1]} cols")
        print(f"Preview (first {n} rows):")
        print(df.head(n))
    else:
        print(f"Dataframe is None.")


class SafetyAdministrationAPI:
    BASE_URL = "https://api.nhtsa.gov"
    # BASE_MPG_SUMMARY_URL = "https://www.fueleconomy.gov/ws/rest/ympg/shared/ympgVehicle"
    # BASE_MPG_DETAIL_URL = "https://www.fueleconomy.gov/ws/rest/ympg/shared/ympgDriverVehicle"
    HEADERS = {"Accept": "application/json", "User-Agent": "MyApp/1.0"}
    ENDPOINTS = {
        "get_years": "/SafetyRatings",
        "get_makes": "/SafetyRatings/modelyear",
        "get_safety_ratings": "/SafetyRatings/VehicleId",
        "get_recalls": "/recalls/recallsByVehicle",
        "get_years_recalls":"/products/vehicle/modelYears",
        "get_makes_recalls":"/products/vehicle/makes",
        "get_models_recalls":"/products/vehicle/models"
    }
    
    ENDPOINTS_DICT = {"years" : {"ratings" : ENDPOINTS["get_years"], 
                                "recalls": ENDPOINTS["get_years_recalls"]
                        },
                      "makes" : {"ratings" : ENDPOINTS["get_makes"], 
                                "recalls": ENDPOINTS["get_makes_recalls"]
                        },
                      "models" : {"ratings" : ENDPOINTS["get_makes"], 
                                "recalls": ENDPOINTS["get_models_recalls"]
                        },
    }
    
    results_naming = {"ratings" : {'results' : 'Results', 'year':'ModelYear', 'make' : 'Make', 'model' : 'Model'}, 
                       "recalls": {'results' : 'results', 'year':'modelYear', 'make' : 'make', 'model' : 'model'},
    }

    def __init__(self, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore):
        self.session = session
        self.semaphore = semaphore

    async def _fetch(self, url: str, params: dict = None):
        """
        Core async request method.
        - Uses aiohttp for non-blocking HTTP calls.
        - Semaphore ensures we only run N requests at a time (avoiding overload).
        """
        async with self.semaphore:
            async with self.session.get(url, headers=self.HEADERS, params=params) as r:
                
                # print(r.status)
                
                if r.status == 204:
                    print(f"Response from {url} sent status {r.status}")
                    return None
                try:
                    return await r.json()
                except Exception:
                    print(f"Response from {url} not in JSON format")
                    return None

    async def _fetch_menu_items(self, endpoint: str, params: dict = None) -> list:
        data = await self._fetch(endpoint, params=params)
        return data

    async def get_years(self, dataset) -> list:
        # endpoint = f"{self.BASE_URL}{self.ENDPOINTS['get_years']}"
        
        if dataset == 'recalls':
            params = {'issueType': 'r'}
        else:
            params=None
            
        endpoint =  f"{self.BASE_URL}{self.ENDPOINTS_DICT['years'][dataset]}"
        data = await self._fetch_menu_items(endpoint, params=params)
        
        results_format = self.results_naming[dataset]['results']
        year_format = self.results_naming[dataset]['year']
            
        years = [result[year_format] for result in data[results_format]]
        return years

    async def get_makes(self, year: int, dataset : str) -> list:
        
        if dataset == 'recalls':
            params = {'issueType': 'r'
                      , 'modelYear' : year}
            endpoint_enriched =  f"{self.BASE_URL}{self.ENDPOINTS_DICT['makes'][dataset]}"
        elif dataset == 'ratings':
            relative_url = f"{year}"
            endpoint_enriched = f"{self.BASE_URL}{self.ENDPOINTS_DICT['makes'][dataset]}/{relative_url}"
            params=None
            
        # relative_url = f"{year}"
        # endpoint = f"{self.BASE_URL}{self.ENDPOINTS['get_makes']}/{relative_url}"
        
        data = await self._fetch_menu_items(endpoint_enriched, params=params)
        
        results_format = self.results_naming[dataset]['results']
        make_format = self.results_naming[dataset]['make']
            
        makes = [result[make_format] for result in data[results_format]]
        return makes
    
        # makes = [result["Make"] for result in data["Results"]]
        # return makes

    async def get_models(self, year: int, make: str, dataset : str) -> list:
        
        if dataset == 'recalls':
            params = {'issueType': 'r'
                      , 'modelYear' : year
                      , 'make' : make}
            
            endpoint_enriched =  f"{self.BASE_URL}{self.ENDPOINTS_DICT['models'][dataset]}"
        elif dataset == 'ratings':
            relative_url = f"{year}/make/{make}"
            endpoint_enriched = f"{self.BASE_URL}{self.ENDPOINTS_DICT['makes'][dataset]}/{relative_url}"
            params=None
            
        # relative_url = f"{year}/make/{make}"
        # endpoint = f"{self.BASE_URL}{self.ENDPOINTS['get_makes']}/{relative_url}"
        data = await self._fetch_menu_items(endpoint_enriched, params=params)
        # models = [result["Model"] for result in data["Results"]]
        
        results_format = self.results_naming[dataset]['results']
        model_format = self.results_naming[dataset]['model']
                    
        models = [result[model_format] for result in data[results_format]]
    
        return models

    async def get_vehicle_ids(self, year: int, make: str, model: str) -> list:
        relative_url = f"{year}/make/{make}/model/{model}"
        endpoint = f"{self.BASE_URL}{self.ENDPOINTS['get_makes']}/{relative_url}"
        data = await self._fetch_menu_items(endpoint)
        
        vehicle_dict = {}

        for r in data["Results"]:
            vehicle_dict[r['VehicleId']] = r['VehicleDescription']
            
        return vehicle_dict
        
    async def get_safety_ratings(self, vehicle_id: str) -> dict:
        relative_url = f"{vehicle_id}"
        endpoint = f"{self.BASE_URL}{self.ENDPOINTS['get_safety_ratings']}/{relative_url}"
        data = await self._fetch_menu_items(endpoint)
        return data["Results"]
    
    async def get_recalls(self, year: int, make: str, model: str) -> list:
        # print(f"Getting recalls for: Y {year} - Make {make} - Model {model}")
        endpoint = f"{self.BASE_URL}{self.ENDPOINTS['get_recalls']}"
        params = {"make" : make, "model" : model, "modelYear" : year}
        data = await self._fetch_menu_items(endpoint, params=params)
        return data["results"]

    async def get_MPG_summary(self, url: str, vehicle_id: str) -> dict:
        endpoint = f"{url}/{vehicle_id}"
        return await self._fetch(endpoint)


class Vehicle:
    def __init__(self, vehicle_id: str, year: int, make: str, model: str, description : str, api_client: SafetyAdministrationAPI):
        self.id = vehicle_id
        self.year = year
        self.make = make
        self.model = model
        self.description = description
        self.api = api_client

    def __repr__(self):
        attributes_to_ignore = ["emissionsList", "fuel_raw", "api", "processed_df", "emissions_df", "mpg_summary_df", "mpg_detail_df"]
        return " | ".join(f"{k} = '{v}'" for k, v in self.__dict__.items() if k not in attributes_to_ignore)
    
    async def get_safety_ratings(self):
        safety_ratings = await self.api.get_safety_ratings(self.id)
        df_ratings = pd.DataFrame(safety_ratings)
        self.df_safety_ratings = df_ratings
        
    
    async def get_fuel_info(self):
        details_json = await self.api.get_vehicle_details(self.id)
        if not details_json:
            self.emissionsList = {}
            self.fuel_raw = {}
            return

        self.emissionsList = details_json.pop("emissionsList", {})
        self.emissions_flag_exist = bool(self.emissionsList)
        self.mpg_flag_summary_exist = False
        self.mpg_flag_detail_exist = False
        self.emissions_df = None
        self.mpg_summary_df = None
        self.mpg_detail_df = None
        self.fuel_raw = details_json

    def process_fuel_info(self):
        vehicle_dict = {"vehicle_id": self.id}
        vehicle_dict.update(self.fuel_raw)
        self.processed_df = pd.DataFrame([vehicle_dict])

    def process_emissions_list(self):
        if self.emissions_flag_exist and len(self.emissionsList) > 0:
                
            data = self.emissionsList["emissionsInfo"]
            if not isinstance(data, list):
                output = [data]
            else:
                output = data
                
            # print(len(self.emissionsList), isinstance(self.emissionsList, list), isinstance(self.emissionsList, dict))
            emissions_df = pd.DataFrame(output)
            
            # print(self.emissionsList["emissionsInfo"])
            
            self.emissions_df = emissions_df
            # sys.exit()
            # except Exception as e:
            #     print(len(self.emissionsList), isinstance(self.emissionsList, list), isinstance(self.emissionsList, dict))
            #     print(self.emissionsList["emissionsInfo"], "\n", f"VEHICLE {self.id}", e)
            #     sys.exit()

    async def get_MPG_summary_info(self):
        mpg_summary = await self.api.get_MPG_summary(url=self.api.BASE_MPG_SUMMARY_URL, vehicle_id=self.id)
        if mpg_summary:
            self.mpg_flag_summary_exist = True
            self.mpg_summary_df = pd.DataFrame([mpg_summary])

    async def get_MPG_detail_info(self):
        mpg_detail = await self.api.get_MPG_summary(url=self.api.BASE_MPG_DETAIL_URL, vehicle_id=self.id)
        if mpg_detail and "yourMpgDriverVehicle" in mpg_detail:
            self.mpg_flag_detail_exist = True
            data = mpg_detail["yourMpgDriverVehicle"]
            if not isinstance(data, list):
                output = [data]
            else:
                output = data
            self.mpg_detail_df = pd.DataFrame(output)


class Model:
    def __init__(self, name: str, make: str, year: int, api_client: SafetyAdministrationAPI):
        self.name = name
        self.make = make
        self.year = year
        self.api = api_client
        self.vehicles: list[Vehicle] = []

    async def fetch_vehicle_ids(self):
        vids = await self.api.get_vehicle_ids(self.year, self.make, self.name)
        self.vehicles = [Vehicle(v_id, self.year, self.make, self.name, v_desc, self.api) for v_id, v_desc in vids.items()]

    def get_vehicle_ids(self):
        return self.vehicles
    
    async def get_recalls(self):
        recalls = await self.api.get_recalls(self.year, self.make, self.name)
        df_recalls = pd.DataFrame(recalls)
        self.df_recalls = df_recalls
        
    def get_recall_info(self):
        return self.df_recalls

    def __repr__(self):
        return f"{self.year} - {self.make} - {self.name} with {len(self.vehicles)} vehicle_ids"


class SafetyAdministrationETL:
    def __init__(self, num_years=1, concurrency=10):
        self.num_years = num_years
        self.vehicles = {}
        self.models = {}
        self.concurrency = concurrency  # limit concurrent requests

    async def _safe_concat(self, df_list):
        if any(curr_df is not None for curr_df in df_list):
            return pd.concat(df_list)
        else:
            return None

    async def extract(self, api: SafetyAdministrationAPI, dataset : str):
        print(f"Started Extracting for dataset {dataset}.....")
        years = await api.get_years(dataset=dataset)
        years.sort(reverse=True)
        years_filtered = [y for y in years if y not in ('9999', '2027')]
        years = years_filtered
        
        years = years[:self.num_years]
        print(f"\t-Dataset '{dataset}' -> Extracted {len(years)} years: {years}")
        
        models = []
        for y in years:
            makes = await api.get_makes(year = y, dataset=dataset)
            # filtered_makes = makes[:20]  # limit for testing
            filtered_makes = makes[:len(makes)]
            print(f"\t-Processing {len(filtered_makes)} makes for {y} - {filtered_makes}")

            for make in filtered_makes:
                model_names = await api.get_models(year = y, make = make, dataset=dataset)
                for model_name in model_names:
                    models.append(Model(model_name, make, int(y), api))
        
        print(f"\t-Dataset '{dataset}' -> Extracted {len(models)} models.")
                
        for idx, mdl in enumerate(models):
            print(f"Model {idx+1} -> {mdl}")
            if idx > 20:
                break

        vids_array = []
        if dataset == 'ratings':
        # fetch vehicle ids concurrently
            await asyncio.gather(*(m.fetch_vehicle_ids() for m in models))

            # vids_array = []
            for mdl in models:
                vids_array.extend(mdl.get_vehicle_ids())

        # # add test vehicles
        # vids_array.append(Vehicle(31873, 2025, "test", "test model", api))
        # vids_array.append(Vehicle(26425, 2021, "Another_test", "test model 2", api))

        # self.vehicles = {}
        # self.models = {}
        self.vehicles[dataset] = vids_array
        self.models[dataset] = models

    async def process(self):
        print(f"Started Processing.....")
        df_ratings_array, df_recalls = [], []
        # df_array, df_emissions_array, df_mpg_summary_array, df_mpg_detail_array = [], [], [], []
        total_vehicles = len(self.vehicles['ratings'])
        print(f"\t-Number of Vehicle_ids extracted = {total_vehicles}")

        processed_count = 0
        next_print_percent = 25  # next milestone to print

        async def process_vehicle(vehicle):
            nonlocal processed_count, next_print_percent

            # Fetch and process data
            await vehicle.get_safety_ratings()
            # await vehicle.get_recalls()
            # await vehicle.get_fuel_info()
            # vehicle.process_fuel_info()
            df_ratings_array.append(vehicle.df_safety_ratings)
            # df_recalls.append(vehicle.df_recalls)

            # vehicle.process_emissions_list()
            # if vehicle.emissions_flag_exist:
            #     df_emissions_array.append(vehicle.emissions_df)

            # await vehicle.get_MPG_summary_info()
            # if vehicle.mpg_flag_summary_exist:
            #     df_mpg_summary_array.append(vehicle.mpg_summary_df)

            # await vehicle.get_MPG_detail_info()
            # if vehicle.mpg_flag_detail_exist:
            #     df_mpg_detail_array.append(vehicle.mpg_detail_df)

            # Update counter and check for milestone prints
            processed_count += 1
            percent_complete = (processed_count / total_vehicles) * 100
            if percent_complete >= next_print_percent:
                print(f"Processing: {int(percent_complete)}% complete ({processed_count}/{total_vehicles} vehicles)")
                next_print_percent += next_print_percent  # increment to next milestone

        # Run all vehicle tasks concurrently
        await asyncio.gather(*(process_vehicle(v) for v in self.vehicles['ratings']))

        # Concatenate DataFrames
        self.df_safety_ratings = await self._safe_concat(df_ratings_array)
        # self.df_recalls = await self._safe_concat(df_recalls)
        
        inspect_df(self.df_safety_ratings, 'ratings_df')
        
        # inspect_df(self.df_recalls, 'recalls_df')
        
        
        # self.emissions_df = await self._safe_concat(df_emissions_array)
        # self.mpg_summary_df = await self._safe_concat(df_mpg_summary_array)
        # self.mpg_detail_df = await self._safe_concat(df_mpg_detail_array)
        
    
    async def process_recalls(self):
        print(f"Started Processing Recalls.....")
        df_recalls = []
        # df_array, df_emissions_array, df_mpg_summary_array, df_mpg_detail_array = [], [], [], []

        async def process_model(model):
            # Fetch and process data
            await model.get_recalls()
            df_recalls.append(model.get_recall_info())

        # Run all vehicle tasks concurrently
        await asyncio.gather(*(process_model(m) for m in self.models['recalls']))

        # Concatenate DataFrames
        self.df_recalls = await self._safe_concat(df_recalls)
        
        inspect_df(self.df_recalls, 'recalls_df')

    def write_to_csv(self, df: pd.DataFrame, filename: str, df_name: str = "DataFrame"):
        if df is not None:
            current_time = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            
            folder_name = "raw_datasets/NHTSafetyAdministration"
            
            os.makedirs(folder_name) if not os.path.isdir(folder_name) else None
            
            # filename = f"{folder_name}/{filename}_{len(self.vehicles)}_vehicles_{current_time}.csv"
            filename = f"{folder_name}/{filename}_{current_time}.csv"

            df.to_csv(filename, index=False)
            print(f"Dataframe '{df_name}' written to file '{filename}' ({df.shape[0]} rows, {df.shape[1]} cols)")

    async def run_all(self):
        async with aiohttp.ClientSession() as session:
            semaphore = asyncio.Semaphore(self.concurrency)
            api = SafetyAdministrationAPI(session, semaphore)

            await self.extract(api, dataset = 'ratings')
            await self.extract(api, dataset = 'recalls')
            
            # for idx, car in enumerate(self.vehicles):
            #     print(f"Vehicle {idx+1} -> {car}")
            #     if idx > 10:
            #         break
                
            await self.process()
            await self.process_recalls()

            self.write_to_csv(df=self.df_safety_ratings, filename="SafetyRatings", df_name="df_safety_ratings")
            self.write_to_csv(df=self.df_recalls, filename="Recalls", df_name="df_recalls")
            # self.write_to_csv(df=self.mpg_summary_df, filename="NEW_MPG_Summary", df_name="mpg_summary")
            # self.write_to_csv(df=self.mpg_detail_df, filename="NEW_MPG_Detail", df_name="mpg_detail")