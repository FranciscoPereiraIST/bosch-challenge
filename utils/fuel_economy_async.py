import asyncio
import aiohttp  # async replacement for requests
import pandas as pd
import sys

def inspect_df(df: pd.DataFrame, name: str = "DataFrame", n: int = 5):
    """Prints basic info about a DataFrame: its name, shape, and head rows."""
    print(f"\nInspecting {name}")
    print(f"Shape: {df.shape[0]} rows × {df.shape[1]} cols")
    print(f"Preview (first {n} rows):")
    print(df.head(n))


class FuelEconomyAPI:
    BASE_URL = "https://fueleconomy.gov/ws/rest/vehicle"
    BASE_MPG_SUMMARY_URL = "https://www.fueleconomy.gov/ws/rest/ympg/shared/ympgVehicle"
    BASE_MPG_DETAIL_URL = "https://www.fueleconomy.gov/ws/rest/ympg/shared/ympgDriverVehicle"
    HEADERS = {"Accept": "application/json"}
    ENDPOINTS = {
        "get_years": "/menu/year",
        "get_makes": "/menu/make",
        "get_models": "/menu/model",
        "get_vehicle_ids": "/menu/options"
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
                if r.status == 204:
                    return None
                try:
                    return await r.json()
                except Exception:
                    print(f"Response from {url} not in JSON format")
                    return None

    async def _fetch_menu_items(self, endpoint: str, params: dict = None) -> list:
        data = await self._fetch(endpoint, params=params)
        if not data or "menuItem" not in data:
            return []

        menu_item = data["menuItem"]
        if isinstance(menu_item, list):
            return [elem["value"] for elem in menu_item]
        else:
            return [menu_item["value"]]

    async def get_years(self) -> list:
        endpoint = f"{self.BASE_URL}{self.ENDPOINTS['get_years']}"
        return await self._fetch_menu_items(endpoint)

    async def get_makes(self, year: int) -> list:
        endpoint = f"{self.BASE_URL}{self.ENDPOINTS['get_makes']}"
        return await self._fetch_menu_items(endpoint, params={"year": year})

    async def get_models(self, year: int, make: str) -> list:
        endpoint = f"{self.BASE_URL}{self.ENDPOINTS['get_models']}"
        return await self._fetch_menu_items(endpoint, params={"year": year, "make": make})

    async def get_vehicle_ids(self, year: int, make: str, model: str) -> list:
        endpoint = f"{self.BASE_URL}{self.ENDPOINTS['get_vehicle_ids']}"
        return await self._fetch_menu_items(endpoint, params={"year": year, "make": make, "model": model})

    async def get_vehicle_details(self, vehicle_id: str) -> dict:
        endpoint = f"{self.BASE_URL}/{vehicle_id}"
        return await self._fetch(endpoint)

    async def get_MPG_summary(self, url: str, vehicle_id: str) -> dict:
        endpoint = f"{url}/{vehicle_id}"
        return await self._fetch(endpoint)


class Vehicle:
    def __init__(self, vehicle_id: str, year: int, make: str, model: str, api_client: FuelEconomyAPI):
        self.id = vehicle_id
        self.year = year
        self.make = make
        self.model = model
        self.api = api_client

    def __repr__(self):
        attributes_to_ignore = ["emissionsList", "fuel_raw", "api", "processed_df", "emissions_df", "mpg_summary_df", "mpg_detail_df"]
        return " | ".join(f"{k} = '{v}'" for k, v in self.__dict__.items() if k not in attributes_to_ignore)

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
    def __init__(self, name: str, make: str, year: int, api_client: FuelEconomyAPI):
        self.name = name
        self.make = make
        self.year = year
        self.api = api_client
        self.vehicles: list[Vehicle] = []

    async def fetch_vehicle_ids(self):
        vids = await self.api.get_vehicle_ids(self.year, self.make, self.name)
        self.vehicles = [Vehicle(vid, self.year, self.make, self.name, self.api) for vid in vids]

    def get_vehicle_ids(self):
        return self.vehicles

    def __repr__(self):
        return f"{self.year} - {self.make} - {self.name} with {len(self.vehicles)} vehicle_ids"


class FuelEconomyETL:
    def __init__(self, num_years=1, concurrency=10):
        self.num_years = num_years
        self.vehicles = []
        self.concurrency = concurrency  # limit concurrent requests

    async def _safe_concat(self, df_list):
        if any(curr_df is not None for curr_df in df_list):
            return pd.concat(df_list)
        else:
            return None

    async def extract(self, api: FuelEconomyAPI):
        print(f"Started Extracting.....")
        years = await api.get_years()
        years.sort(reverse=True)
        years = years[:self.num_years]
        print(f"\t-Extracted {len(years)} years: {years}")

        models = []
        for y in years:
            makes = await api.get_makes(y)
            # filtered_makes = makes[:20]  # limit for testing
            filtered_makes = makes[:len(makes)]
            print(f"\t-Processing {len(filtered_makes)} makes for {y} - {filtered_makes}")

            for make in filtered_makes:
                model_names = await api.get_models(y, make)
                for model_name in model_names:
                    models.append(Model(model_name, make, int(y), api))

        # fetch vehicle ids concurrently
        await asyncio.gather(*(m.fetch_vehicle_ids() for m in models))

        vids_array = []
        for mdl in models:
            vids_array.extend(mdl.get_vehicle_ids())

        # add test vehicles
        vids_array.append(Vehicle(31873, 2025, "test", "test model", api))
        vids_array.append(Vehicle(26425, 2021, "Another_test", "test model 2", api))

        self.vehicles = vids_array

    async def process(self):
        print(f"Started Processing.....")
        df_array, df_emissions_array, df_mpg_summary_array, df_mpg_detail_array = [], [], [], []
        total_vehicles = len(self.vehicles)
        print(f"Number of Vehicle_ids extracted = {total_vehicles}")

        processed_count = 0
        next_print_percent = 25  # next milestone to print

        async def process_vehicle(vehicle):
            nonlocal processed_count, next_print_percent

            # Fetch and process data
            await vehicle.get_fuel_info()
            vehicle.process_fuel_info()
            df_array.append(vehicle.processed_df)

            vehicle.process_emissions_list()
            if vehicle.emissions_flag_exist:
                df_emissions_array.append(vehicle.emissions_df)

            await vehicle.get_MPG_summary_info()
            if vehicle.mpg_flag_summary_exist:
                df_mpg_summary_array.append(vehicle.mpg_summary_df)

            await vehicle.get_MPG_detail_info()
            if vehicle.mpg_flag_detail_exist:
                df_mpg_detail_array.append(vehicle.mpg_detail_df)

            # Update counter and check for milestone prints
            processed_count += 1
            percent_complete = (processed_count / total_vehicles) * 100
            if percent_complete >= next_print_percent:
                print(f"Processing: {int(percent_complete)}% complete ({processed_count}/{total_vehicles} vehicles)")
                next_print_percent += next_print_percent  # increment to next milestone

        # Run all vehicle tasks concurrently
        await asyncio.gather(*(process_vehicle(v) for v in self.vehicles))

        # Concatenate DataFrames
        self.df_fuel = await self._safe_concat(df_array)
        self.emissions_df = await self._safe_concat(df_emissions_array)
        self.mpg_summary_df = await self._safe_concat(df_mpg_summary_array)
        self.mpg_detail_df = await self._safe_concat(df_mpg_detail_array)

    async def process_old(self):
        print(f"Started Processing.....")
        df_array, df_emissions_array, df_mpg_summary_array, df_mpg_detail_array = [], [], [], []
        total_vehicles = len(self.vehicles)
        print(f"Number of Vehicle_ids extracted = {total_vehicles}")

        async def process_vehicle(vehicle):
            await vehicle.get_fuel_info()
            vehicle.process_fuel_info()
            df_array.append(vehicle.processed_df)

            vehicle.process_emissions_list()
            if vehicle.emissions_flag_exist:
                df_emissions_array.append(vehicle.emissions_df)

            await vehicle.get_MPG_summary_info()
            if vehicle.mpg_flag_summary_exist:
                df_mpg_summary_array.append(vehicle.mpg_summary_df)

            await vehicle.get_MPG_detail_info()
            if vehicle.mpg_flag_detail_exist:
                df_mpg_detail_array.append(vehicle.mpg_detail_df)

        # process all vehicles concurrently
        await asyncio.gather(*(process_vehicle(v) for v in self.vehicles))

        self.df_fuel = await self._safe_concat(df_array)
        self.emissions_df = await self._safe_concat(df_emissions_array)
        self.mpg_summary_df = await self._safe_concat(df_mpg_summary_array)
        self.mpg_detail_df = await self._safe_concat(df_mpg_detail_array)

    def write_to_csv(self, df: pd.DataFrame, filename: str, df_name: str = "DataFrame"):
        if df is not None:
            current_time = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            filename = f"raw_datasets/{filename}_{len(self.vehicles)}_vehicles_{current_time}.csv"
            df.to_csv(filename, index=False)
            print(f"Dataframe '{df_name}' written to file '{filename}' ({df.shape[0]} rows, {df.shape[1]} cols)")

    async def run_all(self):
        async with aiohttp.ClientSession() as session:
            semaphore = asyncio.Semaphore(self.concurrency)
            api = FuelEconomyAPI(session, semaphore)

            await self.extract(api)
            await self.process()

            self.write_to_csv(df=self.df_fuel, filename="NEW_FuelEconomy", df_name="fuel_info")
            self.write_to_csv(df=self.emissions_df, filename="NEW_Emissions", df_name="emissions")
            self.write_to_csv(df=self.mpg_summary_df, filename="NEW_MPG_Summary", df_name="mpg_summary")
            self.write_to_csv(df=self.mpg_detail_df, filename="NEW_MPG_Detail", df_name="mpg_detail")