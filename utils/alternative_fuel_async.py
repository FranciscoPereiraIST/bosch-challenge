import asyncio
import aiohttp  # async replacement for requests
import pandas as pd
import os
import json

def inspect_df(df: pd.DataFrame, name: str = "DataFrame", n: int = 5):
    
    if isinstance(df, pd.DataFrame):
        """Prints basic info about a DataFrame: its name, shape, and head rows."""
        print(f"\nInspecting {name}")
        print(f"Shape: {df.shape[0]} rows × {df.shape[1]} cols")
        print(f"Preview (first {n} rows):")
        print(df.head(n))
    else:
        print(f"Dataframe is None.")


class AlternativeFuelAPI:
    BASE_URL = "https://developer.nrel.gov"
    # BASE_MPG_SUMMARY_URL = "https://www.fueleconomy.gov/ws/rest/ympg/shared/ympgVehicle"
    # BASE_MPG_DETAIL_URL = "https://www.fueleconomy.gov/ws/rest/ympg/shared/ympgDriverVehicle"
    HEADERS = {"Accept": "application/json", "User-Agent": "MyApp/1.0"}
    ENDPOINTS = {
        "get_stations": "/api/alt-fuel-stations/v1.json"
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
                
                elif r.status == 403:
                    print(f"Response from {url} sent status {r.status}")
                    return None
                
                try:
                    return await r.json()
                except Exception:
                    print(f"Response from {url} not in JSON format")
                    return None

    async def _fetch_new_version(self, url: str, params: dict = None, retries: int = 3, delay: float = 0.5) -> dict | None:
        """
        Core async request method with retry/backoff.
        """
        async with self.semaphore:
            for attempt in range(retries):
                if attempt > 0:
                    # Backoff between retries
                    wait = 2 ** attempt
                    print(f"Retrying {url} after {wait}s...")
                    await asyncio.sleep(wait)
                else:
                    # Optional small delay even before first call
                    await asyncio.sleep(delay)

                async with self.session.get(url, headers=self.HEADERS, params=params) as r:
                    status = r.status

                    if status == 200:
                        try:
                            return await r.json()
                        except Exception:
                            print(f"Response from {url} not in JSON format")
                            return None

                    elif status == 204:
                        print(f"No content (204) from {url} and params {params}")
                        return None

                    elif status == 403:
                        print(f"403 from {url} and params {params}, attempt {attempt+1}/{retries}")
                        continue  # retry after backoff

                    else:
                        # print(f"Unexpected {status} from {url} and params {params}")
                        return None

            print(f"Giving up on {url} after {retries} retries")
            return None

    async def _fetch_menu_items(self, endpoint: str, params: dict = None) -> list:
        # data = await self._fetch(endpoint, params=params)
        data = await self._fetch_new_version(url=endpoint, params=params)
        return data

    async def get_inspection_locations(self, offset: int, limit: int) -> dict:
    
        endpoint = f"{self.BASE_URL}{self.ENDPOINTS['get_stations']}"
        params = {
            "api_key": "61PcQy8NHlSjJKWx3CD8LXzWYAuA4E9bBQZzp8jQ",  # Required
            "fuel_type": "ELEC",        # Optional, e.g., 'ELEC', 'CNG', etc.
            # "state": "CA",              # Optional, 2-letter state code
            # "zip": "94043",             # Optional, ZIP code
            "status": "E",              # Optional, station status ('E'=Available)
            "access": "public",         # Optional, 'public' or 'private'
            "country": "US",
            "format": "json",            # Optional, 'json', 'xml', 'csv'
            "limit": offset,                # Optional, max number of results
            "offset": limit                # Optional, max number of results
        }
            
        data = await self._fetch_menu_items(endpoint, params=params)
        
        if data["total_results"] > 0:
            # print('GOT RESULTS for state', state)
            return data["Results"]
        else:
            # print('GOT NO RESULTS for state', state)
            return None

class AlternativeFuelETL:
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
        
    async def _reorder_dataframe(self, df : pd.DataFrame, first_columns : list):
        # new_order = ['manufacturer', 'type', 'productYear', 'productMake', 'productModel', 'odiNumber']
        # print([c for c in self.df_complaints.columns if c not in new_order])
        first_columns.extend([c for c in df.columns if c not in first_columns])
        return df[first_columns]
    
    def _check_if_attribute_exists(self, attribute_name : str):
        return hasattr(self, attribute_name) and getattr(self, attribute_name) is not None
        
    async def extract_inspection_locations(self, api: AlternativeFuelAPI):
        
        states = [
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
        "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
        "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
        "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
        "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
        ]
        
        results = await asyncio.gather(*(api.get_inspection_locations(state) for state in states))
        
        # Flatten results, ignoring None and exceptions
        locs_array = []
        for res in results:
            if isinstance(res, Exception):
                print(f"Error: {res}")  # or log properly
            elif res:
                locs_array.extend(res)
                
        df_inspections = pd.DataFrame(locs_array)
        df_inspections = await self._reorder_dataframe(df_inspections, ['State','City', 'Zip','Organization']) 
        inspect_df(df_inspections)
        
        self.df_inspections = df_inspections

    def write_to_csv(self, df: pd.DataFrame, filename: str, df_name: str = "DataFrame"):
        if df is not None:
            current_time = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            
            folder_name = "extracted_data/NHTSafetyAdministration"
            
            os.makedirs(folder_name) if not os.path.isdir(folder_name) else None
            
            # filename = f"{folder_name}/{filename}_{len(self.vehicles)}_vehicles_{current_time}.csv"
            filename = f"{folder_name}/{filename}_{current_time}.csv"

            df.to_csv(filename, index=False)
            print(f"Dataframe '{df_name}' written to file '{filename}' ({df.shape[0]} rows, {df.shape[1]} cols)")

    async def run_all(self):
        async with aiohttp.ClientSession() as session:
            semaphore = asyncio.Semaphore(self.concurrency)
            api = AlternativeFuelAPI(session, semaphore)
            
            await self.extract(api, dataset = 'ratings')

            self.write_to_csv(df=self.df_safety_ratings, filename="SafetyRatings", df_name="df_safety_ratings") if self._check_if_attribute_exists("df_safety_ratings") else None
            