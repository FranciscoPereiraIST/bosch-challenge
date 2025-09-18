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
                            # print(f"Status {status} -> Response from {url} and params {params} in JSON format")
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
        
        # print(f"DATA IS {data}")
        
        return data

    async def get_stations(self, offset: int, limit: int) -> dict:
    
        endpoint = f"{self.BASE_URL}{self.ENDPOINTS['get_stations']}"
        params = {
            "api_key": "61PcQy8NHlSjJKWx3CD8LXzWYAuA4E9bBQZzp8jQ",  # Required
            "fuel_type": "ELEC, HY, LNG",        # Optional, e.g., 'ELEC', 'CNG', etc.
            # "state": "CA",              # Optional, 2-letter state code
            # "zip": "94043",             # Optional, ZIP code
            "status": "E",              # Optional, station status ('E'=Available)
            "access": "public",         # Optional, 'public' or 'private'
            "country": "US",
            "maximum_vehicle_class" : "LD",
            "cards_accepted": "CREDIT, V",
            # "format": "json",            # Optional, 'json', 'xml', 'csv'
            "limit": limit,                # Optional, max number of results
            "offset": offset             # Optional, max number of results
        }
            
        data = await self._fetch_menu_items(endpoint, params=params)
        
        if data:
            
            # print(f"DATA KEYS: {[k for k,v in data.items()]}")
            keys_to_select = ["total_results", "station_counts", "fuel_stations"]

            filtered_data = {k: data[k] for k in keys_to_select if k in data}
        
            if data["total_results"] > 0:
                # print(f"FILTERED KEYS: {[k for k,v in filtered_data.items()]}")

                # print('GOT RESULTS for state', state)
                return filtered_data
        else:
            return None

class AlternativeFuelETL:
    def __init__(self, concurrency=10):
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
        
    async def extract_stations(self, api: AlternativeFuelAPI):
        
         # First, get the total count
        limit = 50
        first_batch = await api.get_stations(offset=0, limit=limit)
        
        print(f"first_batch KEYS: {[k for k,v in first_batch.items()]}")

        total_count = first_batch['total_results']  # total number of stations reported by API
        all_stations = first_batch['fuel_stations']
        
        # print("first_batch - ", type(first_batch))
        # print("total count - ", type(total_count), total_count)
        # print("stations - ",  type(all_stations), len(all_stations)) #, stations[0])
        
        # Generate offsets for the remaining batches
        offsets = range(limit, total_count, limit)
        # offsets = range(limit, 151, limit)
        
        # print(offsets)
        
        results = await asyncio.gather(*(api.get_stations(offset=offset, limit=limit) for offset in offsets))
        print("LEN OF results - ", len(results))
        
        for batch in results:
            all_stations.extend(batch['fuel_stations'])
            
        print("LEN OF ALL STATIONS - ", len(all_stations))
        
        self.stations_list = all_stations
        
        # results = await asyncio.gather(*(api.get_inspection_locations(state) for state in states))
        
        # # Flatten results, ignoring None and exceptions
        # locs_array = []
        # for res in results:
        #     if isinstance(res, Exception):
        #         print(f"Error: {res}")  # or log properly
        #     elif res:
        #         locs_array.extend(res)
                
        # df_stations = pd.DataFrame(all_stations)
        # df_inspections = await self._reorder_dataframe(df_inspections, ['State','City', 'Zip','Organization']) 
        # inspect_df(df_stations)
        
        # self.df_inspections = df_inspections
        
    def get_stations_list(self):
        return self.stations_list
    
    async def process_arrays(self):
        
        data = self.get_stations_list()
        
        array_types = self.array_fields.copy()
        array_types.extend(['id']) 
        
        new_data = []
        for el in data:
            curr_dict = {k: v for k, v in el.items() if k in array_types and v is not None}
            new_data.append(curr_dict) if len(curr_dict) > 1 or (len(curr_dict) == 1 and 'id' not in curr_dict) else None
            
        # print(new_data)
        
                # Step 1: collect data per field
        field_dicts = {}

        for item in new_data:
            for key, value in item.items():
                if key != 'id':
                    if key not in field_dicts.keys():
                        field_dicts[key] = []
                    
                    for v in value:
                    # include id along with this key only
                        field_dicts[key].append({'id': item['id'], key: v})
                    
                    
        print(f"There are {len(field_dicts)} dataframes, one per each array field - {field_dicts.keys()}")

        # Step 2: optionally convert each list to pandas DataFrame

        dfs = {field: pd.DataFrame(lst) for field, lst in field_dicts.items()}
        
        for field_name, curr_df in dfs.items(): 
            # inspect_df(dfs[field_name], n=10, name=field_name)
            setattr(self, f"df_{field_name}", curr_df)
            
        
    async def process_records(self, record_type_field : str): 
        
        print(f"PROCESSING 'record' typed objects -> '{record_type_field}'")   
            
        data = self.get_stations_list()

        # Combine array fields and 'id'
        # record_types = self.record_fields.copy()
        # record_types.extend(['id'])
        
        # print(f"RECORD FIELDS: {record_types}")

        # Step 1: filter data
        
        record_types = ['id', record_type_field]
        
        # count=0
        
        new_data = []
        for el in data:
            
            # print(f"elem -> {el}")
            
            # curr_dict = {k: v for k, v in el.items() if k in record_types and v is not None and (isinstance(v, list) and len(v) > 0)}
            curr_dict = {k: v for k, v in el.items() if k in record_types and v is not None}
            
            # print(f"current dict", curr_dict)
            
            if len(curr_dict) > 1 or (len(curr_dict) == 1 and 'id' not in curr_dict):
                new_data.append(curr_dict)
                
            # count+=1
            
            # if count >= 15:
            #     break
            
        # print("new_data", new_data, "\n")
        
        if not new_data:
            return None
        
        # Keep only non-empty list fields along with 'id'
        filtered_data = []
        for item in new_data:
            new_item = {'id': item['id']}
            for k, v in item.items():
                if k != 'id' and isinstance(v, list) and v:
                    new_item[k] = v
            if len(new_item) > 1:  # Only keep if there is at least one non-empty list field
                filtered_data.append(new_item)

        # print("filtered data", filtered_data, "\n")
        
        # filtered_data = [
        #     {'id': 6505, 'related_stations': [
        #         {'id': 121595, 'access_code': 'private', 'fuel_type_code': 'ELEC'},
                
        #         {'id': 1111111, 'access_code': 'TESSST', 'fuel_type_code': 'OLAAAA'}
        #         ]
        #      },
        #     {'id': 23480, 'related_stations': [{'id': 7041, 'access_code': 'public', 'fuel_type_code': 'CNG'}]},
        #     {'id': 32051, 'related_stations': [{'id': 28523, 'access_code': 'public', 'fuel_type_code': 'CNG'}]},
        #     {'id': 34319, 'related_stations': [{'id': 39196, 'access_code': 'public', 'fuel_type_code': 'CNG'}]},
        #     {'id': 35074, 'related_stations': [{'id': 35073, 'access_code': 'public', 'fuel_type_code': 'CNG'}]},
        #     {'id': 38488, 'related_stations': [{'id': 38487, 'access_code': 'public', 'fuel_type_code': 'CNG'}]}]
         
        
        flattened = []

        for item in filtered_data:
            new_item = {'id': item['id']}
            
            for k, v in item.items():
                if k != 'id' and isinstance(v, list) and v:
                    # take the first element in the list (or you could loop if multiple)
                    if isinstance(v[0], dict):
                        # Nested objects: one row per element
                        for nested in v:
                            new_item = {'id': item['id']}
                            for nk, nv in nested.items():
                                new_item[f"{k}_{nk}"] = nv
                            flattened.append(new_item)
                    
                    # print('nested', nested)
                    # for nk, nv in nested.items():
                    #     new_item[f"{k}_{nk}"] = nv
            
            # flattened.append(new_item)
    
        # print("flattened data", flattened)
        
        df = pd.DataFrame(flattened)
        
        # inspect_df(df, n=10, name=record_type_field)
        
        setattr(self, f"df_{record_type_field}", df)

        # new_data=[]
        
        # for json_dict in new_data:
        #     for k, v in json_dict.items():
        #         json_dict.pop(k) if isinstance(v, list) and len(v)==0 else None
                
        #         # if isinstance(v, list):
        #         #     print(f"Field {k} is of type list {v} and has length {len(v)}")
        
        # print("new_data", new_data)
        
    def get_fields_types(self):
         
        # load schema
        with open("utils/alternative_fuel_schema.json") as f:
            schema = json.load(f)
        
        simple_types = ["Int64", "string", "float64", "boolean"]
        complex_fields = [c for c, dtype in schema.items() if dtype not in simple_types] #object and array 
        
        print(f"Complex type columns ({len(complex_fields)}) -> {complex_fields}")
        
        # for key in complex_fields:
        #     print(f"Field {key} | {schema[key]}")
            
        self.complex_fields = complex_fields
        self.simple_fields = [c for c in schema.items() if c not in complex_fields]
        self.array_fields = [c for c, dtype in schema.items() if dtype == 'array']
        self.record_fields = [c for c, dtype in schema.items() if dtype == 'record']
        
        print(f"Fields of type 'record' -> {self.record_fields}")
        
    async def process_stations(self):
      
        data = self.get_stations_list()
            
        df = pd.DataFrame(data)
        df_cols = df.columns
        df.drop(axis=1, columns=[c for c in self.complex_fields if c in df_cols], inplace=True)
        
        first_cols = ['id', 'station_name', 'fuel_type_code', 'owner_type_code', 'country', 'state', 'city', 'street_address', 'zip', 'open_date', 'updated_at']
        df = await self._reorder_dataframe(df, first_cols) 
        
        inspect_df(df)
        
        self.df_stations = df
        

    def write_to_csv(self, df: pd.DataFrame, filename: str, df_name: str = "DataFrame", sep : str = '|'):
        if df is not None:
            current_time = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            
            folder_name = "extracted_data/AlternativeFuel"
            
            os.makedirs(folder_name) if not os.path.isdir(folder_name) else None
            
            filename = f"{folder_name}/{filename}_{current_time}.csv"

            df.to_csv(filename, index=False, sep=sep, quoting=1)
            print(f"Dataframe '{df_name}' written to file '{filename}' ({df.shape[0]} rows, {df.shape[1]} cols)")
            
    def to_camel_case(self, snake_str):
    # Split by underscore, capitalize each part, and join
        return ''.join(word.capitalize() for word in snake_str.split('_'))
    
    def get_output(self):
        return {
            df.split('_', 1)[1]: getattr(self, df)
            for df in self.__dict__
            if df.startswith('df') and getattr(self, df) is not None
        }
    
    async def run_all(self):
        async with aiohttp.ClientSession() as session:
            semaphore = asyncio.Semaphore(self.concurrency)
            api = AlternativeFuelAPI(session, semaphore)
                        
            self.get_fields_types()
            
            await self.extract_stations(api)
            
            await self.process_stations()
            await self.process_arrays()
            
            special_fields = self.array_fields
            special_fields.extend(self.record_fields)
                        
            for special_typed_field in special_fields:
                dataframe_name = f"df_{special_typed_field}"
                
                await self.process_records(record_type_field=special_typed_field) if special_typed_field in self.record_fields else None
                
                if self._check_if_attribute_exists(dataframe_name):
                    df_attribute_name = getattr(self, dataframe_name)
                                
                    filename = self.to_camel_case(special_typed_field)
                    # inspect_df(df_attribute_name, name = filename)
                    self.write_to_csv(df=df_attribute_name, filename=filename, df_name=dataframe_name)

            self.write_to_csv(df=self.df_stations, filename="Stations", df_name="df_stations") if self._check_if_attribute_exists("df_stations") else None
            