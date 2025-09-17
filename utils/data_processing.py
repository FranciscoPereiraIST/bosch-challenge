import pandas as pd
import json
import os

class Processing:
    def __init__(self, file_dict: dict):
        """
        Initialize with a dictionary of file names, per dataset.
        """
        self.file_dict = file_dict
        self.dataframes = {}  # Store loaded DataFrames
        self.sep_dict = {
            'FuelEconomy' : ',',
            'NHTSafetyAdministration' : ',', 
            'AlternativeFuel' : '|'
        }
        
    def get_output(self):
        return {
            df.split('_', 1)[1]: getattr(self, df)
            for df in self.__dict__
            if df.startswith('df') and getattr(self, df) is not None
        }
        
    def inspect_df(self, df: pd.DataFrame, name: str = "DataFrame", n: int = 5):
        
        if isinstance(df, pd.DataFrame):
            """Prints basic info about a DataFrame: its name, shape, and head rows."""
            print(f"\nInspecting {name}")
            print(f"Shape: {df.shape[0]} rows × {df.shape[1]} cols")
            print(f"Preview (first {n} rows):")
            print(df.head(n))
        else:
            print(f"Dataframe is None.")
    
    def get_schema_file(self, filepath: str):
        
        aux = filepath.split("\\", 3)[2]
        substring_name = aux.split('_', 1)[0]
        
        if 'MPG' in filepath:
            substring_name = "_".join([substring_name, aux.split('_', 2)[1]])
            
        return substring_name
    
    def load_json(self, json_path: str) -> dict:
        """
        Open and read a JSON file.
        Returns the content as a dictionary.
        """
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading JSON file {json_path}: {e}")
            return {}

    def print_keys(self):
        """
        Print only the dictionary keys from self.dataframes.
        """
        def walk(d, indent=0):
            prefix = "  " * indent
            if isinstance(d, dict):
                for k in d.keys():
                    print(f"{prefix}{k}")
                    walk(d[k], indent + 1)

        walk(self.dataframes)
        
    def load_files(self):
        """
        Load CSV files into pandas DataFrames.
        """        
        for dataset, files in self.file_dict.items():
            self.dataframes[dataset] = {}
            for category, filepath in files.items():
                try:
                    substring_name = self.get_schema_file(filepath=filepath)
                    json_schema = f"schemas/{dataset}/{substring_name}.json"
                    
                    # print(f"CATEGORY {category} | DATASET {dataset} | File path {filepath} -> json schema is '{json_schema}'")
                    
                    self.dataframes[dataset][category] = {}
                    self.dataframes[dataset][category]['data'] = filepath #pd.read_csv(filepath, sep = sep_dict[dataset])
                    self.dataframes[dataset][category]['schema'] = json_schema #self.load_json(json_path=json_schema)
                    self.dataframes[dataset][category]['json_object'] = substring_name 
                    # self.dataframes[dataset][category]['data'] = pd.read_csv(filepath, sep = sep_dict[dataset])
                    # self.dataframes[dataset][category]['schema'] = self.load_json(json_path=json_schema)
                    
                except Exception as e:
                    print(f"Error loading {filepath}: {e}")
                    
                    
    def open_json(self, schema_file: str, dataset: str):
        with open(schema_file, "r", encoding="utf-8") as f:
            schema = json.load(f)[dataset]
            
        return schema
    
    def convert_columns_based_on_schema(self, df: pd.DataFrame, schema_file: str, dataset: str, decimals: int = 2) -> pd.DataFrame:
        """
        Convert DataFrame columns based on a schema definition from a JSON file.
        
        Args:
            df (pd.DataFrame): Input DataFrame.
            schema_file (str): Path to JSON schema file.
            decimals (int): Number of decimal places for float rounding.
        
        Returns:
            pd.DataFrame: DataFrame with converted columns.
        """
        with open(schema_file, "r", encoding="utf-8") as f:
            schema = json.load(f)[dataset]
            
        # print(schema)

        for col in schema.keys():
            if col not in df.columns:
                print('aqui', col)
                continue
            try:
                dtype = schema[col]['dtype']
                if dtype.startswith("datetime"):                
                    # print(f"Col {col} | {dtype}")
                    df[col] = pd.to_datetime(df[col], utc=True, errors="raise")
                elif dtype == "string":
                    df[col] = df[col].astype("string")
                elif dtype.startswith("float"):
                    df[col] = pd.to_numeric(df[col], errors="raise").round(decimals)
                elif dtype.startswith("boolean"):
                    # print(f"Converting col '{col}' to boolean...")
                    df[f"{col}_bool"] = self.convert_to_boolean(df, col)
                else:
                    df[col] = df[col].astype(dtype)
            except Exception as e:
                print(f"Could not convert column '{col}' to {dtype}: {e}")
        
        return df

    def get_columns_of_type(self, df: pd.DataFrame, dtype: str) -> list:
        """
        Return a list of columns in df that match the given dtype.

        Args:
            df (pd.DataFrame): The DataFrame to inspect.
            dtype (str): The datatype to filter by (e.g., 'int64', 'float64', 'string', 'datetime').

        Returns:
            list: List of column names matching the dtype.
        """
        result = []

        for col in df.columns:
            col_dtype = df[col].dtype

            # handle string vs object
            if dtype == "string" and pd.api.types.is_string_dtype(col_dtype):
                result.append(col)
            elif dtype == "int64" and pd.api.types.is_integer_dtype(col_dtype):
                result.append(col)
            elif dtype == "float64" and pd.api.types.is_float_dtype(col_dtype):
                result.append(col)
            elif dtype == "boolean": #and pd.api.types.is_bool_dtype(col_dtype):
                result.append(col)
            elif dtype == "datetime" and pd.api.types.is_datetime64_any_dtype(col_dtype):
                result.append(col)
            # allow exact dtype string match as fallback
            elif str(col_dtype) == dtype:
                result.append(col)

        return result
    
    def convert_to_boolean(self, df : pd.DataFrame, bool_col : str):
                
        false_values, true_values = ['N', 'No', 'n', 'NO', 'False', 'FALSE', 'false'], ['Y', 'Yes', 'y', 'YES', 'True', 'TRUE', 'true']
        bool_dict = {f_v: False for f_v in false_values}
        bool_dict.update({t_v: True for t_v in true_values})
        
        df[f"{bool_col}_bool"] = df[bool_col].astype("string")
        df[f"{bool_col}_bool"] = df[f"{bool_col}_bool"].map(lambda x : bool_dict[x] if x is not pd.NA and x.strip() != '' else pd.NA)
                     
        # df[f"{bool_col}_bool"] = df[bool_col].astype("string")
        # df[f"{bool_col}_bool"] = df[f"{bool_col}_bool"].map(lambda x : bool_dict[x] if x is not pd.NA and x.strip() != '' else pd.NA)
        
        return df[f"{bool_col}_bool"]
    
    def lower_first_letter(self, df):
        new_cols = {c: c[0].lower() + c[1:] for c in df.columns}
        return df.rename(columns=new_cols)
        
    def to_camel_case(self, snake_str):
    # Split by underscore, capitalize each part, and join
        return ''.join(word.capitalize() for word in snake_str.split('_'))
       
    def convert_columns_to_camel_case(self, df: pd.DataFrame):
        new_cols = {c: self.to_camel_case(c.replace('-', '')) for c in df.columns if '_' in c and '_bool' not in c}
        
        # print(new_cols)
        
        return df.rename(columns=new_cols)
    
    def fix_null_values(self, df: pd.DataFrame):
        for c in df.columns:
            df[c] = self.to_pandas_null(df[c], null_values=['Not Rated', 'unknown'])
        return df
    
    def to_pandas_null(self, col, null_values=None):
        """
        Convert a pandas Series or list-like object to pandas NULLs (pd.NA)
        for any empty, blank, None, np.nan, or custom null-like values.

        Parameters:
            col: pandas Series or list-like object
            null_values: list of additional values to treat as null (case-insensitive)
        
        Returns:
            pd.Series with pd.NA for null-like values
        """
        if null_values is None:
            null_values = []
        
        # Normalize null_values to lowercase for case-insensitive comparison
        null_values_set = set(v.lower() for v in null_values if isinstance(v, str))
        
        if not isinstance(col, pd.Series):
            col = pd.Series(col)
        
        def clean_value(x):
            if x is None or pd.isna(x):
                return pd.NA
            if isinstance(x, str):
                x_strip = x.strip()
                if x_strip == '' or x_strip.lower() in null_values_set:
                    return pd.NA
            return x
        
        return col.apply(clean_value)
    
    def write_to_csv(self, df: pd.DataFrame, dataset: str, filename: str, df_name: str = "DataFrame"):
        if df is not None:
            current_time = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            
            folder_name = f"processed_data/{dataset}"
            
            os.makedirs(folder_name) if not os.path.isdir(folder_name) else None
            
            # filename = f"{folder_name}/{filename}_{len(self.vehicles)}_vehicles_{current_time}.csv"
            filename = f"{folder_name}/{filename}_{current_time}.csv"

            df.to_csv(filename, index=False)
            print(f"Dataframe '{df_name}' written to file '{filename}' ({df.shape[0]} rows, {df.shape[1]} cols)")
        
    def clean_data(self):
        """
        Perform general cleaning steps (e.g., drop duplicates, trim strings).
        """
        pass
    
    def enrich_data(self):
        """
        Add or transform columns for enrichment.
        """
        pass
    
    def save_processed(self, out_dir: str):
        """
        Save processed DataFrames back to CSV/Parquet.
        """
        pass
    
    def process_dataframe(self, source: str, dataset: str, write_flag: bool = False):
        
        csv_file = self.dataframes[source][dataset]['data']
        json_file = self.dataframes[source][dataset]['schema']
        json_object_name = self.dataframes[source][dataset]['json_object']
        
        print(f"\tProcessing SOURCE {source} | DATASET {dataset} | NAME {json_object_name}...")
        
        # print("file is ", csv_file, "json_file is ", json_file)        
        df = pd.read_csv(csv_file, sep = self.sep_dict[source])
        
        df = self.fix_null_values(df)
        df = self.convert_columns_based_on_schema(df = df, dataset=json_object_name, schema_file=json_file, decimals=3)
        df = self.convert_columns_to_camel_case(df)
        df = self.lower_first_letter(df)
        print(f"Before duplicate removal -> shape is {df.shape}")
        df = df.drop_duplicates()
        print(f"After duplicate removal -> shape is {df.shape}")

        setattr(self, f"df_processed_{dataset}", df)
        
        if write_flag:
            self.write_to_csv(df = df, dataset=source, filename=json_object_name, df_name=json_object_name)

    
    def run_all_OLD(self):
        self.load_files()
        
        dataset_test = 'FuelEconomy'
        category = 'fuel'
        
        # dataset_test = 'NHTSafetyAdministration'
        # category = 'ratings'
        
        csv_file = self.dataframes[dataset_test][category]['data']
        json_file = self.dataframes[dataset_test][category]['schema']
        json_object_name = self.dataframes[dataset_test][category]['json_object']
        
        # print(self.dataframes)
        
        print("file is ", csv_file, "json_file is ", json_file)
        
        df = pd.read_csv(csv_file, sep = ',')
        
        df_fuel_new = self.convert_columns_based_on_schema(df = df, dataset=json_object_name, schema_file=json_file, decimals=3)
        
        # fix_column_names (convert from snake_case to camel case!!!!)
        df = self.convert_columns_to_camel_case(df_fuel_new)
        df = self.lower_first_letter(df)
        df = self.fix_null_values(df)
        
        print(f"Before duplicate removal -> shape is {df.shape}")
        df = df.drop_duplicates()
        print(f"After duplicate removal -> shape is {df.shape}")
    
        # OK -> function to check the bool cols, the unique values!!! 
        # OK -> function to convert blanks to Null
        # OK -> function to remove duplicates
        # drop deste campo, não? cylDeactYesNo
        
        self.inspect_df(df)
        
        self.write_to_csv(df = df, dataset=dataset_test, filename=json_object_name, df_name=json_object_name)
        

    def run_all(self, write_flag: bool = False):
        self.load_files()
        
        # stop = 0
        for source, v in self.dataframes.items():
            # print(source, v)
            # if stop == 1:
            #     break
            for k, values in v.items():
                # print("source is ", source)
                # print("data set is  ", k)

                # if source == 'NHTSafetyAdministration' and k == 'ratings':
                    # print("source is ", source)
                    # print("data set is  ", k)
                self.process_dataframe(source = source, dataset = k, write_flag = write_flag)
                # stop = 1
                # break
            
        