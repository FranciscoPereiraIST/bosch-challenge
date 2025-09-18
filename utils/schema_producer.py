import pandas as pd
import json
import os
from pathlib import Path

def get_most_recent_file(folder: str, substring: str):
    folder_path = Path(folder)
    
    # Find files that contain the substring
    files = [f for f in folder_path.glob("*") if substring in f.name.lower() and f.is_file()]
    
    if not files:
        return None  # nothing found
    
    # Return the file with the most recent modification time
    return str(max(files, key=lambda f: f.stat().st_mtime))

def df_schema_to_json(df: pd.DataFrame, name: str = "dataframe", outfile: str = "schema.json") -> dict:
    """
    Generate schema for a DataFrame with improved type detection:
    - Detect strings, numbers, booleans, and datetime strings.
    """
    result = {}
    cols = {}

    for col in df.columns:
        non_null_series = df[col].dropna()
        example_val = non_null_series.iloc[0] if not non_null_series.empty else None
        dtype = str(df[col].dtype)

        if example_val is not None:
            # Detect datetime strings
            if dtype == "object":
                try:
                    pd.to_datetime(example_val)
                    dtype = "datetime"
                except (ValueError, TypeError):
                    if isinstance(example_val, str):
                        dtype = "string"
                    elif isinstance(example_val, bool):
                        dtype = "boolean"
                    elif isinstance(example_val, int):
                        dtype = "int"
                    elif isinstance(example_val, float):
                        dtype = "float"
            # Already numeric or datetime in pandas
            else:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    dtype = "datetime"
                elif pd.api.types.is_integer_dtype(df[col]):
                    dtype = "int"
                elif pd.api.types.is_float_dtype(df[col]):
                    dtype = "float"
                elif pd.api.types.is_bool_dtype(df[col]):
                    dtype = "boolean"
                    
        if col in ['cylDeact','cylDeactYesNo','mpgData']:
            dtype = 'boolean'
            
        cols[col] = {
            "dtype": dtype,
            "example": example_val
        }

    result[name] = cols

    # Write JSON
    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=4, default=str)

    return result

def produce_schemas(sep_dict : dict, write_json_flag: bool = False, stage_folder: str = 'extracted'):
    
    file_substrings = {}
    file_substrings['FuelEconomy'] = ["fuel", "emissions", "summary", "detail"]
    file_substrings['NHTSafetyAdministration'] = ["inspection", "ratings", "recall", "complaints"]
    file_substrings['AlternativeFuel'] = ["connector", "pressure", "standard", "related", "stations"]
    
    latest_files = {}
    # latest_fuel = [get_most_recent_file("extracted_data/FuelEconomy", sub_str) for sub_str in file_substrings_fuel]
    latest_files['FuelEconomy'] = {k : get_most_recent_file(f"{stage_folder}/FuelEconomy", k) for k in file_substrings['FuelEconomy']}
    latest_files['NHTSafetyAdministration'] = {k : get_most_recent_file(f"{stage_folder}/NHTSafetyAdministration", k) for k in file_substrings['NHTSafetyAdministration']}
    latest_files['AlternativeFuel'] = {k : get_most_recent_file(f"{stage_folder}/AlternativeFuel", k) for k in file_substrings['AlternativeFuel']}
    
    # print(f"LATEST FILES!!!!!!!!!", latest_files)
    
    for dataset, files_in_dataset in latest_files.items():
        
        folder_name = f"{stage_folder}_schemas"
        os.makedirs(folder_name) if not os.path.isdir(folder_name) else None
        
        folder_name = f"{folder_name}/{dataset}"
        os.makedirs(folder_name) if not os.path.isdir(folder_name) else None
            
        for df_name, file_name in files_in_dataset.items():
            
            aux = file_name.split("\\", 3)[2]
            substring_name = aux.split('_', 1)[0]
            
            if 'MPG' in file_name:
                substring_name = "_".join([substring_name, aux.split('_', 2)[1]])
            
            # if dataset == 'AlternativeFuel':
            #     sep = ','
            # else:
            #     sep = ','
                
            df = pd.read_csv(file_name, sep = sep_dict[dataset])
            name = f"{substring_name}"
            
            print(f"Producing schema for '{name}' using file '{file_name}'...")
            
            schema = df_schema_to_json(df, name=name, outfile=f"{folder_name}/{name}.json") if write_json_flag else None
            
    return latest_files
            
        
