import pandas as pd
from sqlalchemy import create_engine, text, NVARCHAR, FLOAT, INTEGER, DateTime, BOOLEAN
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import json
import os
from pathlib import Path

class Loading:
    def __init__(self, server, database, username, password, file_dict: dict):
        """
        Initialize the Loading class with Azure SQL connection using SQLAlchemy.
        """        
        
        self.file_dict = file_dict
        self.dataframes = {}
        self.sep_dict = {
            'FuelEconomy' : ',',
            'NHTSafetyAdministration' : ',', 
            'AlternativeFuel' : '|'
        }
        
        driver = "ODBC Driver 18 for SQL Server".replace(" ", "+")  # URL-encode spaces
        self.conn_str = (
            f"mssql+pyodbc://{username}:{password}@{server}:1433/{database}"
            f"?driver={driver}&Encrypt=yes&TrustServerCertificate=no"
        )
        self.engine = create_engine(self.conn_str)

        # conn_str = f"mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver={driver}"
        # self.conn_str = conn_str
        
        # self.conn_str = (
        #     f"mssql+pyodbc://{username}:{password}@{server}:1433/{database}"
        #     "?driver=ODBC+Driver+17+for+SQL+Server"
        # )

        # self.engine = create_engine(self.conn_str)

    def get_schema_file(self, filepath: str):
        
        aux = filepath.split("\\", 3)[2]
        substring_name = aux.split('_', 1)[0]
        
        if 'MPG' in filepath:
            substring_name = "_".join([substring_name, aux.split('_', 2)[1]])
            
        return substring_name
    
    def insert_dataframe(self, df: pd.DataFrame, table_name: str, schema: str = "dbo", if_exists: str = "append"):
        """
        Insert a pandas DataFrame into a SQL table using to_sql with safe dtype handling.
        """
        if df.empty:
            print("DataFrame is empty, skipping insert")
            return

        # Drop fully empty columns
        # df = df.dropna(axis=1, how='all')

        # Fill NaN for numeric/bool columns to avoid HY104 errors
        # for col in df.columns:
        #     if pd.api.types.is_float_dtype(df[col]):
        #         df[col] = df[col].fillna(0.0)
        #     elif pd.api.types.is_integer_dtype(df[col]):
        #         df[col] = df[col].fillna(0).astype('int64')
        #     elif pd.api.types.is_bool_dtype(df[col]):
        #         df[col] = df[col].fillna(False)
        #     elif pd.api.types.is_object_dtype(df[col]):
        #         df[col] = df[col].fillna('')

        # Map pandas dtypes to SQLAlchemy types
        dtype_map = {}
        for col, dtype in df.dtypes.items():
            if pd.api.types.is_integer_dtype(dtype):
                dtype_map[col] = INTEGER
            elif pd.api.types.is_float_dtype(dtype):
                dtype_map[col] = FLOAT
            elif pd.api.types.is_bool_dtype(dtype):
                dtype_map[col] = BOOLEAN
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                dtype_map[col] = DateTime
            else:
                # Use max string length or 1 if empty
                max_len = df[col].astype(str).map(len).max() or 1
                dtype_map[col] = NVARCHAR(max_len)

        try:
            df.to_sql(
                name=table_name,
                schema=schema,
                con=self.engine,
                if_exists=if_exists,
                index=False,
                dtype=dtype_map
            )
            print(f"DataFrame written to {schema}.{table_name} successfully")
        except SQLAlchemyError as e:
            print("Error writing DataFrame to SQL:", e)

    def insert_dataframe_old(self, df: pd.DataFrame, table_name: str, schema: str = "dbo", if_exists: str = "append"):
        """
        Write a pandas DataFrame to a SQL table in the specified schema.
        
        Parameters:
            df: pandas DataFrame
            table_name: Name of the table in SQL
            schema: Schema name (default 'dbo')
            if_exists: 'fail', 'replace', 'append' (default 'append')
        """
        try:
            df.to_sql(
                name=table_name,
                schema=schema,
                con=self.engine,
                if_exists=if_exists,
                index=False
            )
            print(f"DataFrame written to {schema}.{table_name} successfully")
        except SQLAlchemyError as e:
            print("Error writing DataFrame to SQL:", e)

    def create_schema(self, schema_name: str):
        """
        Create a schema if it doesn't exist.
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(
                    text(
                        f"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema_name}') "
                        f"EXEC('CREATE SCHEMA {schema_name}')"
                    )
                )
                conn.commit()
            print(f"Schema '{schema_name}' is ready")
        except SQLAlchemyError as e:
            print("Error creating schema:", e)
            
    def generate_create_table_sql(self, json_schema: dict, table_name: str, schema: str = "dbo") -> str:
        """
        Generate a CREATE TABLE script from a JSON schema with column names and types.
        
        Parameters:
            json_schema: dict containing columns and types (like your FuelEconomy JSON)
            table_name: name of the SQL table
            schema: schema name (default 'dbo')
        
        Returns:
            str: SQL CREATE TABLE script
        """
        sql_lines = [f"CREATE TABLE [{schema}].[{table_name}] ("]
        col_defs = []

        for col_name, col_info in json_schema.items():
            dtype = col_info.get("dtype", "string").lower()

            if dtype in ["int", "int64"]:
                sql_type = "INT"
            elif dtype in ["float", "float64"]:
                sql_type = "FLOAT"
            elif dtype in ["boolean", "bool"]:
                sql_type = "BIT"
            elif dtype in ["datetime"]:
                sql_type = "DATETIME"
            else:
                # For strings, use NVARCHAR(255) as default
                sql_type = "NVARCHAR(255)"

            col_defs.append(f"    [{col_name}] {sql_type} NULL")

        sql_lines.append(",\n".join(col_defs))
        sql_lines.append(");")

        return "\n".join(sql_lines)
    
    def save_sql_to_file(self, dataset: str, sql_string: str, filename: str):
        """
        Save a SQL string to a .sql file.
        
        Parameters:
            sql_string: The CREATE TABLE or any SQL statement as a string
            filename: Name of the file to save (e.g., 'create_table.sql')
        """
        folder_name = f"sql_scripts/{dataset}"
        os.makedirs(folder_name) if not os.path.isdir(folder_name) else None

        filename = f"{folder_name}/{filename}.sql"

        with open(filename, "w", encoding="utf-8") as f:
            f.write(sql_string)
        print(f"SQL script saved to {filename}")

    def get_most_recent_file(self, folder: str, substring: str):
        folder_path = Path(folder)
        
        # Find files that contain the substring
        files = [f for f in folder_path.glob("*") if substring in f.name.lower() and f.is_file()]
        
        if not files:
            return None  # nothing found
        
        # Return the file with the most recent modification time
        return str(max(files, key=lambda f: f.stat().st_mtime))
        
    def get_latest_files(self, folder_name: str):
    
        file_substrings = {}
        file_substrings['FuelEconomy'] = ["fuel", "emissions", "summary", "detail"]
        file_substrings['NHTSafetyAdministration'] = ["inspection", "ratings", "recall", "complaints"]
        file_substrings['AlternativeFuel'] = ["connector", "pressure", "standard", "related", "stations"]
        
        latest_files = {}
        # latest_fuel = [get_most_recent_file("extracted_data/FuelEconomy", sub_str) for sub_str in file_substrings_fuel]
        latest_files['FuelEconomy'] = {k : self.get_most_recent_file(f"{folder_name}/FuelEconomy", k) for k in file_substrings['FuelEconomy']}
        latest_files['NHTSafetyAdministration'] = {k : self.get_most_recent_file(f"{folder_name}/NHTSafetyAdministration", k) for k in file_substrings['NHTSafetyAdministration']}
        latest_files['AlternativeFuel'] = {k : self.get_most_recent_file(f"{folder_name}/AlternativeFuel", k) for k in file_substrings['AlternativeFuel']} 
        
        return latest_files
            
    def load_files(self, file_dict : dict, stage: str):
        """
        Load CSV files into pandas DataFrames.
        """        
        self.dataframes[stage] = {}
        for dataset, files in file_dict.items():
            self.dataframes[stage][dataset] = {}
            for category, filepath in files.items():
                try:
                    substring_name = self.get_schema_file(filepath=filepath)
                    json_schema = f"schemas/{dataset}/{substring_name}.json"
                    
                    # print(f"CATEGORY {category} | DATASET {dataset} | File path {filepath} -> json schema is '{json_schema}'")
                    
                    self.dataframes[stage][dataset][category] = {}
                    self.dataframes[stage][dataset][category]['data'] = filepath #pd.read_csv(filepath, sep = sep_dict[dataset])
                    self.dataframes[stage][dataset][category]['schema'] = json_schema #self.load_json(json_path=json_schema)
                    self.dataframes[stage][dataset][category]['json_object'] = substring_name 
                    # self.dataframes[dataset][category]['data'] = pd.read_csv(filepath, sep = sep_dict[dataset])
                    # self.dataframes[dataset][category]['schema'] = self.load_json(json_path=json_schema)
                    
                except Exception as e:
                    print(f"Error loading {filepath}: {e}")
                    
    def execute_sql_file(self, file_path: str):
        """
        Execute a .sql file against the connected database.
        
        Parameters:
            file_path: Path to the .sql file
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                sql_script = f.read()

            with self.engine.connect() as conn:
                conn.execute(text(sql_script))
                conn.commit()
            print(f"SQL file '{file_path}' executed successfully")
        except SQLAlchemyError as e:
            print(f"Error executing SQL file '{file_path}': {e}")

    def run_all(self):
        
        self.load_files(file_dict=self.file_dict, stage = 'extract')
        
        source = 'FuelEconomy'
        category = 'fuel'
        
        csv_file_extract = self.dataframes['extract'][source][category]['data']
        json_file = self.dataframes['extract'][source][category]['schema']
        json_object_name = self.dataframes['extract'][source][category]['json_object']
        
        latest_processed_files = self.get_latest_files(folder_name='processed_data')
        
        print(latest_processed_files)
        
        self.load_files(file_dict=latest_processed_files, stage = 'process')
        
        csv_file = self.dataframes['process'][source][category]['data']
        
        print(f"LOADING FILE {csv_file}")
        
        df = pd.read_csv(csv_file, sep = self.sep_dict[source])
        df = df.head(n=100)
        
        self.insert_dataframe(df, table_name=json_object_name, schema="stg", if_exists="append")
                
        # with open(json_file, "r", encoding="utf-8") as f:
        #     schema = json.load(f)[json_object_name]
            
        # print(schema)
    
        # output = self.generate_create_table_sql(json_schema = schema, table_name = json_object_name, schema = "stg")

        # print("output is :\n", output)
        
        # self.save_sql_to_file(dataset = source, sql_string = output, filename = f'CREATE_TABLE_{json_object_name.upper()}')
        
        # self.execute_sql_file(file_path = f"sql_scripts/{source}/CREATE_TABLE_{json_object_name.upper()}.sql")
        
        
        # Sample DataFrame
        # df = pd.DataFrame({
        #     "id": [1, 2, 3],
        #     "name": ["Alice", "Bob", "Charlie"],
        #     "value": [10.5, 20.7, 30.2],
        #     "createdOn": [datetime.now(), datetime.now(), datetime.now()]
        # })
        
        # print(df.head(n=10))

        # Create schema 'stg' if it doesn't exist
        # self.create_schema("stg")

        # Insert DataFrame into stg.TestData (append if exists)
        # self.insert_dataframe(df, table_name="TestData", schema="stg", if_exists="append")
        
        # create audit columns like InsertedAt????
