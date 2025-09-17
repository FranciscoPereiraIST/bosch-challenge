import pandas as pd
from sqlalchemy import create_engine, text, NVARCHAR, FLOAT, INTEGER, DateTime, BOOLEAN
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

class Loading:
    def __init__(self, server, database, username, password):
        """
        Initialize the Loading class with Azure SQL connection using SQLAlchemy.
        """        
        
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
            
            
    def run_all(self):
    
        # Sample DataFrame
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [10.5, 20.7, 30.2],
            "createdOn": [datetime.now(), datetime.now(), datetime.now()]
        })
        
        # print("Connection String is:\n", self.conn_str)
        
        print(df.head(n=10))

        # Create schema 'stg' if it doesn't exist
        # self.create_schema("stg")

        # Insert DataFrame into stg.TestData (append if exists)
        self.insert_dataframe(df, table_name="TestData", schema="stg", if_exists="append")
