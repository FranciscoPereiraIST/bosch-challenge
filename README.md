# Vehicle Data ETL Pipeline

## Introduction
This project is a modular Python pipeline that automates the extraction, transformation, and loading (ETL) of multiple vehicle-related datasets into an Azure SQL database.  
It fetches data from three external sources:  

- [Fuel Economy API](https://fueleconomy.gov/feg/ws/index.shtml) – Vehicle details, emissions, and MPG  
- [NHTSA Datasets and APIs](https://www.nhtsa.gov/nhtsa-datasets-and-apis) – Safety ratings, recalls, complaints, inspection stations  
- [Alternative Fuel Stations API](https://developer.nrel.gov/docs/transportation/alt-fuel-stations-v1/all/) – Electric and alternative fuel stations across the US  

The pipeline processes this data into structured pandas DataFrames, writes intermediate results into CSV files for testing, and finally loads the processed data into SQL staging tables.  

---

## 📂 Folder Structure
```
bosch-challenge/
│
├── extracted_data/             # Raw CSVs saved after extraction
├── processed_data/             # Cleaned CSVs saved after processing
├── extracted_data_schemas/     # Auto-generated JSON schemas for extracted files
├── processed_data_schemas/     # Auto-generated JSON schemas for processed files
├── sql_scripts/                # CREATE TABLE scripts for Azure SQL
│
├── utils/                      # Python modules for each stage
│   ├── fuel_economy_async.py         # Defines FuelEconomyETL
│   ├── highway_safety_admin_async.py # Defines SafetyAdministrationETL
│   ├── alternative_fuel_async.py     # Defines AlternativeFuelETL
│   ├── data_processing.py            # Defines Processing
│   ├── data_loading.py               # Defines Loading
│   ├── schema_producer.py            # Generates schemas for files
│   └── alternative_fuel_schema.json  # Schema for Alternative Fuel API
│
├── main.py                     # Entrypoint to orchestrate ETL pipeline
├── requirements.txt            # Project dependencies
├── connection_config.json      # Database credentials/config
```

---

## ⚙️ Setup and Installation
Open Command Prompt and navigate to your project directory:
```bash
cd C:\Users\.....bosch-challenge
```

Create a virtual environment ('bosch'):
```bash
python -m venv bosch
```

Activate the environment (Windows):
```bash
bosch\Scripts\activate
```

Install dependencies:
```bash
pip install -r requirements.txt
```

---

## ▶️ Running the Pipeline
```bash
python main.py
```

---

## 📂 Module Documentation

⚠️ **Note on naming:** The extraction classes (`FuelEconomyETL`, `SafetyAdministrationETL`, `AlternativeFuelETL`) are somewhat poorly named, since they only perform **extraction** and not the full ETL process.

---

### 1. `main.py`
**Role:** Orchestrator of the full ETL workflow.  
**Flow:**  
1. Runs `FuelEconomyETL`, `SafetyAdministrationETL`, and `AlternativeFuelETL`.  
2. Generates schemas using `schema_producer.py`.  
3. Cleans extracted data using `Processing`.  
4. Loads processed data into Azure SQL with `Loading`.  

**Inputs:**  
- ETL parameters (`num_years`, `concurrency`)  
- DB credentials from `connection_config.json`. In a real production environment, credentials would be injected using environment variables or a secrets manager (e.g., Azure Key Vault) instead of a local JSON file.

**Outputs:**  
- Extracted CSVs in `extracted_data/`  
- Processed CSVs in `processed_data/`  
- JSON schema files created by `schema_producer.py`  
  - Extracted schemas → `extracted_data_schemas/`  
  - Processed schemas → `processed_data_schemas/`  
- Tables created in Azure SQL (schema `stg`)  

---

### 2. Extraction Modules

#### a) `fuel_economy_async.py`
The module `fuel_economy_async.py` defines the class **`FuelEconomyETL`**.

- **Inputs:** `num_years`, `concurrency`  
- **Outputs:** CSVs in `extracted_data/FuelEconomy/` (`FuelEconomy_*.csv`, `Emissions_*.csv`, `MPG_Summary_*.csv`, `MPG_Detail_*.csv`)  
- **Notes:**  
  Complex JSON fields originate new DataFrames instead of exploding into a single one. Required creating helper classes (`Model`, `Vehicle`) to handle API hierarchy.  

#### b) `highway_safety_admin_async.py`
The module `highway_safety_admin_async.py` defines the class **`SafetyAdministrationETL`**.

- **Inputs:** `num_years`, `concurrency`  
- **Outputs:** CSVs in `extracted_data/NHTSafetyAdministration/` (`SafetyRatings_*.csv`, `Recalls_*.csv`, `Complaints_*.csv`, `InspectionsLocation_*.csv`)  
- **Notes:**  
  Filters out invalid years (`9999`, `2027`). Reused skeleton from FuelEconomy classes with tuning for differences in JSON fields. The API structure required multiple endpoint calls: first fetch vehicle IDs, then details.  

#### c) `alternative_fuel_async.py`
The module `alternative_fuel_async.py` defines the class **`AlternativeFuelETL`**.

- **Inputs:** `concurrency`  
- **Outputs:** CSVs in `extracted_data/AlternativeFuel/` (`Stations_*.csv`, `RelatedStations_*.csv`, `EvConnectorTypes_*.csv`, `HyPressures_*.csv`, `HyStandards_*.csv`)  
- **Notes:**  
  Handles arrays and records as separate DataFrames. Uses pagination (limit + offset).  

---

### 3. `schema_producer.py`
The module `schema_producer.py` defines the schema generation logic.

- **Role:** Generates JSON schema definitions for CSVs.  
- **Inputs:** Latest extracted or processed CSVs.  
- **Outputs:** JSON schema files saved in `extracted_data_schemas/` and `processed_data_schemas/`.  

---

### 4. `data_processing.py`
The module `data_processing.py` defines the class **`Processing`**.

- **Role:** Cleans and standardizes extracted CSVs.  
- **Inputs:** Mapping of datasets to CSVs (from `produce_schemas`).  
- **Outputs:** Processed CSVs under `processed_data/<dataset>/`.  

Includes:  
- Null handling  
- Boolean mapping (e.g. `mpgData_bool`)  
- Column renaming (camelCase, lowercase first letter)  
- Deduplication  

---

### 5. `data_loading.py`
The module `data_loading.py` defines the class **`Loading`**.

- **Role:** Loads processed data into Azure SQL staging schema.  
- **Inputs:**  
  - DB credentials (`server`, `database`, `username`, `password`) from `connection_config.json`  
  - Latest processed CSVs (from `produce_schemas`)  
- **Outputs:**  
  - `CREATE TABLE` scripts saved in `sql_scripts/`  
  - Data loaded into staging tables (`stg` schema) in Azure SQL  