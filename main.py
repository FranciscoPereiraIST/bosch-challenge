# from utils.extract_fuel_economy import FuelEconomyETL

from utils.fuel_economy_async import FuelEconomyETL
from utils.highway_safety_admin_async import SafetyAdministrationETL
from utils.alternative_fuel_async import AlternativeFuelETL
from utils.schema_producer import produce_schemas
from utils.data_processing import Processing

from time import perf_counter
import asyncio

def print_output_info(output_dict: dict, dataset : str):
    print(f"\nOutput dataframes for dataset '{dataset}':")
    for k in output_dict.keys():
        df = output_dict[k]
        print(f"Output df named '{k}' has shape ({df.shape[0]}, {df.shape[1]})") if df is not None else print(f"Output df named '{k}' is None.")

def main():
    
    # dataset = 'FuelEconomy'
    # time_before = perf_counter()
    # etl = FuelEconomyETL(num_years=2, concurrency=10)
    # asyncio.run(etl.run_all())
    # duration_in_secs = perf_counter() - time_before
    # print(f"Total time ({dataset}): {duration_in_secs:.3f} s -> {duration_in_secs/60:.1f} min")
    
    # output_fuel_econ = etl.get_output()
    # print_output_info(output_dict=output_fuel_econ, dataset = dataset)

    # dataset = 'NHTSafetyAdmin'
    # time_before = perf_counter()
    # etl_nhtsa = SafetyAdministrationETL(num_years=2, concurrency=5)
    # asyncio.run(etl_nhtsa.run_all())
    # duration_in_secs = perf_counter() - time_before
    # print(f"Total time ({dataset}): {duration_in_secs:.3f} s -> {duration_in_secs/60:.1f} min")
    
    # output_safety = etl_nhtsa.get_output()
    # print_output_info(output_dict=output_safety, dataset = dataset)
    
    # dataset = 'AlternativeFuel'
    # time_before = perf_counter()
    # etl_afdc = AlternativeFuelETL(concurrency=5)
    # asyncio.run(etl_afdc.run_all())
    # duration_in_secs = perf_counter() - time_before
    # print(f"Total time ({dataset}): {duration_in_secs:.3f} s -> {duration_in_secs/60:.1f} min")
    
    # output_alternative_fuel = etl_afdc.get_output()
    # print_output_info(output_dict=output_alternative_fuel, dataset = dataset)
    
    # need to produce schemas every run? cause there may exist new dataframes for the AlternativeFul Data that were not obtained in previous runs
    # although the parameters are fixed for now........
    latest_files = produce_schemas(write_json_flag=False)
    
    print("\n", latest_files)
    
    processing = Processing(file_dict=latest_files)
    processing.run_all(write_flag=True)
    
    dataset = 'TEST'
    output_processing = processing.get_output()
    print_output_info(output_dict=output_processing, dataset = dataset)
    
    
    
if __name__ == "__main__":
    main()