# from utils.extract_fuel_economy import FuelEconomyETL

from utils.fuel_economy_async import FuelEconomyETL
from utils.highway_safety_admin_async import SafetyAdministrationETL
from utils.alternative_fuel_async import AlternativeFuelETL

from time import perf_counter
import asyncio

def main():
    
    # time_before = perf_counter()
    
    # etl = FuelEconomyETL(num_years=2, concurrency=10)
    # asyncio.run(etl.run_all())
    
    # duration_in_secs = perf_counter() - time_before
    # print(f"Total time (FuelEconomy): {duration_in_secs:.3f} s -> {duration_in_secs/60:.1f} min")
    
    # time_before = perf_counter()
    # etl_nhtsa = SafetyAdministrationETL(num_years=2, concurrency=5)
    # asyncio.run(etl_nhtsa.run_all())
    # duration_in_secs = perf_counter() - time_before
    # print(f"Total time (NHTSafetyAdmin): {duration_in_secs:.3f} s -> {duration_in_secs/60:.1f} min")
    
    time_before = perf_counter()
    etl_afdc = AlternativeFuelETL(num_years=2, concurrency=5)
    asyncio.run(etl_afdc.run_all())
    duration_in_secs = perf_counter() - time_before
    print(f"Total time (AlternativeFuel): {duration_in_secs:.3f} s -> {duration_in_secs/60:.1f} min")
    
    output = etl_afdc.get_output()
    
    print("\n")
    for k in output.keys():
        df = output[k]
        print(f"Output df named '{k}' has shape ({df.shape[0]}, {df.shape[1]})")

    
if __name__ == "__main__":
    main()