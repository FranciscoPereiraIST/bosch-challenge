# from utils.extract_fuel_economy import FuelEconomyETL

from utils.fuel_economy_async import FuelEconomyETL
from utils.highway_safety_admin_async import SafetyAdministrationETL

from time import perf_counter
import asyncio

def main():
    
    # time_before = perf_counter()
    
    # etl = FuelEconomyETL(num_years=2, concurrency=10)
    # asyncio.run(etl.run_all())
    
    # duration_in_secs = perf_counter() - time_before
    # print(f"Total time (asynchronous): {duration_in_secs:.3f} s -> {duration_in_secs/60:.1f} min")
    
    etl_nhtsa = SafetyAdministrationETL(num_years=1, concurrency=10)
    asyncio.run(etl_nhtsa.run_all())
    
if __name__ == "__main__":
    main()