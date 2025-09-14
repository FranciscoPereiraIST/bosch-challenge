# from utils.extract_fuel_economy import FuelEconomyETL

from utils.fuel_economy_async import FuelEconomyETL
from time import perf_counter
import asyncio

def main_2():
    
    time_before = perf_counter()
    
    etl = FuelEconomyETL(num_years=2, concurrency=10)
    asyncio.run(etl.run_all())
    
    duration_in_secs = perf_counter() - time_before
    print(f"Total time (asynchronous): {duration_in_secs:.3f} s -> {duration_in_secs/60:.1f} min")

# def main():
#     start_time = time.time()  # start timer
    
#     etl = FuelEconomyETL(num_years = 2)
#     # etl.run_all(filename="TEST")
#     etl.run_all()
    
#     end_time = time.time()  # end timer
#     elapsed_time = end_time - start_time
#     print(f"\nTotal processing time for FuelEconomy source: {elapsed_time/60:.2f} minutes.")
    
if __name__ == "__main__":
    main_2()