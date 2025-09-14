from utils.extract_fuel_economy import FuelEconomyETL
import time

def main():
    start_time = time.time()  # start timer
    
    etl = FuelEconomyETL(num_years = 2)
    # etl.run_all(filename="TEST")
    etl.run_all()
    
    end_time = time.time()  # end timer
    elapsed_time = end_time - start_time
    print(f"\nTotal processing time for FuelEconomy source: {elapsed_time/60:.2f} minutes.")
    
if __name__ == "__main__":
    main()