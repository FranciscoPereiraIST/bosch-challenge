import pandas as pd 
from extract_data_fuel_economy import fetch_FE_data

# do i have a limit for the number of requests with my api key??

def main():
    
    fetch_FE_data()
    
    # AFDC_base_url = "https://developer.nrel.gov/api/alt-fuel-stations/v1.json"
    
    # payload = {
    #     "api_key": "61PcQy8NHlSjJKWx3CD8LXzWYAuA4E9bBQZzp8jQ",
    #     "status": "E",
    #     "cng_vehicle_class":"HD",
    #     "limit": 1
    # }
    
    # r = requests.get(AFDC_base_url, params=payload)
    
    # response_headers = r.headers

    # print(r.url, r.status_code, response_headers)
    
    # print(r.json())
    
    # fuel_economy_base_url = "https://fueleconomy.gov/ws/rest/vehicle"
    
    # headers = {
    # "Accept": "application/json"
    # }

    # # get a list of model years
    # r = requests.get(url = fuel_economy_base_url + "/menu/year",  headers=headers)
    
    # response_headers = r.headers

    # print(r.url, r.status_code, response_headers, sep="\n")
    
    # json_r = r.json()
    
    # for idx, elem in enumerate(json_r['menuItem']):
    #     print(f'\t {idx} - {elem} \n')
    

if __name__ == "__main__":
    main()