import requests
import pandas as pd
import asyncio
import aiohttp
#from backend.backend.views.data.API_KEY import API_KEY

# API key
API_KEY = "Y7aR2DB4.heXriM0CyBgXICzrSlT9jdfkWHY3CYjl"
headers = {"Authorization": "Api-Key " + API_KEY}

#File Directory

# Base URLs
exchanges_url = 'https://www.peeringdb.com/api/ix'
#Required_Fields ="id, org_name,website,net_set,fac_set,carrier_set,ix_set,campus_set,address1,campus_set,address2,city,country,state,zipcode,latitude,longitude,name"
CSV_DIR = 'C:/Users/jfent/Chadwork/Databases/exchange_data.csv'

# Fetch all exchange data
def fetch_all_exchange_data():
    response = requests.get(exchanges_url, headers=headers)
    if response.status_code == 200:
        return response.json()['data']
    else:
        print(f"Failed to fetch exchange data. Status code: {response.status_code}")
        return []

# Asynchronous function to fetch data for a single exchange by ID with rate limiting
async def fetch_exchange_data(session, exchange_id):
    url = f"https://www.peeringdb.com/api/ix/{exchange_id}"
    while True:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                return await response.json()
            elif response.status == 429:
                print(f"Rate limit hit for exchange ID {exchange_id}. Retrying after delay.")
                await asyncio.sleep(5)  # Wait for 5 seconds before retrying
            else:
                print(f"Failed to fetch data for exchange ID {exchange_id}. Status code: {response.status}")
                return None

# Asynchronous function to fetch detailed data for all facilities
async def fetch_all_data(exchange_ids):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for exchange_id in exchange_ids:
            tasks.append(fetch_exchange_data(session, exchange_id))
            await asyncio.sleep(0.9)  # Add a short delay between requests to avoid rate limiting
        results = await asyncio.gather(*tasks)
        return [result for result in results if result is not None]

# Main function to fetch all facilities and then fetch detailed data for each. This will be used to refresh the database
def main():
    # Fetch all facility data
    all_exchange_data = fetch_all_exchange_data()
    
    if not all_exchange_data:
        print("No exchange data fetched.")
        return
    
    # Extract exchange IDs and limit to the first 100
    exchange_ids = [exchange['id'] for exchange in all_exchange_data[:150]]
    
    # Fetch detailed data for the exchanges
    all_data = asyncio.run(fetch_all_data(exchange_ids))
    
    # Normalize the JSON data to a flat table
    flattened_data = []
    for exchange in all_data:
        if 'data' in exchange and len(exchange['data']) > 0:
            flattened_data.append(exchange['data'][0])
    
    df = pd.json_normalize(flattened_data)
    
    file_path = CSV_DIR
    
    # Save to CSV
    df.to_csv(file_path, index=False)
    print(f"Data saved to {file_path}")

if __name__ == "__main__":
    main()
