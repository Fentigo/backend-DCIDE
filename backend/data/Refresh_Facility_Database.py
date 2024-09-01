import requests
import pandas as pd
import asyncio
import aiohttp
from backend.data.API_KEY import API_KEY

# API key
CSV_DIR= '/Databases/facility_data.csv'
headers = {"Authorization": "Api-Key " + API_KEY}

# Base URLs
facilities_url = 'https://www.peeringdb.com/api/fac'
#Required_Fields = 'id,name,org_name,net_count,city,latitude,longitude,country,ix_count,org.carrier_set,address1,address2,zipcode'


# Fetch all facility data
def fetch_all_facility_data():
    response = requests.get(facilities_url, headers=headers)
    if response.status_code == 200:
        return response.json()['data']
    else:
        print(f"Failed to fetch facility data. Status code: {response.status_code}")
        return []

# Asynchronous function to fetch data for a single facility by ID with rate limiting
async def fetch_facility_data(session, facility_id, retry_attempts=5):
    url = f"https://www.peeringdb.com/api/fac/{facility_id}"
    retry_delay = 5
    for attempt in rang(retry_attempts):
         async with session.get(url, headers=headers) as response:
            if response.status == 200:
                return await response.json()
            elif response.status == 429:
                retry_after = int(response.headers.get('Retry-Afeer', retry_delay))
                print(f"Rate limit hit for facility ID {facility_id}. Retrying after {retry_after} seconds")
                await asyncio.sleep(retry_after)
                retry_delay *=2  # Wait for 5 seconds before retrying
            else:
                print(f"Failed to fetch data for facility ID {facility_id}. Status code: {response.status}")
                return None
    print(f"max retries reached for faclity id{facility_id}. Skipping.")
    return None

# Asynchronous function to fetch detailed data for all facilities
async def fetch_all_data(facility_ids):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for facility_id in facility_ids:
            tasks.append(fetch_facility_data(session, facility_id))
            await asyncio.sleep(0.4)  # Add a short delay between requests to avoid rate limiting
        results = await asyncio.gather(*tasks)
        return [result for result in results if result is not None]

# Main function to fetch all facilities and then fetch detailed data for each. This will be used to refresh the database
def main():
    # Fetch all facility data
    all_facility_data = fetch_all_facility_data()
    
    if not all_facility_data:
        print("No facility data fetched.")
        return
    
    # Extract facility IDs and limit to the first 100
    facility_ids = [facility['id'] for facility in all_facility_data[:200]]
    
    # Fetch detailed data for the facilities
    all_data = asyncio.run(fetch_all_data(facility_ids))
    
    # Normalize the JSON data to a flat table
    flattened_data = []
    for facility in all_data:
        if 'data' in facility and len(facility['data']) > 0:
            flattened_data.append(facility['data'][0])
    
    df = pd.json_normalize(flattened_data)
    
    file_path = 'C:/Users/jfent/Chadwork/Databases/facility_data.csv'
    
    # Save to CSV
    df.to_csv(file_path, index=False)
    print(f"Data saved to {file_path}")

if __name__ == "__main__":
    main()
