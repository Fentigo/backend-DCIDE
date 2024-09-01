import requests
import pandas as pd
import asyncio
import aiohttp
from backend.data.API_KEY import API_KEY

# API key

headers = {"Authorization": "Api-Key " + API_KEY}


# Base URLs
network_url = 'https://www.peeringdb.com/api/net'


# Fetch all network data
def fetch_all_network_data():
    response = requests.get(network_url, headers=headers)
    if response.status_code == 200:
        return response.json()['data']
    else:
        print(f"Failed to fetch network data. Status code: {response.status_code}")
        return []

# Asynchronous function to fetch data for a single network by ID with rate limiting
async def fetch_network_data(session, network_id):
    url = f"https://www.peeringdb.com/api/net/{network_id}"
    while True:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                return await response.json()
            elif response.status == 429:
                print(f"Rate limit hit for network ID {network_id}. Retrying after delay.")
                await asyncio.sleep(5)  # Wait for 5 seconds before retrying
            else:
                print(f"Failed to fetch data for network ID {network_id}. Status code: {response.status}")
                return None

# Asynchronous function to fetch detailed data for all networks
async def fetch_all_data(network_ids):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for network_id in network_ids:
            tasks.append(fetch_network_data(session, network_id))
            await asyncio.sleep(0.3)  # Add a short delay between requests to avoid rate limiting
        results = await asyncio.gather(*tasks)
        return [result for result in results if result is not None]

# Main function to fetch all network and then fetch detailed data for each. This will be used to refresh the database
def main():
    # Fetch all network data
    all_network_data = fetch_all_network_data()
    
    if not all_network_data:
        print("No network data fetched.")
        return
    
    # Extract network IDs and limit to the first 100
    network_ids = [network['id'] for network in all_network_data[:200]]
    
    # Fetch detailed data for the networks
    all_data = asyncio.run(fetch_all_data(network_ids))
    
    # Normalize the JSON data to a flat table
    flattened_data = []
    for network in all_data:
        if 'data' in network and len(network['data']) > 0:
            flattened_data.append(network['data'][0])
    
    df = pd.json_normalize(flattened_data)
    
    file_path = 'C:/Users/jfent/Chadwork/Databases/network_data.csv'
    
    # Save to CSV
    df.to_csv(file_path, index=False)
    print(f"Data saved to {file_path}")

if __name__ == "__main__":
    main()
