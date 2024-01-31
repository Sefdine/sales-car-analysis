# This script fetch data from marketcheck api

import requests
import logging
import json
import os
from dotenv import load_dotenv

load_dotenv()

def get_market_data(api_key, start=1, rows=25):
    url = "https://mc-api.marketcheck.com/v2/search/car/active"

    params = {
        'api_key': api_key,
        'car_type': 'used',
        'exclude_certified': 'true',
        'include_relevant_links': 'true',
        'start': start,
        'rows': rows
    }

    headers = {}

    data = []

    while True:
        try:
            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 200:
                data.extend(response.json()['listings'])
                start += rows
                print(f"The length of data is {len(data)}", end="\r")
            else:
                logging.error(f"Error retrieving data. Status code: {response.status_code}")
                break

        except Exception as e:
            logging.error(f"Error retrieving data. {str(e)}")
            break

    return data

def save_to_json(data, filename='marketcheck1.json'):
    with open(filename, 'w') as f:
        f.write(json.dumps(data))
    print(f"JSON file '{filename}' created")

if __name__ == "__main__":
    api_key = os.getenv("api_key")
    market_data = get_market_data(api_key)
    save_to_json(market_data)
