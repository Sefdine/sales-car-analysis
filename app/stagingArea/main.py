# Script for staging area

# Import necessary libraries and load data
import pandas as pd
import uuid
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Read us cars data from csv
us_cars_df = pd.read_csv("../data/us_cars.csv")
logging.info('CSV Data Loaded')
logging.info('Start pre-traitement in csv data')

# Drop unecessary columns
columns_to_drop = [' Genmodel_ID', 'Wheelbase', 'Annual_Tax', 'Wheelbase', 'Top_speed', 'Engine_power', 'Adv_ID']
us_cars_df.drop(columns=columns_to_drop, inplace=True)

# Rename columns
mapper = {
    ' Genmodel': 'Model',
    'Runned_Miles': 'Miles',
    'Seat_num': 'Seatings',
    'Door_num': 'Doors',
    'Engin_size': 'Engine_size'
}
us_cars_df.rename(columns=mapper, inplace=True)
# Add column stat "used"
us_cars_df['Stat'] = "used"
# Add column country
us_cars_df['country'] = 'United Kingdom'

# Generate id for us_cars_df
us_cars_df['ID'] = us_cars_df['Maker'].apply(lambda x: str(uuid.uuid4()))

# Read the data from the json source
marketcheck_df = pd.read_json('../data/marketcheck.json')
logging.info('JSON Data Loaded')
logging.info('Start pre-traitement in json data')

# Rename columns
marketcheck_mapper = {
    'id': 'ID',
    'price': 'Price',
    'miles': 'Miles',
    'base_ext_color': 'Color',
    'inventory_type': 'Stat',
    'first_seen_at_source_date': 'Adv_date',
    'build': 'Build'
}
marketcheck_df = marketcheck_df[marketcheck_mapper.keys()]
marketcheck_df.rename(columns=marketcheck_mapper, inplace=True)

# Retrieve columns from object build
marketcheck_df['Reg_year'] = marketcheck_df['Build'].apply(lambda x: x['year'])
marketcheck_df['Maker'] = marketcheck_df['Build'].apply(lambda x: x['make'])
marketcheck_df['Model'] = marketcheck_df['Build'].apply(lambda x: x['model'])
marketcheck_df['Bodytype'] = marketcheck_df['Build'].apply(lambda x: x['body_type'])
marketcheck_df['Fuel_type'] = marketcheck_df['Build'].apply(lambda x: x['fuel_type'])
marketcheck_df['Engine_size'] = marketcheck_df['Build'].apply(lambda x: x['engine_size'] if 'engine_size' in x.keys() else None)
marketcheck_df['Doors'] = marketcheck_df['Build'].apply(lambda x: x['doors'])
marketcheck_df['Height'] = marketcheck_df['Build'].apply(lambda x: x['overall_height'])
marketcheck_df['Length'] = marketcheck_df['Build'].apply(lambda x: x['overall_length'])
marketcheck_df['Width'] = marketcheck_df['Build'].apply(lambda x: x['overall_width'])
marketcheck_df['Seatings'] = marketcheck_df['Build'].apply(lambda x: x['std_seating'])
marketcheck_df['Gearbox'] = marketcheck_df['Build'].apply(lambda x: x['transmission'])
marketcheck_df['Highway_mpg'] = marketcheck_df['Build'].apply(lambda x: x['highway_mpg'] if 'highway_mpg' in x.keys() else None)
marketcheck_df['City_mpg'] = marketcheck_df['Build'].apply(lambda x: x['city_mpg'] if 'city_mpg' in x.keys() else None)

# Drop Build column
marketcheck_df.drop(columns=['Build'], inplace=True)

# Add country column
marketcheck_df['country'] = 'United States'

# Transform datetime type
marketcheck_df['Adv_date'] = pd.to_datetime(marketcheck_df['Adv_date'])

# Create hierarchy
marketcheck_df['Adv_year'] = marketcheck_df['Adv_date'].dt.year
marketcheck_df['Adv_month'] = marketcheck_df['Adv_date'].dt.month
marketcheck_df.drop(columns=['Adv_date'], inplace=True)

# Calculate average mpg
marketcheck_df['Average_mpg'] = (marketcheck_df['Highway_mpg'] + marketcheck_df['City_mpg']) / 2

# Drop highway and city mpg
marketcheck_df.drop(columns=['Highway_mpg', 'City_mpg'], inplace=True)

logging.info('Pre-traitement complete, begin merging sources')
# Concat the two dataframes and create a final dataframe
final_df = pd.concat([us_cars_df, marketcheck_df], ignore_index=True)

# Load concatenated data in a staging area
logging.info('Load merged data into a staging area')
final_df.to_csv('../data/final_df.csv', index=False)
logging.info('Data loaded successfully !')