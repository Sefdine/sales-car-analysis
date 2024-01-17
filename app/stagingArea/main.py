# Script for staging area

# Import necessary libraries and load data
import pandas as pd
import uuid
from sqlalchemy import create_engine, MetaData, text, Table, Column, Integer, Float, String, VARCHAR

# Read us cars data from csv
us_cars_df = pd.read_csv("../data/us_cars.csv")

# Drop unecessary columns
columns_to_drop = [' Genmodel_ID', 'Wheelbase', 'Annual_Tax', 'Wheelbase', 'Top_speed', 'Engine_power']
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

# Generate id for us_cars_df
us_cars_df['ID'] = us_cars_df['Adv_ID'].apply(lambda x: str(uuid.uuid4()))

# Read the data from the json source
marketcheck_df = pd.read_json('../data/marketcheck.json')

# Rename columns
marketcheck_mapper = {
    'id': 'ID',
    'price': 'Price',
    'miles': 'Miles',
    'base_ext_color': 'Color',
    'inventory_type': 'Stat',
    'first_seen_at_source_date': 'Adv_date',
    'first_seen_at_source': 'Adv_ID',
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

# Concat the two dataframes and create a final dataframe
final_df = pd.concat([us_cars_df, marketcheck_df], ignore_index=True)

# Function to connect with sql server
def get_sql_server_connection():
    server_name = 'LIKE-YOUCODE-DA\SQLEXPRESS'
    database_name = 'us_car_sales_staging'
    trusted_connection = 'yes'

    if trusted_connection == 'no':
        connection_url = f'mssql+pyodbc://{server_name}/{database_name}?driver=SQL+Server'
    else:
        username = 'sa'
        password = 'YourPassword123'
        connection_url = f"mssql+pyodbc://{username}:{password}@{server_name}/{database_name}?driver=ODBC+Driver+17+for+SQL+Server"

    engine = create_engine(connection_url)
    return engine

# Create a table in the database
try:
    # Retrieve the sql server engine
    engine = get_sql_server_connection()
    print(f"Connection to the staging area created successfully.")

    # Inititialize the metadata object
    meta = MetaData()

    # Create the table
    us_cars_table = Table(
        'us_cars_sales_table', meta,
        Column('id', String(125), primary_key=True),
        Column('maker', String(125)),
        Column('model', String(125)),
        Column('adv_id', String(125)),
        Column('adv_year', Integer),
        Column('adv_month', Integer),
        Column('color', String(125)),
        Column('reg_year', Integer),
        Column('bodytype', String(125)),
        Column('miles', String(125)),
        Column('engine_size', String(125)),
        Column('gearbox', String(125)),
        Column('fuel_type', String(125)),
        Column('price', Float),
        Column('height', String(125)),
        Column('width', String(125)),
        Column('lenght', String(125)),
        Column('average_mpg', String(125)),
        Column('seatings', String(125)),
        Column('doors', String(125)),
        Column('stats', String(125)),
        schema='dbo'
    )

    try:
        # Execute the table creation
        meta.create_all(engine)
        print('us_car_sales_table created successfully')
    except Exception as e:
        print(f'Failed to create the table. {str(e)}')
except Exception as ex:
    print("Connection could not be made due to the following error: \n", ex)

# Table name
table_name = 'us_cars_sales_table'

# Insert the DataFrame into the database
final_df.to_sql(table_name, con=engine, index=False, if_exists='replace')

# Dispose of the engine 
engine.dispose()