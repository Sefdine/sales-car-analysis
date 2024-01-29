# Import necessary packages
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Read the data from csv
df = pd.read_csv('../data/final_df.csv')
logging.info('Merged Data Loaded')
logging.info('Start transformation')

# Retrieve percentage of null values
transform_dict = {}

for column in df.columns:
    transform_dict[column] = round(((df.loc[df[column].isnull()].shape[0] - df.shape[0]) / df.shape[0] * 100) + 100, 3)

# Put 12 to month upper than 12 
df.loc[df['Adv_month'] > 12, 'Adv_month'] = 12

# Create Adv_date
df['Adv_date'] = pd.to_datetime(df['Adv_year'].astype('str') + '-' + df['Adv_month'].astype('str') + '-' + '01')

# Drop column with null values that is lower than 0.1%
null_column_lower_01 = [x for x in transform_dict if transform_dict[x] > 0 and transform_dict[x] < 1]

for column in null_column_lower_01:
    df.drop(df.loc[df[column].isnull()].index, inplace=True)

# Transform Reg_year type to int
df['Reg_year'] = df['Reg_year'].astype('int')

# Replace 1 mile to 1
df.loc[df['Miles'] == '1 mile', 'Miles'] = 1
# Transform Miles type into int
df['Miles'] = df['Miles'].astype('int')

# Remove L in Engine_size and convert type into float
df['Engine_size'] = df['Engine_size'].astype('str').str.rstrip('L').astype('float')
# Replace null values in Engine size by the average Engine_size by Maker, Model
df['Engine_size'].fillna(df.groupby(['Maker', 'Model'])['Engine_size'].transform('mean'), inplace=True)
# Replace the remaining null values in Engine size by the average Engine_size by Maker
df['Engine_size'].fillna(df.groupby('Maker')['Engine_size'].transform('mean'), inplace=True)
# Drop the remaining null values that is 0.001%
df.dropna(subset=['Engine_size'], inplace=True)

# Unify the same fuel types wroten differently
df['Fuel_category'] = df['Fuel_type'].replace({
    'Petrol Plug-in Hybrid': 'Plug-in Hybrid',
    'Hybrid  Petrol/Electric Plug-in': 'Plug-in Hybrid',
    'Petrol Ethanol': 'Ethanol',
    'E85 / Unleaded': 'Ethanol',
    'Hybrid  Diesel/Electric': 'Diesel Hybrid',
    'Hybrid  Diesel/Electric Plug-in': 'Plug-in Diesel Hybrid',
    'Hybrid  Petrol/Electric': 'Petrol Hybrid',
    'Petrol Hybrid': 'Petrol Hybrid',
    'Diesel Hybrid': 'Diesel Hybrid',
    'Diesel Plug-in Hybrid': 'Plug-in Diesel Hybrid'
})

# Fill null values in Color column
df['Color'].fillna(df.groupby(['Maker', 'Model', 'Reg_year', 'Gearbox', 'Fuel_type', 'Fuel_category'])['Color'].transform('first'), inplace=True)
df['Color'].fillna(df.groupby(['Maker', 'Model', 'Reg_year', 'Gearbox'])['Color'].transform('first'), inplace=True)

# Handle missing values in Height, Width and Length
for column in ['Height', 'Width', 'Length']:
    df[column].fillna(df.groupby(['Maker', 'Model', 'Bodytype', 'Fuel_category'])[column].transform('mean'), inplace=True)
    df[column].fillna(df.groupby(['Maker', 'Model'])[column].transform('mean'), inplace=True)
    df[column].fillna(df.groupby(['Maker'])[column].transform('mean'), inplace=True)
df.dropna(subset=['Height', 'Width', 'Length'], inplace=True)

# Remove mpg in average mpg and change type into float
df['Average_mpg'] = df['Average_mpg'].astype('str').str.rstrip('mpg').str.strip().astype('float')
# Fill null values in average mpg
df['Average_mpg'].fillna(df.groupby(['Maker', 'Model', 'Reg_year'])['Average_mpg'].transform('mean'), inplace=True)
df['Average_mpg'].fillna(df.groupby(['Maker', 'Reg_year'])['Average_mpg'].transform('mean'), inplace=True)
df['Average_mpg'].fillna(df.groupby(['Maker'])['Average_mpg'].transform('mean'), inplace=True)

# Handle Seatings missing values
df['Seatings'].fillna(df.groupby(['Maker', 'Model', 'Reg_year', 'Bodytype'])['Seatings'].transform('first'), inplace=True)
df['Seatings'].fillna(df.groupby(['Maker', 'Model', 'Reg_year'])['Seatings'].transform('first'), inplace=True)
df['Seatings'].fillna(df.groupby(['Maker', 'Model'])['Seatings'].transform('first'), inplace=True)
df['Seatings'].fillna(df.groupby(['Maker'])['Seatings'].transform('first'), inplace=True)
df['Seatings'].fillna(df.groupby(['Reg_year'])['Seatings'].transform('first'), inplace=True)
# Change seantings type to int
df['Seatings'] = df['Seatings'].astype('int')

# Handle Doors missing values
df['Doors'].fillna(df.groupby(['Maker', 'Model', 'Bodytype', 'Reg_year'])['Doors'].transform('first'), inplace=True)
df['Doors'].fillna(df.groupby(['Maker', 'Model', 'Reg_year'])['Doors'].transform('first'), inplace=True)
df['Doors'].fillna(df.groupby(['Maker', 'Bodytype', 'Reg_year'])['Doors'].transform('first'), inplace=True)
df['Doors'].fillna(df.groupby(['Model', 'Bodytype', 'Reg_year'])['Doors'].transform('first'), inplace=True)
df['Doors'].fillna(df.groupby(['Bodytype', 'Reg_year'])['Doors'].transform('first'), inplace=True)
df['Doors'].fillna(df.groupby(['Maker', 'Model'])['Doors'].transform('first'), inplace=True)
df['Doors'].fillna(df.groupby(['Maker'])['Doors'].transform('first'), inplace=True)

logging.info('Transformation done !')
logging.info('Load transformed data into csv')

# Load transformed data into csv
df.to_csv('../data/transformed_data.csv', index=False)
logging.info('Transformed data loaded successfully')