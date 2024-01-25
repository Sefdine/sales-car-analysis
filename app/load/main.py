# Import necessary packages
from sqlalchemy import create_engine, MetaData, text, Table, Column, Integer, Float, String, VARCHAR

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