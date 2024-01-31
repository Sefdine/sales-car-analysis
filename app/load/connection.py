# Import necessary packages
from sqlalchemy import create_engine

# Connect to database
def connection():
    # Replace these values with your SQL Server credentials and connection details
    username = 'sa'
    password = 'YourPassword123'
    database = 'cars_sales_warehouse'
    server = 'localhost'

    # Construct the connection string
    connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"

    try:
        # Create the SQLAlchemy engine
        engine = create_engine(connection_string, echo=False)
        return engine
    except:
        return None