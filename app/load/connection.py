# Import necessary packages
from sqlalchemy import create_engine
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        logging.info('Success : Connected to the data warehouse !')
        return engine
    except:
        logging.info('Error : Failed to connect to the data warehouse.')
        return None