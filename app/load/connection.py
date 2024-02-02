# Import necessary packages
from sqlalchemy import create_engine
import logging
from sqlalchemy.sql import text

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Connect to database
def connection():
    # Replace these values with your SQL Server credentials and connection details
    username = 'sa'
    password = 'YourPassword123'
    database = 'cars_sales_warehouse'
    # server = 'sql_server'
    server = 'LIKE-YOUCODE-DA\SQLEXPRESS'

    # Construct the connection string
    connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"

    try:
        # Create the SQLAlchemy engine
        engine = create_engine(connection_string, echo=False)
        logging.info('Success: Connected to the data warehouse !')
        return engine
    except Exception as e:
        logging.info(f"Error: Failed to connect to the data warehouse {str(e)}.")
        return None