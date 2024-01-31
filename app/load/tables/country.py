# Import necessary packages
import pandas as pd
from sqlalchemy.orm import sessionmaker
import logging
import sys
sys.path.append("../")
from connection import connection
from create_tables import Country_Dimension, engine

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load transformed data
df = pd.read_csv('../../data/transformed_data.csv')
logging.info("Data retrieved successfully")

# Connect to the data warehouse
# engine = connection()

if not engine:
    sys.exit()
else:
    try:
        # Create session
        Session = sessionmaker(bind=engine)
        session = Session()

        # Insert into country
        for row in df['country'].unique():
            # Check the line
            existing_country = session.query(Country_Dimension).filter_by(Country=row).first()
            if not existing_country:
                new_country = Country_Dimension(Country=row)
                session.add(new_country)
        # Commit the changes
        session.commit()
        logging.info("Success: country loaded successfully !")
    except Exception as e:
        logging.error(f"Error: Failed to load country into data warehouse. {e}")

    # Dispose engine
    engine.dispose()