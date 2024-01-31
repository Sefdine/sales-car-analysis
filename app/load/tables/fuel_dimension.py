# Import necessary packages
import pandas as pd
from sqlalchemy.orm import sessionmaker
import logging
import sys
sys.path.append("../")
from create_tables import Fuel_Dimension, engine

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load transformed data
df = pd.read_csv('../../data/transformed_data.csv')
logging.info("Data retrieved successfully")

if not engine:
    sys.exit()
else:
    try:
        # Create session
        Session = sessionmaker(bind=engine)
        session = Session()

        # Insert into FuelCategory
        for row in df['Fuel_category'].unique():
            # Check if exist
            existing_fuel_category = session.query(Fuel_Dimension).filter_by(FuelCategory=row).first()
            if not existing_fuel_category:
                new_fuel_category = Fuel_Dimension(FuelCategory=row)
                session.add(new_fuel_category)
        session.commit()
        logging.info("Success: fuel_dimension loaded successfully !")
    except Exception as e:
        logging.error(f"Error: Failed to load fuel_dimension into data warehouse. {e}")

    # Dispose engine
    engine.dispose()