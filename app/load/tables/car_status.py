# Import necessary packages
import pandas as pd
from sqlalchemy.orm import sessionmaker
import logging
import sys
sys.path.append("../")
from create_tables import CarStatus_Dimension, engine

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

        # Insert into CarStatus
        for row in df['Stat'].unique():
            # Check the line
            existing_stat = session.query(CarStatus_Dimension).filter_by(CarStatus=row).first()
            if not existing_stat:
                new_stat = CarStatus_Dimension(CarStatus=row)
                session.add(new_stat)
        session.commit()
        logging.info("Success: carStatus loaded successfully !")
    except Exception as e:
        logging.error(f"Error: Failed to load carStatus into data warehouse. {e}")

    # Dispose engine
    engine.dispose()