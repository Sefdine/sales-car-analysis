# Import necessary packages
import pandas as pd
from sqlalchemy.orm import sessionmaker
import logging
import sys
sys.path.append("../")
from create_tables import Maker_Dimension, engine

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

        # Insert into maker
        for row in df['Maker'].unique():
            # Check the line
            existing_maker = session.query(Maker_Dimension).filter_by(Maker=row).first()
            if not existing_maker:
                new_maker = Maker_Dimension(Maker=row)
                session.add(new_maker)
        # Commit the changes
        session.commit()
        logging.info("Success: maker loaded successfully !")
    except Exception as e:
        logging.error(f"Error: Failed to load maker into data warehouse. {e}")

    # Dispose engine
    engine.dispose()