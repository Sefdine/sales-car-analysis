# Import necessary packages
import pandas as pd
from sqlalchemy.orm import sessionmaker
import logging
import sys
sys.path.append("../")
from create_tables import Gearbox_Dimension, engine

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

        # Insert into Geabox
        for row in df['Gearbox'].unique():
            # Check the line
            existing_gearbox = session.query(Gearbox_Dimension).filter_by(Gearbox=row).first()
            if not existing_gearbox:
                new_gearbox = Gearbox_Dimension(Gearbox=row)
                session.add(new_gearbox)
        session.commit()
        logging.info("Success: gearbox loaded successfully !")
    except Exception as e:
        logging.error(f"Error: Failed to load gearbox into data warehouse. {e}")

    # Dispose engine
    engine.dispose()