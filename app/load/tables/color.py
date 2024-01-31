# Import necessary packages
import pandas as pd
from sqlalchemy.orm import sessionmaker
import logging
import sys
sys.path.append("../")
from create_tables import Color_Dimension, engine

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

        # Insert into color
        for row in df['Color'].unique():
            # Check the line
            existing_color = session.query(Color_Dimension).filter_by(Color=row).first()
            if not existing_color:
                new_color = Color_Dimension(Color=row)
                session.add(new_color)
        # Commit the changes
        session.commit()
        logging.info("Success: color loaded successfully !")
    except Exception as e:
        logging.error(f"Error: Failed to load color into data warehouse. {e}")

    # Dispose engine
    engine.dispose()