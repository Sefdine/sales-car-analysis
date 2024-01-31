# Import necessary packages
import pandas as pd
from sqlalchemy.orm import sessionmaker
import logging
import sys
sys.path.append("../")
from create_tables import Model_Dimension, engine

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

        # Insert into Model
        model_df = df[['Model', 'Bodytype']].drop_duplicates().reset_index(drop=True)
        for i in range(model_df.shape[0]):
            # Check existing model line
            existing_model = (
                session.query(Model_Dimension)
                .filter_by(Model=model_df['Model'][i], Bodytype=model_df['Bodytype'][i])
                .first()
            )
            if not existing_model:
                new_model = Model_Dimension(Model=model_df['Model'][i], Bodytype=model_df['Bodytype'][i])
                session.add(new_model)
        session.commit()
        logging.info("Success: Model loaded successfully !")
    except Exception as e:
        logging.error(f"Error: Failed to load Model into data warehouse. {e}")

    # Dispose engine
    engine.dispose()