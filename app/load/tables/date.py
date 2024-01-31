# Import necessary packages
import pandas as pd
from sqlalchemy.orm import sessionmaker
import logging
import sys
from datetime import datetime
sys.path.append("../")
from create_tables import Date_Dimension, engine

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

        # Insert into Date
        date_df = df[['Adv_year', 'Adv_month', 'Adv_date']].drop_duplicates().reset_index(drop=True)

        for i in range(date_df.shape[0]):
            # Convert Timestamp to datetime.date
            adv_date = datetime.strptime(date_df['Adv_date'][i], "%Y-%m-%d").date()
            # Convert Adv_year and Adv_month to integers
            adv_year = int(date_df['Adv_year'][i])
            adv_month = int(date_df['Adv_month'][i])

            # Check existing
            existing_date = (
                session.query(Date_Dimension)
                .filter_by(Adv_year=adv_year, Adv_month=adv_month, Adv_date=adv_date)
                .first()
            )
            if not existing_date:
                new_date = Date_Dimension(Adv_year=adv_year, Adv_month=adv_month, Adv_date=adv_date)
                session.add(new_date)
        session.commit()
        logging.info("Success: date loaded successfully !")
    except Exception as e:
        logging.error(f"Error: Failed to load date into data warehouse. {e}")

    # Dispose engine
    engine.dispose()