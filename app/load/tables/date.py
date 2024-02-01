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

        for year in range(df['Adv_year'].min(), df['Adv_year'].max() + 1):
            for month in range(1, 13):
                for day in range(1, 32):
                    try:
                        # Create date object
                        adv_date = datetime(year, month, day).date()

                        # Check existing
                        existing_date = (
                            session.query(Date_Dimension)
                            .filter_by(Adv_year=year, Adv_month=month, Adv_date=adv_date)
                            .first()
                        )

                        if not existing_date:
                            # Insert into Date_Dimension
                            new_date = Date_Dimension(Adv_year=year, Adv_month=month, Adv_date=adv_date)
                            session.add(new_date)
                            logging.info(f"{year}-{month}-{day} inserted successfully !")
                            session.commit()

                    except ValueError:
                        # Handle cases where the day exceeds the number of days in the month
                        logging.info(f"{year}-{month}-{day} already exists")
                        session.rollback()
                        pass
        logging.info("Success: date loaded successfully !")
    except Exception as e:
        logging.error(f"Error: Failed to load date into data warehouse. {e}")

    # Dispose engine
    engine.dispose()