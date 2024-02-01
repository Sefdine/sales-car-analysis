# Import necessary packages
import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import logging
import sys
sys.path.append("../")
from create_tables import Fuel_SubDimension, engine

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

        # Insert into fuel type
        fuel_subdimension_df = df[['Fuel_category', 'Fuel_type']].drop_duplicates().reset_index(drop=True)
        for i in range(fuel_subdimension_df.shape[0]):
            Fuel_type = fuel_subdimension_df['Fuel_type'][i]
            FuelCategory = fuel_subdimension_df['Fuel_category'][i]

            # Retrieve the fuel categiry id
            conn = engine.connect()
            FuelCategory_ID = conn.execute(text(f"SELECT FuelCategory_ID FROM Fuel_Dimension WHERE FuelCategory = '{FuelCategory}';")).fetchone()[0]
            # Check if exists
            existing_fuel_type = session.query(Fuel_SubDimension).filter_by(Fuel_type=Fuel_type).first()
            conn.close()
            if not existing_fuel_type:
                new_fuel_type = Fuel_SubDimension(Fuel_type=Fuel_type, FuelCategory_ID=FuelCategory_ID)
                session.add(new_fuel_type)
        session.commit()
        logging.info("Success: fuel_subdimension loaded successfully !")
    except Exception as e:
        logging.error(f"Error: Failed to load fuel_subdimension into data warehouse. {e}")

    # Dispose engine
    engine.dispose()