# Import necessary packages
import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import logging
import sys
from datetime import datetime
sys.path.append("../")
from create_tables import Cars_Dimension, engine

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

        # Insert into Cars_Dimension
        cars_dim_columns = [
            'Reg_year', 'Engine_size', 'Height', 'Width', 'Length', 'Seatings', 'Doors', 'Maker', 'Model', 'Bodytype', 'Color', 'Gearbox', 'Fuel_category', 'Stat'
        ]
        cars_dim_df = df[cars_dim_columns].drop_duplicates().reset_index(drop=True)

        err_count = 0

        for i in range(500):
            Reg_year = int(cars_dim_df['Reg_year'][i])
            Engine_size = float(cars_dim_df['Engine_size'][i])
            Height = int(cars_dim_df['Height'][i])
            Width = int(cars_dim_df['Width'][i])
            Length = int(cars_dim_df['Length'][i])
            Seatings = int(cars_dim_df['Seatings'][i])
            Doors = int(cars_dim_df['Doors'][i])
            Maker = cars_dim_df['Maker'][i]
            Model = cars_dim_df['Model'][i]
            Bodytype = cars_dim_df['Bodytype'][i]
            Color = cars_dim_df['Color'][i]
            Gearbox = cars_dim_df['Gearbox'][i]
            Fuel_category = cars_dim_df['Fuel_category'][i]
            CarStatus = cars_dim_df['Stat'][i]


            try:
                # Retrieve the foreign keys
                Maker_ID = engine.connect().execute(text(f"SELECT Maker_ID FROM Maker_Dimension WHERE Maker = '{Maker}';")).fetchone()[0]
                Model_ID = engine.connect().execute(text(f"SELECT Model_ID FROM Model_Dimension WHERE Model = '{Model}' AND Bodytype = '{Bodytype}';")).fetchone()[0]
                Color_ID = engine.connect().execute(text(f"SELECT Color_ID FROM Color_Dimension WHERE Color = '{Color}';")).fetchone()[0]
                Gearbox_ID = engine.connect().execute(text(f"SELECT Gearbox_ID FROM Gearbox_Dimension WHERE Gearbox = '{Gearbox}';")).fetchone()[0]
                FuelCategory_ID = engine.connect().execute(text(f"SELECT FuelCategory_ID FROM Fuel_Dimension WHERE FuelCategory = '{Fuel_category}';")).fetchone()[0]
                CarStatus_ID = engine.connect().execute(text(f"SELECT CarStatus_ID FROM CarStatus_Dimension WHERE CarStatus = '{CarStatus}';")).fetchone()[0]
            
                # Check if exist
                existing_cars_dim = (
                    session.query(Cars_Dimension)
                    .filter_by(
                        Reg_year=Reg_year,
                        Engine_size=Engine_size,
                        Height=Height,
                        Width=Width,
                        Length=Length,
                        Seatings=Seatings,
                        Doors=Doors,
                        Maker_ID=Maker_ID,
                        Model_ID=Model_ID,
                        Color_ID=Color_ID,
                        Gearbox_ID=Gearbox_ID,
                        FuelCategory_ID=FuelCategory_ID,
                        CarStatus_ID=CarStatus_ID
                    ).first()
                )
                if not existing_cars_dim:
                    new_car_dim = Cars_Dimension(
                        Reg_year=Reg_year,
                        Engine_size=Engine_size,
                        Height=Height,
                        Width=Width,
                        Length=Length,
                        Seatings=Seatings,
                        Doors=Doors,
                        Maker_ID=Maker_ID,
                        Model_ID=Model_ID,
                        Color_ID=Color_ID,
                        Gearbox_ID=Gearbox_ID,
                        FuelCategory_ID=FuelCategory_ID,
                        CarStatus_ID=CarStatus_ID
                    )
                    session.add(new_car_dim)
                    session.commit()
                    logging.info(f"Row {i+1} / {cars_dim_df.shape[0]} inserted successfully")
                else:
                    logging.info(f"Row {i+1} / {cars_dim_df.shape[0]} already exists")
                
            except Exception as e:
                logging.error(f"Error N° {i+1} {str(e)}")
                session.rollback()
                if err_count > 5:
                    break
                err_count += 1

        logging.info("Success: cars_dimension loaded successfully !")
    except Exception as e:
        logging.error(f"Error: Failed to load cars_dimension into data warehouse. {e}")

    # Dispose engine
    engine.dispose()