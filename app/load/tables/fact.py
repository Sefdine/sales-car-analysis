# Import necessary packages
import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import logging
import sys
from datetime import datetime
sys.path.append("../")
from create_tables import FactSales, engine

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

        # Insert into fact
        err_count = 0
        for i in range(df.shape[0]):
            Sale_ID = df['ID'][i]
            Price = float(df['Price'][i])
            Miles = int(df['Miles'][i])
            Average_mpg = df['Average_mpg'][i]
            Adv_date = datetime.strptime(df['Adv_date'][0], "%Y-%m-%d").date()
            Country = df['country'][i]
            Reg_year = int(df['Reg_year'][i])
            Engine_size = float(df['Engine_size'][i])
            Height = int(df['Height'][i])
            Width = int(df['Width'][i])
            Length = int(df['Length'][i])
            Seatings = int(df['Seatings'][i])
            Doors = int(df['Doors'][i])
            Maker = df['Maker'][i]
            Model = df['Model'][i]
            Bodytype = df['Bodytype'][i]
            Color = df['Color'][i]
            Gearbox = df['Gearbox'][i]
            Fuel_category = df['Fuel_category'][i]
            CarStatus = df['Stat'][i]


            try:
                # Retrieve the foreign keys
                Maker_ID = engine.connect().execute(text(f"SELECT Maker_ID FROM Maker_Dimension WHERE Maker = '{Maker}';")).fetchone()[0]
                Model_ID = engine.connect().execute(text(f"SELECT Model_ID FROM Model_Dimension WHERE Model = '{Model}' AND Bodytype = '{Bodytype}';")).fetchone()[0]
                Color_ID = engine.connect().execute(text(f"SELECT Color_ID FROM Color_Dimension WHERE Color = '{Color}';")).fetchone()[0]
                Gearbox_ID = engine.connect().execute(text(f"SELECT Gearbox_ID FROM Gearbox_Dimension WHERE Gearbox = '{Gearbox}';")).fetchone()[0]
                FuelCategory_ID = engine.connect().execute(text(f"SELECT FuelCategory_ID FROM Fuel_Dimension WHERE FuelCategory = '{Fuel_category}';")).fetchone()[0]
                CarStatus_ID = engine.connect().execute(text(f"SELECT CarStatus_ID FROM CarStatus_Dimension WHERE CarStatus = '{CarStatus}';")).fetchone()[0]

                Car_ID = engine.connect().execute(text(f'''
                                SELECT Car_ID FROM Cars_Dimension 
                                WHERE Reg_year = '{Reg_year}'
                                AND Engine_size = '{Engine_size}'
                                AND Height = '{Height}'
                                AND Width = '{Width}'
                                AND Length = '{Length}'
                                AND Seatings = '{Seatings}'
                                AND Doors = '{Doors}'
                                AND Maker_ID = '{Maker_ID}'
                                AND Model_ID = '{Model_ID}'
                                AND Color_ID = '{Color_ID}'
                                AND Gearbox_ID = '{Gearbox_ID}'
                                AND FuelCategory_ID = '{FuelCategory_ID}'
                                AND CarStatus_ID = '{CarStatus_ID}';
                        ''')).fetchone()[0]
                Country_ID = engine.connect().execute(text(f"SELECT Country_ID FROM Country_Dimension WHERE Country = '{Country}';")).fetchone()[0]
                Adv_ID = engine.connect().execute(text(f"SELECT Adv_ID FROM Date_Dimension WHERE Adv_date = '{Adv_date}';")).fetchone()[0]

                # Check if exists
                existing_fact = (
                    session.query(FactSales)
                    .filter_by(
                        Price=Price,
                        Miles=Miles,
                        Average_mpg=Average_mpg,
                        Adv_ID=Adv_ID,
                        Car_ID=Car_ID,
                        Country_ID=Country_ID
                    ).first()
                )
                if not existing_fact:
                    new_fact = FactSales(
                        Sale_ID=Sale_ID,
                        Price=Price,
                        Miles=Miles,
                        Average_mpg=Average_mpg,
                        Adv_ID=Adv_ID,
                        Car_ID=Car_ID,
                        Country_ID=Country_ID
                    )
                    session.add(new_fact)
                    session.commit()
                    print(f"Row {i+1} / {df.shape[0]} inserted successfully !")
                else:
                    print(f"Row {i+1} / {df.shape[0]} already exists")
            except Exception as e:
                session.rollback()
                print(f"Error: Failed to insert into factSales {str(e)}")
                if err_count > 5:
                    break
                err_count += 1

        logging.info("Success: cars_dimension loaded successfully !")
    except Exception as e:
        logging.error(f"Error: Failed to load cars_dimension into data warehouse. {e}")

    # Dispose engine
    engine.dispose()