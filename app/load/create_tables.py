# Import necessary packages
from sqlalchemy import Column, Integer, String, ForeignKey, Date, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import logging
from connection import connection
from sys import exit

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Connect to the data warehouse
engine = connection()

if not engine:
    exit()
else:
    # Create tables
    Base = declarative_base()

    class FactSales(Base):
        __tablename__ = 'FactSales'
        __table_args__ = {'extend_existing': True}

        Sale_ID = Column(String(255), primary_key=True)
        Price = Column(Float)
        Miles = Column(Integer)
        Average_mpg = Column(Float)
        Adv_ID = Column(Integer, ForeignKey('Date_Dimension.Adv_ID'))
        Car_ID = Column(Integer, ForeignKey('Cars_Dimension.Car_ID'))
        Country_ID = Column(Integer, ForeignKey('Country_Dimension.Country_ID'))

        date_dimension = relationship('Date_Dimension')
        cars_dimension = relationship('Cars_Dimension')
        country_dimension = relationship('Country_Dimension')

    class Cars_Dimension(Base):
        __tablename__ = 'Cars_Dimension'
        __table_args__ = {'extend_existing': True}

        Car_ID = Column(Integer, primary_key=True, autoincrement=True)
        Reg_year = Column(Integer)
        Engine_size = Column(Float)
        Height = Column(Integer)
        Width = Column(Integer)
        Length = Column(Integer)
        Seatings = Column(Integer)
        Doors = Column(Integer)
        Maker_ID = Column(Integer, ForeignKey('Maker_Dimension.Maker_ID'))
        Model_ID = Column(Integer, ForeignKey('Model_Dimension.Model_ID'))
        Color_ID = Column(Integer, ForeignKey('Color_Dimension.Color_ID'))
        Gearbox_ID = Column(Integer, ForeignKey('Gearbox_Dimension.Gearbox_ID'))
        FuelCategory_ID = Column(Integer, ForeignKey('Fuel_Dimension.FuelCategory_ID'))
        CarStatus_ID = Column(Integer, ForeignKey('CarStatus_Dimension.CarStatus_ID'))

        maker_dimension = relationship('Maker_Dimension')
        model_dimension = relationship('Model_Dimension')
        color_dimension = relationship('Color_Dimension')
        gearbox_dimension = relationship('Gearbox_Dimension')
        fuel_dimension = relationship('Fuel_Dimension')
        car_status_dimension = relationship('CarStatus_Dimension')

    class Date_Dimension(Base):
        __tablename__ = 'Date_Dimension'
        __table_args__ = {'extend_existing': True}

        Adv_ID = Column(Integer, primary_key=True, autoincrement=True)
        Adv_year = Column(Integer)
        Adv_month = Column(Integer)
        Adv_date = Column(Date)

    class Country_Dimension(Base):
        __tablename__ = 'Country_Dimension'
        __table_args__ = {'extend_existing': True}

        Country_ID = Column(Integer, primary_key=True, autoincrement=True)
        Country = Column(String(125), index=True)

    class Maker_Dimension(Base):
        __tablename__ = 'Maker_Dimension'
        __table_args__ = {'extend_existing': True}

        Maker_ID = Column(Integer, primary_key=True, autoincrement=True)
        Maker = Column(String(255), index=True, unique=True)

    class Model_Dimension(Base):
        __tablename__ = 'Model_Dimension'
        __table_args__ = {'extend_existing': True}

        Model_ID = Column(Integer, primary_key=True, autoincrement=True)
        Model = Column(String(255), index=True)
        Bodytype = Column(String)

    class Color_Dimension(Base):
        __tablename__ = 'Color_Dimension'
        __table_args__ = {'extend_existing': True}

        Color_ID = Column(Integer, primary_key=True, autoincrement=True)
        Color = Column(String(125), unique=True)

    class Gearbox_Dimension(Base):
        __tablename__ = 'Gearbox_Dimension'
        __table_args__ = {'extend_existing': True}

        Gearbox_ID = Column(Integer, primary_key=True, autoincrement=True)
        Gearbox = Column(String(125), unique=True)

    class Fuel_Dimension(Base):
        __tablename__ = 'Fuel_Dimension'
        __table_args__ = {'extend_existing': True}

        FuelCategory_ID = Column(Integer, primary_key=True, autoincrement=True)
        FuelCategory = Column(String(255), unique=True)

        fuel_type = relationship('Fuel_SubDimension')

    class Fuel_SubDimension(Base):
        __tablename__ = 'Fuel_SubDimension'
        __table_args__ = {'extend_existing': True}

        FuelType_ID = Column(Integer, primary_key=True, autoincrement=True)
        Fuel_type = Column(String(255), unique=True)

        # Define a relationship to Fuel_Dimension
        FuelCategory_ID = Column(Integer, ForeignKey('Fuel_Dimension.FuelCategory_ID'))
        fuel_dimension = relationship('Fuel_Dimension', back_populates='fuel_type')

    class CarStatus_Dimension(Base):
        __tablename__ = 'CarStatus_Dimension'
        __table_args__ = {'extend_existing': True}

        CarStatus_ID = Column(Integer, primary_key=True, autoincrement=True)
        CarStatus = Column(String)

    try:
        # Create tables
        Base.metadata.create_all(engine)
        logging.info("Success: All tables created succefully !")
    except:
        logging.error("Error: Failed to create tables")