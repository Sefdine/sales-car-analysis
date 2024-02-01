# Import necessary packages
from sqlalchemy import Column, Integer, String, ForeignKey, Date, Float
from sqlalchemy.orm import relationship, declarative_base
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

        Sale_ID = Column(String(255), primary_key=True, nullable=False)
        Price = Column(Float, nullable=False)
        Miles = Column(Integer, nullable=False)
        Average_mpg = Column(Float, nullable=False)
        Adv_ID = Column(Integer, ForeignKey('Date_Dimension.Adv_ID'), nullable=False)
        Car_ID = Column(Integer, ForeignKey('Cars_Dimension.Car_ID'), nullable=False)
        Country_ID = Column(Integer, ForeignKey('Country_Dimension.Country_ID'), nullable=False)

        date_dimension = relationship('Date_Dimension')
        cars_dimension = relationship('Cars_Dimension')
        country_dimension = relationship('Country_Dimension')

    class Cars_Dimension(Base):
        __tablename__ = 'Cars_Dimension'
        __table_args__ = {'extend_existing': True}

        Car_ID = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
        Reg_year = Column(Integer, nullable=False)
        Engine_size = Column(Float, nullable=False)
        Height = Column(Integer, nullable=False)
        Width = Column(Integer, nullable=False)
        Length = Column(Integer, nullable=False)
        Seatings = Column(Integer, nullable=False)
        Doors = Column(Integer, nullable=False)
        Maker_ID = Column(Integer, ForeignKey('Maker_Dimension.Maker_ID'), nullable=False)
        Model_ID = Column(Integer, ForeignKey('Model_Dimension.Model_ID'), nullable=False)
        Color_ID = Column(Integer, ForeignKey('Color_Dimension.Color_ID'), nullable=False)
        Gearbox_ID = Column(Integer, ForeignKey('Gearbox_Dimension.Gearbox_ID'), nullable=False)
        FuelCategory_ID = Column(Integer, ForeignKey('Fuel_Dimension.FuelCategory_ID'), nullable=False)
        CarStatus_ID = Column(Integer, ForeignKey('CarStatus_Dimension.CarStatus_ID'), nullable=False)

        maker_dimension = relationship('Maker_Dimension')
        model_dimension = relationship('Model_Dimension')
        color_dimension = relationship('Color_Dimension')
        gearbox_dimension = relationship('Gearbox_Dimension')
        fuel_dimension = relationship('Fuel_Dimension')
        car_status_dimension = relationship('CarStatus_Dimension')

    class Date_Dimension(Base):
        __tablename__ = 'Date_Dimension'
        __table_args__ = {'extend_existing': True}

        Adv_ID = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
        Adv_year = Column(Integer, nullable=False)
        Adv_month = Column(Integer, nullable=False)
        Adv_date = Column(Date, nullable=False)

    class Country_Dimension(Base):
        __tablename__ = 'Country_Dimension'
        __table_args__ = {'extend_existing': True}

        Country_ID = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
        Country = Column(String(125), index=True, nullable=False)

    class Maker_Dimension(Base):
        __tablename__ = 'Maker_Dimension'
        __table_args__ = {'extend_existing': True}

        Maker_ID = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
        Maker = Column(String(255), index=True, unique=True, nullable=False)

    class Model_Dimension(Base):
        __tablename__ = 'Model_Dimension'
        __table_args__ = {'extend_existing': True}

        Model_ID = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
        Model = Column(String(255), index=True, nullable=False)
        Bodytype = Column(String, nullable=False)

    class Color_Dimension(Base):
        __tablename__ = 'Color_Dimension'
        __table_args__ = {'extend_existing': True}

        Color_ID = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
        Color = Column(String(125), unique=True, nullable=False)

    class Gearbox_Dimension(Base):
        __tablename__ = 'Gearbox_Dimension'
        __table_args__ = {'extend_existing': True}

        Gearbox_ID = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
        Gearbox = Column(String(125), unique=True, nullable=False)

    class Fuel_Dimension(Base):
        __tablename__ = 'Fuel_Dimension'
        __table_args__ = {'extend_existing': True}

        FuelCategory_ID = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
        FuelCategory = Column(String(255), unique=True, nullable=False)

        fuel_type = relationship('Fuel_SubDimension')

    class Fuel_SubDimension(Base):
        __tablename__ = 'Fuel_SubDimension'
        __table_args__ = {'extend_existing': True}

        FuelType_ID = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
        Fuel_type = Column(String(255), unique=True, nullable=False)

        FuelCategory_ID = Column(Integer, ForeignKey('Fuel_Dimension.FuelCategory_ID'), nullable=False)
        fuel_dimension = relationship('Fuel_Dimension', back_populates='fuel_type')

    class CarStatus_Dimension(Base):
        __tablename__ = 'CarStatus_Dimension'
        __table_args__ = {'extend_existing': True}

        CarStatus_ID = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
        CarStatus = Column(String, nullable=False)

    try:
        # Create tables
        Base.metadata.create_all(engine)
        logging.info("Success: All tables created successfully!")
    except Exception as e:
        logging.error(f"Error: Failed to create tables. {str(e)}")
