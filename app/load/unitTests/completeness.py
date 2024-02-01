# Import necessary packages
from sqlalchemy.orm import sessionmaker
import logging
import sys
sys.path.append("../")
from create_tables import FactSales, Cars_Dimension, Date_Dimension, Country_Dimension, engine

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if not engine:
    sys.exit()
else:
    try:
        # Create session
        Session = sessionmaker(bind=engine)
        session = Session()

        # Check if required fields are not null
        assert session.query(FactSales).filter(FactSales.Sale_ID.is_(None)).count() == 0
        assert session.query(Cars_Dimension).filter(Cars_Dimension.Car_ID.is_(None)).count() == 0
        assert session.query(Date_Dimension).filter(Date_Dimension.Adv_ID.is_(None)).count() == 0
        assert session.query(Country_Dimension).filter(Country_Dimension.Country_ID.is_(None)).count() == 0

        # Ensure primary key constraints are unique and not null
        assert session.query(FactSales).count() == session.query(FactSales.Sale_ID).distinct().count()
        assert session.query(Cars_Dimension).count() == session.query(Cars_Dimension.Car_ID).distinct().count()
        assert session.query(Date_Dimension).count() == session.query(Date_Dimension.Adv_ID).distinct().count()
        assert session.query(Country_Dimension).count() == session.query(Country_Dimension.Country_ID).distinct().count()

        # Verify foreign key constraints are satisfied
        assert session.query(FactSales).filter(FactSales.Adv_ID.is_(None) | FactSales.Car_ID.is_(None) | FactSales.Country_ID.is_(None)).count() == 0
        assert session.query(Cars_Dimension).filter(Cars_Dimension.Maker_ID.is_(None) | Cars_Dimension.Model_ID.is_(None) | Cars_Dimension.Color_ID.is_(None) | Cars_Dimension.Gearbox_ID.is_(None) | Cars_Dimension.FuelCategory_ID.is_(None) | Cars_Dimension.CarStatus_ID.is_(None)).count() == 0

        # Check if all dimensions and facts have been populated
        assert session.query(FactSales).count() > 0
        assert session.query(Cars_Dimension).count() > 0
        assert session.query(Date_Dimension).count() > 0
        assert session.query(Country_Dimension).count() > 0
        session.close()
        logging.info("Success: Completeness validated !")
    except Exception as e:
        logging.error(f"Error: Completeness failed. {e}")