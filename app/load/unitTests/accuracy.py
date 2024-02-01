# Import necessary packages
from sqlalchemy.orm import sessionmaker
import logging
import sys
sys.path.append("../")
from create_tables import FactSales, Cars_Dimension, engine

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

        # Verify that the data in the tables is accurate according to business rules
        assert all(row.Price > 0 for row in session.query(FactSales).all())
        assert all(row.Miles >= 0 for row in session.query(FactSales).all())
        assert all(row.Average_mpg >= 0 for row in session.query(FactSales).all())

        # Compare aggregate values with expected results
        total_sales = session.query(FactSales).count()
        total_cars = session.query(Cars_Dimension).count()
        assert total_sales == session.query(FactSales).distinct(FactSales.Car_ID).count()
        assert total_cars == session.query(Cars_Dimension).distinct(Cars_Dimension.Car_ID).count()
        session.close()
        logging.info("Success: Accuracy validated !")
    except Exception as e:
        logging.error(f"Error: Accuracy failed. {e}")