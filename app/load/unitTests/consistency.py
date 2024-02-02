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

        # Verify relationships between tables
        assert session.query(FactSales).join(Date_Dimension).count() == session.query(FactSales).count()
        assert session.query(FactSales).join(Cars_Dimension).count() == session.query(FactSales).count()
        assert session.query(FactSales).join(Country_Dimension).count() == session.query(FactSales).count()

        # Check if data types match the specified schema
        assert all(isinstance(row.Price, float) for row in session.query(FactSales).all())
        assert all(isinstance(row.Miles, int) for row in session.query(FactSales).all())
        assert all(isinstance(row.Average_mpg, float) for row in session.query(FactSales).all())
        session.close()
        logging.info("Success: Accuracy validated !")
    except Exception as e:
        logging.error(f"Error: Accuracy failed. {e}")