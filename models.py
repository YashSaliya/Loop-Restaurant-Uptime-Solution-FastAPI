from sqlalchemy import Column, Integer, PrimaryKeyConstraint, String, Boolean, DateTime
from sqlalchemy.orm import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os 
load_dotenv()
db_connection_url = os.getenv('DB_CONNECTION_URL')

if db_connection_url is None:
    raise ValueError("DB_CONNECTION_URL environment variable is not set")


engine = create_engine(db_connection_url)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

class StoreStatus(Base):
    __tablename__ = 'store_status'
    store_id = Column(String(255), primary_key=True)
    status = Column(String(255))
    timestamp_utc = Column(DateTime, primary_key=True)

class StoreWorkingHours(Base):
    __tablename__ = 'store_working_hours'
    store_id = Column(String(255), primary_key=True)
    day = Column(Integer, primary_key=True)
    start_time_local = Column(DateTime)
    end_time_local = Column(DateTime)

class StoreTime(Base):
    __tablename__ = 'store_timezone'
    store_id = Column(String(255), primary_key=True)
    timezone_str = Column(String(255))


Base.metadata.create_all(engine)
