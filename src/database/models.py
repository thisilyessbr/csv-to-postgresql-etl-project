from datetime import datetime

from sqlalchemy import Column, DECIMAL, Integer, String, DateTime, Text, CheckConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class NYCTaxiTrip(Base):
    __tablename__ = 'nyc_taxi_trips'

    id = Column(Integer, primary_key=True)
    vendor_id = Column(Integer)
    pickup_datetime = Column(DateTime, nullable=False)
    dropoff_datetime = Column(DateTime, nullable=False)
    passenger_count = Column(Integer, default=1)
    trip_distance = Column(DECIMAL(10, 2))
    rate_code_id = Column(Integer)
    store_and_fwd_flag = Column(String(1))
    payment_type = Column(Integer)
    fare_amount = Column(DECIMAL(10, 2))
    extra = Column(DECIMAL(10, 2))
    mta_tax = Column(DECIMAL(10, 2))
    tip_amount = Column(DECIMAL(10, 2))
    tolls_amount = Column(DECIMAL(10, 2))
    improvement_surcharge = Column(DECIMAL(10, 2))
    total_amount = Column(DECIMAL(10, 2))
    pickup_location_id = Column(Integer)
    dropoff_location_id = Column(Integer)
    congestion_surcharge = Column(DECIMAL(10, 2))
    airport_fee = Column(DECIMAL(10, 2))

    trip_duration_minutes = Column(DECIMAL(10, 2))
    pickup_year = Column(Integer)
    pickup_month = Column(Integer)
    pickup_day = Column(Integer)
    pickup_hour = Column(Integer)
    dropoff_hour = Column(Integer)
    tip_percentage = Column(DECIMAL(5, 2))
    avg_speed_mph = Column(DECIMAL(10, 2))

    created_at = Column(DateTime, default=datetime.now)


class ETLLog(Base):
    __tablename__ = 'etl_log'

    id = Column(Integer, primary_key=True)
    pipeline_name = Column(String(100), nullable=False)
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime)
    status = Column(String(20), nullable=False)  # 'RUNNING', 'SUCCESS', 'FAILED'
    records_processed = Column(Integer)
    error_message = Column(Text)
    created_at = Column(DateTime, default=datetime.now)
