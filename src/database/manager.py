import psycopg2
from sqlalchemy import create_engine, text, MetaData
from sqlalchemy.orm import sessionmaker
import pandas as pd
import logging
from typing import Dict, Any, Optional, List
from contextlib import contextmanager
from config.config import Config
from src.database.models import Base, NYCTaxiTrip, ETLLog

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self):
        self.config = Config()
        self.engine = None
        self.SessionLocal = None
        self._connection_pool = None

    def connect(self) -> bool:
        """Create database connection with connection pooling"""
        try:
            self.engine = create_engine(
                self.config.database_url,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,  # Validate connections before use
                pool_recycle=3600,  # Recycle connections every hour
                echo=False  # Set to True for SQL debugging
            )

            # Create session factory
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine
            )

            logger.info("Database connection established successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False

    def test_connection(self) -> bool:
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test"))
                test_result = result.fetchone()
                if test_result and test_result[0] == 1:
                    logger.info("Database connection test successful")
                    return True
                else:
                    logger.error("Database connection test failed - unexpected result")
                    return False

        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False

    def create_tables(self) -> bool:
        """Create all tables defined in models"""
        try:
            # Create all tables from models
            Base.metadata.create_all(bind=self.engine)
            logger.info("All tables created successfully from models")

            # Create additional indexes for performance
            self._create_indexes()

            return True

        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            return False

    def _create_indexes(self):
        """Create additional performance indexes"""
        indexes_sql = """
         -- Performance indexes for nyc_taxi_trips
         CREATE INDEX IF NOT EXISTS idx_pickup_datetime ON nyc_taxi_trips(pickup_datetime);
         CREATE INDEX IF NOT EXISTS idx_dropoff_datetime ON nyc_taxi_trips(dropoff_datetime);
         CREATE INDEX IF NOT EXISTS idx_trip_distance ON nyc_taxi_trips(trip_distance);
         CREATE INDEX IF NOT EXISTS idx_total_amount ON nyc_taxi_trips(total_amount);
         CREATE INDEX IF NOT EXISTS idx_vendor_payment ON nyc_taxi_trips(vendor_id, payment_type);
         CREATE INDEX IF NOT EXISTS idx_pickup_location ON nyc_taxi_trips(pickup_latitude, pickup_longitude);
         CREATE INDEX IF NOT EXISTS idx_dropoff_location ON nyc_taxi_trips(dropoff_latitude, dropoff_longitude);

         -- Indexes for etl_log
         CREATE INDEX IF NOT EXISTS idx_etl_log_pipeline ON etl_log(pipeline_name);
         CREATE INDEX IF NOT EXISTS idx_etl_log_status ON etl_log(status);
         CREATE INDEX IF NOT EXISTS idx_etl_log_start_time ON etl_log(start_time);
         """

        try:
            with self.engine.connect() as conn:
                conn.execute(text(indexes_sql))
                conn.commit()
                logger.info("Performance indexes created successfully")
        except Exception as e:
            logger.warning(f"Some indexes may already exist: {e}")

    @contextmanager
    def get_session(self):
        """Context manager for database sessions"""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Session rolled back due to error: {e}")
            raise
        finally:
            session.close()

    def insert_dataframe(self, df: pd.DataFrame, table_name: str = 'nyc_taxi_trips',
                         batch_size: int = 10000, if_exists: str = 'append') -> bool:
        try:
            total_rows = len(df)
            logger.info(f"Starting to insert {total_rows} rows into {table_name}")

            # Insert in batches for better performance
            for i in range(0, total_rows, batch_size):
                batch_df = df.iloc[i:i + batch_size]

                batch_df.to_sql(
                    table_name,
                    self.engine,
                    if_exists=if_exists,
                    index=False,
                    method='multi'  # Use multi-insert for better performance
                )

                logger.info(f"Inserted batch {i // batch_size + 1}: {len(batch_df)} rows")

                # Only append for subsequent batches
                if_exists = 'append'

            logger.info(f"Successfully inserted all {total_rows} rows into {table_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to insert DataFrame into {table_name}: {e}")
            return False

    def execute_command(self, command: str, params: Dict = None) -> bool:
        try:
            with self.engine.connect() as conn:
                conn.execute(text(command), params or {})
                conn.commit()
                logger.info("Command executed successfully")
                return True

        except Exception as e:
            logger.error(f"Failed to execute command: {e}")
            return False

    def execute_query(self, query: str, params: Dict = None) -> List[Dict]:
        """Execute a SELECT query and return results as list of dictionaries"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                columns = result.keys()
                rows = result.fetchall()

                return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            raise

    def close(self):
        """Close database connections and cleanup"""
        try:
            if self.engine:
                self.engine.dispose()
                logger.info("Database connections closed successfully")
        except Exception as e:
            logger.error(f"Error closing database connections: {e}")

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()

    def get_data_quality_metrics(self, table_name: str = 'nyc_taxi_trips') -> Optional[Dict[str, Any]]:
        """Get data quality metrics for the table"""
        try:
            quality_query = f"""
             SELECT 
                 COUNT(*) as total_rows,
                 COUNT(*) - COUNT(pickup_datetime) as missing_pickup_datetime,
                 COUNT(*) - COUNT(dropoff_datetime) as missing_dropoff_datetime,
                 COUNT(*) - COUNT(trip_distance) as missing_trip_distance,
                 COUNT(*) - COUNT(total_amount) as missing_total_amount,
                 COUNT(CASE WHEN trip_distance < 0 THEN 1 END) as negative_distance,
                 COUNT(CASE WHEN total_amount < 0 THEN 1 END) as negative_amount,
                 COUNT(CASE WHEN pickup_datetime >= dropoff_datetime THEN 1 END) as invalid_trip_duration,
                 COUNT(CASE WHEN passenger_count <= 0 OR passenger_count > 8 THEN 1 END) as invalid_passenger_count
             FROM {table_name}
             """

            result = self.execute_query(quality_query)

            if result:
                metrics = result[0]
                logger.info(f"Retrieved data quality metrics for table {table_name}")
                return metrics
            else:
                return None

        except Exception as e:
            logger.error(f"Failed to get data quality metrics for {table_name}: {e}")
            return None

    def get_table_stats(self, table_name: str = 'nyc_taxi_trips') -> Optional[Dict[str, Any]]:
        """Get comprehensive statistics about a table"""
        try:
            stats_query = f"""
               SELECT 
                   COUNT(*) as total_records,
                   MIN(pickup_datetime) as earliest_trip,
                   MAX(pickup_datetime) as latest_trip,
                   AVG(trip_distance) as avg_distance,
                   AVG(total_amount) as avg_fare,
                   MIN(total_amount) as min_fare,
                   MAX(total_amount) as max_fare,
                   COUNT(DISTINCT vendor_id) as unique_vendors,
                   COUNT(DISTINCT payment_type) as unique_payment_types
               FROM {table_name}
               WHERE pickup_datetime IS NOT NULL
               """

            result = self.execute_query(stats_query)

            if result:
                stats = result[0]
                # Convert decimals to float for JSON serialization
                for key, value in stats.items():
                    if hasattr(value, '__float__'):
                        stats[key] = float(value)

                logger.info(f"Retrieved stats for table {table_name}")
                return stats
            else:
                return None

        except Exception as e:
            logger.error(f"Failed to get table stats for {table_name}: {e}")
            return None
