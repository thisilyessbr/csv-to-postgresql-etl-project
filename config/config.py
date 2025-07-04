import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class Config:
    """Configuration class for database and application settings"""

    # Database configuration
    POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
    POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
    POSTGRES_DB = os.getenv('POSTGRES_DB', 'nyc_taxi_db')
    POSTGRES_USER = os.getenv('POSTGRES_USER', 'etl_user_ilyes')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'Ilyes123@')

    # NEW: Spark configuration
    SPARK_MASTER_URL = os.getenv('SPARK_MASTER_URL','spark://localhost:7077')
    SPARK_APP_NAME = os.getenv('SPARK_APP_NAME', 'NYCTaxiETL')
    SPARK_EXECUTOR_MEMORY = os.getenv('SPARK_EXECUTOR_MEMORY', '4g')
    SPARK_DRIVER_MEMORY = os.getenv('SPARK_DRIVER_MEMORY', '2g')

    # NEW: Multiple months support
    DATA_YEARS = [2023]
    DATA_MONTHS = list(range(1, 2))  # All 12 months

    # Data source configuration
    PARQUET_URL = os.getenv('CSV_URL',
                            'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet')

    PARQUET_URL_TEMPLATE = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet'
    # File paths
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data')
    RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
    PROCESSED_DATA_DIR = os.path.join(DATA_DIR, 'raw')
    SPARK_DATA_DIR = os.path.join(DATA_DIR, 'spark')
    LOGS_DIR = os.path.join(DATA_DIR, 'logs')

    # ETL Configuration
    BATCH_SIZE = 10000
    MAX_RETRIES = 3

    @property
    def database_url(self):
        """Generate database URL for SQLAlchemy"""
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    @property
    def spark_config(self):
        return {
            "spark.master": self.SPARK_MASTER_URL,
            "spark.app.name": self.SPARK_APP_NAME,
            "spark.executor.memory": self.SPARK_EXECUTOR_MEMORY,
            "spark.driver.memory": self.SPARK_DRIVER_MEMORY,
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
        }

    @classmethod
    def validate_config(cls):
        """Validate that all required configuration is present"""
        required_vars = ['POSTGRES_USER', 'POSTGRES_DB', 'POSTGRES_PASSWORD']
        missing_vars = [var for var in required_vars if not getattr(cls, var)]

        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")

        return True
