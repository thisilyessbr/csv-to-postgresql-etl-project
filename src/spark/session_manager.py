from pyspark.sql import SparkSession
from typing import Optional
import logging
from config.config import Config

logger = logging.getLogger(__name__)


class SparkSessionManager:
    _instance: Optional[SparkSession] = None

    @classmethod
    def get_spark_session(cls) -> SparkSession:
        if cls._instance is None:
            cls._instance = cls._create_spark_session()
        return cls._instance

    @classmethod
    def _create_spark_session(cls):

        config = Config()
        builder = SparkSession.builder

        for key, value in config.spark_config.items():
            builder = builder.config(key, value)

        # Additional performance configurations
        builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000") \
            .config("spark.sql.parquet.writeLegacyFormat", "false") \
            .config("spark.sql.parquet.enableVectorizedReader", "true") \
            .config("spark.master", "local[6]") \
            .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
            .config("spark.python.worker.faulthandler.enabled", "true")

        try:
            spark = builder.getOrCreate()

            spark.sparkContext.setLogLevel("WARN")

            logger.info(f"Spark session created successfully")
            logger.info(f"Spark version: {spark.version}")
            logger.info(f"Master URL: {spark.sparkContext.master}")

            return spark

        except Exception as e:
            logger.error(f"Failed to create Spark session: {e}")
            raise

    @classmethod
    def stop_spark_session(cls):
        if cls._instance is not None:
            cls._instance.stop()
            cls._instance = None
            logger.info("Spark session stopped")
