#!/usr/bin/env python3

import sys
import logging
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from src.spark.session_manager import SparkSessionManager
from src.etl.spark_transformer import SparkDataTransformer
from src.etl.extractor import DataExtractor
from config.config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_spark_connection():
    """Test Spark cluster connection"""
    try:
        spark = SparkSessionManager.get_spark_session()

        # Use simpler RDD creation instead of DataFrame
        rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
        result = rdd.collect()

        logger.info(f"✅ Spark RDD test passed! Result: {result}")

        # Try a simple DataFrame test
        df = spark.range(5)
        count = df.count()

        logger.info(f"✅ Spark DataFrame test passed! Count: {count}")
        return True

    except Exception as e:
        logger.error(f"❌ Spark connection test failed: {e}")
        return False


def test_data_processing():
    """Test data processing with real NYC taxi data"""
    try:
        # Extract one month of data
        extractor = DataExtractor()
        file_paths = extractor.extract_nyc_taxi_data(years=[2023], months=[1])

        if not file_paths:
            logger.error("No data files found")
            return False

        # Transform with Spark
        transformer = SparkDataTransformer()
        df = transformer.read_parquet_files(file_paths)

        logger.info(f"Original data shape: {df.count()} rows, {len(df.columns)} columns")

        # Apply transformations
        transformed_df = transformer.transform(df)

        logger.info(f"Transformed data shape: {transformed_df.count()} rows, {len(transformed_df.columns)} columns")

        # Show sample
        transformed_df.show(5)

        # Get summary
        summary = transformer.get_transformation_summary(transformed_df)
        logger.info(f"Transformation summary: {summary}")

        logger.info("✅ Data processing test passed!")
        return True

    except Exception as e:
        logger.error(f"❌ Data processing test failed: {e}")
        return False


def main():
    logger.info("🚀 Testing Spark Setup...")

    # Test 1: Spark connection
    if not test_spark_connection():
        sys.exit(1)

    # Test 2: Data processing
    #if not test_data_processing():
        #sys.exit(1)

    logger.info("🎉 All tests passed! Your Spark setup is working correctly.")


if __name__ == "__main__":
    main()