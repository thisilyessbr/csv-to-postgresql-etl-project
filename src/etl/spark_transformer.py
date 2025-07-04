from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, regexp_replace, unix_timestamp,
    year, month, dayofmonth, hour, minute,
    abs as spark_abs, round as spark_round
)
from pyspark.sql.types import DoubleType, IntegerType
import logging
from typing import Dict, Any
from src.spark.session_manager import SparkSessionManager

logger = logging.getLogger(__name__)


class SparkDataTransformer:

    def __init__(self):
        self.spark = SparkSessionManager.get_spark_session()
        self.column_mapping = {
            'VendorID': 'vendor_id',
            'tpep_pickup_datetime': 'pickup_datetime',
            'tpep_dropoff_datetime': 'dropoff_datetime',
            'passenger_count': 'passenger_count',
            'trip_distance': 'trip_distance',
            'RatecodeID': 'rate_code_id',
            'store_and_fwd_flag': 'store_and_fwd_flag',
            'payment_type': 'payment_type',
            'fare_amount': 'fare_amount',
            'extra': 'extra',
            'mta_tax': 'mta_tax',
            'tip_amount': 'tip_amount',
            'tolls_amount': 'tolls_amount',
            'improvement_surcharge': 'improvement_surcharge',
            'total_amount': 'total_amount',
            # Handle location IDs (newer format)
            'PULocationID': 'pickup_location_id',
            'DOLocationID': 'dropoff_location_id',
            # Handle schema variations
            'congestion_surcharge': 'congestion_surcharge',
            'airport_fee': 'airport_fee',
            'Airport_fee': 'airport_fee'  # Handle case variation

        }

    def read_parquet_files(self, file_paths: list) -> DataFrame:
        try:
            # Read files individually to handle schema differences
            dataframes = []

            for file_path in file_paths:
                df = self.spark.read.parquet(file_path)
                logger.info(f"Read {df.count():,} records from {file_path}")
                dataframes.append(df)

            # Union all dataframes (Spark will handle schema alignment)
            if len(dataframes) == 1:
                combined_df = dataframes[0]
            else:
                combined_df = dataframes[0]
                for df in dataframes[1:]:
                    combined_df = combined_df.unionByName(df, allowMissingColumns=True)

            logger.info(f"Successfully combined {combined_df.count():,} total records from {len(file_paths)} file(s)")
            return combined_df

        except Exception as e:
            logger.error(f"Failed to read parquet files: {e}")
            raise

    def transform(self, df: DataFrame) -> DataFrame:

        logger.info(f"Starting transformation of {df.count():,} records")
        # Step 1: Column mapping and selection
        df = self._map_and_select_columns(df)

        # Step 2: Data cleaning
        df = self._clean_data(df)

        # Step 3: Feature engineering
        df = self._add_derived_features(df)

        # Step 4: Final validation
        df = self._validate_and_filter(df)

        final_count = df.count()

        logger.info(f"Transformation completed. {final_count:,} records remaining")

        return df

    def _map_and_select_columns(self, df: DataFrame):

        available_columns = {}

        for source_col in df.columns:
            if source_col in self.column_mapping:
                target_col = self.column_mapping[source_col]
                available_columns[target_col] = col(source_col).alias(target_col)

        if available_columns:
            df = df.select(*available_columns.values())  # Use .values() to get the Column objects
            logger.info(f"Mapped {len(available_columns)} columns")
        else:
            logger.warning("No columns were mapped!")

        return df

    def _clean_data(self, df: DataFrame) -> DataFrame:
        logger.info("Applying data cleaning transformations")

        # Handle null values
        df = df.fillna({
            'passenger_count': 1,
            'rate_code_id': 1,
            'store_and_fwd_flag': 'N',
            'congestion_surcharge': 0.0,
            'airport_fee': 0.0
        })

        # Clean store_and_fwd_flag
        df = df.withColumn('store_and_fwd_flag',
                           when(col('store_and_fwd_flag').isin(['Y', 'N']), col('store_and_fwd_flag')).otherwise('N'))

        # Ensure proper data types
        numeric_columns = [
            'vendor_id', 'passenger_count', 'trip_distance',
            'rate_code_id', 'payment_type', 'fare_amount',
            'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
            'improvement_surcharge', 'total_amount',
            'congestion_surcharge', 'airport_fee'
        ]

        for column in numeric_columns:
            if column in df.columns:
                df = df.withColumn(column, col(column).cast(DoubleType()))

        # Cast ID columns to IntegerType for consistency
        id_columns = ['vendor_id', 'rate_code_id', 'payment_type', 'pickup_location_id', 'dropoff_location_id']
        for column in id_columns:
            if column in df.columns:
                df = df.withColumn(column, col(column).cast(IntegerType()))

        return df

    def _add_derived_features(self, df: DataFrame) -> DataFrame:

        logger.info("Adding derived features")

        df = df.withColumn('trip_duration_minutes',
                           (unix_timestamp(col('dropoff_datetime')) - unix_timestamp(col('pickup_datetime'))) / 60)

        df = df.withColumn('pickup_year', year(col('pickup_datetime'))) \
            .withColumn('pickup_month', month(col('pickup_datetime'))) \
            .withColumn('pickup_day', dayofmonth(col('pickup_datetime'))) \
            .withColumn('pickup_hour', hour(col('pickup_datetime'))) \
            .withColumn('dropoff_hour', hour(col('dropoff_datetime')))

        # Tip percentage
        df = df.withColumn(
            'tip_percentage',
            when(col('fare_amount') > 0,
                 spark_round((col('tip_amount') / col('fare_amount')) * 100, 2))
            .otherwise(0)
        )

        # Add average speed calculation (THIS WAS MISSING!)
        df = df.withColumn(
            'avg_speed_mph',
            when((col('trip_duration_minutes') > 0) & (col('trip_distance') > 0),
                 spark_round((col('trip_distance') / (col('trip_duration_minutes') / 60)), 2))
            .otherwise(0)
        )

        return df

    def _validate_and_filter(self, df: DataFrame) -> DataFrame:
        """Apply business rules and data quality filters"""
        logger.info("Applying validation and filtering")

        initial_count = df.count()

        df = df.filter(
            # Basic validations
            (col('pickup_datetime').isNotNull()) &
            (col('dropoff_datetime').isNotNull()) &
            (col('pickup_datetime') < col('dropoff_datetime')) &

            # Reasonable trip distance (0 to 100 miles)
            (col('trip_distance') >= 0) &
            (col('trip_distance') <= 100) &

            # Reasonable fare amount
            (col('total_amount') >= 0) &
            (col('total_amount') <= 1000) &

            # Reasonable passenger count
            (col('passenger_count') > 0) &
            (col('passenger_count') <= 8) &

            # Reasonable trip duration (1 minute to 24 hours)
            (col('trip_duration_minutes') >= 1) &
            (col('trip_duration_minutes') <= 1440) &

            # Reasonable speed (0 to 100 mph)
            (col('avg_speed_mph') >= 0) &
            (col('avg_speed_mph') <= 100)
        )

        final_count = df.count()
        filtered_count = initial_count - final_count

        logger.info(f"Filtered out {filtered_count:,} invalid records ({filtered_count / initial_count * 100:.1f}%)")

        return df
