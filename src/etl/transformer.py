import pandas as pd
import numpy as np
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class DataTransformer:
    def __init__(self):
        self.column_mapping = {
            'VendorID': 'vendor_id',
            'tpep_pickup_datetime': 'pickup_datetime',
            'tpep_dropoff_datetime': 'dropoff_datetime',
            'passenger_count': 'passenger_count',
            'trip_distance': 'trip_distance',
            'pickup_longitude': 'pickup_longitude',
            'pickup_latitude': 'pickup_latitude',
            'RatecodeID': 'rate_code_id',
            'store_and_fwd_flag': 'store_and_fwd_flag',
            'dropoff_longitude': 'dropoff_longitude',
            'dropoff_latitude': 'dropoff_latitude',
            'payment_type': 'payment_type',
            'fare_amount': 'fare_amount',
            'extra': 'extra',
            'mta_tax': 'mta_tax',
            'tip_amount': 'tip_amount',
            'tolls_amount': 'tolls_amount',
            'improvement_surcharge': 'improvement_surcharge',
            'total_amount': 'total_amount'
        }

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info(f"Starting transformation of {len(df)} rows")
        # Map columns
        df = self._map_columns(df)

        logger.info(f"Transformation completed. {len(df)} rows remaining")

        return df

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:

        available_columns = {col: self.column_mapping[col]
                             for col in df.columns
                             if col in self.column_mapping}

        df = df[list(available_columns.keys())].rename(columns=available_columns)
        logger.info(f"Mapped {len(available_columns)} columns")
        return df

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:

        return df


