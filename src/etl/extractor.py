import pandas as pd
import requests
import os
from typing import Optional
import logging
from config.config import Config

logger = logging.getLogger(__name__)


class DataExtractor:

    def __init__(self):
        self.config = Config()
        os.makedirs(self.config.RAW_DATA_DIR, exist_ok=True)

    def extract_from_url(self, url: str, filename: Optional[str] = None) -> str:

        try:
            if filename is None:
                filename = url.split('/')[-1]

            filepath = os.path.join(self.config.RAW_DATA_DIR, filename)

            if not os.path.exists(filepath):
                logger.info(f"Downloading data from {url}")
                response = requests.get(url, stream=True)
                response.raise_for_status()

                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

                logger.info(f"Data downloaded to {filepath}")
            else:
                logger.info(f"Using existing file: {filepath}")

            return filepath

        except Exception as e:
            logger.error(f"Failed to extract data from {url}: {e}")
            raise

    def read_parquet(self, filepath: str) -> pd.DataFrame:
        """Read parquet file into DataFrame"""
        try:
            df = pd.read_parquet(filepath)
            logger.info(f"Successfully read {len(df)} rows from {filepath}")
            return df
        except Exception as e:
            logger.error(f"Failed to read parquet file {filepath}: {e}")
            raise

    def extract_nyc_taxi_data(self) -> pd.DataFrame:
        """Extract NYC taxi data from configured source"""
        filepath = self.extract_from_url(self.config.PARQUET_URL)
        return self.read_parquet(filepath)
