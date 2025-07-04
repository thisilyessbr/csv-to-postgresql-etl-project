import pandas as pd
import requests
import os
from typing import Optional, List
import logging
from config.config import Config
from concurrent.futures import ThreadPoolExecutor, as_completed
logger = logging.getLogger(__name__)


class DataExtractor:

    def __init__(self):
        self.config = Config()
        os.makedirs(self.config.RAW_DATA_DIR, exist_ok=True)

    def extract_multiple_months(self, years: List[int] = None, months: List[int] = None) -> List[str]:
        if years is None:
            years = self.config.DATA_YEARS
        if months is None:
            months = self.config.DATA_MONTHS

        urls_and_files = []
        for year in years:
            for month in months:
                url = self.config.PARQUET_URL_TEMPLATE.format(year=year, month=month)
                filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
                urls_and_files.append((url, filename))

        downloaded_files = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_file = {
                executor.submit(self.extract_from_url, url, filename): filename
                for url, filename in urls_and_files
            }

        for future in as_completed(future_to_file):
            filename = future_to_file[future]
            try:
                filepath = future.result()
                downloaded_files.append(filepath)
                logger.info(f"Successfully downloaded: {filename}")
            except Exception as e:
                logger.error(f"Failed to download {filename}: {e}")

        return downloaded_files

    def extract_from_url(self, url: str, filename: Optional[str] = None) -> str:
        """Extract single file from URL (enhanced with progress tracking)"""
        try:
            if filename is None:
                filename = url.split('/')[-1]

            filepath = os.path.join(self.config.RAW_DATA_DIR, filename)

            if not os.path.exists(filepath):
                logger.info(f"Downloading data from {url}")

                response = requests.get(url, stream=True)
                response.raise_for_status()

                total_size = int(response.headers.get('content-length', 0))
                downloaded_size = 0

                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                        downloaded_size += len(chunk)

                        # Log progress every 10MB
                        if downloaded_size % (10 * 1024 * 1024) == 0 and total_size > 0:
                            progress = (downloaded_size / total_size) * 100
                            logger.info(f"Download progress for {filename}: {progress:.1f}%")

                logger.info(f"Data downloaded to {filepath} ({downloaded_size / 1024 / 1024:.1f} MB)")
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

    def extract_nyc_taxi_data(self, years: List[int] = None, months: List[int] = None) -> List[str]:
        """Extract NYC taxi data for specified years and months"""
        return self.extract_multiple_months(years, months)
