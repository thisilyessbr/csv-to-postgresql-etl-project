import logging
from datetime import datetime
from typing import Dict, Any
from src.etl.extractor import DataExtractor
from src.etl.transformer import DataTransformer
from src.etl.loader import DataLoader
from src.database.manager import DatabaseManager

logger = logging.getLogger(__name__)


class NYCTaxiETLPipeline:

    def __init__(self):
        self.extractor = DataExtractor()
        self.transformer = DataTransformer()
        self.loader = DataLoader()
        self.db_manager = DatabaseManager()
        self.pipeline_name = "nyc_taxi_etl"

    def run(self) -> Dict[str, Any]:
        """Execute the complete ETL pipeline"""
        start_time = datetime.now()
        log_id = None

        try:
            # Initialize database
            logger.info("Starting NYC Taxi ETL Pipeline")
            if not self.db_manager.connect():
                raise Exception("Failed to connect to database")

            # Log pipeline start
            log_id = self.loader.log_etl_run(
                pipeline_name=self.pipeline_name,
                start_time=start_time,
                status='RUNNING'
            )

            # Extract
            logger.info("Step 1: Extracting data")
            raw_data = self.extractor.extract_nyc_taxi_data()

            # Transform
            logger.info("Step 2: Transforming data")
            transformed_data = self.transformer.transform(raw_data)

            # Load
            logger.info("Step 3: Loading data")
            self.loader.load_dataframe(transformed_data)

            # Log success
            end_time = datetime.now()
            if log_id:
                self.loader.log_etl_run(
                    pipeline_name=self.pipeline_name,
                    start_time=start_time,
                    end_time=end_time,
                    status='SUCCESS',
                    records_processed=len(transformed_data)
                )

                # Get final stats
            final_stats = self.db_manager.get_table_stats()

            result = {
                'status': 'SUCCESS',
                'start_time': start_time,
                'end_time': end_time,
                'duration': (end_time - start_time).total_seconds(),
                'records_processed': len(transformed_data),
                'final_stats': final_stats
            }

            logger.info(f"Pipeline completed successfully: {result}")
            return result

        except Exception as e:
            end_time = datetime.now()
            error_msg = str(e)
            logger.error(f"Pipeline failed: {error_msg}")

            # Log failure
            if log_id:
                self.loader.log_etl_run(
                    pipeline_name=self.pipeline_name,
                    start_time=start_time,
                    end_time=end_time,
                    status='FAILED',
                    error_message=error_msg
                )

            return {
                'status': 'FAILED',
                'start_time': start_time,
                'end_time': end_time,
                'error': error_msg
            }

        finally:
            self.db_manager.close()