import pandas as pd
from typing import Optional
import logging
from datetime import datetime
from src.database.manager import DatabaseManager
from src.database.models import ETLLog

logger = logging.getLogger(__name__)


class DataLoader:

    def __init__(self):
        self.db_manager = DatabaseManager()

    def load_dataframe(self, df: pd.DataFrame, table_name: str = 'nyc_taxi_trips',
                       batch_size: int = 10000) -> bool:
        """Load DataFrame into database in batches"""
        try:
            if not self.db_manager.connect():
                raise Exception("Failed to connect to database")

            total_rows = len(df)
            logger.info(f"Loading {total_rows} rows into {table_name}")

            # Load in batches
            for i in range(0, total_rows, batch_size):
                batch = df.iloc[i:i + batch_size]
                batch.to_sql(table_name, self.db_manager.engine,
                             if_exists='append', index=False, method='multi')
                logger.info(f"Loaded batch {i // batch_size + 1}: {len(batch)} rows")

            logger.info(f"Successfully loaded all {total_rows} rows")
            return True
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            raise
        finally:
            self.db_manager.close()

    def log_etl_run(self, pipeline_name: str, start_time: datetime,
                    end_time: Optional[datetime] = None, status: str = 'RUNNING',
                    records_processed: Optional[int] = None,
                    error_message: Optional[str] = None) -> int:
        """Log ETL pipeline run"""
        try:
            if not self.db_manager.connect():
                raise Exception("Failed to connect to database")

            log_entry = ETLLog(
                pipeline_name=pipeline_name,
                start_time=start_time,
                end_time=end_time,
                status=status,
                records_processed=records_processed,
                error_message=error_message
            )

            # Insert log entry
            with self.db_manager.engine.connect() as conn:
                result = conn.execute(
                    ETLLog.__table__.insert().values(
                        pipeline_name=pipeline_name,
                        start_time=start_time,
                        end_time=end_time,
                        status=status,
                        records_processed=records_processed,
                        error_message=error_message
                    )
                )
                conn.commit()
                return result.inserted_primary_key[0]

        except Exception as e:
            logger.error(f"Failed to log ETL run: {e}")
            raise
        finally:
            self.db_manager.close()
