#!/usr/bin/env python3
"""
Main runner for NYC Taxi ETL Pipeline
"""

import sys
import logging
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from src.etl.pipeline import NYCTaxiETLPipeline
from src.database.manager import DatabaseManager
from config.config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def setup_database():
    """Initialize database and create tables"""
    logger.info("Setting up database...")

    db_manager = DatabaseManager()

    try:
        # Test connection
        if not db_manager.connect():
            logger.error("Failed to connect to database")
            return False

        if not db_manager.test_connection():
            logger.error("Database connection test failed")
            return False

        # Create tables
        if not db_manager.create_tables():
            logger.error("Failed to create database tables")
            return False

        logger.info("Database setup completed successfully")
        return True

    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        return False
    finally:
        db_manager.close()


def run_pipeline():
    """Run the complete ETL pipeline"""
    logger.info("Starting NYC Taxi ETL Pipeline...")

    try:
        # Initialize and run pipeline
        pipeline = NYCTaxiETLPipeline()
        result = pipeline.run()

        if result['status'] == 'SUCCESS':
            logger.info("=" * 50)
            logger.info("PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 50)
            logger.info(f"Duration: {result['duration']:.2f} seconds")
            logger.info(f"Records processed: {result['records_processed']:,}")

            if result.get('final_stats'):
                stats = result['final_stats']
                logger.info(f"Total records in database: {stats.get('total_records', 'N/A'):,}")
                logger.info(f"Date range: {stats.get('earliest_trip', 'N/A')} to {stats.get('latest_trip', 'N/A')}")
                logger.info(f"Average trip distance: {stats.get('avg_distance', 'N/A'):.2f} miles")
                logger.info(f"Average fare: ${stats.get('avg_fare', 'N/A'):.2f}")

        else:
            logger.error("=" * 50)
            logger.error("PIPELINE FAILED")
            logger.error("=" * 50)
            logger.error(f"Error: {result['error']}")

        return result

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        return {'status': 'FAILED', 'error': str(e)}


def main():
    """Main execution function"""
    try:
        # Validate configuration
        Config.validate_config()
        logger.info("Configuration validated successfully")

        # Setup database
        if not setup_database():
            logger.error("Database setup failed. Exiting.")
            sys.exit(1)

        # Run pipeline
        result = run_pipeline()

        # Exit with appropriate code
        if result['status'] == 'SUCCESS':
            logger.info("ETL Pipeline completed successfully!")
            sys.exit(0)
        else:
            logger.error("ETL Pipeline failed!")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Application failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()