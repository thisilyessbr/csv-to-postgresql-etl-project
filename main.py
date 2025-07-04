# main.py
import logging
from src.etl.pipeline import NYCTaxiETLPipeline

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Create and run pipeline
    pipeline = NYCTaxiETLPipeline()
    result = pipeline.run()

    # Print result
    print(f"Pipeline execution completed with status: {result['status']}")
    if result['status'] == 'SUCCESS':
        print(f"Records processed: {result['records_processed']}")
        print(f"Duration: {result['duration']:.2f} seconds")
    else:
        print(f"Error: {result['error']}")