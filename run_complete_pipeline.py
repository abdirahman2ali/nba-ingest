"""
Complete NBA Data Pipeline

This script runs the complete NBA data collection and loading pipeline.
"""

import os
import sys
import subprocess
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('nba_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_command(command, description):
    """Run a command and log the result."""
    logger.info(f"🔄 {description}...")
    
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            cwd=os.path.dirname(os.path.abspath(__file__))
        )
        
        if result.returncode == 0:
            logger.info(f"✅ {description} completed successfully")
            if result.stdout:
                logger.info(f"Output: {result.stdout}")
            return True
        else:
            logger.error(f"❌ {description} failed")
            if result.stderr:
                logger.error(f"Error: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error running {description}: {e}")
        return False

def main():
    """Run the complete NBA data pipeline."""
    logger.info("🏀 NBA Complete Data Pipeline Started")
    logger.info("=" * 50)
    
    start_time = datetime.now()
    
    # Step 1: Test database connection
    logger.info("Step 1: Testing database connection...")
    if not run_command("python3 -c \"from database_loader import NBADatabaseLoader; loader = NBADatabaseLoader(); print('✅ Database connection successful!' if loader.connect() else '❌ Database connection failed!')\"", "Database connection test"):
        logger.error("Database connection failed. Exiting.")
        return False
    
    # Step 2: Collect NBA data
    logger.info("Step 2: Collecting NBA data...")
    if not run_command("python3 nba_ingest.py", "NBA data collection"):
        logger.error("Data collection failed. Exiting.")
        return False
    
    # Step 3: Load data into PostgreSQL
    logger.info("Step 3: Loading data into PostgreSQL...")
    if not run_command("python3 database_loader.py", "Database loading"):
        logger.error("Database loading failed. Exiting.")
        return False
    
    # Step 4: Verify data
    logger.info("Step 4: Verifying loaded data...")
    if not run_command("python3 -c \"from database_loader import NBADatabaseLoader; loader = NBADatabaseLoader(); loader.connect(); loader.get_table_counts()\"", "Data verification"):
        logger.warning("Data verification failed, but data may still be loaded correctly.")
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    logger.info("🎉 NBA Data Pipeline Completed Successfully!")
    logger.info(f"⏱️  Total time: {duration}")
    logger.info("📊 Data is now available in your PostgreSQL database")
    logger.info("🔍 You can query the data using the 'nba' schema")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
