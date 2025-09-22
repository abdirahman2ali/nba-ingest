"""
Cron Job Automation for NBA Data Pipeline

This script sets up automated data collection and database loading using cron jobs.
"""

import os
import subprocess
import sys
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CronAutomation:
    """Handles cron job setup for NBA data automation."""
    
    def __init__(self):
        self.project_dir = os.path.dirname(os.path.abspath(__file__))
        self.python_path = sys.executable
        
    def create_automation_script(self):
        """Create the main automation script."""
        script_content = f'''#!/bin/bash
# NBA Data Automation Script
# Generated on {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

# Set working directory
cd {self.project_dir}

# Activate virtual environment if it exists
if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

# Set environment variables
export PYTHONPATH={self.project_dir}
export AUTO_CONFIRM=true

# Log start time
echo "$(date): Starting NBA data collection and loading..."

# Step 1: Collect NBA data
echo "$(date): Collecting NBA data..."
{python_path} nba_ingest.py >> nba_automation.log 2>&1

# Check if data collection was successful
if [ $? -eq 0 ]; then
    echo "$(date): Data collection completed successfully"
    
    # Step 2: Load data into PostgreSQL
    echo "$(date): Loading data into PostgreSQL..."
    {python_path} database_loader.py >> nba_automation.log 2>&1
    
    if [ $? -eq 0 ]; then
        echo "$(date): Data loading completed successfully"
    else
        echo "$(date): ERROR: Data loading failed"
        exit 1
    fi
else
    echo "$(date): ERROR: Data collection failed"
    exit 1
fi

echo "$(date): NBA data pipeline completed successfully"
'''
        
        script_path = os.path.join(self.project_dir, 'run_nba_pipeline.sh')
        
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        # Make script executable
        os.chmod(script_path, 0o755)
        
        logger.info(f"✅ Created automation script: {script_path}")
        return script_path
    
    def setup_cron_job(self, schedule: str = "0 6 * * *"):
        """
        Set up cron job for automated data collection.
        
        Args:
            schedule: Cron schedule (default: daily at 6 AM)
                     Examples:
                     - "0 6 * * *" (daily at 6 AM)
                     - "0 */6 * * *" (every 6 hours)
                     - "0 6 * * 1" (weekly on Monday at 6 AM)
        """
        script_path = self.create_automation_script()
        
        # Create cron job entry
        cron_entry = f"{schedule} {script_path}"
        
        # Get current crontab
        try:
            result = subprocess.run(['crontab', '-l'], capture_output=True, text=True)
            current_crontab = result.stdout
        except subprocess.CalledProcessError:
            current_crontab = ""
        
        # Check if NBA automation is already in crontab
        if "run_nba_pipeline.sh" in current_crontab:
            logger.info("NBA automation cron job already exists")
            return True
        
        # Add new cron job
        new_crontab = current_crontab + f"\n# NBA Data Automation\n{cron_entry}\n"
        
        try:
            # Write new crontab
            subprocess.run(['crontab', '-'], input=new_crontab, text=True, check=True)
            logger.info(f"✅ Cron job added successfully!")
            logger.info(f"Schedule: {schedule}")
            logger.info(f"Script: {script_path}")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Failed to add cron job: {e}")
            return False
    
    def remove_cron_job(self):
        """Remove NBA automation cron job."""
        try:
            result = subprocess.run(['crontab', '-l'], capture_output=True, text=True)
            current_crontab = result.stdout
            
            # Remove NBA automation lines
            lines = current_crontab.split('\n')
            filtered_lines = []
            skip_next = False
            
            for line in lines:
                if "NBA Data Automation" in line or "run_nba_pipeline.sh" in line:
                    skip_next = True
                    continue
                if skip_next and line.strip() == "":
                    skip_next = False
                    continue
                if not skip_next:
                    filtered_lines.append(line)
            
            new_crontab = '\n'.join(filtered_lines)
            
            # Write updated crontab
            subprocess.run(['crontab', '-'], input=new_crontab, text=True, check=True)
            logger.info("✅ NBA automation cron job removed")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Failed to remove cron job: {e}")
            return False
    
    def list_cron_jobs(self):
        """List current cron jobs."""
        try:
            result = subprocess.run(['crontab', '-l'], capture_output=True, text=True)
            if result.returncode == 0:
                logger.info("Current cron jobs:")
                logger.info(result.stdout)
            else:
                logger.info("No cron jobs found")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to list cron jobs: {e}")
    
    def test_automation(self):
        """Test the automation script."""
        script_path = os.path.join(self.project_dir, 'run_nba_pipeline.sh')
        
        if not os.path.exists(script_path):
            logger.error("Automation script not found. Creating it first...")
            self.create_automation_script()
        
        logger.info("🧪 Testing NBA automation pipeline...")
        
        try:
            result = subprocess.run([script_path], cwd=self.project_dir, capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info("✅ Automation test completed successfully!")
                logger.info("Output:")
                logger.info(result.stdout)
            else:
                logger.error("❌ Automation test failed!")
                logger.error("Error output:")
                logger.error(result.stderr)
                
        except Exception as e:
            logger.error(f"❌ Error running automation test: {e}")

def main():
    """Main function for cron automation setup."""
    logger.info("🤖 NBA Cron Automation Setup")
    logger.info("=" * 40)
    
    automation = CronAutomation()
    
    print("\nWhat would you like to do?")
    print("1. Set up daily automation (6 AM)")
    print("2. Set up weekly automation (Monday 6 AM)")
    print("3. Set up custom schedule")
    print("4. Test automation script")
    print("5. List current cron jobs")
    print("6. Remove NBA automation")
    print("7. Exit")
    
    choice = input("\nEnter your choice (1-7): ").strip()
    
    if choice == "1":
        automation.setup_cron_job("0 6 * * *")
        print("✅ Daily automation set up (6 AM)")
    elif choice == "2":
        automation.setup_cron_job("0 6 * * 1")
        print("✅ Weekly automation set up (Monday 6 AM)")
    elif choice == "3":
        schedule = input("Enter cron schedule (e.g., '0 6 * * *' for daily at 6 AM): ").strip()
        if schedule:
            automation.setup_cron_job(schedule)
            print(f"✅ Custom automation set up: {schedule}")
    elif choice == "4":
        automation.test_automation()
    elif choice == "5":
        automation.list_cron_jobs()
    elif choice == "6":
        automation.remove_cron_job()
    elif choice == "7":
        print("Goodbye!")
    else:
        print("Invalid choice")

if __name__ == "__main__":
    main()
