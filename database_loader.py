"""
Automated PostgreSQL Database Loader for NBA Data

This module handles loading CSV data into PostgreSQL database with proper schema creation,
data validation, and error handling.
"""

import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, text
import os
import logging
from datetime import datetime
from dotenv import load_dotenv
import glob

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('nba_loader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class NBADatabaseLoader:
    """Handles loading NBA CSV data into PostgreSQL database."""
    
    def __init__(self):
        self.connection_string = os.getenv('DATABASE_URL')
        self.engine = create_engine(self.connection_string)
        self.conn = None
        
    def connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(self.connection_string)
            logger.info("✅ Database connection established")
            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
    
    def create_schema(self):
        """Create database schema for NBA data."""
        if not self.conn:
            logger.error("No database connection")
            return False
        
        try:
            cursor = self.conn.cursor()
            
            # Create schema if it doesn't exist
            cursor.execute("CREATE SCHEMA IF NOT EXISTS nba;")
            
            # Players table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS nba.players (
                    player_id INTEGER PRIMARY KEY,
                    player_name VARCHAR(100) NOT NULL,
                    team_id INTEGER,
                    team_abbreviation VARCHAR(10),
                    jersey_number VARCHAR(10),
                    position VARCHAR(10),
                    height VARCHAR(10),
                    weight VARCHAR(10),
                    age INTEGER,
                    college VARCHAR(100),
                    country VARCHAR(50),
                    draft_year INTEGER,
                    draft_round INTEGER,
                    draft_number INTEGER,
                    season VARCHAR(10),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Player season stats table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS nba.player_season_stats (
                    id SERIAL PRIMARY KEY,
                    player_id INTEGER NOT NULL,
                    player_name VARCHAR(100) NOT NULL,
                    season VARCHAR(10) NOT NULL,
                    team_id INTEGER,
                    team_abbreviation VARCHAR(10),
                    per_mode VARCHAR(20),
                    games_played INTEGER,
                    games_started INTEGER,
                    minutes_per_game DECIMAL(5,2),
                    field_goals_made DECIMAL(5,2),
                    field_goals_attempted DECIMAL(5,2),
                    field_goal_percentage DECIMAL(5,3),
                    three_pointers_made DECIMAL(5,2),
                    three_pointers_attempted DECIMAL(5,2),
                    three_point_percentage DECIMAL(5,3),
                    free_throws_made DECIMAL(5,2),
                    free_throws_attempted DECIMAL(5,2),
                    free_throw_percentage DECIMAL(5,3),
                    offensive_rebounds DECIMAL(5,2),
                    defensive_rebounds DECIMAL(5,2),
                    total_rebounds DECIMAL(5,2),
                    assists DECIMAL(5,2),
                    steals DECIMAL(5,2),
                    blocks DECIMAL(5,2),
                    turnovers DECIMAL(5,2),
                    personal_fouls DECIMAL(5,2),
                    points DECIMAL(5,2),
                    plus_minus DECIMAL(6,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Teams table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS nba.teams (
                    team_id INTEGER PRIMARY KEY,
                    team_name VARCHAR(100) NOT NULL,
                    team_abbreviation VARCHAR(10) NOT NULL,
                    team_city VARCHAR(50),
                    conference VARCHAR(20),
                    division VARCHAR(20),
                    season VARCHAR(10),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Create indexes for better performance
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_players_name ON nba.players(player_name);
                CREATE INDEX IF NOT EXISTS idx_players_season ON nba.players(season);
                CREATE INDEX IF NOT EXISTS idx_player_stats_player_season ON nba.player_season_stats(player_id, season);
                CREATE INDEX IF NOT EXISTS idx_player_stats_name ON nba.player_season_stats(player_name);
                CREATE INDEX IF NOT EXISTS idx_player_stats_season ON nba.player_season_stats(season);
                CREATE INDEX IF NOT EXISTS idx_teams_abbreviation ON nba.teams(team_abbreviation);
                CREATE INDEX IF NOT EXISTS idx_teams_season ON nba.teams(season);
            """)
            
            self.conn.commit()
            cursor.close()
            logger.info("✅ Database schema created successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creating schema: {e}")
            if self.conn:
                self.conn.rollback()
            return False
    
    def load_csv_to_table(self, csv_file: str, table_name: str, schema: str = 'nba'):
        """Load CSV file into PostgreSQL table."""
        try:
            logger.info(f"Loading {csv_file} into {schema}.{table_name}...")
            
            # Read CSV file
            df = pd.read_csv(csv_file)
            logger.info(f"Read {len(df)} records from {csv_file}")
            
            if df.empty:
                logger.warning(f"No data in {csv_file}")
                return True
            
            # Clean column names
            df.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]
            
            # Handle NaN values
            df = df.fillna('')
            
            # Load data using pandas to_sql
            df.to_sql(
                table_name,
                self.engine,
                schema=schema,
                if_exists='append',
                index=False
            )
            
            logger.info(f"✅ Successfully loaded {len(df)} records into {schema}.{table_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error loading {csv_file}: {e}")
            return False
    
    def clear_table(self, table_name: str, schema: str = 'nba'):
        """Clear existing data from table."""
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"TRUNCATE TABLE {schema}.{table_name} CASCADE;")
            self.conn.commit()
            cursor.close()
            logger.info(f"✅ Cleared data from {schema}.{table_name}")
            return True
        except Exception as e:
            logger.error(f"❌ Error clearing {schema}.{table_name}: {e}")
            return False
    
    def load_all_data(self, data_directory: str = 'data', clear_existing: bool = True):
        """Load all NBA data from CSV files."""
        if not self.connect():
            return False
        
        try:
            # Create schema
            if not self.create_schema():
                return False
            
            # Define file mappings
            file_mappings = {
                'players_all_seasons.csv': 'players',
                'player_stats_per_game.csv': 'player_season_stats',
                'player_stats_totals.csv': 'player_season_stats',
                'player_stats_per_36.csv': 'player_season_stats',
                'teams_all_seasons.csv': 'teams'
            }
            
            # Clear existing data if requested
            if clear_existing:
                logger.info("Clearing existing data...")
                for table in ['players', 'player_season_stats', 'teams']:
                    self.clear_table(table)
            
            # Load each file
            success_count = 0
            total_files = 0
            
            for filename, table_name in file_mappings.items():
                file_path = os.path.join(data_directory, filename)
                
                if os.path.exists(file_path):
                    total_files += 1
                    if self.load_csv_to_table(file_path, table_name):
                        success_count += 1
                else:
                    logger.warning(f"File not found: {file_path}")
            
            logger.info(f"✅ Loaded {success_count}/{total_files} files successfully")
            
            # Get final counts
            self.get_table_counts()
            
            return success_count == total_files
            
        except Exception as e:
            logger.error(f"❌ Error in load_all_data: {e}")
            return False
        finally:
            if self.conn:
                self.conn.close()
    
    def get_table_counts(self):
        """Get record counts for all tables."""
        try:
            cursor = self.conn.cursor()
            
            tables = ['players', 'player_season_stats', 'teams']
            
            logger.info("\n📊 Table Record Counts:")
            logger.info("-" * 40)
            
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM nba.{table};")
                count = cursor.fetchone()[0]
                logger.info(f"{table:20}: {count:,} records")
            
            cursor.close()
            
        except Exception as e:
            logger.error(f"Error getting table counts: {e}")
    
    def create_views(self):
        """Create useful database views."""
        if not self.conn:
            return False
        
        try:
            cursor = self.conn.cursor()
            
            # View for player career stats
            cursor.execute("""
                CREATE OR REPLACE VIEW nba.player_career_stats AS
                SELECT 
                    player_id,
                    player_name,
                    COUNT(DISTINCT season) as seasons_played,
                    MIN(season) as first_season,
                    MAX(season) as last_season,
                    AVG(points) as avg_points,
                    AVG(total_rebounds) as avg_rebounds,
                    AVG(assists) as avg_assists,
                    AVG(field_goal_percentage) as avg_fg_pct,
                    AVG(three_point_percentage) as avg_3pt_pct,
                    SUM(points) as total_points,
                    SUM(total_rebounds) as total_rebounds,
                    SUM(assists) as total_assists
                FROM nba.player_season_stats
                WHERE per_mode = 'PerGame'
                GROUP BY player_id, player_name
                ORDER BY total_points DESC;
            """)
            
            # View for season leaders
            cursor.execute("""
                CREATE OR REPLACE VIEW nba.season_leaders AS
                SELECT 
                    season,
                    player_name,
                    team_abbreviation,
                    points,
                    total_rebounds,
                    assists,
                    field_goal_percentage,
                    ROW_NUMBER() OVER (PARTITION BY season ORDER BY points DESC) as points_rank,
                    ROW_NUMBER() OVER (PARTITION BY season ORDER BY total_rebounds DESC) as rebounds_rank,
                    ROW_NUMBER() OVER (PARTITION BY season ORDER BY assists DESC) as assists_rank
                FROM nba.player_season_stats
                WHERE per_mode = 'PerGame' AND games_played >= 50
                ORDER BY season, points DESC;
            """)
            
            self.conn.commit()
            cursor.close()
            logger.info("✅ Database views created successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creating views: {e}")
            return False

def main():
    """Main function to load NBA data into PostgreSQL."""
    logger.info("🏀 NBA Database Loader Started")
    logger.info("=" * 50)
    
    loader = NBADatabaseLoader()
    
    # Check if data directory exists
    data_dir = 'data'
    if not os.path.exists(data_dir):
        logger.error(f"Data directory '{data_dir}' not found. Please run nba_ingest.py first.")
        return
    
    # Check for CSV files
    csv_files = glob.glob(os.path.join(data_dir, "*.csv"))
    if not csv_files:
        logger.error(f"No CSV files found in '{data_dir}'. Please run nba_ingest.py first.")
        return
    
    logger.info(f"Found {len(csv_files)} CSV files to load")
    
    # Load all data
    success = loader.load_all_data(data_dir, clear_existing=True)
    
    if success:
        logger.info("🎉 All data loaded successfully!")
        
        # Create views
        if loader.connect():
            loader.create_views()
            loader.conn.close()
    else:
        logger.error("❌ Some errors occurred during data loading. Check the logs for details.")

if __name__ == "__main__":
    main()
