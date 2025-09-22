"""
Comprehensive NBA Data Ingestion Module (1980-2025)

This module generates comprehensive NBA data from 1980 to 2025, including:
- Historical players and teams
- Current NBA stars with realistic stats
- Complete season coverage with filtering options
"""

import pandas as pd
import numpy as np
import random
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ComprehensiveNBAIngester:
    """Comprehensive NBA data ingester for historical and current data (1980-2025)."""
    
    def __init__(self, start_year: int = 1980, end_year: int = 2025):
        self.start_year = start_year
        self.end_year = end_year
        self.seasons = [f"{year}-{str(year+1)[2:]}" for year in range(start_year, end_year + 1)]
        
        # Create data directory
        os.makedirs('data', exist_ok=True)
    
        # Historical teams (1980s-1990s)
        self.historical_teams = {
            'ATL': {'name': 'Atlanta Hawks', 'city': 'Atlanta', 'conference': 'Eastern', 'division': 'Central'},
            'BOS': {'name': 'Boston Celtics', 'city': 'Boston', 'conference': 'Eastern', 'division': 'Atlantic'},
            'CHI': {'name': 'Chicago Bulls', 'city': 'Chicago', 'conference': 'Eastern', 'division': 'Central'},
            'CLE': {'name': 'Cleveland Cavaliers', 'city': 'Cleveland', 'conference': 'Eastern', 'division': 'Central'},
            'DAL': {'name': 'Dallas Mavericks', 'city': 'Dallas', 'conference': 'Western', 'division': 'Midwest'},
            'DEN': {'name': 'Denver Nuggets', 'city': 'Denver', 'conference': 'Western', 'division': 'Midwest'},
            'DET': {'name': 'Detroit Pistons', 'city': 'Detroit', 'conference': 'Eastern', 'division': 'Central'},
            'GSW': {'name': 'Golden State Warriors', 'city': 'San Francisco', 'conference': 'Western', 'division': 'Pacific'},
            'HOU': {'name': 'Houston Rockets', 'city': 'Houston', 'conference': 'Western', 'division': 'Midwest'},
            'IND': {'name': 'Indiana Pacers', 'city': 'Indianapolis', 'conference': 'Eastern', 'division': 'Central'},
            'LAC': {'name': 'LA Clippers', 'city': 'Los Angeles', 'conference': 'Western', 'division': 'Pacific'},
            'LAL': {'name': 'Los Angeles Lakers', 'city': 'Los Angeles', 'conference': 'Western', 'division': 'Pacific'},
            'MIA': {'name': 'Miami Heat', 'city': 'Miami', 'conference': 'Eastern', 'division': 'Atlantic'},
            'MIL': {'name': 'Milwaukee Bucks', 'city': 'Milwaukee', 'conference': 'Eastern', 'division': 'Central'},
            'NJN': {'name': 'New Jersey Nets', 'city': 'East Rutherford', 'conference': 'Eastern', 'division': 'Atlantic'},
            'NYK': {'name': 'New York Knicks', 'city': 'New York', 'conference': 'Eastern', 'division': 'Atlantic'},
            'ORL': {'name': 'Orlando Magic', 'city': 'Orlando', 'conference': 'Eastern', 'division': 'Atlantic'},
            'PHI': {'name': 'Philadelphia 76ers', 'city': 'Philadelphia', 'conference': 'Eastern', 'division': 'Atlantic'},
            'PHX': {'name': 'Phoenix Suns', 'city': 'Phoenix', 'conference': 'Western', 'division': 'Pacific'},
            'POR': {'name': 'Portland Trail Blazers', 'city': 'Portland', 'conference': 'Western', 'division': 'Pacific'},
            'SAC': {'name': 'Sacramento Kings', 'city': 'Sacramento', 'conference': 'Western', 'division': 'Pacific'},
            'SAS': {'name': 'San Antonio Spurs', 'city': 'San Antonio', 'conference': 'Western', 'division': 'Midwest'},
            'SEA': {'name': 'Seattle SuperSonics', 'city': 'Seattle', 'conference': 'Western', 'division': 'Pacific'},
            'UTA': {'name': 'Utah Jazz', 'city': 'Salt Lake City', 'conference': 'Western', 'division': 'Midwest'},
            'WAS': {'name': 'Washington Bullets', 'city': 'Washington', 'conference': 'Eastern', 'division': 'Atlantic'},
        }
        
        # Modern teams (2000s-2025)
        self.modern_teams = {
            'ATL': {'name': 'Atlanta Hawks', 'city': 'Atlanta', 'conference': 'Eastern', 'division': 'Southeast'},
            'BOS': {'name': 'Boston Celtics', 'city': 'Boston', 'conference': 'Eastern', 'division': 'Atlantic'},
            'BKN': {'name': 'Brooklyn Nets', 'city': 'Brooklyn', 'conference': 'Eastern', 'division': 'Atlantic'},
            'CHA': {'name': 'Charlotte Hornets', 'city': 'Charlotte', 'conference': 'Eastern', 'division': 'Southeast'},
            'CHI': {'name': 'Chicago Bulls', 'city': 'Chicago', 'conference': 'Eastern', 'division': 'Central'},
            'CLE': {'name': 'Cleveland Cavaliers', 'city': 'Cleveland', 'conference': 'Eastern', 'division': 'Central'},
            'DAL': {'name': 'Dallas Mavericks', 'city': 'Dallas', 'conference': 'Western', 'division': 'Southwest'},
            'DEN': {'name': 'Denver Nuggets', 'city': 'Denver', 'conference': 'Western', 'division': 'Northwest'},
            'DET': {'name': 'Detroit Pistons', 'city': 'Detroit', 'conference': 'Eastern', 'division': 'Central'},
            'GSW': {'name': 'Golden State Warriors', 'city': 'San Francisco', 'conference': 'Western', 'division': 'Pacific'},
            'HOU': {'name': 'Houston Rockets', 'city': 'Houston', 'conference': 'Western', 'division': 'Southwest'},
            'IND': {'name': 'Indiana Pacers', 'city': 'Indianapolis', 'conference': 'Eastern', 'division': 'Central'},
            'LAC': {'name': 'LA Clippers', 'city': 'Los Angeles', 'conference': 'Western', 'division': 'Pacific'},
            'LAL': {'name': 'Los Angeles Lakers', 'city': 'Los Angeles', 'conference': 'Western', 'division': 'Pacific'},
            'MEM': {'name': 'Memphis Grizzlies', 'city': 'Memphis', 'conference': 'Western', 'division': 'Southwest'},
            'MIA': {'name': 'Miami Heat', 'city': 'Miami', 'conference': 'Eastern', 'division': 'Southeast'},
            'MIL': {'name': 'Milwaukee Bucks', 'city': 'Milwaukee', 'conference': 'Eastern', 'division': 'Central'},
            'MIN': {'name': 'Minnesota Timberwolves', 'city': 'Minneapolis', 'conference': 'Western', 'division': 'Northwest'},
            'NOP': {'name': 'New Orleans Pelicans', 'city': 'New Orleans', 'conference': 'Western', 'division': 'Southwest'},
            'NYK': {'name': 'New York Knicks', 'city': 'New York', 'conference': 'Eastern', 'division': 'Atlantic'},
            'OKC': {'name': 'Oklahoma City Thunder', 'city': 'Oklahoma City', 'conference': 'Western', 'division': 'Northwest'},
            'ORL': {'name': 'Orlando Magic', 'city': 'Orlando', 'conference': 'Eastern', 'division': 'Southeast'},
            'PHI': {'name': 'Philadelphia 76ers', 'city': 'Philadelphia', 'conference': 'Eastern', 'division': 'Atlantic'},
            'PHX': {'name': 'Phoenix Suns', 'city': 'Phoenix', 'conference': 'Western', 'division': 'Pacific'},
            'POR': {'name': 'Portland Trail Blazers', 'city': 'Portland', 'conference': 'Western', 'division': 'Northwest'},
            'SAC': {'name': 'Sacramento Kings', 'city': 'Sacramento', 'conference': 'Western', 'division': 'Pacific'},
            'SAS': {'name': 'San Antonio Spurs', 'city': 'San Antonio', 'conference': 'Western', 'division': 'Southwest'},
            'TOR': {'name': 'Toronto Raptors', 'city': 'Toronto', 'conference': 'Eastern', 'division': 'Atlantic'},
            'UTA': {'name': 'Utah Jazz', 'city': 'Salt Lake City', 'conference': 'Western', 'division': 'Northwest'},
            'WAS': {'name': 'Washington Wizards', 'city': 'Washington', 'conference': 'Eastern', 'division': 'Southeast'},
        }
        
        # Legendary players by era
        self.legendary_players = {
            '1980s': [
                {'name': 'Magic Johnson', 'position': 'PG', 'teams': ['LAL'], 'era': '1980s'},
                {'name': 'Larry Bird', 'position': 'SF', 'teams': ['BOS'], 'era': '1980s'},
                {'name': 'Kareem Abdul-Jabbar', 'position': 'C', 'teams': ['LAL'], 'era': '1980s'},
                {'name': 'Isiah Thomas', 'position': 'PG', 'teams': ['DET'], 'era': '1980s'},
                {'name': 'Dominique Wilkins', 'position': 'SF', 'teams': ['ATL'], 'era': '1980s'},
                {'name': 'Julius Erving', 'position': 'SF', 'teams': ['PHI'], 'era': '1980s'},
                {'name': 'Moses Malone', 'position': 'C', 'teams': ['PHI'], 'era': '1980s'},
                {'name': 'James Worthy', 'position': 'SF', 'teams': ['LAL'], 'era': '1980s'},
            ],
            '1990s': [
                {'name': 'Michael Jordan', 'position': 'SG', 'teams': ['CHI'], 'era': '1990s'},
                {'name': 'Scottie Pippen', 'position': 'SF', 'teams': ['CHI'], 'era': '1990s'},
                {'name': 'Dennis Rodman', 'position': 'PF', 'teams': ['CHI', 'DET'], 'era': '1990s'},
                {'name': 'Karl Malone', 'position': 'PF', 'teams': ['UTA'], 'era': '1990s'},
                {'name': 'John Stockton', 'position': 'PG', 'teams': ['UTA'], 'era': '1990s'},
                {'name': 'Hakeem Olajuwon', 'position': 'C', 'teams': ['HOU'], 'era': '1990s'},
                {'name': 'Charles Barkley', 'position': 'PF', 'teams': ['PHI', 'PHX'], 'era': '1990s'},
                {'name': 'Patrick Ewing', 'position': 'C', 'teams': ['NYK'], 'era': '1990s'},
                {'name': 'David Robinson', 'position': 'C', 'teams': ['SAS'], 'era': '1990s'},
                {'name': 'Clyde Drexler', 'position': 'SG', 'teams': ['POR', 'HOU'], 'era': '1990s'},
            ],
            '2000s': [
                {'name': 'Tim Duncan', 'position': 'PF', 'teams': ['SAS'], 'era': '2000s'},
                {'name': 'Kobe Bryant', 'position': 'SG', 'teams': ['LAL'], 'era': '2000s'},
                {'name': 'Shaquille O\'Neal', 'position': 'C', 'teams': ['LAL', 'MIA'], 'era': '2000s'},
                {'name': 'LeBron James', 'position': 'SF', 'teams': ['CLE'], 'era': '2000s'},
                {'name': 'Dwyane Wade', 'position': 'SG', 'teams': ['MIA'], 'era': '2000s'},
                {'name': 'Kevin Garnett', 'position': 'PF', 'teams': ['MIN', 'BOS'], 'era': '2000s'},
                {'name': 'Dirk Nowitzki', 'position': 'PF', 'teams': ['DAL'], 'era': '2000s'},
                {'name': 'Steve Nash', 'position': 'PG', 'teams': ['PHX'], 'era': '2000s'},
                {'name': 'Tracy McGrady', 'position': 'SG', 'teams': ['HOU'], 'era': '2000s'},
                {'name': 'Vince Carter', 'position': 'SG', 'teams': ['TOR'], 'era': '2000s'},
            ],
            '2010s': [
                {'name': 'LeBron James', 'position': 'SF', 'teams': ['MIA', 'CLE', 'LAL'], 'era': '2010s'},
                {'name': 'Stephen Curry', 'position': 'PG', 'teams': ['GSW'], 'era': '2010s'},
                {'name': 'Kevin Durant', 'position': 'SF', 'teams': ['OKC', 'GSW'], 'era': '2010s'},
                {'name': 'Russell Westbrook', 'position': 'PG', 'teams': ['OKC'], 'era': '2010s'},
                {'name': 'James Harden', 'position': 'SG', 'teams': ['HOU'], 'era': '2010s'},
                {'name': 'Chris Paul', 'position': 'PG', 'teams': ['LAC', 'HOU'], 'era': '2010s'},
                {'name': 'Anthony Davis', 'position': 'PF', 'teams': ['NOP', 'LAL'], 'era': '2010s'},
                {'name': 'Kawhi Leonard', 'position': 'SF', 'teams': ['SAS', 'TOR', 'LAC'], 'era': '2010s'},
                {'name': 'Paul George', 'position': 'SF', 'teams': ['IND', 'OKC', 'LAC'], 'era': '2010s'},
                {'name': 'Blake Griffin', 'position': 'PF', 'teams': ['LAC', 'DET'], 'era': '2010s'},
            ],
            '2020s': [
                {'name': 'LeBron James', 'position': 'SF', 'teams': ['LAL'], 'era': '2020s'},
                {'name': 'Stephen Curry', 'position': 'PG', 'teams': ['GSW'], 'era': '2020s'},
                {'name': 'Kevin Durant', 'position': 'SF', 'teams': ['BKN', 'PHX'], 'era': '2020s'},
                {'name': 'Giannis Antetokounmpo', 'position': 'PF', 'teams': ['MIL'], 'era': '2020s'},
                {'name': 'Luka Doncic', 'position': 'PG', 'teams': ['DAL'], 'era': '2020s'},
                {'name': 'Jayson Tatum', 'position': 'SF', 'teams': ['BOS'], 'era': '2020s'},
                {'name': 'Nikola Jokic', 'position': 'C', 'teams': ['DEN'], 'era': '2020s'},
                {'name': 'Joel Embiid', 'position': 'C', 'teams': ['PHI'], 'era': '2020s'},
                {'name': 'Anthony Davis', 'position': 'PF', 'teams': ['LAL'], 'era': '2020s'},
                {'name': 'Jimmy Butler', 'position': 'SF', 'teams': ['MIA'], 'era': '2020s'},
            ]
        }
    
    def get_teams_for_era(self, year: int) -> Dict:
        """Get teams appropriate for the given year."""
        if year < 2000:
            return self.historical_teams
        else:
            return self.modern_teams
    
    def get_players_for_era(self, year: int) -> List[Dict]:
        """Get players appropriate for the given year."""
        players = []
        
        # Add legendary players for each era
        if 1980 <= year <= 1989:
            players.extend(self.legendary_players['1980s'])
        if 1990 <= year <= 1999:
            players.extend(self.legendary_players['1990s'])
        if 2000 <= year <= 2009:
            players.extend(self.legendary_players['2000s'])
        if 2010 <= year <= 2019:
            players.extend(self.legendary_players['2010s'])
        if 2020 <= year <= 2025:
            players.extend(self.legendary_players['2020s'])
        
        return players
    
    def generate_comprehensive_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Generate comprehensive NBA data for all seasons."""
        logger.info(f"🏀 Generating Comprehensive NBA Data ({self.start_year}-{self.end_year})")
        logger.info("=" * 70)
        
        all_teams = []
        all_players = []
        all_stats = []
        
        team_id_counter = 1
        player_id_counter = 1
        
        for season in self.seasons:
            year = int(season.split('-')[0])
            logger.info(f"Processing season {season}...")
            
            # Get teams for this era
            teams_dict = self.get_teams_for_era(year)
            
            # Create teams for this season
            for abbrev, team_info in teams_dict.items():
                all_teams.append({
                    'team_id': team_id_counter,
                    'team_name': team_info['name'],
                    'team_abbreviation': abbrev,
                    'team_city': team_info['city'],
                    'conference': team_info['conference'],
                    'division': team_info['division'],
                    'season': season
                })
                team_id_counter += 1
            
            # Get legendary players for this era
            legendary_players = self.get_players_for_era(year)
            
            # Add legendary players
            for player_info in legendary_players:
                player_name = player_info['name']
                position = player_info['position']
                possible_teams = player_info['teams']
                
                # Choose team based on timeline and player history
                team_abbrev = self._get_team_for_player(player_name, possible_teams, year)
                
                # Find team_id for this team in this season
                team_id = None
                for team in all_teams:
                    if team['team_abbreviation'] == team_abbrev and team['season'] == season:
                        team_id = team['team_id']
                        break
                
                if team_id is None:
                    continue
                
                # Create player record
                all_players.append({
                    'player_id': player_id_counter,
                    'player_name': player_name,
                    'team_id': team_id,
                    'team_abbreviation': team_abbrev,
                    'jersey_number': str(random.randint(0, 99)),
                    'position': position,
                    'height': f"{random.randint(6, 7)}-{random.randint(0, 11)}",
                    'weight': str(random.randint(180, 280)),
                    'age': random.randint(22, 38),
                    'college': random.choice(['Duke', 'Kentucky', 'North Carolina', 'UCLA', 'Kansas', 'Michigan State']),
                    'country': random.choice(['USA', 'Canada', 'France', 'Germany', 'Australia', 'Spain']),
                    'draft_year': random.randint(max(1975, year-20), year),
                    'draft_round': random.randint(1, 2),
                    'draft_number': random.randint(1, 60),
                    'season': season
                })
                
                # Create realistic stats based on era and player
                stats = self._generate_era_appropriate_stats(player_name, position, year, player_info.get('era', ''))
                
                all_stats.append({
                    'player_id': player_id_counter,
                    'player_name': player_name,
                    'season': season,
                    'team_id': team_id,
                    'team_abbreviation': team_abbrev,
                    'per_mode': 'PerGame',
                    **stats
                })
                
                player_id_counter += 1
            
            # Add additional random players each season
            num_extra_players = random.randint(40, 80)
            for i in range(num_extra_players):
                first_names = ['Alex', 'Marcus', 'Devin', 'Tyler', 'Jordan', 'Mason', 'Logan', 'Ethan', 'Noah', 'Liam', 'James', 'Michael', 'David', 'Chris', 'Kevin']
                last_names = ['Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Wilson', 'Smith', 'Anderson', 'Taylor', 'Thomas', 'Jackson']
                
                player_name = f"{random.choice(first_names)} {random.choice(last_names)}"
                position = random.choice(['PG', 'SG', 'SF', 'PF', 'C'])
                team_abbrev = random.choice(list(teams_dict.keys()))
                
                # Find team_id
                team_id = None
                for team in all_teams:
                    if team['team_abbreviation'] == team_abbrev and team['season'] == season:
                        team_id = team['team_id']
                        break
                
                if team_id is None:
                    continue
                
                # Create player record
                all_players.append({
                    'player_id': player_id_counter,
                    'player_name': player_name,
                    'team_id': team_id,
                    'team_abbreviation': team_abbrev,
                    'jersey_number': str(random.randint(0, 99)),
                    'position': position,
                    'height': f"{random.randint(6, 7)}-{random.randint(0, 11)}",
                    'weight': str(random.randint(180, 280)),
                    'age': random.randint(20, 35),
                    'college': random.choice(['Duke', 'Kentucky', 'North Carolina', 'UCLA', 'Kansas', 'Michigan State']),
                    'country': random.choice(['USA', 'Canada', 'France', 'Germany', 'Australia', 'Spain']),
                    'draft_year': random.randint(max(1975, year-20), year),
                    'draft_round': random.randint(1, 2),
                    'draft_number': random.randint(1, 60),
                    'season': season
                })
                
                # Create stats
                stats = self._generate_era_appropriate_stats(player_name, position, year, 'regular')
                
                all_stats.append({
                    'player_id': player_id_counter,
                    'player_name': player_name,
                    'season': season,
                    'team_id': team_id,
                    'team_abbreviation': team_abbrev,
                    'per_mode': 'PerGame',
                    **stats
                })
                
                player_id_counter += 1
        
        # Convert to DataFrames
        teams_df = pd.DataFrame(all_teams).drop_duplicates(subset=['team_id', 'season'])
        players_df = pd.DataFrame(all_players).drop_duplicates(subset=['player_id', 'season'])
        stats_df = pd.DataFrame(all_stats)
        
        logger.info(f"Generated {len(teams_df):,} team records")
        logger.info(f"Generated {len(players_df):,} player records")
        logger.info(f"Generated {len(stats_df):,} stat records")
        
        return teams_df, players_df, stats_df
    
    def _get_team_for_player(self, player_name: str, possible_teams: List[str], year: int) -> str:
        """Get the appropriate team for a player based on their career timeline."""
        # Handle specific player team changes
        if player_name == 'LeBron James':
            if year <= 2010:
                return 'CLE'
            elif 2011 <= year <= 2014:
                return 'MIA'
            elif 2015 <= year <= 2018:
                return 'CLE'
            else:
                return 'LAL'
        elif player_name == 'Kevin Durant':
            if year <= 2016:
                return 'OKC'
            elif 2017 <= year <= 2019:
                return 'GSW'
            elif 2020 <= year <= 2022:
                return 'BKN'
            else:
                return 'PHX'
        elif player_name == 'Shaquille O\'Neal':
            if year <= 1996:
                return 'ORL'
            elif 1997 <= year <= 2004:
                return 'LAL'
            elif 2005 <= year <= 2008:
                return 'MIA'
            else:
                return 'PHX'
        elif player_name == 'Dennis Rodman':
            if year <= 1993:
                return 'DET'
            else:
                return 'CHI'
        elif player_name == 'Charles Barkley':
            if year <= 1992:
                return 'PHI'
            else:
                return 'PHX'
        elif player_name == 'Clyde Drexler':
            if year <= 1995:
                return 'POR'
            else:
                return 'HOU'
        elif player_name == 'Russell Westbrook':
            if year <= 2019:
                return 'OKC'
            elif 2020 <= year <= 2021:
                return 'HOU'
            elif 2022 <= year <= 2023:
                return 'LAL'
            else:
                return 'LAC'
        elif player_name == 'James Harden':
            if year <= 2012:
                return 'OKC'
            elif 2013 <= year <= 2020:
                return 'HOU'
            elif 2021 <= year <= 2022:
                return 'BKN'
            elif 2023 <= year <= 2024:
                return 'PHI'
            else:
                return 'LAC'
        elif player_name == 'Chris Paul':
            if year <= 2011:
                return 'NOP'
            elif 2012 <= year <= 2017:
                return 'LAC'
            elif 2018 <= year <= 2021:
                return 'HOU'
            elif 2022 <= year <= 2023:
                return 'PHX'
            else:
                return 'GSW'
        elif player_name == 'Anthony Davis':
            if year <= 2019:
                return 'NOP'
            else:
                return 'LAL'
        elif player_name == 'Kawhi Leonard':
            if year <= 2018:
                return 'SAS'
            elif year == 2019:
                return 'TOR'
            else:
                return 'LAC'
        elif player_name == 'Paul George':
            if year <= 2017:
                return 'IND'
            elif 2018 <= year <= 2019:
                return 'OKC'
            else:
                return 'LAC'
        elif player_name == 'Blake Griffin':
            if year <= 2018:
                return 'LAC'
            else:
                return 'DET'
        elif player_name == 'Kevin Garnett':
            if year <= 2007:
                return 'MIN'
            else:
                return 'BOS'
        elif player_name == 'Damian Lillard':
            if year <= 2023:
                return 'POR'
            else:
                return 'MIL'
        
        # Default to first team in list
        return possible_teams[0]
    
    def _generate_era_appropriate_stats(self, player_name: str, position: str, year: int, era: str) -> Dict:
        """Generate realistic stats based on era and player."""
        
        # Base stats by position
        position_bases = {
            'PG': {'points': 12, 'assists': 6, 'rebounds': 3, 'steals': 1.2},
            'SG': {'points': 14, 'assists': 3, 'rebounds': 4, 'steals': 1.0},
            'SF': {'points': 13, 'assists': 3, 'rebounds': 5, 'steals': 1.1},
            'PF': {'points': 12, 'assists': 2, 'rebounds': 7, 'blocks': 0.8},
            'C': {'points': 11, 'assists': 1, 'rebounds': 8, 'blocks': 1.2}
        }
        
        base = position_bases.get(position, position_bases['SF'])
        
        # Era adjustments
        era_multipliers = {
            '1980s': {'points': 0.9, 'assists': 1.1, 'rebounds': 1.0, 'three_pct': 0.3},
            '1990s': {'points': 1.0, 'assists': 1.0, 'rebounds': 1.0, 'three_pct': 0.4},
            '2000s': {'points': 1.0, 'assists': 1.0, 'rebounds': 1.0, 'three_pct': 0.5},
            '2010s': {'points': 1.1, 'assists': 1.0, 'rebounds': 0.95, 'three_pct': 0.7},
            '2020s': {'points': 1.2, 'assists': 1.1, 'rebounds': 0.9, 'three_pct': 0.8},
            'regular': {'points': 1.0, 'assists': 1.0, 'rebounds': 1.0, 'three_pct': 0.5}
        }
        
        era_mult = era_multipliers.get(era, era_multipliers['regular'])
        
        # Adjust for modern NBA (2020s have higher scoring)
        if year >= 2020:
            era_mult['points'] *= 1.2
            era_mult['three_pct'] = 0.8
        
        # Superstar adjustments
        if player_name in ['Michael Jordan', 'LeBron James', 'Kobe Bryant', 'Kevin Durant', 'Giannis Antetokounmpo', 'Luka Doncic']:
            base['points'] *= 2.0
            base['assists'] *= 1.5
            base['rebounds'] *= 1.3
        elif player_name in ['Magic Johnson', 'Stephen Curry', 'Damian Lillard', 'Trae Young']:
            base['assists'] *= 2.0
            base['points'] *= 1.7
        elif player_name in ['Shaquille O\'Neal', 'Hakeem Olajuwon', 'David Robinson', 'Nikola Jokic', 'Joel Embiid']:
            base['rebounds'] *= 1.6
            base['blocks'] = base.get('blocks', 0) * 2.5
            base['points'] *= 1.5
        elif player_name in ['Dennis Rodman', 'Ben Wallace']:
            base['rebounds'] *= 2.5
            base['points'] *= 0.7
        
        # Generate stats
        games_played = random.randint(65, 82)
        
        points = max(0.1, base['points'] * era_mult['points'] * random.uniform(0.8, 1.4))
        assists = max(0.1, base['assists'] * era_mult['assists'] * random.uniform(0.7, 1.4))
        rebounds = max(0.1, base['rebounds'] * era_mult['rebounds'] * random.uniform(0.8, 1.3))
        
        # Era-appropriate shooting percentages
        fg_pct = random.uniform(0.40, 0.55)
        three_pct = random.uniform(0.25, 0.45) * era_mult['three_pct']
        ft_pct = random.uniform(0.70, 0.90)
        
        return {
            'games_played': games_played,
            'games_started': random.randint(games_played // 2, games_played),
            'minutes_per_game': round(random.uniform(25, 40), 1),
            'field_goals_made': round(points * 0.38, 1),
            'field_goals_attempted': round((points * 0.38) / fg_pct, 1),
            'field_goal_percentage': round(fg_pct, 3),
            'three_pointers_made': round(points * 0.15, 1),
            'three_pointers_attempted': round((points * 0.15) / three_pct, 1),
            'three_point_percentage': round(three_pct, 3),
            'free_throws_made': round(points * 0.22, 1),
            'free_throws_attempted': round((points * 0.22) / ft_pct, 1),
            'free_throw_percentage': round(ft_pct, 3),
            'offensive_rebounds': round(rebounds * 0.25, 1),
            'defensive_rebounds': round(rebounds * 0.75, 1),
            'total_rebounds': round(rebounds, 1),
            'assists': round(assists, 1),
            'steals': round(base.get('steals', 1.0) * random.uniform(0.6, 1.4), 1),
            'blocks': round(base.get('blocks', 0.5) * random.uniform(0.4, 2.2), 1),
            'turnovers': round(random.uniform(1.8, 4.2), 1),
            'personal_fouls': round(random.uniform(1.8, 3.8), 1),
            'points': round(points, 1)
        }
    
    def save_data(self, teams_df: pd.DataFrame, players_df: pd.DataFrame, stats_df: pd.DataFrame):
        """Save data to CSV files."""
        logger.info("Saving comprehensive NBA data to CSV files...")
        
        # Save to main data directory
        teams_df.to_csv('data/teams_all_seasons.csv', index=False)
        players_df.to_csv('data/players_all_seasons.csv', index=False)
        stats_df.to_csv('data/player_season_stats.csv', index=False)
        
        logger.info(f"✅ Saved {len(teams_df):,} team records")
        logger.info(f"✅ Saved {len(players_df):,} player records")
        logger.info(f"✅ Saved {len(stats_df):,} stat records")
        
        # Create postgres_ready directory for database loading
        os.makedirs('data/postgres_ready', exist_ok=True)
        teams_df.to_csv('data/postgres_ready/teams_all_seasons.csv', index=False)
        players_df.to_csv('data/postgres_ready/players_all_seasons.csv', index=False)
        stats_df.to_csv('data/postgres_ready/player_season_stats.csv', index=False)
        
        logger.info("📁 Files created:")
        logger.info("  - data/teams_all_seasons.csv")
        logger.info("  - data/players_all_seasons.csv")
        logger.info("  - data/player_season_stats.csv")
        logger.info("  - data/postgres_ready/ (for database loading)")

def main():
    """Main function to generate comprehensive NBA data."""
    # Allow filtering via environment variables
    start_year = int(os.getenv('START_YEAR', 1980))
    end_year = int(os.getenv('END_YEAR', 2025))
    
    logger.info(f"🏀 NBA Data Generation ({start_year}-{end_year})")
    logger.info("=" * 60)
    
    # Create ingester
    ingester = ComprehensiveNBAIngester(start_year=start_year, end_year=end_year)
    
    # Generate comprehensive data
    teams_df, players_df, stats_df = ingester.generate_comprehensive_data()
    
    # Save data
    ingester.save_data(teams_df, players_df, stats_df)
    
    logger.info("🎉 Comprehensive NBA data generation completed!")
    logger.info(f"📊 Total records generated: {len(teams_df) + len(players_df) + len(stats_df):,}")

if __name__ == "__main__":
    main()
