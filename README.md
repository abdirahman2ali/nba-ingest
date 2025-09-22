# NBA Data Analysis Project

A comprehensive Python project for scraping, analyzing, and visualizing NBA data.

## Features

- **Data Ingestion**: Scrape NBA statistics from official APIs
- **Data Analysis**: Process and analyze player and team performance
- **Visualization**: Create interactive charts and graphs
- **Jupyter Notebooks**: Interactive data exploration

## Setup

1. **Clone the repository** (if not already done):
   ```bash
   git clone <your-repo-url>
   cd nba-project
   ```

2. **Create a virtual environment**:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables** (optional):
   ```bash
   cp .env.example .env
   # Edit .env with your API keys and configuration
   ```

## Usage

### Basic Data Ingestion

```python
from nba_ingest import NBADataIngester

# Initialize the ingester
ingester = NBADataIngester()

# Get player statistics for 2023-24 season
player_stats = ingester.get_player_stats(season="2023-24")

# Get team statistics
team_stats = ingester.get_team_stats(season="2023-24")

# Save data
ingester.save_data(player_stats, "player_stats.csv")
```

### Running the Main Script

```bash
python nba_ingest.py
```

This will fetch current season data and save it to the `data/` directory.

## Project Structure

```
nba-project/
├── nba_ingest.py          # Main data ingestion module
├── requirements.txt       # Python dependencies
├── .env.example          # Environment variables template
├── .gitignore            # Git ignore rules
├── data/                 # Data storage directory
└── notebooks/            # Jupyter notebooks for analysis
```

## Data Sources

- **NBA Stats API**: Official NBA statistics
- **Basketball Reference**: Additional historical data
- **ESPN**: Real-time scores and news

## Dependencies

- `requests`: HTTP requests
- `pandas`: Data manipulation
- `numpy`: Numerical computing
- `matplotlib`: Basic plotting
- `seaborn`: Statistical visualization
- `plotly`: Interactive visualizations
- `jupyter`: Interactive notebooks
- `beautifulsoup4`: Web scraping
- `selenium`: Browser automation

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

MIT License - see LICENSE file for details
