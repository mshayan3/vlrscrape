# VLR Scrape

A comprehensive data scraping and ingestion pipeline for Valorant Champions Tour (VCT) esports data from VLR.gg.

## Overview

This project scrapes match data from VLR.gg and stores it in a normalized SQLite database for analysis. It captures detailed statistics including player performance, round-by-round data, economy tracking, map vetoes, and player vs player kill matrices.

## Project Structure

```
vlrscrape/
├── main_scrape.py                          # Main scraping orchestrator
├── scrape_*.py                             # Specialized scrapers
│   ├── scrape_event.py                     # Event-level scraping
│   ├── scrape_economy.py                   # Round economy data
│   ├── scrape_performance.py               # Player performance metrics
│   ├── scrape_pick_ban.py                  # Map veto/pick-ban phase
│   ├── scrape_player_stats.py              # Player statistics
│   ├── scrape_rounds.py                    # Round-by-round data
│   └── scrape_global.py                    # Global match data
├── ingest_valorant_champions_2025.py       # Data ingestion pipeline
├── db_helpers.py                           # Database utility functions
├── utils.py                                # Shared utility functions
├── schema.sql                              # Database schema definition
├── requirements.txt                        # Python dependencies
├── valorant_champions_2025.db              # SQLite database
└── VCT Events/                             # Scraped data directory
```

## Features

- **Comprehensive Data Collection**
  - Match metadata (teams, stages, events)
  - Map veto/pick-ban phases
  - Player statistics per map
  - Round-by-round outcomes
  - Economy tracking per round
  - Player vs player kill matrices
  - Agent selections

- **Normalized Database Schema**
  - Efficient relational structure
  - Foreign key constraints
  - Indexed for query performance
  - See `schema.sql` for full details

## Installation

```bash
# Install dependencies
pip install -r requirements.txt
```

## Usage

### Scraping Match Data

```bash
# Scrape a single match
python main_scrape.py --url "https://www.vlr.gg/MATCH_ID/..."

# Use the scrape workflow for events
# See .agent/workflows/ for available workflows
```

### Ingesting Data into Database

```bash
# Ingest Valorant Champions 2025 data
python ingest_valorant_champions_2025.py
```

This will:
1. Initialize the database with `schema.sql`
2. Process all CSV files in `VCT Events/2025/Valorant_Champions_2025/`
3. Populate normalized tables with match data

## Database Schema

The database follows a normalized structure with the following main tables:

- `events` - Tournament events
- `teams` - Team information
- `players` - Player roster
- `agents` - Valorant agents
- `matches` - Match metadata
- `maps` - Individual maps played
- `rounds` - Round-by-round results
- `map_veto` - Pick/ban phase data
- `player_map_stats` - Player performance per map
- `round_economy` - Economy data per round
- `player_vs_player_kills` - Kill matrix data

See `schema.sql` for complete schema definition.

## Core Modules

### `main_scrape.py`
Main scraping orchestrator that coordinates all specialized scrapers. Creates folder structure and manages the scraping workflow.

### `ingest_valorant_champions_2025.py`
Data ingestion pipeline that transforms scraped CSV files into the normalized database structure.

### `db_helpers.py`
Database utility class (`DBHelpers`) providing:
- Team/player/agent normalization
- ID generation and slugification
- Data parsing helpers (credits, buy tiers)
- Transaction management

### `utils.py`
Shared utility functions for HTTP requests and common operations.

## Data Flow

```
VLR.gg → main_scrape.py → CSV files → ingest_*.py → SQLite DB
```

1. **Scraping**: `main_scrape.py` fetches match data and saves to CSV
2. **Storage**: CSVs organized in `VCT Events/` directory structure
3. **Ingestion**: `ingest_*.py` transforms CSVs into normalized database
4. **Analysis**: Query the SQLite database for insights

## Requirements

- Python 3.x
- See `requirements.txt` for package dependencies

## Database Location

- **Active Database**: `valorant_champions_2025.db`

## AI Chatbot

Query the database using natural language with the RAG-based AI chatbot!

### Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set up API key
cp .env.template .env
# Edit .env and add your OpenAI or Anthropic API key

# 3. Run the chatbot
python chatbot.py
```

### Example Questions

- "Who are the top 10 players by ACS?"
- "Show me all matches for Team Heretics"
- "What agents does aspas play most?"
- "Which team won the most rounds on Ascent?"

See [CHATBOT_SETUP.md](CHATBOT_SETUP.md) for detailed setup instructions and more examples.

## Web Frontend

Interactive Valorant-themed web interface for the chatbot!

### Quick Start

```bash
# 1. Install Flask dependencies
pip install flask flask-cors

# 2. Start the API server
python api.py

# 3. Open the web interface
open web/index.html
```

### Features

- 🎮 **Valorant Theme** - Authentic red/black color scheme with glowing effects
- 💬 **Real-time Chat** - Interactive interface with typing indicators
- 📊 **Table Results** - Beautiful formatted tables for query results
- 📱 **Responsive** - Works on desktop and mobile
- 🚀 **Deployable** - Ready for GitHub Pages + backend deployment

### GitHub Pages Deployment

1. Push to GitHub
2. Enable Pages in repo settings → `/web` folder
3. Deploy backend to Render/Railway (or run locally)
4. Update API_URL in `web/script.js`

See [WEB_SETUP.md](WEB_SETUP.md) for complete deployment guide.

---

## Future Enhancements

<!-- Add new features, improvements, and plans below this line -->

