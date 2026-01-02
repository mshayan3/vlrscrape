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

---

## Future Enhancements

<!-- Add new features, improvements, and plans below this line -->

