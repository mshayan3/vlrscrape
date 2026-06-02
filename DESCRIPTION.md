# Project Description

## What This Is

A data pipeline and query tool for VCT (VALORANT Champions Tour) match data. It scrapes structured match data from VLR.gg, stores it in a local SQLite database, and exposes a natural language query interface powered by OpenAI.

## What It Does

### 1. Scraper (`scraper/`)
Fetches raw match data from VLR.gg for a full event or a single match. Saves everything as CSVs organised by `data/VCT Events/{year}/{event}/{stage}/{match}/`. Each match folder contains:
- `player_stats/` — per-player stats per map per side (ACS, K/D/A, KAST, ADR, HS%, FK, FD)
- `rounds/` — round-by-round results (winner, win method, score)
- `economy/` — buy type breakdown and round-by-round bank/loadout values
- `performance/` — multi-kill counts (2K–5K), clutch counts (1v1–1v5), econ/plant/defuse
- `map_veto/` — pick/ban order per match

### 2. Ingestion Pipeline (`pipeline/`)
Reads all CSVs and loads them into a normalised SQLite database (`db/vlr_v2.db`). The schema has 14 tables across 4 layers:
- **Dimensions** — `events`, `teams`, `team_aliases`, `players`, `agents`
- **Match backbone** — `matches`, `maps`, `rounds`, `map_veto`
- **Player performance** — `player_map_stats`, `player_map_agents`, `player_map_advanced`, `player_vs_player_kills`
- **Economy** — `map_economy_summary`, `round_economy`

Ingestion is idempotent — re-running on the same data is safe.

### 3. Orchestrator (`scrape_and_ingest.py`)
Single entry point that runs scraping then ingestion in sequence. Supports `--event`, `--match`, `--ingest-only`, `--wipe`, and `--skip` flags.

### 4. Text-to-SQL Frontend (`app.py`)
A local Flask web app that takes a plain-English question, generates SQL via OpenAI (GPT-4o or GPT-4o-mini), runs it against the database, and returns a formatted results table. Reads the OpenAI API key from a `.env` file.

---

## Current Data

The database is populated with **VCT Champions 2025** data:
- 34 matches (Group Stage + Playoffs)
- 16 teams, ~80 players
- 88 maps, 2,169 rounds
- Full economy, performance, and kill matrix data

---

## Known Issues

- The main issue i have with the current state of the project is that i feel as if we can improve the quality of how the data is scraped so when we create relational db from it. Either that or what can be a better way to save the saved data so it can be either queried upon or feteched for answering questions. 

---

