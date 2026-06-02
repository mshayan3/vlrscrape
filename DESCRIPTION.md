# Project Description

## What This Is

A data pipeline and query tool for VCT (VALORANT Champions Tour) match data. It scrapes structured match data from VLR.gg, stores it in a normalised local SQLite database, and exposes a natural-language query interface powered by OpenAI.

## What It Does

### 1. Scraper (`scraper/`)
Fetches raw match data from VLR.gg for a full event or a single match. Saves everything as CSVs under `data/VCT Events/{year}/{event}/{stage}/{match}/`. Each match folder contains:
- `player_stats/` — per-player stats per map per side (ACS, K/D/A, KAST, ADR, HS%, FK, FD)
- `rounds/` — round-by-round results (winner, win method, score)
- `economy/` — buy-type breakdown and round-by-round bank/loadout values
- `performance/` — multi-kill counts (2K–5K), clutch counts (1v1–1v5), econ/plant/defuse
- `map_veto/` — pick/ban order per match

### 2. Ingestion Pipeline (`pipeline/`)
Reads all CSVs and loads them into a normalised SQLite database (`db/vlr_v3.db`) using `schema_v3.sql`. The schema has 16 tables across 5 layers plus 6 views:
- **Dimensions** — `events`, `teams`, `team_aliases`, `players`, `agents`
- **Match backbone** — `matches`, `maps`, `rounds`, `map_veto`
- **Player performance** — `player_map_stats`, `player_map_agents`, `player_map_advanced`, `player_vs_player_kills`
- **Economy** — `map_economy_summary`, `round_economy`
- **Audit** — `raw_files`

Ingestion is idempotent — re-running on the same data is safe. The ERD is in `db/erd_v3.png`.

### 3. Orchestrator (`scrape_and_ingest.py`)
Single entry point that runs scraping then ingestion in sequence. Supports `--event`, `--match`, `--ingest-only`, `--wipe`, and `--skip` flags.

### 4. Text-to-SQL Frontend (`app.py`)
A local Flask web app that takes a plain-English question, generates SQL via OpenAI, runs it against the database, and returns a formatted results table. Reads the OpenAI API key from a `.env` file.

---

## Schema v3 (vs v2)

- `matches`: + `team_a_map_score` / `team_b_map_score` (series score) and `best_of`
- `maps`: + `pick_team_id` (who picked the map) and `duration_seconds`
- `rounds`: + `is_overtime`; ingest skips padded/blank round rows
- `player_map_stats`: + generated `kd_ratio`
- Data-quality `CHECK` constraints on enumerated columns (sides, win methods, buy types)

---

## Current Data

The database is populated with **VCT Champions 2025**:
- 34 matches (Group Stage + Playoffs)
- 16 teams, ~80 players
- 88 maps, ~2,100 rounds
- Full economy, performance, and kill-matrix data

---

## Notes / Next Steps

- The pipeline currently holds a single event. The schema is built for many — scraping additional events/years enables cross-event trends, roster tracking (`players.current_team_id` vs `player_map_stats.team_id`), and meta evolution over time.
- Potential next layers: an analytics dashboard, scouting reports per team, and round/map outcome models built on the economy + side + agent features.
