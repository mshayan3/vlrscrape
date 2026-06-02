# vlrscrape

Scrape VALORANT match data from [VLR.gg](https://www.vlr.gg), load it into a normalised SQLite database, and query it in plain English.

## What it does

1. **Scraper** — fetches raw match data from VLR.gg and saves it as CSVs organised by event/stage/match.
2. **Pipeline** — reads those CSVs and ingests them into a normalised SQLite database (`db/vlr_v3.db`).
3. **Orchestrator** — `scrape_and_ingest.py` runs scrape → ingest in one command.
4. **App** — `app.py` is a local Flask text-to-SQL frontend: ask a question in English, it generates SQL via OpenAI, runs it, and returns a table.

The database captures everything VLR.gg exposes: player stats per map per side (attack/defend/overall), round-by-round results, economy data (buy tier + bank per round), kill matrices (all kills / first kills / operator kills), multi-kill and clutch counts, agent picks, and map veto.

The schema (`pipeline/schema_v3.sql`) is visualised in **`db/erd_v3.png`**.

---

## Folder structure

```
vlrscrape/
├── scrape_and_ingest.py   # orchestrator — scrape an event/match then ingest
├── app.py                 # Flask text-to-SQL web app (OpenAI-powered)
├── requirements.txt
├── DESCRIPTION.md
│
├── scraper/               # VLR.gg scraping scripts
│   ├── main_scrape.py          # scrape a single match URL
│   ├── scrape_event.py         # scrape all matches in an event
│   ├── scrape_economy.py
│   ├── scrape_player_stats.py
│   ├── scrape_rounds.py
│   ├── scrape_performance.py
│   ├── scrape_pick_ban.py
│   ├── scrape_global.py
│   └── utils.py
│
├── pipeline/              # CSV → SQLite ingestion
│   ├── ingest_v2.py            # main ingestion script
│   └── schema_v3.sql           # database schema (v3)
│
├── db/                    # SQLite database + ERD
│   ├── vlr_v3.db               # (git-ignored; regenerate via ingestion)
│   ├── erd_v3.png              # entity-relationship diagram
│   └── erd_v3.mermaid
│
├── data/                  # raw CSV data (output of scraper)
│   └── VCT Events/{year}/{event}/{stage}/{Team_A}_vs_{Team_B}_{stage}/
│       ├── player_stats/
│       ├── rounds/
│       ├── economy/
│       ├── performance/
│       └── map_veto/
│
└── tests/
```

> `.env` and `*.db` are git-ignored. Copy `.env.example` to `.env` and add your OpenAI key before running `app.py`.

---

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env        # then add your OPENAI_API_KEY
```

---

## Quick start (full pipeline)

Scrape an entire event and ingest it into a fresh database in one command:

```bash
python scrape_and_ingest.py --event https://www.vlr.gg/event/2283/valorant-champions-2025 --wipe
```

Other modes:

```bash
# Scrape + ingest a single match
python scrape_and_ingest.py --match https://www.vlr.gg/542272/nrg-vs-fnatic-valorant-champions-2025-gf

# Re-ingest existing CSVs without scraping
python scrape_and_ingest.py --ingest-only

# Skip specific scrape sections
python scrape_and_ingest.py --event <url> --skip economy performance
```

---

## Scraping (standalone)

```bash
# single match
python scraper/main_scrape.py https://www.vlr.gg/542272/nrg-vs-fnatic-valorant-champions-2025-gf

# whole event
python scraper/scrape_event.py https://www.vlr.gg/event/2283/valorant-champions-2025
```

Raw CSVs are written to `data/VCT Events/{year}/{event}/{stage}/{match}/`.

---

## Ingestion (standalone)

```bash
python pipeline/ingest_v2.py                                   # everything in data/
python pipeline/ingest_v2.py --event "Valorant_Champions_2025" # one event
python pipeline/ingest_v2.py --match "NRG_vs_FNATIC_Playoffs-_Grand_Final"
python pipeline/ingest_v2.py --db db/custom.db                 # custom DB path
```

Ingestion is idempotent — re-running on the same data is safe.

---

## Query app

```bash
python app.py        # then open http://localhost:5000
```

Ask questions like *"Who had the highest ACS in the playoffs?"* — the app generates SQL against the schema, runs it, and shows the result. Requires `OPENAI_API_KEY` in `.env`.

---

## Current data

The database ships populated with **VCT Champions 2025**:

- 34 matches (Group Stage + Playoffs), 16 teams, ~80 players
- 88 maps, ~2,100 rounds
- Full economy, performance, agent-pick, and kill-matrix data

---

## Database schema

16 tables across 5 layers, plus 6 convenience views. See `db/erd_v3.png` for the full diagram.

**Dimensions**
| Table | Description |
|---|---|
| `events` | Tournament / event |
| `teams` | Teams with canonical ID |
| `team_aliases` | Maps short names (FNC, NRG) to canonical team_id |
| `players` | Players with current team + VLR id |
| `agents` | Agent roster |

**Match backbone**
| Table | Description |
|---|---|
| `matches` | One row per match; links to event + two teams. *v3:* `team_a/b_map_score`, `best_of` |
| `maps` | One row per map played. *v3:* `pick_team_id`, `duration_seconds` |
| `rounds` | One row per round — score, winner, side, win method. *v3:* `is_overtime` |
| `map_veto` | Pick/ban order per match |

**Player performance**
| Table | Description |
|---|---|
| `player_map_stats` | ACS, K/D/A, KAST, ADR, HS%, FK/FD per player/map/side (all/attack/defend). *v3:* generated `kd_ratio` |
| `player_map_agents` | Which agent each player ran on each map |
| `player_map_advanced` | Multi-kills (2K–5K), clutches (1v1–1v5), econ/plant/defuse |
| `player_vs_player_kills` | Kill matrix — kills_count per killer/victim pair, split by kill_type (all/fk/op) |

**Economy**
| Table | Description |
|---|---|
| `map_economy_summary` | Buy-type breakdown per team per map (pistol / eco / semi / full buy) |
| `round_economy` | Bank start + loadout value + buy tier per team per round |

**Audit**
| Table | Description |
|---|---|
| `raw_files` | Ingestion trail — which CSV produced which rows |

**Views:** `v_player_map_overview`, `v_rounds_with_econ`, `v_kill_matrix`, `v_player_tournament_totals`, `v_team_map_record`, `v_agent_pick_rate`.

---

## Sample queries

**Top players by ACS across an event:**
```sql
SELECT p.player_name, t.team_name,
       ROUND(AVG(pms.acs), 1) AS avg_acs,
       SUM(pms.kills) AS total_kills
FROM player_map_stats pms
JOIN players p ON p.player_id = pms.player_id
JOIN teams   t ON t.team_id   = pms.team_id
WHERE pms.side = 'all'
GROUP BY pms.player_id
ORDER BY avg_acs DESC
LIMIT 10;
```

**Agent pick rate by stage** (uses a built-in view):
```sql
SELECT stage, agent_name, picks
FROM v_agent_pick_rate
ORDER BY stage, picks DESC;
```

**Kill matrix for a match (who killed who most):**
```sql
SELECT killer, killer_team, victim, victim_team, kills_count
FROM v_kill_matrix
WHERE match_name LIKE '%Grand_Final%'
  AND kill_type = 'all'
ORDER BY kills_count DESC;
```
