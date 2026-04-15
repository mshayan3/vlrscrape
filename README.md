# vlrscrape

Scrape match data from [VLR.gg](https://www.vlr.gg) and load it into a structured SQLite database for analysis.

## What it does

1. **Scraper** — fetches raw match data from VLR.gg and saves it as CSVs organised by event/stage/match
2. **Pipeline** — reads those CSVs and ingests them into a normalised SQLite database

The database captures everything VLR.gg exposes: player stats per map per side (attack/defend/overall), round-by-round results, economy data (buy tier + bank per round), kill matrices (all kills / first kills / operator kills), multi-kill and clutch counts, agent picks, and map veto.

---

## Folder structure

```
vlrscrape/
├── scraper/          # VLR.gg scraping scripts
│   ├── main_scrape.py          # entry point — scrape a single match URL
│   ├── scrape_event.py         # scrape all matches in an event
│   ├── scrape_economy.py
│   ├── scrape_player_stats.py
│   ├── scrape_rounds.py
│   ├── scrape_performance.py
│   ├── scrape_pick_ban.py
│   ├── scrape_global.py
│   └── utils.py
│
├── pipeline/         # CSV → SQLite ingestion
│   ├── ingest_v2.py            # main ingestion script
│   └── schema_v2.sql           # database schema
│
├── db/               # SQLite database
│   └── vlr_v2.db
│
├── data/             # raw CSV data (output of scraper)
│   └── VCT Events/
│       └── {year}/
│           └── {event}/
│               └── {stage}/
│                   └── {Team_A}_vs_{Team_B}_{stage}/
│                       ├── player_stats/
│                       ├── rounds/
│                       ├── economy/
│                       ├── performance/
│                       └── map_veto/
│
└── requirements.txt
```

---

## Setup

```bash
pip install -r requirements.txt
```

---

## Scraping

Scrape a single match by passing its VLR.gg URL:

```bash
python scraper/main_scrape.py https://www.vlr.gg/542272/nrg-vs-fnatic-valorant-champions-2025-gf
```

Scrape an entire event:

```bash
python scraper/scrape_event.py https://www.vlr.gg/event/2097/valorant-champions-2025
```

Raw CSVs are written to `data/VCT Events/{year}/{event}/{stage}/{match}/`.

---

## Ingestion

Convert scraped CSVs into the SQLite database:

```bash
# Ingest everything in data/
python pipeline/ingest_v2.py

# Ingest a single event
python pipeline/ingest_v2.py --event "Valorant_Champions_2025"

# Ingest a single match
python pipeline/ingest_v2.py --match "NRG_vs_FNATIC_Playoffs-_Grand_Final"

# Custom DB path
python pipeline/ingest_v2.py --db db/custom.db
```

The ingestion is idempotent — re-running it on the same data is safe.

---

## Database schema

14 tables across 4 layers:

**Dimensions**
| Table | Description |
|---|---|
| `events` | Tournament / event |
| `teams` | Teams with canonical ID |
| `team_aliases` | Maps short names (FNC, NRG) to canonical team_id |
| `players` | Players with current team |
| `agents` | Agent roster |

**Match backbone**
| Table | Description |
|---|---|
| `matches` | One row per match, links to event + two teams |
| `maps` | One row per map played, links to match |
| `rounds` | One row per round, with score, winner, side, win method |
| `map_veto` | Pick/ban order per match |

**Player performance**
| Table | Description |
|---|---|
| `player_map_stats` | ACS, K/D/A, KAST, ADR, HS%, FK/FD — per player per map per side (all/attack/defend) |
| `player_map_agents` | Which agent each player ran on each map |
| `player_map_advanced` | Multi-kills (2K–5K), clutches (1v1–1v5), ECON/PL/DE |
| `player_vs_player_kills` | Kill matrix — kills_count per killer/victim pair, split by kill_type (all/fk/op) |

**Economy**
| Table | Description |
|---|---|
| `map_economy_summary` | Buy type breakdown per team per map (pistol / eco / semi / full buy wins) |
| `round_economy` | Bank start + buy tier per team per round |

Three convenience views: `v_player_map_overview`, `v_rounds_with_econ`, `v_kill_matrix`.

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

**Round economy for a specific map:**
```sql
SELECT re.round_number, t.team_name, re.bank_start, re.buy_tier,
       r.winning_side, r.win_method
FROM round_economy re
JOIN teams  t  ON t.team_id  = re.team_id
JOIN maps   m  ON m.map_id   = re.map_id
JOIN rounds r  ON r.map_id   = re.map_id AND r.round_number = re.round_number
JOIN matches mc ON mc.match_id = m.match_id
WHERE mc.match_name LIKE '%Grand_Final%'
  AND m.map_name = 'Corrode'
ORDER BY re.round_number, t.team_name;
```

**Kill matrix for a match (who killed who most):**
```sql
SELECT killer, killer_team, victim, victim_team, kills_count
FROM v_kill_matrix
WHERE match_name LIKE '%Grand_Final%'
  AND map_name = 'Corrode'
  AND kill_type = 'all'
ORDER BY kills_count DESC;
```

**Agent meta across a tournament:**
```sql
SELECT a.agent_name, COUNT(*) AS appearances
FROM player_map_agents pma
JOIN agents a ON a.agent_id = pma.agent_id
GROUP BY a.agent_id
ORDER BY appearances DESC;
```
