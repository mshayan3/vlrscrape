"""
Database schema context for RAG-based chatbot.
Provides natural language descriptions of the database structure for LLM context.
"""

SCHEMA_CONTEXT = """
# Valorant Champions 2025 Database Schema

## Overview
This database contains comprehensive match data from Valorant Champions 2025, including player statistics, 
round-by-round data, economy tracking, and team information.

## Core Tables

### events
Tournament events information.
- event_id (TEXT, PK): Unique event identifier
- event_name (TEXT): Full event name
- season_year (INTEGER): Year of the season
- source (TEXT): Data source (default: 'vlr.gg')

### teams
Team information and metadata.
- team_id (TEXT, PK): Unique team identifier (slugified)
- team_name (TEXT): Official team name
- region (TEXT): Team's region

### players
Player roster information.
- player_id (TEXT, PK): Unique player identifier (slugified)
- player_name (TEXT): Player's in-game name
- current_team_id (TEXT, FK): Reference to teams table

### agents
Valorant agent information.
- agent_id (TEXT, PK): Unique agent identifier
- agent_name (TEXT): Agent name (e.g., Jett, Omen, Sage)

### matches
Match-level information.
- match_id (TEXT, PK): Unique match identifier
- event_id (TEXT, FK): Reference to events table
- match_name (TEXT): Match description
- stage (TEXT): Tournament stage (e.g., Playoffs, Group Stage)
- match_date (TEXT): Match date
- team_a_id (TEXT, FK): First team
- team_b_id (TEXT, FK): Second team
- winner_team_id (TEXT, FK): Winning team
- source_url (TEXT): Original match URL

### maps
Individual maps played within a match.
- map_id (TEXT, PK): Unique map identifier
- match_id (TEXT, FK): Reference to matches table
- map_number (INTEGER): Map number in series (1, 2, 3, etc.)
- map_name (TEXT): Map name (e.g., Ascent, Bind, Haven)
- team_a_score (INTEGER): Team A rounds won
- team_b_score (INTEGER): Team B rounds won
- winner_team_id (TEXT, FK): Map winner

### rounds
Round-by-round results.
- round_id (TEXT, PK): Unique round identifier
- map_id (TEXT, FK): Reference to maps table
- round_number (INTEGER): Round number (1-25+)
- score (TEXT): Current score (e.g., "5-3")
- winning_team_id (TEXT, FK): Round winner
- winning_side (TEXT): Attacking or Defending
- win_method (TEXT): How the round was won (e.g., elimination, spike defused)

### map_veto
Map pick/ban phase data.
- veto_id (TEXT, PK): Unique veto identifier
- match_id (TEXT, FK): Reference to matches table
- order_no (INTEGER): Order in veto sequence
- team_id (TEXT, FK): Team making the action (NULL for decider)
- action_type (TEXT): 'pick', 'ban', or 'decider'
- map_name (TEXT): Map being picked/banned

### player_map_stats
Player performance statistics per map.
- pms_id (TEXT, PK): Unique stat identifier
- map_id (TEXT, FK): Reference to maps table
- player_id (TEXT, FK): Reference to players table
- team_id (TEXT, FK): Player's team
- agent_id (TEXT, FK): Agent played
- acs (REAL): Average Combat Score
- kills (INTEGER): Total kills
- deaths (INTEGER): Total deaths
- assists (INTEGER): Total assists
- adr (REAL): Average Damage per Round
- kast (REAL): Kill/Assist/Survive/Trade percentage
- hs_pct (REAL): Headshot percentage
- rating (REAL): Overall rating (R2.0)
- fk (INTEGER): First kills
- fd (INTEGER): First deaths

### round_economy
Economy data per round and team.
- econ_id (TEXT, PK): Unique economy identifier
- map_id (TEXT, FK): Reference to maps table
- round_number (INTEGER): Round number
- team_id (TEXT, FK): Team
- credits (INTEGER): Team credits
- loadout_value (INTEGER): Total loadout value
- buy_tier (TEXT): Buy type (e.g., 'full', 'eco', 'pistol')

### player_vs_player_kills
Kill matrix showing player vs player eliminations.
- pvpk_id (TEXT, PK): Unique identifier
- map_id (TEXT, FK): Reference to maps table
- killer_player_id (TEXT, FK): Player who got the kill
- victim_player_id (TEXT, FK): Player who was killed
- killer_team_id (TEXT, FK): Killer's team
- victim_team_id (TEXT, FK): Victim's team
- kills_count (INTEGER): Number of times killer eliminated victim

## Common Query Patterns

### Player Statistics
- Top players by ACS: `SELECT player_name, AVG(acs) FROM player_map_stats JOIN players GROUP BY player_id ORDER BY AVG(acs) DESC`
- Player performance on specific agent: Join player_map_stats with agents
- Head-to-head kills: Query player_vs_player_kills table

### Team Performance
- Team match history: Query matches table with team_a_id or team_b_id
- Team map win rate: Aggregate maps table by winner_team_id
- Team economy efficiency: Join round_economy with rounds on winning_team_id

### Match Analysis
- Map veto patterns: Query map_veto grouped by team_id and action_type
- Round win methods: Aggregate rounds by win_method
- Economy impact: Correlate round_economy.buy_tier with rounds.winning_team_id

## Important Notes
- All IDs are TEXT (slugified strings, not integers)
- Foreign keys are enforced (PRAGMA foreign_keys = ON)
- Use JOINs to get human-readable names (team names, player names, etc.)
- Percentages (kast, hs_pct) are stored as decimals (e.g., 75.5 not 0.755)
- Multiple maps can exist per match (best-of-3 or best-of-5)
"""

SAMPLE_QUERIES = {
    "top_players_acs": {
        "question": "Who are the top 10 players by average ACS?",
        "sql": """
            SELECT p.player_name, t.team_name, AVG(pms.acs) as avg_acs, COUNT(*) as maps_played
            FROM player_map_stats pms
            JOIN players p ON pms.player_id = p.player_id
            JOIN teams t ON pms.team_id = t.team_id
            GROUP BY p.player_id
            HAVING maps_played >= 3
            ORDER BY avg_acs DESC
            LIMIT 10;
        """
    },
    "team_matches": {
        "question": "Show all matches for Team Heretics",
        "sql": """
            SELECT m.match_name, m.stage, t1.team_name as team_a, t2.team_name as team_b, 
                   tw.team_name as winner
            FROM matches m
            JOIN teams t1 ON m.team_a_id = t1.team_id
            JOIN teams t2 ON m.team_b_id = t2.team_id
            LEFT JOIN teams tw ON m.winner_team_id = tw.team_id
            WHERE t1.team_name LIKE '%Heretics%' OR t2.team_name LIKE '%Heretics%';
        """
    },
    "agent_usage": {
        "question": "What agents does aspas play most?",
        "sql": """
            SELECT a.agent_name, COUNT(*) as times_played, AVG(pms.acs) as avg_acs
            FROM player_map_stats pms
            JOIN players p ON pms.player_id = p.player_id
            JOIN agents a ON pms.agent_id = a.agent_id
            WHERE p.player_name = 'aspas'
            GROUP BY a.agent_id
            ORDER BY times_played DESC;
        """
    },
    "map_win_rate": {
        "question": "Which team has the best win rate on Ascent?",
        "sql": """
            SELECT t.team_name, 
                   COUNT(*) as maps_played,
                   SUM(CASE WHEN m.winner_team_id = t.team_id THEN 1 ELSE 0 END) as wins,
                   ROUND(100.0 * SUM(CASE WHEN m.winner_team_id = t.team_id THEN 1 ELSE 0 END) / COUNT(*), 2) as win_rate
            FROM maps m
            JOIN teams t ON (m.team_a_score IS NOT NULL AND (t.team_id IN (
                SELECT team_a_id FROM matches WHERE match_id = m.match_id
                UNION
                SELECT team_b_id FROM matches WHERE match_id = m.match_id
            )))
            WHERE m.map_name = 'Ascent'
            GROUP BY t.team_id
            HAVING maps_played >= 3
            ORDER BY win_rate DESC;
        """
    }
}

def get_schema_context():
    """Returns the full schema context for LLM."""
    return SCHEMA_CONTEXT

def get_sample_queries():
    """Returns sample queries for few-shot learning."""
    return SAMPLE_QUERIES
