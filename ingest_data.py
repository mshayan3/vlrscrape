import sqlite3
import os
import csv
import re
import pandas as pd
from datetime import datetime

DB_NAME = "valorant_analytics.db"

def init_db():
    """Initialize the database schema."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Events Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            region TEXT
        )
    ''')

    # Matches Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id INTEGER,
            team_a TEXT,
            team_b TEXT,
            stage TEXT,
            match_dir_name TEXT UNIQUE,
            FOREIGN KEY(event_id) REFERENCES Events(id)
        )
    ''')

    # Maps Table (Represents a played map within a match)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Maps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id INTEGER,
            map_name TEXT,
            winner_team TEXT,
            score_team_a INTEGER,
            score_team_b INTEGER,
            FOREIGN KEY(match_id) REFERENCES Matches(id)
        )
    ''')

    # PlayerStats Table (The heavy lifter)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS PlayerStats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id INTEGER,
            map_name TEXT, 
            player_name TEXT,
            team_name TEXT,
            agent TEXT,
            rating_2_0 REAL,
            acs INTEGER,
            kills INTEGER,
            deaths INTEGER,
            assists INTEGER,
            kd_ratio REAL,
            kast_percent TEXT,
            adr INTEGER,
            hs_percent TEXT,
            first_kills INTEGER,
            first_deaths INTEGER,
            fk_fd_diff INTEGER,
            side TEXT,
            FOREIGN KEY(match_id) REFERENCES Matches(id)
        )
    ''')

    conn.commit()
    conn.close()
    print(f"Database {DB_NAME} initialized.")

def parse_folder_name(folder_name):
    """
    Parses 'TeamA_vs_TeamB_EventStep' into components.
    Example: NRG_vs_FNATIC_Playoffs-_Grand_Final
    """
    # Regex is tricky because team names can have spaces/underscores
    # Strategy: Split by '_vs_' first
    if '_vs_' not in folder_name:
        return None, None, None

    parts = folder_name.split('_vs_')
    team_a = parts[0].replace('_', ' ')
    
    # The second part contains TeamB and the Stage
    # This is ambiguous if we don't know where the team name ends.
    # However, create_folder_structure in main_scrape used: "{team1}_vs_{team2}_{event_name}"
    # So the suffix is whatever was in event_name for that match.
    
    # Let's just store the raw parsed strings for now and maybe clean them later
    # For now, let's treat the rest as Team B + Stage mix
    # Actually, main_scrape.py did:
    # event_name = re.sub(r'[\\/*?:"<>|]', '-', event_name)
    # folder_name = f"{team1}_vs_{team2}_{event_name}"
    # whitespace to underscore
    
    remainder = parts[1]
    # This is hard to split perfectly without heuristics
    # Let's Assume the last part after the last underscore is stage? No.
    # We will store team_b as everything remaining for now, or improve scraping to store metadata.
    
    team_b = remainder # Placeholder
    stage = "Unknown"
    
    # Refined hack: Team names usually don't have "-" inside them often, but event steps do?
    # Actually, looking at the examples: "NRG_vs_FNATIC_Playoffs-_Grand_Final"
    # It seems "Playoffs-_Grand_Final" is the event info.
    # We can try to split by the first known separator or just fuzzy it.
    
    return team_a, team_b, stage

def ingest_data(root_dir):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Walk through Event Folders
    for event_folder in os.listdir(root_dir):
        event_path = os.path.join(root_dir, event_folder)
        if not os.path.isdir(event_path) or event_folder.startswith('.') or event_folder == 'dashboard':
            continue

        print(f"Processing Event: {event_folder}")
        
        # Insert Event
        try:
            cursor.execute("INSERT OR IGNORE INTO Events (name) VALUES (?)", (event_folder,))
            cursor.execute("SELECT id FROM Events WHERE name = ?", (event_folder,))
            event_id = cursor.fetchone()[0]
        except Exception as e:
            print(f"Error inserting event {event_folder}: {e}")
            continue

        # Walk through Match Folders
        for match_folder in os.listdir(event_path):
            match_path = os.path.join(event_path, match_folder)
            if not os.path.isdir(match_path):
                continue

            # Parse Match Info
            team_a, team_b, stage = parse_folder_name(match_folder)
            
            # Check if match already exists
            cursor.execute("SELECT id FROM Matches WHERE match_dir_name = ?", (match_folder,))
            existing = cursor.fetchone()
            if existing:
                match_id = existing[0]
            else:
                cursor.execute('''
                    INSERT INTO Matches (event_id, team_a, team_b, stage, match_dir_name)
                    VALUES (?, ?, ?, ?, ?)
                ''', (event_id, team_a, team_b, stage, match_folder))
                match_id = cursor.lastrowid

            # Process Player Stats (All_Maps.csv is the summary)
            # We can also look at specific maps.
            # Let's ingest 'All_Maps.csv' as map_name='All'
            
            stats_dir = os.path.join(match_path, 'player_stats')
            if os.path.exists(stats_dir):
                for stat_file in os.listdir(stats_dir):
                    if not stat_file.endswith('.csv'):
                        continue
                        
                    # Filename example: "All_Maps.csv" or "1Ascent.csv"
                    map_name_file = stat_file.replace('.csv', '')
                    
                    # Read CSV
                    try:
                        df = pd.read_csv(os.path.join(stats_dir, stat_file))
                        
                        # Columns: Player,Team,Map,Side,Agents,R2.0,ACS,K,D,A,K/D,KAST,ADR,HS%,FK,FD,FK/FD
                        # Filter to only "All" side usually, or ingest rows where Side='All' to simplify summary stats?
                        # For advanced analytics, we might want Split sides (Atk/Def), but let's stick to 'All' for general queries first.
                        
                        df_all_sides = df[df['Side'] == 'All']
                        
                        for _, row in df_all_sides.iterrows():
                            cursor.execute('''
                                INSERT INTO PlayerStats (
                                    match_id, map_name, player_name, team_name, agent,
                                    rating_2_0, acs, kills, deaths, assists, kd_ratio,
                                    kast_percent, adr, hs_percent, first_kills, first_deaths, fk_fd_diff
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                match_id,
                                map_name_file,
                                row.get('Player'),
                                row.get('Team'),
                                row.get('Agents'),
                                row.get('R2.0'),
                                row.get('ACS'),
                                row.get('K'),
                                row.get('D'),
                                row.get('A'),
                                row.get('K/D'),
                                row.get('KAST'),
                                row.get('ADR'),
                                row.get('HS%'),
                                row.get('FK'),
                                row.get('FD'),
                                row.get('FK/FD')
                            ))
                    except Exception as e:
                        print(f"Error reading stats {stat_file} in {match_folder}: {e}")

    conn.commit()
    conn.close()
    print("Ingestion complete.")

if __name__ == "__main__":
    init_db()
    ingest_data(os.getcwd())
