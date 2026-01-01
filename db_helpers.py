"""
Database helper utilities for VLR data ingestion.
Handles team/player name normalization and ID generation.
"""
import re
import sqlite3
from typing import Dict, Set

class DBHelpers:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.cursor = conn.cursor()
        
        # Cache for lookups
        self.team_cache: Dict[str, str] = {}
        self.player_cache: Dict[str, str] = {}
        self.agent_cache: Set[str] = set()
        
        # Known team variations
        self.team_aliases = {
            'fnc': 'fnatic',
            'prx': 'paper-rex',
            'nrg': 'nrg',
            'drx': 'drx',
            'th': 'team-heretics',
            'mibr': 'mibr',
            'g2': 'g2-esports',
            'gia': 'giantx',
        }
    
    def slugify(self, text: str) -> str:
        """Convert text to slug format."""
        text = text.lower()
        text = re.sub(r'[^\w\s-]', '', text)
        text = re.sub(r'[\s_]+', '-', text)
        text = text.strip('-')
        return text
    
    def normalize_team_name(self, team_name: str) -> str:
        """
        Normalize team name to team_id.
        Returns: team_id (slug format)
        """
        if not team_name or team_name.strip() == '':
            return None
            
        # Check cache
        if team_name in self.team_cache:
            return self.team_cache[team_name]
        
        # Generate slug
        team_slug = self.slugify(team_name)
        
        # Check for known aliases
        for alias, canonical in self.team_aliases.items():
            if alias in team_slug:
                team_slug = canonical
                break
        
        # Cache and return
        self.team_cache[team_name] = team_slug
        return team_slug
    
    def normalize_player_name(self, player_name: str, team_name: str = None) -> str:
        """
        Normalize player name to player_id.
        Returns: player_id (slug format, optionally with team suffix)
        """
        if not player_name or player_name.strip() == '':
            return None
        
        cache_key = f"{player_name}_{team_name}" if team_name else player_name
        
        # Check cache
        if cache_key in self.player_cache:
            return self.player_cache[cache_key]
        
        # Generate slug
        player_slug = self.slugify(player_name)
        
        # Add team suffix if provided
        if team_name:
            team_slug = self.normalize_team_name(team_name)
            if team_slug:
                player_slug = f"{player_slug}-{team_slug}"
        
        # Cache and return
        self.player_cache[cache_key] = player_slug
        return player_slug
    
    def normalize_agent_name(self, agent_name: str) -> str:
        """Normalize agent name to agent_id."""
        if not agent_name or agent_name.strip() == '':
            return None
        return self.slugify(agent_name)
    
    def ensure_team(self, team_name: str, region: str = None) -> str:
        """Ensure team exists in database, return team_id."""
        team_id = self.normalize_team_name(team_name)
        if not team_id:
            return None
        
        self.cursor.execute(
            "INSERT OR IGNORE INTO teams (team_id, team_name, region) VALUES (?, ?, ?)",
            (team_id, team_name, region)
        )
        return team_id
    
    def ensure_player(self, player_name: str, team_name: str = None) -> str:
        """Ensure player exists in database, return player_id."""
        player_id = self.normalize_player_name(player_name, team_name)
        if not player_id:
            return None
        
        team_id = self.normalize_team_name(team_name) if team_name else None
        
        self.cursor.execute(
            "INSERT OR IGNORE INTO players (player_id, player_name, current_team_id) VALUES (?, ?, ?)",
            (player_id, player_name, team_id)
        )
        return player_id
    
    def ensure_agent(self, agent_name: str) -> str:
        """Ensure agent exists in database, return agent_id."""
        agent_id = self.normalize_agent_name(agent_name)
        if not agent_id:
            return None
        
        if agent_id not in self.agent_cache:
            self.cursor.execute(
                "INSERT OR IGNORE INTO agents (agent_id, agent_name) VALUES (?, ?)",
                (agent_id, agent_name)
            )
            self.agent_cache.add(agent_id)
        
        return agent_id
    
    def parse_credits(self, credit_str: str) -> int:
        """
        Parse credit string to integer.
        Examples: '8.5k' -> 8500, '300' -> 300, '$$$' -> None
        """
        if not credit_str or credit_str.strip() == '':
            return None
        
        credit_str = credit_str.strip().lower()
        
        # Handle special symbols (VLR economy notation)
        if '$' in credit_str:
            return None  # Loss bonus markers, not actual credits
        
        # Handle 'k' notation
        if 'k' in credit_str:
            try:
                value = float(credit_str.replace('k', ''))
                return int(value * 1000)
            except ValueError:
                return None
        
        # Try direct integer
        try:
            return int(float(credit_str))
        except ValueError:
            return None
    
    def infer_buy_tier(self, credits: int) -> str:
        """Infer buy tier from credit amount."""
        if credits is None:
            return None
        
        if credits < 3000:
            return 'eco'
        elif credits < 5000:
            return 'semi'
        else:
            return 'full'
    
    def commit(self):
        """Commit transaction."""
        self.conn.commit()
