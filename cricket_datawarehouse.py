# Cricket Data Warehouse - Star Schema ETL

import sqlite3
import json
import re
from pathlib import Path


class CricketDataWarehouse:
    def __init__(self, db_path="cricket_warehouse.db"):
        self.db_path = db_path
        self.conn = None
        
    def connect(self):
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        return self.conn.cursor()
    
    def close(self):
        if self.conn:
            self.conn.close()
            
    def create_schema(self):
        cursor = self.connect()
        
        # Dimension: Teams
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_teams (
                team_id INTEGER PRIMARY KEY AUTOINCREMENT,
                team_name TEXT UNIQUE NOT NULL
            )
        """)
        
        # Dimension: Players
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_players (
                player_id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_name TEXT UNIQUE NOT NULL,
                team_id INTEGER,
                FOREIGN KEY (team_id) REFERENCES dim_teams(team_id)
            )
        """)
        
        # Dimension: Venues
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_venues (
                venue_id INTEGER PRIMARY KEY AUTOINCREMENT,
                venue_name TEXT,
                city TEXT,
                full_venue TEXT UNIQUE
            )
        """)
        
        # Dimension: Match Types
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_match_types (
                match_type_id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_type TEXT UNIQUE NOT NULL,
                overs_limit INTEGER,
                description TEXT
            )
        """)
        
        # Seed match types
        match_types = [
            ('T20I', 20, 'Twenty20 International'),
            ('ODI', 50, 'One Day International'),
            ('Test', None, 'Test Match'),
            ('T20', 20, 'Twenty20')
        ]
        cursor.executemany(
            "INSERT OR IGNORE INTO dim_match_types (match_type, overs_limit, description) VALUES (?, ?, ?)",
            match_types
        )
        
        # Fact: Matches
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fact_matches (
                match_id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_key TEXT UNIQUE,
                match_title TEXT,
                team1_id INTEGER,
                team2_id INTEGER,
                team1_score TEXT,
                team1_runs INTEGER,
                team1_wickets INTEGER,
                team1_overs REAL,
                team2_score TEXT,
                team2_runs INTEGER,
                team2_wickets INTEGER,
                team2_overs REAL,
                winner_id INTEGER,
                result TEXT,
                potm_player_id INTEGER,
                venue_id INTEGER,
                match_type_id INTEGER,
                FOREIGN KEY (team1_id) REFERENCES dim_teams(team_id),
                FOREIGN KEY (team2_id) REFERENCES dim_teams(team_id),
                FOREIGN KEY (winner_id) REFERENCES dim_teams(team_id),
                FOREIGN KEY (potm_player_id) REFERENCES dim_players(player_id),
                FOREIGN KEY (venue_id) REFERENCES dim_venues(venue_id),
                FOREIGN KEY (match_type_id) REFERENCES dim_match_types(match_type_id)
            )
        """)
        
        # Fact: Batting Performance
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fact_batting (
                batting_id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id INTEGER NOT NULL,
                player_id INTEGER NOT NULL,
                team_id INTEGER,
                innings_number INTEGER,
                batting_position INTEGER,
                runs INTEGER DEFAULT 0,
                balls INTEGER DEFAULT 0,
                fours INTEGER DEFAULT 0,
                sixes INTEGER DEFAULT 0,
                strike_rate REAL DEFAULT 0,
                dismissal_type TEXT,
                is_not_out BOOLEAN DEFAULT 0,
                FOREIGN KEY (match_id) REFERENCES fact_matches(match_id),
                FOREIGN KEY (player_id) REFERENCES dim_players(player_id),
                FOREIGN KEY (team_id) REFERENCES dim_teams(team_id)
            )
        """)
        
        # Fact: Bowling Performance
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fact_bowling (
                bowling_id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id INTEGER NOT NULL,
                player_id INTEGER NOT NULL,
                team_id INTEGER,
                innings_number INTEGER,
                overs REAL DEFAULT 0,
                maidens INTEGER DEFAULT 0,
                runs_conceded INTEGER DEFAULT 0,
                wickets INTEGER DEFAULT 0,
                economy REAL DEFAULT 0,
                FOREIGN KEY (match_id) REFERENCES fact_matches(match_id),
                FOREIGN KEY (player_id) REFERENCES dim_players(player_id),
                FOREIGN KEY (team_id) REFERENCES dim_teams(team_id)
            )
        """)
        
        # Fact: Playing XI
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fact_playing_xi (
                playing_xi_id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id INTEGER NOT NULL,
                team_id INTEGER NOT NULL,
                player_id INTEGER NOT NULL,
                designation TEXT,
                FOREIGN KEY (match_id) REFERENCES fact_matches(match_id),
                FOREIGN KEY (team_id) REFERENCES dim_teams(team_id),
                FOREIGN KEY (player_id) REFERENCES dim_players(player_id),
                UNIQUE(match_id, team_id, player_id)
            )
        """)
        
        # Performance indexes
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_batting_match ON fact_batting(match_id)",
            "CREATE INDEX IF NOT EXISTS idx_batting_player ON fact_batting(player_id)",
            "CREATE INDEX IF NOT EXISTS idx_bowling_match ON fact_bowling(match_id)",
            "CREATE INDEX IF NOT EXISTS idx_bowling_player ON fact_bowling(player_id)",
            "CREATE INDEX IF NOT EXISTS idx_matches_winner ON fact_matches(winner_id)",
            "CREATE INDEX IF NOT EXISTS idx_playing_xi_match ON fact_playing_xi(match_id)"
        ]
        for idx in indexes:
            cursor.execute(idx)
        
        self.conn.commit()
        print("Schema created successfully")
        
    def get_or_create_team(self, cursor, team_name):
        if not team_name:
            return None
        team_name = team_name.strip().title()
        cursor.execute("SELECT team_id FROM dim_teams WHERE team_name = ?", (team_name,))
        row = cursor.fetchone()
        if row:
            return row[0]
        cursor.execute("INSERT INTO dim_teams (team_name) VALUES (?)", (team_name,))
        return cursor.lastrowid
    
    def get_or_create_player(self, cursor, player_name, team_id=None):
        if not player_name:
            return None
        player_name = player_name.strip()
        
        cursor.execute("SELECT player_id, team_id FROM dim_players WHERE player_name = ?", (player_name,))
        row = cursor.fetchone()
        
        if row:
            player_id, existing_team_id = row
            if existing_team_id is None and team_id is not None:
                cursor.execute("UPDATE dim_players SET team_id = ? WHERE player_id = ?", (team_id, player_id))
            return player_id
        
        cursor.execute("INSERT INTO dim_players (player_name, team_id) VALUES (?, ?)", (player_name, team_id))
        return cursor.lastrowid
    
    def get_or_create_venue(self, cursor, venue_text):
        if not venue_text:
            return None
        venue_text = venue_text.strip()
        cursor.execute("SELECT venue_id FROM dim_venues WHERE full_venue = ?", (venue_text,))
        row = cursor.fetchone()
        if row:
            return row[0]
        
        parts = venue_text.split(',')
        venue_name = parts[0].strip() if parts else venue_text
        city = parts[-1].strip() if len(parts) > 1 else None
        
        cursor.execute("INSERT INTO dim_venues (venue_name, city, full_venue) VALUES (?, ?, ?)",
                      (venue_name, city, venue_text))
        return cursor.lastrowid
    
    def get_match_type_id(self, cursor, match_title):
        if not match_title:
            return None
        title_lower = match_title.lower()
        
        if 't20i' in title_lower or 't20' in title_lower:
            match_type = 'T20I'
        elif 'odi' in title_lower:
            match_type = 'ODI'
        elif 'test' in title_lower:
            match_type = 'Test'
        else:
            match_type = 'T20'
        
        cursor.execute("SELECT match_type_id FROM dim_match_types WHERE match_type = ?", (match_type,))
        row = cursor.fetchone()
        return row[0] if row else None
    
    def parse_score(self, score_text):
        if not score_text:
            return None, None, None
        
        match = re.search(r'(\d+)/(\d+)\s*\(?([\d.]+)?\s*[Oo]v\)?', str(score_text))
        if match:
            return int(match.group(1)), int(match.group(2)), float(match.group(3)) if match.group(3) else None
        
        match = re.search(r'(\d+)', str(score_text))
        if match:
            return int(match.group(1)), None, None
        
        return None, None, None
    
    def parse_batting_stats(self, entry):
        runs = self._safe_int(entry.get('runs', 0))
        balls = self._safe_int(entry.get('balls', 0))
        fours = self._safe_int(entry.get('fours', entry.get('4s', 0)))
        sixes = self._safe_int(entry.get('sixes', entry.get('6s', 0)))
        sr = self._safe_float(entry.get('strike_rate', entry.get('sr', 0)))
        
        dismissal = entry.get('dismissal', '')
        is_not_out = 'not out' in str(dismissal).lower() if dismissal else False
        
        return runs, balls, fours, sixes, sr, is_not_out, dismissal
    
    def parse_bowling_stats(self, entry):
        overs = self._safe_float(entry.get('overs', 0))
        maidens = self._safe_int(entry.get('maidens', 0))
        runs = self._safe_int(entry.get('runs', 0))
        wickets = self._safe_int(entry.get('wickets', 0))
        economy = self._safe_float(entry.get('economy', 0))
        return overs, maidens, runs, wickets, economy
    
    def _safe_int(self, val):
        try:
            return int(val)
        except:
            return 0
    
    def _safe_float(self, val):
        try:
            return float(val)
        except:
            return 0.0
    
    def load_json_data(self, json_path):
        cursor = self.connect()
        
        with open(json_path, 'r', encoding='utf-8') as f:
            matches = json.load(f)
        
        print(f"Loading {len(matches)} matches...")
        loaded, skipped = 0, 0
        
        for match in matches:
            match_info = match.get('match_info', {})
            playing_11 = match.get('playing_11', {})
            scorecard = match.get('scorecard', [])
            
            match_key = match.get('match_url', f"{match_info.get('team1_name')}_{match_info.get('team2_name')}_{match.get('match_title')}")
            match_title = match.get('match_title', match_info.get('match_title', 'Unknown'))
            
            # Skip duplicates
            cursor.execute("SELECT match_id FROM fact_matches WHERE match_key = ?", (match_key,))
            if cursor.fetchone():
                skipped += 1
                continue
            
            # Get dimension IDs
            team1_id = self.get_or_create_team(cursor, match_info.get('team1_name'))
            team2_id = self.get_or_create_team(cursor, match_info.get('team2_name'))
            winner_id = self.get_or_create_team(cursor, match_info.get('winner'))
            venue_id = self.get_or_create_venue(cursor, match_info.get('venue'))
            match_type_id = self.get_match_type_id(cursor, match.get('match_title', ''))
            
            # Parse scores
            t1_runs, t1_wkts, t1_overs = self.parse_score(match_info.get('team1_score'))
            t2_runs, t2_wkts, t2_overs = self.parse_score(match_info.get('team2_score'))
            
            # Get POTM
            potm_name = match_info.get('player_of_match', '')
            potm_id = self.get_or_create_player(cursor, potm_name, winner_id) if potm_name else None
            
            # Insert match
            cursor.execute("""
                INSERT INTO fact_matches (
                    match_key, match_title, team1_id, team2_id,
                    team1_score, team1_runs, team1_wickets, team1_overs,
                    team2_score, team2_runs, team2_wickets, team2_overs,
                    winner_id, result, potm_player_id, venue_id, match_type_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                match_key, match_title, team1_id, team2_id,
                match_info.get('team1_score'), t1_runs, t1_wkts, t1_overs,
                match_info.get('team2_score'), t2_runs, t2_wkts, t2_overs,
                winner_id, match_info.get('result'), potm_id, venue_id, match_type_id
            ))
            match_id = cursor.lastrowid
            
            # Build player-team mapping from Playing XI
            player_team_map = {}
            
            for team_key in ['team1', 'team2']:
                team_id = team1_id if team_key == 'team1' else team2_id
                team_data = playing_11.get(team_key, {})
                
                players = team_data.get('players', []) if isinstance(team_data, dict) else (team_data if isinstance(team_data, list) else [])
                
                for player_entry in players:
                    if isinstance(player_entry, dict):
                        player_name = player_entry.get('name', '')
                        designation = player_entry.get('designation', 'Player')
                    else:
                        player_name = str(player_entry) if player_entry else ''
                        designation = 'Player'
                    
                    if player_name:
                        player_team_map[player_name.strip().lower()] = team_id
                        player_id = self.get_or_create_player(cursor, player_name, team_id)
                        cursor.execute("""
                            INSERT OR IGNORE INTO fact_playing_xi (match_id, team_id, player_id, designation)
                            VALUES (?, ?, ?, ?)
                        """, (match_id, team_id, player_id, designation))
            
            # Load batting data
            for inn_idx, innings in enumerate(scorecard, 1):
                for bat_pos, bat_entry in enumerate(innings.get('batting', []), 1):
                    player_name = bat_entry.get('batsman', bat_entry.get('player', ''))
                    if not player_name:
                        continue
                    
                    player_key = player_name.strip().lower()
                    bat_team_id = player_team_map.get(player_key, team1_id if inn_idx % 2 == 1 else team2_id)
                    player_id = self.get_or_create_player(cursor, player_name, bat_team_id)
                    
                    runs, balls, fours, sixes, sr, is_not_out, dismissal = self.parse_batting_stats(bat_entry)
                    
                    cursor.execute("""
                        INSERT INTO fact_batting (
                            match_id, player_id, team_id, innings_number, batting_position,
                            runs, balls, fours, sixes, strike_rate, dismissal_type, is_not_out
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (match_id, player_id, bat_team_id, inn_idx, bat_pos,
                          runs, balls, fours, sixes, sr, dismissal, is_not_out))
            
            # Load bowling data
            for inn_idx, innings in enumerate(scorecard, 1):
                for bowl_entry in innings.get('bowling', []):
                    player_name = bowl_entry.get('bowler', bowl_entry.get('player', ''))
                    if not player_name:
                        continue
                    
                    player_key = player_name.strip().lower()
                    bowl_team_id = player_team_map.get(player_key, team2_id if inn_idx % 2 == 1 else team1_id)
                    player_id = self.get_or_create_player(cursor, player_name, bowl_team_id)
                    
                    overs, maidens, runs, wickets, economy = self.parse_bowling_stats(bowl_entry)
                    
                    cursor.execute("""
                        INSERT INTO fact_bowling (
                            match_id, player_id, team_id, innings_number,
                            overs, maidens, runs_conceded, wickets, economy
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (match_id, player_id, bowl_team_id, inn_idx,
                          overs, maidens, runs, wickets, economy))
            
            loaded += 1
        
        self.conn.commit()
        print(f"Loaded: {loaded}, Skipped: {skipped}")
    
    def print_summary(self):
        cursor = self.connect()
        
        print("\n" + "="*50)
        print("DATA WAREHOUSE SUMMARY")
        print("="*50)
        
        tables = [
            ('dim_teams', 'Teams'),
            ('dim_players', 'Players'),
            ('dim_venues', 'Venues'),
            ('fact_matches', 'Matches'),
            ('fact_batting', 'Batting Records'),
            ('fact_bowling', 'Bowling Records'),
            ('fact_playing_xi', 'Playing XI Records')
        ]
        
        for table, label in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  {label:25}: {count:,}")
        
        print("="*50)


def main():
    print("\n" + "="*50)
    print("CRICKET DATA WAREHOUSE - ETL")
    print("="*50)
    
    warehouse = CricketDataWarehouse("cricket_warehouse.db")
    
    print("\nCreating schema...")
    warehouse.create_schema()
    
    json_file = "international_data.json"
    if Path(json_file).exists():
        warehouse.load_json_data(json_file)
        warehouse.print_summary()
    else:
        print(f"Error: {json_file} not found")
        return
    
    warehouse.close()
    print("\nData warehouse ready")


if __name__ == "__main__":
    main()
