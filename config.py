# Config settings

# URLs
RECENT_URL = "https://www.cricbuzz.com/cricket-match/live-scores/recent-matches"

# Output files
OUTPUT_FILE = "international_data.json"
DATABASE_FILE = "cricket_warehouse.db"

# Timing
WAIT_TIME = 3
PAGE_LOAD_TIMEOUT = 30
WEBDRIVER_WAIT = 15

# Team abbreviation mappings
TEAM_ABBREVIATIONS = {
    'ind': 'india', 
    'nz': 'new zealand', 
    'aus': 'australia', 
    'eng': 'england',
    'pak': 'pakistan', 
    'sa': 'south africa', 
    'sl': 'sri lanka', 
    'ban': 'bangladesh',
    'wi': 'west indies', 
    'afg': 'afghanistan', 
    'zim': 'zimbabwe', 
    'ire': 'ireland',
    'ita': 'italy', 
    'sco': 'scotland', 
    'ned': 'netherlands', 
    'uae': 'uae',
    'nam': 'namibia', 
    'oma': 'oman', 
    'usa': 'usa', 
    'can': 'canada', 
    'nep': 'nepal'
}

# Skip patterns for match filtering
SKIP_PATTERNS = {
    'url': ['u19', 'under-19', 'legends', 'womens'],
    'text': ['under 19', 'legends']
}

# Live match indicators (skip these)
LIVE_INDICATORS = ['opt to', 'need ', 'trail', 'lead', ' - live', 'day ', 'session']

# Completed match indicators
COMPLETED_INDICATORS = [' won', ')', 'wickets', 'runs']
