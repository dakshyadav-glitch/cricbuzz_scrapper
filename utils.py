# Utility functions

import re


def clean_player_name(name):
    if not name:
        return ""
    roles = [
        "WK-Batter", "WK-Batsman", "Wicketkeeper", "Batting Allrounder", 
        "Bowling Allrounder", "Allrounder", "Batter", "Batsman", "Bowler", 
        "Spinner", "Pacer"
    ]
    cleaned = name.strip()
    for role in roles:
        cleaned = cleaned.replace(role, "")
    return " ".join(cleaned.split()).strip()


def get_designation(name):
    designation = []
    name_lower = name.lower() if name else ""
    if "(c)" in name_lower:
        designation.append("Captain")
    if "(wk)" in name_lower or "†" in name:
        designation.append("Wicketkeeper")
    return ", ".join(designation) if designation else "Player"


def remove_markers(name):
    if not name:
        return ""
    return (name.replace("(c)", "")
                .replace("(C)", "")
                .replace("(wk)", "")
                .replace("(WK)", "")
                .replace("†", "")
                .strip())


def is_valid_player_name(name):
    if not name or len(name) < 3:
        return False
    if name.replace(".", "").replace("-", "").isdigit():
        return False
    invalid = [
        "extras", "total", "did not bat", "fall of wickets", "batter", 
        "bowler", "not out", "run out", "yet to bat", "dnb", "info", 
        "commentary"
    ]
    if name.lower() in invalid:
        return False
    return any(c.isalpha() for c in name)


def parse_dismissal(text):
    if not text:
        return False
    patterns = [
        r"^c .* b ", r"^c & b ", r"^st .* b ", r"^lbw b ", 
        r"^b ", r"^run out", r"^not out$", r"^retired", r"^hit wicket"
    ]
    text_lower = text.lower().strip()
    return any(re.match(p, text_lower) for p in patterns)


def is_numeric(text):
    try:
        float(text)
        return True
    except:
        return False


def match_team_abbreviation(abbr, team1_name, team2_name, abbr_map):
    abbr_lower = abbr.lower()
    team1_lower = team1_name.lower()
    team2_lower = team2_name.lower()
    mapped_name = abbr_map.get(abbr_lower, abbr_lower)
    
    is_team1 = (
        abbr_lower in team1_lower or 
        team1_lower in abbr_lower or 
        mapped_name in team1_lower or 
        team1_lower in mapped_name or
        abbr_lower == team1_lower[:3]
    )
    
    is_team2 = (
        abbr_lower in team2_lower or 
        team2_lower in abbr_lower or
        mapped_name in team2_lower or 
        team2_lower in mapped_name or
        abbr_lower == team2_lower[:3]
    )
    
    if is_team1:
        return 'team1'
    elif is_team2:
        return 'team2'
    return None


def extract_score_from_text(text):
    # Pattern: runs/wickets(overs)
    score_match = re.match(r'^(\d+)[/-](\d+)\s*\(([\d.]+)\)?', text)
    if score_match:
        return f"{score_match.group(1)}/{score_match.group(2)} ({score_match.group(3)} Ov)"
    
    # All out format: runs(overs)
    all_out_match = re.match(r'^(\d+)\(([\d.]+)\)$', text)
    if all_out_match:
        return f"{all_out_match.group(1)}/10 ({all_out_match.group(2)} Ov)"
    
    return None
