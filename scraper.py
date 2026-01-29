# Match scraper

import time
from selenium.webdriver.common.by import By

from config import WAIT_TIME
from extractors import (
    create_empty_match_data,
    extract_title_and_teams,
    extract_scores,
    extract_result,
    extract_player_of_match,
    extract_match_facts,
    extract_playing_xi,
    extract_scorecard
)


def scrape_match(driver, match_url):
    match_data = create_empty_match_data(match_url)
    
    # Get match page
    driver.get(match_url)
    time.sleep(WAIT_TIME)
    
    page_text = driver.find_element(By.TAG_NAME, "body").text
    lines = [l.strip() for l in page_text.split('\n') if l.strip()]
    
    # Extract data
    extract_title_and_teams(driver, match_data)
    extract_scores(lines, match_data)
    extract_result(lines, match_data)
    extract_player_of_match(lines, match_data)
    extract_match_facts(driver, match_url, match_data)
    extract_playing_xi(driver, match_url, match_data)
    extract_scorecard(driver, match_url, match_data)
    
    return match_data
