# Main entry point

import json
import time

from config import OUTPUT_FILE
from driver import driver_manager
from collector import collect_international_matches
from scraper import scrape_match


def main():
    print("CRICBUZZ SCRAPER")
    print("=" * 60)
    
    try:
        # Setup WebDriver
        driver, wait = driver_manager.setup()
        
        # Collect match URLs
        match_urls = collect_international_matches(driver, wait)
        
        if not match_urls:
            print("\nNo matches found to scrape.")
            return
        
        # Scrape each match
        all_matches = scrape_all_matches(driver, match_urls)
        
        # Save results
        save_results(all_matches)
        
    except Exception as e:
        print(f"\nError: {e}")
    finally:
        driver_manager.quit()


def scrape_all_matches(driver, match_urls):
    all_matches = []
    
    for idx, url in enumerate(match_urls, start=1):
        print(f"\n[{idx}/{len(match_urls)}] Scraping...")
        
        try:
            match_data = scrape_match(driver, url)
            print_match_summary(match_data)
            all_matches.append(match_data)
            
        except Exception as e:
            print(f"    Error: {str(e)[:60]}")
        
        time.sleep(2)
    
    return all_matches


def print_match_summary(match_data):
    info = match_data["match_info"]
    
    print(f"    {match_data['match_title'][:50]}...")
    print(f"    {info['team1_name']} {info['team1_score']} vs {info['team2_name']} {info['team2_score']}")
    print(f"    Winner: {info['winner']} | POTM: {info['player_of_match']}")
    


def save_results(all_matches):
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_matches, f, indent=4, ensure_ascii=False)
    
    print("\n" + "=" * 60)
    print("SCRAPING COMPLETE!")
    print("=" * 60)
    print(f"Total matches: {len(all_matches)}")
    print(f"Saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
