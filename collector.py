# Match URL collector

import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from config import RECENT_URL, WAIT_TIME, SKIP_PATTERNS, LIVE_INDICATORS, COMPLETED_INDICATORS


def collect_international_matches(driver, wait):
    print("\nOpening Recent Matches page...")
    driver.get(RECENT_URL)
    time.sleep(WAIT_TIME)
    
    print("Clicking International filter...")
    try:
        intl_btn = wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//div[contains(text(), 'International')]")
        ))
        intl_btn.click()
        print("International filter clicked!")
        time.sleep(5)
    except Exception as e:
        print(f"Could not click filter: {e}")
    
    print("Collecting COMPLETED International matches...")
    
    match_urls = []
    all_links = driver.find_elements(By.XPATH, "//a[contains(@href, '/live-cricket-scores/')]")
    
    for link in all_links:
        try:
            url = _extract_valid_url(link)
            if url and url not in match_urls:
                match_urls.append(url)
                short_name = url.split('/')[-1][:50]
                print(f"    {short_name}")
        except:
            continue
    
    print(f"\nFound {len(match_urls)} COMPLETED international matches")
    return match_urls


def _extract_valid_url(link):
    href = link.get_attribute("href")
    link_text = link.text.strip() if link.text else ""
    
    if not link_text or len(link_text) < 20:
        return None
    
    if "preview" in link_text.lower():
        return None
    
    if any(word in link_text.lower() for word in LIVE_INDICATORS):
        return None
    
    is_completed = any(word in link_text.lower() for word in COMPLETED_INDICATORS)
    
    if not (href and "/live-cricket-scores/" in href and is_completed):
        return None
    
    if _should_skip_match(href, link_text):
        return None
    
    return href


def _should_skip_match(href, link_text):
    href_lower = href.lower()
    text_lower = link_text.lower()
    
    for pattern in SKIP_PATTERNS['url']:
        if pattern in href_lower:
            return True
    