from utils import fetch_url
from bs4 import BeautifulSoup
import argparse
import os
import re
from urllib.parse import urljoin
from main_scrape import process_match
from datetime import datetime

def parse_event_year(soup):
    """
    Extract year from event dates by parsing .wf-subnav-item date strings.
    Falls back to current year if parsing fails.
    """
    # Try to find dates in subnav items (e.g., "Sep 25–Oct 5")
    date_elements = soup.select('.wf-subnav-item .ge-text-light')
    
    for elem in date_elements:
        date_text = elem.text.strip()
        # Look for month names followed by numbers
        month_pattern = r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d+'
        if re.search(month_pattern, date_text):
            # Extract year from date if present
            year_match = re.search(r'\b(20\d{2})\b', date_text)
            if year_match:
                return year_match.group(1)
    
    # Fallback: try to extract year from event title
    title_elem = soup.select_one('.wf-title')
    if title_elem:
        year_match = re.search(r'\b(20\d{2})\b', title_elem.text)
        if year_match:
            return year_match.group(1)
    
    # Ultimate fallback: use current year
    return str(datetime.now().year)

def get_event_stages(event_url):
    """
    Extract all stages from an event (Group Stage, Playoffs, etc.).
    Returns list of tuples: (stage_name, stage_url)
    """
    print(f"Fetching event stages from: {event_url}")
    response = fetch_url(event_url)
    if not response or response.status_code != 200:
        print(f"Failed to fetch event page.")
        return []
    
    soup = BeautifulSoup(response.text, 'html.parser')
    stages = []
    
    # Look for subnav items which contain stages
    subnav_items = soup.select('.wf-subnav-item')
    
    for item in subnav_items:
        stage_title_elem = item.select_one('.wf-subnav-item-title')
        if stage_title_elem:
            stage_name = stage_title_elem.text.strip()
            stage_href = item.get('href')
            
            if stage_href:
                # Clean stage name for folder
                clean_stage_name = re.sub(r'[\\/*?:"<>|]', '-', stage_name)
                clean_stage_name = re.sub(r'\s+', '_', clean_stage_name)
                
                full_stage_url = urljoin("https://www.vlr.gg", stage_href)
                stages.append((clean_stage_name, full_stage_url))
    
    return stages

def get_stage_matches(stage_url):
    """
    Get all match URLs from a specific stage page.
    """
    # Ensure URL ends with /matches
    if '/matches' not in stage_url:
        if stage_url.endswith('/'):
            stage_url += 'matches'
        else:
            stage_url += '/matches'
    
    print(f"Fetching matches from stage: {stage_url}")
    response = fetch_url(stage_url)
    if not response or response.status_code != 200:
        print(f"Failed to fetch stage page.")
        return []
    
    soup = BeautifulSoup(response.text, 'html.parser')
    match_urls = []
    
    # Find all match links
    links = soup.select('a[href*="/"]')
    seen_ids = set()
    
    for link in links:
        href = link.get('href')
        if not href:
            continue
        
        # Match pattern: /12345/team-vs-team
        match = re.search(r'^/(\d+)(/.*)?$', href)
        if match:
            match_id = match.group(1)
            
            if match_id in seen_ids:
                continue
            
            seen_ids.add(match_id)
            full_url = urljoin("https://www.vlr.gg", href)
            match_urls.append(full_url)
    
    return match_urls

def process_event(event_url, skip_types=None, check_existing=False):
    """
    Process a VCT event with multiple stages.
    """
    if skip_types is None:
        skip_types = []
    
    # Fetch main event page
    response = fetch_url(event_url)
    if not response or response.status_code != 200:
        print(f"Failed to fetch event page: {event_url}")
        return
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Extract event name
    event_header = soup.select_one('.wf-title')
    event_name = "Unknown_Event"
    if event_header:
        event_name = event_header.text.strip()
        event_name = re.sub(r'[\\/*?:"<>|]', '-', event_name)
        event_name = re.sub(r'\s+', '_', event_name)
    
    # Parse year
    year = parse_event_year(soup)
    print(f"Event: {event_name} | Year: {year}")
    
    # Get all stages
    stages = get_event_stages(event_url)
    
    if not stages:
        print(f"No stages found for event. Will try default /matches endpoint.")
        # Fallback to old behavior
        stages = [("Main_Event", event_url)]
    
    print(f"Found {len(stages)} stage(s): {[s[0] for s in stages]}")
    
    # Create base path: VCT Events/YEAR/EventName/
    vct_base = os.path.join(os.getcwd(), "VCT Events", year, event_name)
    
    # Process each stage
    for stage_name, stage_url in stages:
        print(f"\n===== Processing Stage: {stage_name} =====")
        
        # Get matches for this stage
        match_urls = get_stage_matches(stage_url)
        
        if not match_urls:
            print(f"No matches found for stage: {stage_name}")
            continue
        
        print(f"Found {len(match_urls)} matches in {stage_name}")
        
        # Create stage folder path
        stage_folder = os.path.join(vct_base, stage_name)
        if not os.path.exists(stage_folder):
            os.makedirs(stage_folder)
        
        # Process each match
        for i, url in enumerate(match_urls, 1):
            print(f"\nProcessing match {i}/{len(match_urls)}: {url}")
            try:
                process_match(url, skip_types, base_path=stage_folder, check_existing=check_existing)
            except Exception as e:
                print(f"Error scraping match {url}: {e}")

def main():
    parser = argparse.ArgumentParser(description='Scrape all matches from a VLR.gg event.')
    parser.add_argument('event_url', help='URL of the VLR.gg event')
    parser.add_argument('--skip', nargs='+', choices=['veto', 'stats', 'rounds', 'economy', 'performance'],
                        help='Skip specific data types')
    
    args = parser.parse_args()
    process_event(args.event_url, args.skip)

if __name__ == "__main__":
    main()
