import argparse
from bs4 import BeautifulSoup
from utils import fetch_url
from scrape_event import process_event
import os
import re

def scrape_global(start_page=1, end_page=6, check_existing=True):
    """
    Iterates through VLR events pages and triggers scraping for each.
    """
    # tier=60 targets VCT (Tier 1) events, region=all for global coverage
    base_url = "https://www.vlr.gg/events/?tier=60&region=all&page="
    
    for page in range(start_page, end_page + 1):
        print(f"\n===== Scanning Events Page {page} =====")
        url = base_url + str(page)
        
        response = fetch_url(url)
        if not response:
            print(f"Skipping page {page} due to fetch failure.")
            continue
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Selector for event items
        event_links = soup.select('a.event-item')
        
        print(f"Found {len(event_links)} events on page {page}.")
        
        for link in event_links:
            href = link.get('href')
            if not href:
                continue
            
            # Construct full URL
            # href is typically /event/1234/event-name
            full_event_url = f"https://www.vlr.gg{href}"
            
            cleaned_title = "Unknown Event"
            title_div = link.select_one('.event-item-title')
            if title_div:
                cleaned_title = title_div.text.strip()
            
            print(f"Global: Discovered event {cleaned_title} ({full_event_url})")
            
            try:
                # We enable check_existing to skip matches we already have
                process_event(full_event_url, check_existing=check_existing)
            except Exception as e:
                print(f"Error processing event {full_event_url}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Mass scrape VLR events.')
    parser.add_argument('--start', type=int, default=1, help='Start page number')
    parser.add_argument('--end', type=int, default=5, help='End page number')
    parser.add_argument('--force', action='store_true', help='Force re-scrape (disable check_existing)')
    
    args = parser.parse_args()
    
    scrape_global(args.start, args.end, check_existing=(not args.force))
