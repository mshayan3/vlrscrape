import requests
from bs4 import BeautifulSoup

url = "https://www.vlr.gg/events/completed?tier=60&page=1"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

try:
    print(f"Fetching {url}...")
    response = requests.get(url, headers=headers, timeout=10)
    print(f"Status Code: {response.status_code}")
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        print(f"Page Title: {soup.title.string.strip() if soup.title else 'No Title'}")
        
        events = soup.select('a.event-item')
        print(f"Found {len(events)} events.")
        if events:
            print(f"First event: {events[0].get('href')}")
    else:
        print("Response content (first 500 chars):")
        print(response.text[:500])
except Exception as e:
    print(f"Error: {e}")
