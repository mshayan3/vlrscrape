import requests
import time
from threading import Lock

# Rate limiting configuration
REQUEST_DELAY = 0.2  # 5 requests per second = 1 / 5 = 0.2s delay
LAST_REQUEST_TIME = 0
LOCK = Lock()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def fetch_url(url, retries=3):
    """
    Fetches a URL with rate limiting and retry logic.
    """
    global LAST_REQUEST_TIME
    
    with LOCK:
        current_time = time.time()
        elapsed = current_time - LAST_REQUEST_TIME
        if elapsed < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - elapsed)
        LAST_REQUEST_TIME = time.time()
    
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS)
            if response.status_code == 200:
                return response
            elif response.status_code == 429:
                print(f"Rate limited (429). Waiting longer... (Attempt {attempt+1}/{retries})")
                time.sleep(2 * (attempt + 1)) # Exponential backoffish
            else:
                print(f"Failed to fetch {url}. Status: {response.status_code}")
                # Don't retry on 404
                if response.status_code == 404:
                    return response
        except requests.RequestException as e:
            print(f"Request exception: {e}")
            
        time.sleep(1) # Wait a bit before retry
        
    return None
