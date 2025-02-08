import json
import time
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional, Union
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from functools import wraps
from concurrent.futures import ThreadPoolExecutor

# Constants
WAIT_TIME = 20
PAGE_LOAD_TIME = 5
BASE_URL = 'https://preview.valobox.space/?VSIKI5'


@dataclass
class PlayerDetails:
    IGN: str
    Nickname: str
    Title: str
    Region: str
    Level: str
    Country: str
    Total_Value: str
    Rank: str


def retry_on_failure(retries=3, delay=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == retries - 1:
                        print(f"❌ {func.__name__} failed after {retries} attempts: {e}")
                        return {}
                    time.sleep(delay)
            return {}

        return wrapper

    return decorator


class ValoboxScraper:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.setup_driver()

    def setup_driver(self):
        """Initialize the Chrome WebDriver with optimized settings."""
        options = Options()
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-infobars')
        options.add_argument('--headless')
        options.add_argument('--window-size=1920,1080')
        options.add_experimental_option('excludeSwitches', ['enable-automation'])
        options.add_experimental_option('useAutomationExtension', False)

        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        options.add_argument(f'user-agent={user_agent}')

        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        self.wait = WebDriverWait(self.driver, WAIT_TIME)

    def safe_find_element(self, by: By, value: str) -> Optional[str]:
        """Safely find and extract text from an element."""
        try:
            element = self.driver.find_element(by, value)
            return element.text.strip()
        except NoSuchElementException:
            return None

    def safe_find_elements(self, by: By, value: str) -> List[str]:
        """Safely find and extract text from multiple elements."""
        try:
            elements = self.driver.find_elements(by, value)
            return [element.text.strip() for element in elements]
        except NoSuchElementException:
            return []

    @retry_on_failure()
    def scrape_details(self) -> Dict[str, str]:
        """Scrape player details with enhanced error handling."""
        # Find all elements with id="ign"
        ign_elements = self.safe_find_elements(By.ID, "ign")

        # The first element is IGN, the second is Nickname
        ign = ign_elements[0] if len(ign_elements) > 0 else "N/A"
        nickname = ign_elements[1] if len(ign_elements) > 1 else "N/A"

        details = PlayerDetails(
            IGN=ign,
            Nickname=nickname,
            Title=self.safe_find_element(By.ID, "title") or "N/A",
            Region=self.safe_find_element(By.ID, "region") or "N/A",
            Level=self.safe_find_element(By.CLASS_NAME, "circle-inner") or "N/A",
            Country=self.safe_find_element(By.ID, "country") or "N/A",
            Total_Value=self.safe_find_element(By.ID, "totalvalue") or "N/A",
            Rank=self.safe_find_element(By.ID, "currentRank") or "N/A"
        )
        return asdict(details)

    def navigate_to_page(self, page_name: str) -> bool:
        """Navigate to a specific page using the menu buttons."""
        try:
            button = self.wait.until(EC.element_to_be_clickable(
                (By.XPATH, f"//a[contains(@data-page, '{page_name}')]")
            ))
            button.click()
            time.sleep(PAGE_LOAD_TIME)
            return True
        except TimeoutException as e:
            print(f"❌ Failed to navigate to {page_name}: {e}")
            return False

    def get_collection_data(self, page_name: str, card_class: str,
                            additional_fields: List[str] = None) -> Dict[str, Any]:
        """Generic method to scrape collection data with simplified output for name-only items."""
        if not self.navigate_to_page(page_name):
            return {}

        try:
            # Get all skin-spent elements
            spent_elements = self.driver.find_elements(By.CLASS_NAME, "skin-spent")
            result = {}

            # Handle skins and battlepass pages differently
            if page_name in ['skins', 'battlepass']:
                if len(spent_elements) >= 2:
                    total_items = spent_elements[0].text.split(":")[-1].strip() if spent_elements[0].text else "0"
                    total_vp = spent_elements[1].text.split(":")[-1].strip() if spent_elements[1].text else "0"
                    result[f"Total {page_name.capitalize()}"] = total_items
                    result["Total VP Spent"] = total_vp
                else:
                    result[f"Total {page_name.capitalize()}"] = "0"
                    result["Total VP Spent"] = "0"
            else:
                # For other pages, just get the total count
                total_text = spent_elements[0].text if spent_elements else ""
                total_count = total_text.split(":")[-1].strip() if total_text else "0"
                result[f"Total {page_name.capitalize()}"] = total_count

            # Get items
            elements = self.driver.find_elements(By.CLASS_NAME, card_class)

            if additional_fields:
                # For items with additional fields
                items = []
                if isinstance(additional_fields, list):
                    # Handle skins
                    for element in elements:
                        item_data = {"name": element.find_element(By.CLASS_NAME, "name").text.strip()}
                        for field in additional_fields:
                            try:
                                value = element.find_element(By.CLASS_NAME, field).text.strip()
                                item_data[field] = value
                            except NoSuchElementException:
                                item_data[field] = "N/A"
                        items.append(item_data)
                else:
                    # Handle battlepass (additional_fields is a string 'act')
                    for element in elements:
                        try:
                            act = element.find_element(By.CLASS_NAME, "act").text.strip()
                            name = element.find_element(By.CLASS_NAME, "name").text.strip()
                            items.append({"act": act, "name": name})
                        except NoSuchElementException:
                            continue
            else:
                # For simple items (like buddies, cards, etc.), just store the names
                items = [element.find_element(By.CLASS_NAME, "name").text.strip()
                         for element in elements]

            result[page_name] = items
            return result

        except Exception as e:
            print(f"❌ Failed to scrape {page_name}: {e}")
            return {}

    def scrape_all_data(self) -> Dict[str, Any]:
        """Scrape all data concurrently where possible."""
        scraped_data = {}

        try:
            self.driver.get(BASE_URL)
            time.sleep(PAGE_LOAD_TIME)

            # Scrape details first as they're on the main page
            scraped_data['details'] = self.scrape_details()

            # Define scraping tasks with additional fields only where needed
            scraping_tasks = {
                'skins': ('skins', 'skin-card', ['price', 'tier']),  # List for multiple fields
                'battlepass': ('battlepass', 'battlepass-card', 'act'),  # String for single field
                'buddies': ('buddies', 'buddy-card', None),  # Simple name list
                'cards': ('cards', 'card', None),  # Simple name list
                'sprays': ('sprays', 'spray-card', None),  # Simple name list
                'titles': ('titles', 'title-card', None),  # Simple name list
                'agents': ('agents', 'agent-card', None)  # Simple name list
            }

            # Execute scraping tasks
            with ThreadPoolExecutor(max_workers=1) as executor:
                future_to_task = {
                    category: executor.submit(self.get_collection_data, page_name, card_class, additional_fields)
                    for category, (page_name, card_class, additional_fields) in scraping_tasks.items()
                }

                for category, future in future_to_task.items():
                    try:
                        scraped_data[category] = future.result()
                    except Exception as e:
                        print(f"❌ Error scraping {category}: {e}")
                        scraped_data[category] = {}

            return scraped_data

        except Exception as e:
            print(f"❌ Failed to scrape data: {e}")
            return {}

        finally:
            self.driver.quit()


def main():
    scraper = ValoboxScraper()
    scraped_data = scraper.scrape_all_data()

    with open("scraped_data.json", "w", encoding="utf-8") as json_file:
        json.dump(scraped_data, json_file, indent=4, ensure_ascii=False)

    print("✅ Data saved successfully to scraped_data.json")


if __name__ == "__main__":
    main()