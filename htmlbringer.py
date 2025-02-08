import requests
from bs4 import BeautifulSoup

MATCH_URL = "https://preview.valobox.space/?VCyqZx"

# Headers to mimic a real browser request
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}


def fetch_html(url):
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        return response.text
    else:
        print("Failed to fetch the webpage.")
        return None


# Fetch the HTML content
html_content = fetch_html(MATCH_URL)

if html_content:
    # Parse the HTML
    soup = BeautifulSoup(html_content, 'html.parser')

    # Save the HTML content to a text file
    with open("match_page.html", "w", encoding="utf-8") as file:
        file.write(soup.prettify())

    print("HTML content saved to 'match_page.html'")
else:
    print("No HTML content to save.")