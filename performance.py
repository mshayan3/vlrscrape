import requests
from bs4 import BeautifulSoup
import pandas as pd

# URL of the webpage to scrape
url = 'https://www.vlr.gg/428005/100-thieves-vs-nrg-esports-champions-tour-2025-americas-kickoff-lr1/?game=all&tab=overview'

# Send a GET request to the webpage
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
}
response = requests.get(url, headers=headers)
response.raise_for_status()  # Ensure the request was successful

# Parse the webpage content
soup = BeautifulSoup(response.text, 'html.parser')

# Find the div with class 'vm-stats-container'
stats_container = soup.find('div', class_='vm-stats-container')

if not stats_container:
    print("No stats container found. The structure of the website may have changed.")
    exit()

# Initialize a list to store DataFrames
tables = []

# Iterate over all tables within the stats container
for i, table in enumerate(stats_container.find_all('table')):
    # Extract table headers
    headers = [th.get_text(strip=True) for th in table.find_all('th')]

    if not headers:  # If no headers are found, use generic column names
        headers = [f"Column {j+1}" for j in range(len(table.find_all('tr')[0].find_all(['td', 'th'])))]

    # Extract table rows
    rows = []
    for tr in table.find_all('tr'):
        cells = tr.find_all(['td', 'th'])
        
        # Use " ".join(cell.stripped_strings) to preserve spacing correctly
        row = [" ".join(cell.stripped_strings) for cell in cells]

        if row:
            rows.append(row)

    # Ensure headers and rows have the same number of columns
    max_cols = max(len(row) for row in rows)
    headers = headers[:max_cols]  # Trim headers if necessary
    for row in rows:
        while len(row) < max_cols:
            row.append('')  # Fill missing values with empty strings

    # Create a DataFrame with unique column names
    df = pd.DataFrame(rows, columns=[f"{col}_{i}" for col in headers])
    tables.append(df)

# Concatenate all DataFrames with reset_index to avoid duplicate indices
if tables:
    final_df = pd.concat(tables, axis=1)  # Use `axis=1` to merge tables side-by-side
    final_df.to_csv('test_data.csv', index=False)
    print('Data has been successfully extracted and saved to extracted_data.csv')
else:
    print("No tables found in the stats container.")
