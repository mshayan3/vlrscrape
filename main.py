import requests

# Define the API URL with the match ID
url = "https://vlr.orlandomm.net/api/v1/results/429379"

# Send GET request
response = requests.get(url)

# Check the response status
if response.status_code == 200:
    data = response.json()  # Parse JSON response
    print(data)  # Output the data
else:
    print(f"Error: {response.status_code}, {response.text}")
