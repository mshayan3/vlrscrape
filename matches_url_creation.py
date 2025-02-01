import os
import json
import csv

# Define the base URL for the match links
base_url = "https://www.vlr.gg/"

# Define the base folder where subfolders exist
base_folder = "Champs Kickoff Results"

# Loop through the subfolders: Americas, Pacific, China, EMEA
for subfolder in ["Americas", "Pacific", "China", "EMEA"]:
    subfolder_path = os.path.join(base_folder, subfolder)

    # Define the match_links folder path inside the region folder
    match_links_folder = os.path.join(subfolder_path, "match_links")

    # Create the match_links folder if it doesn't exist
    os.makedirs(match_links_folder, exist_ok=True)

    # Define the output CSV file for the region inside the match_links folder
    csv_filename = os.path.join(match_links_folder, f"{subfolder}_match_links.csv")

    # Check if the subfolder exists
    if os.path.exists(subfolder_path):
        # Open the region-specific CSV file for writing
        with open(csv_filename, mode='w', newline='') as file:
            writer = csv.writer(file)

            # Write header row for the CSV
            writer.writerow(["Match URL"])

            # Loop through the JSON files in the subfolder
            for filename in os.listdir(subfolder_path):
                if filename.endswith(".json"):
                    # Load the JSON data
                    with open(os.path.join(subfolder_path, filename), 'r') as f:
                        match_data = json.load(f)

                        # Extract necessary details
                        match_id = match_data["id"]
                        team_a = match_data["teams"][0]["name"].replace(" ", "-").lower()
                        team_b = match_data["teams"][1]["name"].replace(" ", "-").lower()
                        tournament = match_data["tournament"].replace(" ", "-").replace(":", "").lower()
                        event_code = match_data["event"].split("–")[-1].strip().lower().replace(" ",
                                                                                                "-")  # Simplify event names

                        # Construct the match URL
                        match_url = f"{base_url}{match_id}/{team_a}-vs-{team_b}-{tournament}-{event_code}"

                        # Write the match URL to the region-specific CSV file
                        writer.writerow([match_url])

        print(f"CSV file '{csv_filename}' has been created with match links for the {subfolder} region.")
