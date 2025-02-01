import os
import shutil

# Define the tournament names and their corresponding subfolder names
tournament_names = {
    "Champions_Tour_2025_EMEA_Kickoff": "EMEA",
    "Champions_Tour_2025_China_Kickoff": "China",
    "Champions_Tour_2025_Americas_Kickoff": "Americas",
    "Champions_Tour_2025_Pacific_Kickoff": "Pacific"
}

# Create the base folder for the results if it doesn't exist
base_folder = "Champs Kickoff Results"
os.makedirs(base_folder, exist_ok=True)

# Loop through the tournament names and create subfolders
for tournament_name, subfolder in tournament_names.items():
    # Create the subfolder inside the base folder
    subfolder_path = os.path.join(base_folder, subfolder)
    os.makedirs(subfolder_path, exist_ok=True)

    # Scan the "matches" folder for files that contain the tournament name
    for filename in os.listdir("matches"):
        if tournament_name in filename:
            # Build the full source path and destination path
            src_file = os.path.join("matches", filename)
            dest_file = os.path.join(subfolder_path, filename)

            # Move the file to the appropriate subfolder
            shutil.move(src_file, dest_file)
            print(f"Moved {filename} to {subfolder_path}")

print("Files have been organized successfully.")
