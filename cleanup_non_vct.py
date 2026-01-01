import os
import shutil

# Directories to always keep
KEEP_DIRS = {'.git', '.idea', '.agent', 'dashboard', '__pycache__', 'venv', '.gemini'}

# Valid prefixes for VCT events
VCT_PREFIXES = (
    'VCT_',
    'Valorant_Champions_',
    'Valorant_Masters_',
    'Champions_Tour_'
)

def cleanup():
    root_dir = os.getcwd()
    print(f"Cleaning up {root_dir}...")
    
    deleted_count = 0
    kept_count = 0
    
    for item in os.listdir(root_dir):
        item_path = os.path.join(root_dir, item)
        
        # Skip files
        if not os.path.isdir(item_path):
            continue
            
        # Skip protected keys
        if item in KEEP_DIRS:
            continue
            
        # Check if it matches VCT criteria
        if item.startswith(VCT_PREFIXES):
            print(f"Keeping: {item}")
            kept_count += 1
            continue
            
        # If we get here, delete it
        try:
            try:
                print(f"Deleting: {item}")
            except UnicodeEncodeError:
                print(f"Deleting: {item.encode('utf-8', 'ignore')}")
                
            shutil.rmtree(item_path)
            deleted_count += 1
        except Exception as e:
            try:
                print(f"Error deleting {item}: {e}") 
            except:
                print("Error deleting directory (name encoding error)")
            
    print(f"\nCleanup Complete.")
    print(f"Deleted: {deleted_count} folders")
    print(f"Kept: {kept_count} event folders")

if __name__ == "__main__":
    cleanup()
