from pathlib import Path
import os
import zipfile
import shutil

# root directory
PROJECT_ROOT = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Zip file path
ZIP_FILE = PROJECT_ROOT / "ttpd_data.zip"

# Data paths
DATA_DIR = PROJECT_ROOT / "ttpd_data"

# File patterns for different data files
PEOPLE_FILE_PATTERN = "*.csv"
AUTOMOBILES_FILE_PATTERN = "*.xml"
TICKETS_FILE_PATTERN = "*.json"

# Ticket fee table
TICKET_FEES = {
    "BASE": 30,
    "SCHOOL_ZONE": 60,
    "WORK_ZONE": 60,
    "BOTH_ZONES": 120
}

def setup_data_directory():
    """Extracting data from zip file"""
    
    # Extracting zip file
    if not ZIP_FILE.exists():
        raise FileNotFoundError(f"Zip file not found: {ZIP_FILE}")

    with zipfile.ZipFile(ZIP_FILE, 'r') as zip_ref:
        zip_ref.extractall(PROJECT_ROOT)
    
    if not DATA_DIR.exists():
        raise FileNotFoundError(f"Data directory not found after extraction: {DATA_DIR}") 