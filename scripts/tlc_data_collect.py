from urllib.request import urlretrieve
import os

def collect_tlc_yellow_data():
    # Define the output directory relative to the current working directory
    output_relative_dir = '../data/raw/'

    # Check if the base directory exists; if not, create it
    if not os.path.exists(output_relative_dir):
        os.makedirs(output_relative_dir)

    # Define the target directory where the data will be saved
    target_dir = 'tlc_data/yellow'
    target_path = os.path.join(output_relative_dir, target_dir)
    
    # Create the target directory (including all intermediate directories) if it doesn't exist
    if not os.path.exists(target_path):
        os.makedirs(target_path)

    YEAR = '2023'
    MONTHS = range(7, 13)

    URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"

    for month in MONTHS:
        # Format month with leading zero
        month_str = str(month).zfill(2)
        print(f"Begin month {month_str}")
        
        # Generate the URL for the data file
        url = f'{URL_TEMPLATE}{YEAR}-{month_str}.parquet'
        # Define the path for saving the downloaded file
        output_file = os.path.join(target_path, f"{YEAR}-{month_str}.parquet")
        # Download the file
        urlretrieve(url, output_file)
        
        print(f"Completed month {month_str}")

def collect_tlc_green_data():
    # Define the output directory relative to the current working directory
    output_relative_dir = '../data/raw/'

    # Check if the base directory exists; if not, create it
    if not os.path.exists(output_relative_dir):
        os.makedirs(output_relative_dir)

    # Define the target directory where the data will be saved
    target_dir = 'tlc_data/green'
    target_path = os.path.join(output_relative_dir, target_dir)
    
    # Create the target directory (including all intermediate directories) if it doesn't exist
    if not os.path.exists(target_path):
        os.makedirs(target_path)
        
    YEAR = '2023'
    MONTHS = range(7, 13)

    URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_"

    for month in MONTHS:
        # Format month with leading zero
        month_str = str(month).zfill(2)
        print(f"Begin month {month_str}")
        
        # Generate the URL for the data file
        url = f'{URL_TEMPLATE}{YEAR}-{month_str}.parquet'
        # Define the path for saving the downloaded file
        output_file = os.path.join(target_path, f"{YEAR}-{month_str}.parquet")
        # Download the file
        urlretrieve(url, output_file)
        
        print(f"Completed month {month_str}")

