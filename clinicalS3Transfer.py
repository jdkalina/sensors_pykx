import boto3
import os
from pathlib import Path

def download_cwa_files(bucket_name, dataset_name, machine_id, local_download_dir="downloads"):
    """
    Download all .cwa.gz files for a specific machine ID from S3
    
    Args:
        bucket_name (str): Name of the S3 bucket
        dataset_name (str): Name of the dataset directory
        machine_id (str): Machine ID to filter files
        local_download_dir (str): Local directory to save downloaded files
    """
    
    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    # Create local download directory if it doesn't exist
    Path(local_download_dir).mkdir(parents=True, exist_ok=True)
    
    # Construct the prefix to search for files
    prefix = f"{dataset_name}/"
    
    try:
        # List all objects in the bucket with the dataset prefix
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        
        downloaded_count = 0
        
        for page in pages:
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                key = obj['Key']
                
                # Check if this file matches our criteria:
                # 1. Contains the machine_id in the path
                # 2. Ends with .cwa.gz
                if machine_id in key and key.endswith('.cwa.gz'):
                    # Extract the filename from the S3 key
                    filename = os.path.basename(key)
                    
                    # Create local file path
                    local_file_path = os.path.join(local_download_dir, filename)
                    
                    # Download the file
                    print(f"Downloading: {key}")
                    s3_client.download_file(bucket_name, key, local_file_path)
                    downloaded_count += 1
                    print(f"Saved to: {local_file_path}")
        
        print(f"\nDownload complete! Downloaded {downloaded_count} files to '{local_download_dir}'")
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")

def download_cwa_files_specific_structure(bucket_name, dataset_name, machine_id, local_download_dir="downloads"):
    """
    More specific version that expects exact structure: dataset/participant/machine_id/file.cwa.gz
    """
    
    s3_client = boto3.client('s3')
    Path(local_download_dir).mkdir(parents=True, exist_ok=True)
    
    # First, get all participant directories
    prefix = f"{dataset_name}/"
    
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
        
        participant_dirs = []
        for page in pages:
            if 'CommonPrefixes' in page:
                for prefix_info in page['CommonPrefixes']:
                    participant_dirs.append(prefix_info['Prefix'])
        
        downloaded_count = 0
        
        # For each participant directory, look for the specific machine_id
        for participant_dir in participant_dirs:
            machine_prefix = f"{participant_dir}{machine_id}/"
            
            # List files in this specific machine directory
            machine_pages = paginator.paginate(Bucket=bucket_name, Prefix=machine_prefix)
            
            for page in machine_pages:
                if 'Contents' not in page:
                    continue
                    
                for obj in page['Contents']:
                    key = obj['Key']
                    
                    if key.endswith('.cwa.gz'):
                        filename = os.path.basename(key)
                        local_file_path = os.path.join(local_download_dir, filename)
                        
                        print(f"Downloading: {key}")
                        s3_client.download_file(bucket_name, key, local_file_path)
                        downloaded_count += 1
                        print(f"Saved to: {local_file_path}")
        
        print(f"\nDownload complete! Downloaded {downloaded_count} files to '{local_download_dir}'")
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")

if __name__ == "__main__":
    # Configuration
    BUCKET_NAME = "your-bucket-name"
    DATASET_NAME = "your-dataset-name"
    MACHINE_ID = "your-machine-id"
    LOCAL_DIR = "downloads"
    
    # Download files
    # Use the first function for a broader search
    download_cwa_files(BUCKET_NAME, DATASET_NAME, MACHINE_ID, LOCAL_DIR)
    
    # Or use the second function for exact directory structure matching
    # download_cwa_files_specific_structure(BUCKET_NAME, DATASET_NAME, MACHINE_ID, LOCAL_DIR)
