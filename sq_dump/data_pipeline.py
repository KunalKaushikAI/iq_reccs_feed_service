import os
import json
import paramiko
import boto3
import pandas as pd
from datetime import datetime

# Configuration for SFTP and AWS S3
CONFIG = {
    "sftp": {
        "host": "sftp.example.com",
        "port": 22,
        "username": "your_username",
        "password": "your_password",
        "directories": {
            "VN": "/path/to/VN/",
            "PH": "/path/to/PH/",
            "TH": "/path/to/TH/"
        }
    },
    "s3": {
        "bucket_name": "your-s3-bucket",
        "folders": {
            "VN": "VN/",
            "PH": "PH/",
            "TH": "TH/"
        }
    },
    "local_path": "/local/data/path/"
}

# Function to download files from SFTP
def download_from_sftp(sftp_details, local_directory):
    transport = paramiko.Transport((sftp_details['host'], sftp_details['port']))
    transport.connect(username=sftp_details['username'], password=sftp_details['password'])
    sftp = paramiko.SFTPClient.from_transport(transport)

    try:
        for region, remote_dir in sftp_details['directories'].items():
            files = sftp.listdir(remote_dir)
            for file in files:
                local_filepath = os.path.join(local_directory, region, file)
                remote_filepath = os.path.join(remote_dir, file)
                sftp.get(remote_filepath, local_filepath)
                print(f"Downloaded {file} from {region}")
    except Exception as e:
        log_error(f"Failed to download files: {str(e)}")
    finally:
        sftp.close()
        transport.close()

# Error logging function
def log_error(message):
    with open("error_log.txt", "a") as log_file:
        log_file.write(f"{datetime.now()} - {message}\n")

# Function to process data files (replace these with actual processing steps)
def process_files(local_directory):
    try:
        for region in os.listdir(local_directory):
            region_path = os.path.join(local_directory, region)
            for file_name in os.listdir(region_path):
                file_path = os.path.join(region_path, file_name)
                # Example processing (adjust according to actual processing needs)
                df = pd.read_csv(file_path)
                df['Processed'] = True  # Example modification
                output_path = file_path.replace('.csv', '_processed.csv')
                df.to_csv(output_path, index=False)
                print(f"Processed file saved to {output_path}")
    except Exception as e:
        log_error(f"Failed to process files: {str(e)}")

# Function to upload files to AWS S3
def upload_to_s3(local_directory, bucket_name, s3_folders):
    s3 = boto3.client('s3')
    try:
        for region in os.listdir(local_directory):
            region_path = os.path.join(local_directory, region)
            for file_name in os.listdir(region_path):
                if '_processed' in file_name:
                    s3_path = os.path.join(s3_folders[region], file_name)
                    local_path = os.path.join(region_path, file_name)
                    s3.upload_file(local_path, bucket_name, s3_path)
                    print(f"Uploaded {file_name} to S3 at {s3_path}")
    except Exception as e:
        log_error(f"Failed to upload files to S3: {str(e)}")

# Main execution function
def main():
    download_from_sftp(CONFIG['sftp'], CONFIG['local_path'])
    process_files(CONFIG['local_path'])
    upload_to_s3(CONFIG['local_path'], CONFIG['s3']['bucket_name'], CONFIG['s3']['folders'])

if __name__ == "__main__":
    main()
import os
import json
import paramiko
import boto3
import pandas as pd
from datetime import datetime

# Configuration for SFTP and AWS S3
CONFIG = {
    "sftp": {
        "host": "sftp.example.com",
        "port": 22,
        "username": "your_username",
        "password": "your_password",
        "directories": {
            "VN": "/path/to/VN/",
            "PH": "/path/to/PH/",
            "TH": "/path/to/TH/"
        }
    },
    "s3": {
        "bucket_name": "your-s3-bucket",
        "folders": {
            "VN": "VN/",
            "PH": "PH/",
            "TH": "TH/"
        }
    },
    "local_path": "/local/data/path/"
}

# Function to download files from SFTP
def download_from_sftp(sftp_details, local_directory):
    transport = paramiko.Transport((sftp_details['host'], sftp_details['port']))
    transport.connect(username=sftp_details['username'], password=sftp_details['password'])
    sftp = paramiko.SFTPClient.from_transport(transport)

    try:
        for region, remote_dir in sftp_details['directories'].items():
            files = sftp.listdir(remote_dir)
            for file in files:
                local_filepath = os.path.join(local_directory, region, file)
                remote_filepath = os.path.join(remote_dir, file)
                sftp.get(remote_filepath, local_filepath)
                print(f"Downloaded {file} from {region}")
    except Exception as e:
        log_error(f"Failed to download files: {str(e)}")
    finally:
        sftp.close()
        transport.close()

# Error logging function
def log_error(message):
    with open("error_log.txt", "a") as log_file:
        log_file.write(f"{datetime.now()} - {message}\n")

# Function to process data files (replace these with actual processing steps)
def process_files(local_directory):
    try:
        for region in os.listdir(local_directory):
            region_path = os.path.join(local_directory, region)
            for file_name in os.listdir(region_path):
                file_path = os.path.join(region_path, file_name)
                # Example processing (adjust according to actual processing needs)
                df = pd.read_csv(file_path)
                df['Processed'] = True  # Example modification
                output_path = file_path.replace('.csv', '_processed.csv')
                df.to_csv(output_path, index=False)
                print(f"Processed file saved to {output_path}")
    except Exception as e:
        log_error(f"Failed to process files: {str(e)}")

# Function to upload files to AWS S3
def upload_to_s3(local_directory, bucket_name, s3_folders):
    s3 = boto3.client('s3')
    try:
        for region in os.listdir(local_directory):
            region_path = os.path.join(local_directory, region)
            for file_name in os.listdir(region_path):
                if '_processed' in file_name:
                    s3_path = os.path.join(s3_folders[region], file_name)
                    local_path = os.path.join(region_path, file_name)
                    s3.upload_file(local_path, bucket_name, s3_path)
                    print(f"Uploaded {file_name} to S3 at {s3_path}")
    except Exception as e:
        log_error(f"Failed to upload files to S3: {str(e)}")

# Main execution function
def main():
    download_from_sftp(CONFIG['sftp'], CONFIG['local_path'])
    process_files(CONFIG['local_path'])
    upload_to_s3(CONFIG['local_path'], CONFIG['s3']['bucket_name'], CONFIG['s3']['folders'])

if __name__ == "__main__":
    main()
