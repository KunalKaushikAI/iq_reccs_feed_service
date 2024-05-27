import os
import json
import paramiko
import boto3
import pandas as pd
from datetime import datetime

def download_from_sftp(region, sftp_details, local_directory):
    transport = paramiko.Transport((sftp_details['host'], sftp_details['port']))
    transport.connect(username=sftp_details['username'], password=sftp_details['password'])
    sftp = paramiko.SFTPClient.from_transport(transport)
    try:
        if region in sftp_details['directories']:
            remote_dir = sftp_details['directories'][region]
            files = sftp.listdir_attr(remote_dir)
            
            # Find the latest file by modification time
            latest_file = max(files, key=lambda file: file.st_mtime)
            
            local_region_directory = os.path.join(local_directory, region)
            os.makedirs(local_directory, exist_ok=True)
            local_filepath = os.path.join(local_directory, "sq_daily.json")
            remote_filepath = os.path.join(remote_dir, latest_file.filename)
            sftp.get(remote_filepath, local_filepath)
            print(f"Downloaded {latest_file.filename} from {region}")
        else:
            print(f"Invalid region specified: {region}")
    except Exception as e:
        log_error(f"Failed to download files for {region}: {str(e)}")
    finally:
        sftp.close()
        transport.close()

# Error logging function
def log_error(message):
    with open("error_log.txt", "a") as log_file:
        log_file.write(f"{datetime.now()} - {message}\n")


# Function to upload files to AWS S3
def upload_to_s3(file_path, bucket_name, sitekeys):
    s3 = boto3.client('s3')
    current_date = datetime.now().strftime('%Y-%m-%d')
    
    try:
        file_name = os.path.basename(file_path)
        for sitekey in sitekeys:
            s3_path_date = f"unilever/iq_data/{sitekey}/{current_date}/{file_name}"
            s3_path_latest = f"unilever/iq_data/{sitekey}/latest/{file_name}"
            
            s3.upload_file(file_path, bucket_name, s3_path_date)
            print(f"Uploaded {file_name} to S3 at {s3_path_date}")
            
            s3.upload_file(file_path, bucket_name, s3_path_latest)
            print(f"Uploaded {file_name} to S3 at {s3_path_latest}")
    except Exception as e:
        log_error(f"Failed to upload files to S3: {str(e)}")

