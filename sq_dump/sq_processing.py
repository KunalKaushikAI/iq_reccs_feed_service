import json
import pandas as pd
import os
import argparse

from data_pipeline import download_from_sftp
from data_pipeline import upload_to_s3

# python3 sq_processing.py --region VN --sitekeys stage-ump13731617272171 pre-prod-ump13731624976343 prod-ump13731617272191 qa-ump13731625738584
# python3 sq_processing.py --region PH --sitekeys qa-ph13731629461908 stage-ph13731629461939 prod-ph13731629461969 dev-ph13731629452743 
# python3 sq_processing.py --region ID --sitekeys ss-unbxd-dev-ID-unilever13731696257305 ss-unbxd-qa-ID-unilever13731696257515 ss-unbxd-pre-prod-ID-unilever13731696257632 ss-unbxd-prod-ID-unilever13731696257710 ss-unbxd-dev-ID-unilever-Bahasa29411703237672 ss-unbxd-qa-ID-unilever-Bahasa29411703237931 ss-unbxd-pre-prod-ID-unilever-Bahasa29411703238013 ss-unbxd-prod-ID-unilever-Bahasa29411703238089 
# python3 sq_processing.py --region TH --sitekeys ss-unbxd-qa-th-thailand13731637232574 ss-unbxd-stage-thai-thailand13731653028212 ss-unbxd-prod-th-thailand13731636957583 ss-unbxd-dev-th-thailand13731645530858 ss-unbxd-qa-eng-thailand13731638360293 ss-unbxd-stage-eng-thailand13731636957403 ss-unbxd-prod-eng-thailand13731636957760 ss-unbxd-dev-eng-thailand13731645530958	
CONFIG = {
    "sftp": {
        "host": "sftp.unbxdapi.com",
        "port": 22,
        "username": "unileveriq",
        "password": "upD:C4jR",
        "directories": {
            "VN": "/files/Daily/VN/",
            "PH": "/files/Daily/PH/",
            "TH": "/files/Daily/TH/",
            "ID": "/files/Daily/ID"
        }
    },
    "s3": {
        "bucket_name": "unbxd-qa-pub",
        "folders": {
            "VN": "VN/",
            "PH": "PH/",
            "TH": "TH/"
        }
    },
    "local_path": "iq_reccs_feed_service/sq_dump/"
}

# Specify the path to the large JSON file
json_file_path = '/Users/unbxd/Desktop/UNILEVER/IQ_site_key/sq_dump/PH/sq_daily_0511.json'  # Update this with the path to your JSON file
error_log_path = '/Users/unbxd/Desktop/UNILEVER/IQ_site_key/sq_dump/PH/error_logs.txt'  # Path to save the error log file

# Function to process the JSON and save data to CSV


def process_json_to_csv(json_file_path, output_csv_path, error_log_path):
    try:
        # Ensure the directory for the error log exists
        error_log_dir = os.path.dirname(error_log_path)
        if not os.path.exists(error_log_dir):
            os.makedirs(error_log_dir, exist_ok=True)
        
        # Load the JSON data from the file
        with open(json_file_path, 'r') as file:
            json_data = json.load(file)
        outlet_purchases = json_data['DDT_OutletMTDPurchases_RNA_Data_Out']['OutletPurchases']
        
        # Normalize the JSON data into a flat DataFrame
        records = []
        for entry in outlet_purchases:
            customer_code = entry['CustomerCode']
            for product in entry['Products']:
                records.append({
                    'uid': customer_code,  # Rename 'CustomerCode' to 'uid'
                    'uniqueId': product['SKU'],  # Rename 'SKU' to 'uniqueId'
                    'SQ': product['SQ']
                })
        
        df = pd.DataFrame(records)

        # Ensure no negative values in 'SQ'
        df['SQ'] = df['SQ'].apply(lambda x: max(int(x), 0))

        # Save the DataFrame to a CSV file
        df.to_csv(output_csv_path, index=False)
        print(f"Data has been processed and saved to: {output_csv_path}")

    except Exception as e:
        # Log any errors
        with open(error_log_path, 'a') as log_file:
            log_file.write(f"Error processing the file {json_file_path}: {str(e)}\n")
        print(f"Error logged in: {error_log_path}")


# Path to save the processed CSV file
output_csv_path = '/Users/unbxd/Desktop/UNILEVER/IQ_site_key/sq_dump/PH/sq_daily_0511.csv'  # Update with the desired output path

# Process the JSON and generate the CSV

class Main():
    def Start(self):
        dest = CONFIG['local_path']+REGION+"/"
        download_from_sftp(REGION,CONFIG['sftp'],dest)
        process_json_to_csv(dest+"sq_daily.json", dest+"sq_daily.csv", dest+"error_logs.txt")
        upload_to_s3(dest+"sq_daily.csv", CONFIG['s3']['bucket_name'], SITE_KEY)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--region', required=True, help='Site_region')
    parser.add_argument('--sitekeys', nargs='+', help='List of site keys')
    options, args = parser.parse_known_args()
    SITE_KEY = options.sitekeys
    REGION = options.region
    Main().Start()