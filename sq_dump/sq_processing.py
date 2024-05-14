import json
import pandas as pd
import os

# Specify the path to the large JSON file
json_file_path = '/Users/unbxd/Desktop/UNILEVER/IQ_site_key/sq_dump/PH/sq_daily_0511.json'  # Update this with the path to your JSON file
error_log_path = '/Users/unbxd/Desktop/UNILEVER/IQ_site_key/sq_dump/PH/error_logs.txt'  # Path to save the error log file

# Function to process the JSON and save data to CSV
def process_json_to_csv(json_file_path, output_csv_path, error_log_path):
    try:
        # Load the JSON data from the file
        with open(json_file_path, 'r') as file:
            json_data = json.load(file)
        
        # Access the specific part of JSON data
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
process_json_to_csv(json_file_path, output_csv_path, error_log_path)

