import json
import pandas as pd
import os
import glob

# Specify the directory containing your JSON files
json_files_dir = '/Users/unbxd/Desktop/UNILEVER/IQ_site_key/sq_dump/TH/' 
# Update this with the path to your JSON file  # Update this with the actual path to your directory
#json_files_dir = '/Users/unbxd/Desktop/Workspace/unilever_iq/soq_sku_daily/input_files/monthly_files_vn'

# Use glob to find all JSON files in the specified directory
json_files = glob.glob(os.path.join(json_files_dir, '*.json'))

# Function to process each JSON file and save it as a CSV
def process_and_save_json(json_file_path):
    # Load the JSON data from the file
    with open(json_file_path, 'r') as file:
        json_data = json.load(file)

    # Convert the loaded JSON data into a pandas DataFrame
    df = pd.DataFrame(json_data)

    # Rename columns as per requirements: 'SKU' to 'uniqueId', 'CustomerCode' to 'uid'
    df.rename(columns={'SKU': 'uniqueId', 'CustomerCode': 'uid'}, inplace=True)

    # Data validation: Ensure no negative values in 'SOQ', if 'SOQ' column exists
    if 'SOQ' in df.columns:
        df['SOQ'] = df['SOQ'].apply(lambda x: max(int(x), 0))

    # Construct the output path for the CSV file, replacing the .json extension with .csv
    output_csv_path = os.path.splitext(json_file_path)[0] + '.csv'

    # Save the processed DataFrame to a CSV file, without the index column
    df.to_csv(output_csv_path, index=False)

    print(f"Processed and saved CSV: {output_csv_path}")

# Iterate over each JSON file found and process it
# for json_file in json_files:
#     process_and_save_json(json_file)
json_file ="/Users/unbxd/Desktop/UNILEVER/IQ_site_key/sq_dump/TH/soq_monthly_2.json"
process_and_save_json(json_file)

