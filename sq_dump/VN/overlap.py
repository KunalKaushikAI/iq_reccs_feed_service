import pandas as pd

# Paths to your CSV files
csv_file_path1 = 'sq_daily.csv'
csv_file_path2 = 'soq_monthly.csv'
output_csv_path = 'overlap.csv'

# Load CSV files into DataFrames
df1 = pd.read_csv(csv_file_path1)
df2 = pd.read_csv(csv_file_path2)

# Ensure that both 'uid' and 'uniqueId' are treated as strings to prevent type mismatch
df1['uid'] = df1['uid'].astype(str)
df1['uniqueId'] = df1['uniqueId'].astype(str)
df2['uid'] = df2['uid'].astype(str)
df2['uniqueId'] = df2['uniqueId'].astype(str)

# Merge the DataFrames based on 'uid' and 'uniqueId' to find common entries
merged_df = pd.merge(df1, df2, on=['uid', 'uniqueId'], how='inner')

# If there are matches, save them to a new CSV file
if not merged_df.empty:
    # You might want to select specific columns to save, e.g., just 'uid' and 'uniqueId'
    merged_df[['uid', 'uniqueId']].to_csv(output_csv_path, index=False)
    print(f"Common entries found and saved to: {output_csv_path}")
else:
    print("No common entries found.")


