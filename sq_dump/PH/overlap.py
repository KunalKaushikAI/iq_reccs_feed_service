import pandas as pd

def load_and_concatenate(csv_file_path1, csv_file_path2):
    # Load CSV files into DataFrames
    df1 = pd.read_csv(csv_file_path1)
    df2 = pd.read_csv(csv_file_path2)
    
    # Concatenate DataFrames end to end
    concatenated_df = pd.concat([df1, df2], axis=0)
    return concatenated_df

def save_dataframe_to_csv(dataframe, output_path):
    # Save the DataFrame to a CSV file
    dataframe.to_csv(output_path, index=False)
    print(f"Data saved to {output_path}")

def find_and_save_overlap(concatenated_df, csv_file_path3, output_csv_path):
    # Load third CSV file
    df3 = pd.read_csv(csv_file_path3)

    # Ensure that both 'uid' and 'uniqueId' are treated as strings to prevent type mismatch
    concatenated_df['uid'] = concatenated_df['uid'].astype(str)
    concatenated_df['uniqueId'] = concatenated_df['uniqueId'].astype(str)
    df3['uid'] = df3['uid'].astype(str)
    df3['uniqueId'] = df3['uniqueId'].astype(str)

    # Merge the DataFrames based on 'uid' and 'uniqueId' to find common entries
    merged_df = pd.merge(concatenated_df, df3, on=['uid', 'uniqueId'], how='inner')

    # If there are matches, save them to a new CSV file
    if not merged_df.empty:
        merged_df[['uid', 'uniqueId']].to_csv(output_csv_path, index=False)
        return True
    else:
        return False

# Example usage within the script (comment out in production)
if __name__ == "__main__":
    concatenated_df = load_and_concatenate('sq_monthly_2.csv', 'sq_monthly_1.csv')
    save_dataframe_to_csv(concatenated_df, 'soq_monthly.csv')
    result = find_and_save_overlap(concatenated_df, 'sq_daily_0511.csv', 'overlap.csv')
    print("Overlap found and saved." if result else "No overlap found.")



