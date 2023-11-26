import pandas as pd


def convert_timestamps_to_epoch_seconds(df):
    for column in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[column]):
            df[column] = df[column].astype('int64') // 10**9
    return df


# Replace 'data.csv' with the path to your CSV file
file_path = 'data.csv'

# Load the CSV file into a pandas DataFrame
df = pd.read_csv(file_path,  parse_dates=['BaseDateTime'])
df = df.rename(columns=str.lower)

df = convert_timestamps_to_epoch_seconds(df)
column_arrays = {column: df[column].values for column in df.columns}

# # Convert each row in the DataFrame to a dictionary
# # The keys in the dictionary will be the column names from the CSV file
# list_of_dicts = df.to_dict(orient='records')

# # Example: Display the list of dictionaries
# for record in list_of_dicts:
#     print(record)
for i in column_arrays:
    print(i, column_arrays[i])
