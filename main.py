import pandas as pd

# Replace 'data.csv' with the path to your CSV file
file_path = 'data.csv'

# Load the CSV file into a pandas DataFrame
df = pd.read_csv(file_path)

# Convert each row in the DataFrame to a dictionary
# The keys in the dictionary will be the column names from the CSV file
list_of_dicts = df.to_dict(orient='records')

# Example: Display the list of dictionaries
for record in list_of_dicts:
    print(record)

