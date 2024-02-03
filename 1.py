import os
import pandas as pd
from sqlalchemy import create_engine
import logging

df1 = []
logging.basicConfig(filename="sample.log", level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")

def remove_nans(filename):
    try:
        df = pd.read_csv(filename, header=None)
        header_row = df[df.apply(lambda row: 'OrderDate' in row.values, axis=1)].index[0]
        df = pd.read_csv(filename, header=header_row)
        df['OrderDate'] = pd.to_datetime(df['OrderDate'], format="%d/%m/%y").dt.strftime("%d/%m/%y")
        df['Payment'] = df['Payment'].replace({'YES': 'Yes', 'yes': 'Yes', 'Y': 'Yes', 'y': 'Yes', 'NO': 'No', 'N': 'No', 'no': 'No', 'NaN': 'No'})
        df['Region'] = df['Region'].replace({'East': 'E', 'West': 'W', 'Central': 'C'})
        df['Total'] = df['Total'].apply(lambda x: f"₹{x:.2f}")
        return df
    except Exception as e:
        logging.error(f"Error processing {filename}: {e}")
        return None

# Replace with your actual MySQL credentials and database details
mysql_username = 'root'
mysql_password = 'YES'  # Replace 'your_password' with your actual MySQL password
mysql_host = 'localhost'
mysql_port = 3306
mysql_database = 'p'

# Connect to the database using the engine
engine = create_engine(f'mysql+mysqlconnector://{mysql_username}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_database}?charset=utf8mb4', echo=False)

# Get a connection object from the engine
connection = engine.connect()

path = r'C:\Users\hp\Desktop'
logging.info(f"Access All files from {path}")

for file in os.listdir(path):
    if file.endswith(".csv"):
        complete_path = os.path.join(path, file)
        print(f"Processing file: {complete_path}")
        all_data = remove_nans(complete_path)
        if all_data is not None:
            df1.append(all_data)

# Check if df1 is not empty before concatenating
if df1:
    df = pd.concat(df1)

    # Write DataFrame to MySQL table
    table_name = 'ss'
    df.to_sql(name='ss', con=engine, if_exists='replace', index=False)

    print(f"Data successfully written to the MySQL table '{table_name}'.")
else:
    print("No valid CSV data to process.")

# Dispose of the engine and close the connection
connection.close()
engine.dispose()