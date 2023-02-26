import pandas as pd
import json
import os
import argparse
import csv
from tqdm import tqdm
from mysqldb import MySQLDB

# Get the directory where the script is located
script_dir = os.path.dirname(os.path.realpath(__file__))

# Define the command-line arguments
parser = argparse.ArgumentParser(description='Extract data from MySQL tables to CSV files.')
parser.add_argument('--config', type=str, default=os.path.join(script_dir, 'config.json'), help='the configuration file to use')
parser.add_argument('--output-path', type=str, default=os.path.join(script_dir, 'output'), help='the folder to output to')
args = parser.parse_args()

# Open the configuration file and load the database and table information
with open(args.config, 'r') as config_file:
    config = json.load(config_file)

def save_config():
    """Saves the configuration file.
    """

    with open(args.config, 'w') as config_file:
        json.dump(config, config_file, indent=4)


# Given table info, determine output filename.
def get_output_filename(table_info):
    if 'output' in table_info:
        return os.path.join(args.output_path, '{}'.format(table_info['output']))
    else:
        return os.path.join(args.output_path, '{}_{}.csv'.format(db_name, table_name))

# Check if table file exists, if so get last_id from the file id column, otherwise check if last_id set in config, otherwise return 0
def get_last_id(table_info):
    if 'last_id' in table_info:
        return table_info['last_id']
    elif os.path.exists(get_output_filename(table_info)):
        return pd.read_csv(get_output_filename(table_info))['id'].max()
    else:
        return 0

# Loop through each database and table in the configuration
for db_name, db_info in config.items():
    database = MySQLDB(db_info)

    try:
        print( 'Connecting to database {}...'.format(db_name) )
        # Connect to the database
        database.connect()

        # Loop through each table in the database and extract the data
        for table_name, table_info in db_info['tables'].items():
            # Name of the CSV file to create for this table
            csv_filename = get_output_filename(table_info=table_info)

            # Get last id from stored table info
            last_id = get_last_id(table_info=table_info)

            # Get the total number of rows in the table
            total_rows = database.run_query('SELECT COUNT(*) FROM {}'.format(table_name)).iloc[0,0]

            print( 'Found {} rows in {} table...'.format(total_rows, table_name) )

            # Print the number of rows that will be extracted
            if last_id:
                rows_to_extract = database.run_query('SELECT COUNT(*) FROM {} WHERE id > {}'.format(table_name, last_id)).iloc[0,0]
                print( 'Found {} new rows to extract...'.format(rows_to_extract) )
            else:
                print( 'Extracting all rows...' )
                rows_to_extract = total_rows

            print()

            # Check if CSV exists, if not create it with header row by querying table schema
            if not os.path.exists(csv_filename):
                # Get table schema
                table_schema = database.run_query('DESCRIBE {}'.format(table_name))

                print()
                # Print the fields in comma list to the console
                print( 'Table schema:', ', '.join(table_schema['Field']) )
                print()

                # Create a CSV file and write the header row, create path if it doesn't exist
                if not os.path.exists(os.path.dirname(csv_filename)):
                    os.makedirs(os.path.dirname(csv_filename))

                with open(csv_filename, 'w', newline='') as csv_file:
                    csv_writer = csv.writer(csv_file)
                    csv_writer.writerow(table_schema['Field'])

            # Open CSV file in append mode
            with open(csv_filename, 'a', newline='') as csv_file:
                csv_writer = csv.writer(csv_file)

                # Set up the initial offset and limit for fetching rows
                offset = 0
                limit = 1000
                starting_id = last_id

                # Loop through the table, fetching rows in chunks and writing them to the CSV file
                # Wrap the loop with tqdm to add a progress bar
                with tqdm(total=rows_to_extract) as pbar:
                    # Loop through the table, fetching rows in chunks and writing them to the CSV file
                    while offset < rows_to_extract:
                        if starting_id:
                            rows = database.run_query('SELECT * FROM {} WHERE id > {} LIMIT {} OFFSET {}'.format(table_name, starting_id, limit, offset))
                        else:
                            rows = database.run_query('SELECT * FROM {} LIMIT {} OFFSET {}'.format(table_name, limit, offset))

                        if rows.empty:
                            break

                        # for each row, write it to the CSV file, headers already exist
                        for row in rows.itertuples(index=False):
                            csv_writer.writerow(row)
                            last_id = row[0]

                            # Update the last synced id in the config file
                            config[db_name]['tables'][table_name]['last_id'] = int(last_id)

                        pbar.update(len(rows))

                        # Save the config file
                        save_config()

                        offset += limit

        # Disconnect from the database
        database.disconnect()

    except Exception as e:
        print(f"Error connecting to database: {e}")
        database.disconnect()

        # Save the config file
        save_config()
