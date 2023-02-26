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
parser.add_argument('-c', '--config', type=str, default=os.path.join(script_dir, 'config.json'), help='the configuration file to use')
parser.add_argument('-o', '--output-path', type=str, default=os.path.join(script_dir, 'output'), help='the folder to output to')
parser.add_argument('-v', '--verbose', action='store_true', help='should the script print verbose output')
args = parser.parse_args()

# Log messages if verbose is True
def log(message: str):
    """Logs a message if verbose is True.

    Args:
        message (str): The message to log.
        verbose (bool): Whether or not to log the message.
    """

    if args.verbose:
        print(message)

# Open the configuration file and load the database and table information
with open(args.config, 'r') as config_file:
    config = json.load(config_file)

def save_config():
    """Saves the configuration file.
    """

    with open(args.config, 'w') as config_file:
        json.dump(config, config_file, indent=4)

def get_output_filename(table_info):
    """Returns the output filename for the table.

    Args:
        table_info (dict): The table information from the config file.

    Returns:
        str: The output filename for the table.
    """

    if 'output' in table_info:
        return os.path.join(args.output_path, '{}'.format(table_info['output']))
    else:
        return os.path.join(args.output_path, '{}_{}.csv'.format(db_name, table_name))

# Check if table file exists, if so get last_id from the file id column, otherwise check if last_id set in config, otherwise return 0
def get_last_id(table_info):
    """Returns the last id for the table.

    Args:
        table_info (dict): The table information from the config file.

    Returns:
        int: The last id for the table.
    """

    if 'last_id' in table_info:
        return table_info['last_id']
    elif os.path.exists(get_output_filename(table_info)):
        return pd.read_csv(get_output_filename(table_info))['id'].max()
    else:
        return 0

def create_output_file_if_not_exists(table_info):
    """Creates the output file if it doesn't exist.

    Args:
        table_info (dict): The table information from the config file.
    """

    # Check if CSV exists, if not create it with header row by querying table schema
    if not os.path.exists(get_output_filename(table_info)):
        # Get table schema
        table_schema = database.run_query('DESCRIBE {}'.format(table_name))

        # Print the table name to the console with new line.
        log( "".join(['\r\nTable name: ', format( table_name ) ]))

        # Print the fields in comma list to the console
        log( "".join(['\r\nTable schema:', ', '.join(table_schema['Field'])]) )

        # Create a CSV file and write the header row, create path if it doesn't exist
        if not os.path.exists(os.path.dirname(get_output_filename(table_info))):
            os.makedirs(os.path.dirname(get_output_filename(table_info)))

        with open(get_output_filename(table_info), 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(table_schema['Field'])
            csv_file.close()

def fetch_rows_from_table(database, db_name:str, table_name: str, table_info, rows_to_extract:int):
    """Fetches rows from the table and writes them to the CSV file.

    Args:
        table_info (dict): The table information from the config file.
    """

    # Name of the CSV file to create for this table
    csv_filename = get_output_filename(table_info)

    # Get last id from stored table info
    last_id = get_last_id(table_info)

    # Open CSV file in append mode
    with open(csv_filename, 'a', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)

        # Set up the initial offset and limit for fetching rows
        offset = 0
        limit = 1000
        starting_id = last_id

        if args.verbose:
            pbar = tqdm(total=rows_to_extract)
        else:
            pbar = None

        # Loop through the table, fetching rows in chunks and writing them to the CSV file
        while offset < rows_to_extract:
            if starting_id:
                rows = database.run_query('SELECT * FROM {} WHERE id > {} LIMIT {} OFFSET {}'.format(table_name, starting_id, limit, offset))
            else:
                rows = database.run_query('SELECT * FROM {} LIMIT {} OFFSET {}'.format(table_name, limit, offset))

            if rows.empty:
                break

            # Write the rows to the CSV file
            csv_writer.writerows(rows.values)

            # # Get the last id from the rows
            last_id = rows['id'].max()

            # # Update the last synced id in the config file
            config[db_name]['tables'][table_name]['last_id'] = int(last_id)

            # Update the progress bar
            if pbar is not None:
                pbar.update(len(rows))

            # Save the config file
            save_config()

            # Increment the offset
            offset += limit

        if pbar is not None:
            pbar.close()

    log( 'Finished fetching rows from table.' )

def verify_csv_file(csv_filename:str, total_rows:int):
    """Verifies that the CSV file has the correct number of rows.

    Args:
        csv_file (str): The path to the CSV file.
        total_count (int): The total number of rows in the table.

    Returns:
        bool: True if the CSV file has the correct number of rows, False otherwise.
    """

    # Get the number of rows in the CSV file without header row
    csv_rows = pd.read_csv(csv_filename).shape[0]
    if csv_rows == total_rows:
        log( 'CSV file contains all rows from the table.' )

        return True
    else:
        log( 'WARNING: CSV file contains {} rows, but table contains {} rows.'.format(csv_rows, total_rows) )
        log( 'NOTE: This could be due to data being added to the table while the script is running.' )

        return False

# Loop through each database and table in the configuration
for db_name, db_info in config.items():
    database = MySQLDB(db_info)

    try:
        log( 'Connecting to database {}...'.format(db_name) )

        # Connect to the database
        database.connect()

        # Loop through each table in the database and extract the data
        for table_name, table_info in db_info['tables'].items():
            # Name of the CSV file to create for this table
            csv_filename = get_output_filename(table_info)

            # Get last id from stored table info
            last_id = get_last_id(table_info)

            # Get the total number of rows in the table
            total_rows = database.run_query('SELECT COUNT(*) FROM {}'.format(table_name)).iloc[0,0]

            log( 'Found {} rows in {} table...'.format(total_rows, table_name) )

            # Print the number of rows that will be extracted
            if last_id:
                rows_to_extract = database.run_query('SELECT COUNT(*) FROM {} WHERE id > {}'.format(table_name, last_id)).iloc[0,0]
                log( 'Found {} new rows to extract...'.format(rows_to_extract) )
            else:
                rows_to_extract = total_rows
                log( 'Extracting all rows...' )

            # Create the output file if it doesn't exist
            create_output_file_if_not_exists(table_info)

            # Fetch rows from the table and write them to the CSV file
            fetch_rows_from_table(database, db_name, table_name, table_info, rows_to_extract)

            # Verify the CSV file has the correct number of rows
            verify_csv_file(csv_filename, total_rows)

        log( 'Wrote {} rows to {}...'.format(rows_to_extract, table_info['output']) )

        # Disconnect from the database
        database.disconnect()

    except Exception as e:
        database.disconnect()

        log(f"{e}")

        # Save the config file
        save_config()
