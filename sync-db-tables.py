import pandas as pd
import json
import os
import argparse
import csv
from tqdm import tqdm
from mysqldb import MySQLDB
from config import Config
import time

script_start_time = time.time()

total_query_time = 0
total_write_time = 0

# Get the directory where the script is located
script_dir = os.path.dirname(os.path.realpath(__file__))

# Define the command-line arguments
parser = argparse.ArgumentParser(description='Extract data from MySQL tables to CSV files.')
parser.add_argument('-c', '--config', type=str, default=os.path.join(script_dir, 'config.json'), help='the configuration file to use')
parser.add_argument('-o', '--output-path', type=str, default=os.path.join(script_dir, 'output'), help='the folder to output to')
parser.add_argument('-v', '--verbose', action='store_true', help='should the script print verbose output')
parser.add_argument('-d', '--debug', action='store_true', default=True if os.environ.get('PYTHONDEBUG') else False, help='should the script print verbose output')
parser.add_argument('-b', '--batch-size', type=int, default=1000, help='should the script print verbose output')
args = parser.parse_args()

# Log messages if verbose is True
def log(message: str, blank_line: bool = False, force: bool = False, debug: bool = False):
    """Logs a message if verbose is True.

    Args:
        message (str): The message to log.
        verbose (bool): Whether or not to log the message.
    """

    if debug and not args.debug:
        return

    if args.verbose or force or (args.debug and debug):
        if blank_line:
            print()
        print(message)

# Open the configuration file and load the database and table information
config = Config(args.config)

table_schemas = {}

def get_table_schema(database, table_name):
    """Gets the table schema for the given table name.

    Args:
        database (MySQLDB): The database instance.
        db_name (str): The name of the database.
        table_name (str): The name of the table.

    Returns:
        table_schema (pandas.DataFrame): The schema of the table.
    """

    global table_schemas

    # Check if table schema is cached, if yes, return it
    if table_name in table_schemas:
        return table_schemas[table_name]

    # Get table schema from the database
    table_schema = database.run_query('DESCRIBE {}'.format(table_name))

    # Print the table name to the console with new line.
    log( "".join(['\r\nTable name: ', format( table_name ) ]), debug=True)

    # Print the fields in comma list to the console
    log( "".join(['\r\nTable schema:', ', '.join(table_schema['Field'])]), debug=True)

    # Cache the table schema
    table_schemas[table_name] = table_schema

    return table_schema

def create_output_file_if_not_exists(database, db_name, table_name):
    """Creates the output file if it doesn't exist.

    Args:
        table_info (dict): The table information from the config file.
    """

    # Check if CSV exists, if not create it with header row by querying table schema
    if not os.path.exists(config.get_output_filename(db_name, table_name, args.output_path)):
        # Get table schema
        table_schema = get_table_schema(database, table_name)

        # Create a CSV file and write the header row, create path if it doesn't exist
        if not os.path.exists(os.path.dirname(config.get_output_filename(db_name, table_name,  args.output_path))):
            os.makedirs(os.path.dirname(config.get_output_filename(db_name, table_name, args.output_path)))

        with open(config.get_output_filename(db_name, table_name, args.output_path), 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(table_schema['Field'])
            csv_file.close()

def fetch_rows_from_table(database:MySQLDB, db_name:str, table, rows_to_extract:int):
    """Fetches rows from the table and writes them to the CSV file.

    Args:
        table_info (dict): The table information from the config file.
    """

    table_name = table['name']
    primary_key = table['primary_key'] if 'primary_key' in table else 'id'

    # Name of the CSV file to create for this table
    csv_filename = config.get_output_filename(db_name, table_name, args.output_path)

    # Get last id from stored table info
    last_id = config.get_last_id(db_name, table_name)

    table_schema = get_table_schema(database, table_name)

    # Open CSV file in append mode
    with open(csv_filename, 'a', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)

        # Set up the initial offset and limit for fetching rows
        offset = 0
        limit = args.batch_size * 5
        starting_id = last_id

        # Get a cursor object
        cursor = database.get_cursor()

        if args.verbose:
            pbar = tqdm(total=rows_to_extract, desc='🤖 Processing table {}'.format(table_name), unit='records')
        else:
            pbar = None

        # Loop through the table, fetching rows in chunks and writing them to the CSV file
        while offset < rows_to_extract:
            if starting_id:
                query = 'SELECT * FROM {} WHERE {} > {} LIMIT {} OFFSET {}'.format(table_name, primary_key, starting_id, limit, offset)
            else:
                query = 'SELECT * FROM {} LIMIT {} OFFSET {}'.format(table_name, limit, offset)

            # Print the SQL query
            log("Executing query: {}".format(query), debug=True)
            cursor.execute(query)

            # Loop through the table, fetching rows in chunks and writing them to the CSV file
            while True:

                # Measure the time taken to fetch rows
                start_time = time.time()
                rows = cursor.fetchmany(args.batch_size)
                end_time = time.time()

                global total_query_time
                total_query_time += end_time - start_time

                if not rows:
                    break

                # Write the rows to the CSV file
                start_time = time.time()
                rows_list = [list(row) for row in rows]
                csv_writer.writerows(rows_list)
                end_time = time.time()

                global total_write_time
                total_write_time += end_time - start_time

                # # Get the last id from the rows
                last_row = rows[-1]
                primary_key_index = table_schema['Field'].to_list().index(primary_key)
                last_id = last_row[primary_key_index]

                # # Update the last synced id in the config file
                config.set_last_id(db_name, table_name, int(last_id))

                # Update the progress bar
                if pbar is not None:
                    pbar.update(len(rows_list))

                # Save the config file
                config.save_config()

            offset += limit

        if pbar is not None:
            pbar.close()

        csv_file.close()

    log( '⏰ Finished fetching rows from table.' )

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
        log( '✅ CSV file contains all rows from the table.' )

        return True
    else:
        log( '⚠️ WARNING: CSV file contains {} rows, but table contains {} rows.'.format(csv_rows, total_rows) )
        log( '👉 NOTE: This could be due to data being added to the table while the script is running.' )

        return False


def sync_db(db_name):
    """Syncs a database.

    Args:
        db_name (str): The name of the database to sync.
    """

    # Get the database information
    db_info = config.get_database_info(db_name)

    # Create a MySQLDB object
    database = MySQLDB(db_info)

    try:
        log( 'Connecting to database {}...'.format(db_name), blank_line=True )

        # Connect to the database
        database.connect()

        # Loop through each table in the database and extrct the data
        for table in config.get_db_tables(db_name):
            table_name = table['name']
            primary_key = table['primary_key'] if 'primary_key' in table else 'id'

            # Get the total number of rows in the table
            total_rows = database.run_query('SELECT COUNT(*) FROM {}'.format(table_name)).iloc[0,0]
            log( 'Found total of {} rows in {} table...'.format(total_rows, table_name), blank_line=True )

            # Get last id from stored table info
            last_id = config.get_last_id(db_name, table_name)

            # Print the number of rows that will be extracted
            if last_id:
                # select the max id from the table where the id is greater than the last id
                last_record_in_db = database.run_query('SELECT MAX({}) as id FROM {} WHERE {} > {}'.format(primary_key, table_name, primary_key, last_id)).iloc[0,0]
                #if no result
                if last_record_in_db is None:
                    last_record_in_db = last_id

                rows_to_extract = last_record_in_db - last_id
                log( '📑 {} new rows to extract...'.format(rows_to_extract) )
            else:
                rows_to_extract = total_rows

            # Create the output file if it doesn't exist
            create_output_file_if_not_exists(database, db_name, table_name)

            # Fetch rows from the table and write them to the CSV file
            fetch_rows_from_table(database, db_name, table, rows_to_extract)

            # Verify that the CSV file has the correct number of rows
            csv_filename = config.get_output_filename(db_name, table_name, args.output_path)
            verify_csv_file(csv_filename, total_rows)

    except Exception as e:
        log( 'ERROR: {}'.format(e) )

    finally:
        # Close the database connection
        database.disconnect()

        # Save the config file
        config.save_config()

# Loop through each database and table in the configuration
for [db_name, db_info] in config.get_db_configs().items():
    sync_db(db_name)

# Print the total time elapsed
log( '✅ DB Tables Successfully Synced. Finished in {} seconds.'.format(time.time() - script_start_time), force=True, blank_line=True)

log( '📊 Total time taken to fetch rows: {} seconds.'.format(total_query_time), debug=True )
log( '📊 Total time taken to write to CSV: {} seconds.'.format(total_write_time), debug=True )