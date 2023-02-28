"""Extracts data from MySQL tables to CSV files."""
import time
import os
import argparse
import csv
from tqdm import tqdm
import pandas as pd
import pymysql
from mysqldb import MySQLDB
from config import Config
from logger import Logger

TOTAL_QUERY_TIME = 0
TOTAL_WRITE_TIME = 0

# Get the directory where the script is located
script_dir = os.path.dirname(os.path.realpath(__file__))

# Define the command-line arguments
parser = argparse.ArgumentParser(description='Extract data from MySQL tables to CSV files.')
parser.add_argument('-c', '--config', type=str, default=os.path.join(script_dir,
                    'config.json'), help='the configuration file to use')
parser.add_argument('-o', '--output-path', type=str,
                    default=os.path.join(script_dir, 'output'), help='the folder to output to')
parser.add_argument('-v', '--verbose', action='store_true',
                    help='should the script print verbose output')
parser.add_argument('-d', '--debug', action='store_true', default=bool(os.environ.get(
    'PYTHONDEBUG')), help='should the script print verbose output')
parser.add_argument('-b', '--batch-size', type=int, default=1000,
                    help='should the script print verbose output')
args = parser.parse_args()

# Open the configuration file and load the database and table information
config = Config(args.config)
logger = Logger(verbose=args.verbose, debug=args.debug)

TABLE_SCHEMAS = {}


def get_table_schema(database, table_name):
    """Gets the table schema for the given table name.

    Args:
        database (MySQLDB): The database instance.
        db_name (str): The name of the database.
        table_name (str): The name of the table.

    Returns:
        table_schema (pandas.DataFrame): The schema of the table.
    """

    # Check if table schema is cached, if yes, return it
    if table_name in TABLE_SCHEMAS:
        return TABLE_SCHEMAS[table_name]

    # Get table schema from the database
    table_schema = database.run_query(f'DESCRIBE {table_name}')

    # Print the table name to the console with new line.
    logger.log("".join(['\r\nTable name: ', format(table_name)]), debug=True)

    # Print the fields in comma list to the console
    logger.log("".join(['\r\nTable schema:', ', '.join(table_schema['Field'])]), debug=True)

    # Cache the table schema
    TABLE_SCHEMAS[table_name] = table_schema

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
        if not os.path.exists(os.path.dirname(
                config.get_output_filename(db_name, table_name,  args.output_path))):
            os.makedirs(os.path.dirname(config.get_output_filename(
                db_name, table_name, args.output_path)))

        with open(config.get_output_filename(db_name, table_name, args.output_path),
                  'w', newline='', encoding="utf8") as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(table_schema['Field'])
            csv_file.close()


def fetch_rows_from_table(database: MySQLDB, db_name: str, table, rows_to_extract: int):
    """Fetches rows from the table and writes them to the CSV file.

    Args:
        table_info (dict): The table information from the config file.
    """

    table_name = table['name']
    primary_key = table['primary_key'] if 'primary_key' in table else 'id'
    incremental = table['incremental'] if 'incremental' in table else False

    # Name of the CSV file to create for this table
    csv_filename = config.get_output_filename(db_name, table_name, args.output_path)

    # Get last id from stored table info
    last_id = config.get_last_id(db_name, table_name) if incremental else None

    table_schema = get_table_schema(database, table_name)

    # Open CSV file in append mode
    with open(csv_filename, 'a', newline='', encoding="utf8") as csv_file:
        csv_writer = csv.writer(csv_file)

        # Set up the initial offset and limit for fetching rows
        offset = 0
        limit = args.batch_size * 5
        starting_id = last_id

        # Get a cursor object
        cursor = database.get_cursor()

        if args.verbose:
            pbar = tqdm(total=rows_to_extract,
                        desc=f'🤖 Processing table {table_name}', unit='records')
        else:
            pbar = None

        # Loop through the table, fetching rows in chunks and writing them to the CSV file
        while offset < rows_to_extract:
            if starting_id:
                query = f'SELECT * FROM {table_name} WHERE {primary_key} > {starting_id} LIMIT {limit} OFFSET {offset}'
            else:
                query = f'SELECT * FROM {table_name} LIMIT {limit} OFFSET {offset}'

            # Print the SQL query
            logger.log(f"Executing query: {query}", debug=True)
            cursor.execute(query)

            # Loop through the table, fetching rows in chunks and writing them to the CSV file
            while True:

                # Measure the time taken to fetch rows
                start_time = time.time()
                rows = cursor.fetchmany(args.batch_size)
                end_time = time.time()

                global TOTAL_QUERY_TIME
                TOTAL_QUERY_TIME += end_time - start_time

                if not rows:
                    break

                # Write the rows to the CSV file
                start_time = time.time()
                rows_list = [list(row) for row in rows]
                csv_writer.writerows(rows_list)
                end_time = time.time()

                global TOTAL_WRITE_TIME
                TOTAL_WRITE_TIME += end_time - start_time

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

    logger.log('⏰ Finished fetching rows from table.')


def verify_csv_file(csv_filename: str, total_rows: int):
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
        logger.log('✅ CSV file contains all rows from the table.')

        return True
    else:
        logger.log(
            f'⚠️ WARNING: CSV file contains {csv_rows} rows, but table contains {total_rows} rows.')
        logger.log(
            '👉 NOTE: This could be due to data being added to the table while the script is running.')

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
        logger.log(f'Connecting to database {db_name}...', blank_line=True)

        # Connect to the database
        database.connect()

        # Loop through each table in the database and extrct the data
        for table in config.get_db_tables(db_name):
            table_name = table['name']
            primary_key = table['primary_key'] if 'primary_key' in table else 'id'
            incremental = table['incremental'] if 'incremental' in table else False

            # Get the total number of rows in the table
            total_rows = database.run_query(f'SELECT COUNT(*) FROM {table_name}').iloc[0, 0]
            logger.log(
                f'Found total of {total_rows} rows in {table_name} table...', blank_line=True)

            # Get last id from stored table info
            last_id = incremental and config.get_last_id(db_name, table_name) or None

            # Print the number of rows that will be extracted
            if incremental and last_id:
                # select the max id from the table where the id is greater than the last id
                last_record_in_db = database.run_query(
                    f'SELECT MAX({primary_key}) as id FROM {table_name} WHERE {primary_key} > {last_id}').iloc[0, 0]

                # if no result
                if last_record_in_db is None:
                    last_record_in_db = last_id

                rows_to_extract = last_record_in_db - last_id
                logger.log(f'📑 {rows_to_extract} new rows to extract...')
            else:
                rows_to_extract = total_rows

            if not incremental:
                # Delete the CSV file if it exists
                csv_filename = config.get_output_filename(db_name, table_name, args.output_path)
                if os.path.exists(csv_filename):
                    os.remove(csv_filename)

            # Create the output file if it doesn't exist
            create_output_file_if_not_exists(database, db_name, table_name)

            # Fetch rows from the table and write them to the CSV file
            fetch_rows_from_table(database, db_name, table, rows_to_extract)

            # Verify that the CSV file has the correct number of rows
            csv_filename = config.get_output_filename(db_name, table_name, args.output_path)
            verify_csv_file(csv_filename, total_rows)

    except pymysql.Error as error:
        logger.log(f'ERROR: {error}', force=True, blank_line=True)

    finally:
        # Close the database connection
        database.disconnect()

        # Save the config file
        config.save_config()


def main():
    """The main function.
    """

    # Get the start time of the script
    script_start_time = time.time()

    logger.clear_screen()

    # Print the script start message
    logger.log('🏁 Starting script...', force=True, blank_line=True)

    # Loop through each database and table in the configuration
    for [db_name, db_info] in config.get_db_configs().items():
        sync_db(db_name)

    script_time_elapsed = time.time() - script_start_time

    # Print the total time elapsed
    logger.log(
        f'✅ DB Tables Successfully Synced. Finished in {script_time_elapsed} seconds.', force=True, blank_line=True)
    logger.log(f'📊 Total time taken to fetch rows: {TOTAL_QUERY_TIME} seconds.', debug=True)
    logger.log('📊 Total time taken to write to CSV: {TOTAL_WRITE_TIME} seconds.', debug=True)


if __name__ == '__main__':
    main()
