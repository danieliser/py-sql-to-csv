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
from _version import __version__

# pylint: disable=global-statement

# Get the directory where the script is located
script_dir = os.path.dirname(os.path.realpath(__file__))

# Define the command-line arguments
parser = argparse.ArgumentParser(description='Extract data from MySQL tables to CSV files.',
                                 prog='MySQL Sync')

parser.add_argument('-c', '--config', type=str, default=os.path.join(script_dir,
                    'config.json'), help='the configuration file to use')

parser.add_argument('-o', '--output-path', type=str,
                    default=os.path.join(script_dir, 'output'), help='the folder to output to')

parser.add_argument('--verbose', action='store_true',
                    help='should the script print verbose output')

parser.add_argument('-d', '--debug', action='store_true', default=bool(os.environ.get(
    'PYTHONDEBUG')), help='should the script print verbose output')

parser.add_argument('--batch-size', type=int, default=1000,
                    help='should the script print verbose output')

parser.add_argument('-l', '--log', type=str,
                    help='save logs to a file')

parser.add_argument('--skip-validation', action='store_true', default=False,
                    help='skip validation of the row counts')

parser.add_argument('--incremental-only', action='store_true', default=False,
                    help='only run sync on incremental tables')

parser.add_argument('--skip-incremental', action='store_true', default=False,
                    help='skip incremental tables')

parser.add_argument('--tables', type=str, default=None,
                    help='comma separated list of tables to sync. ex db1.table1')

parser.add_argument('-v', '--version', action='version', version='%(prog)s ' + __version__)

args = parser.parse_args()

if args.debug:
    TOTAL_QUERY_TIME = 0
    TOTAL_WRITE_TIME = 0

# Open the configuration file and load the database and table information
config = Config(args.config)
logger = Logger(verbose=args.verbose, debug=args.debug, file=args.log)

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
    logger.log(f'Table Name: {table_name}', debug=True)

    # Print the fields in comma list to the console
    logger.log(f"Table schema: {','.join(table_schema['Field'])}", debug=True)

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


def fetch_rows_from_table(database: MySQLDB, db_name: str, table, rows_to_extract: int, where: str =None):
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

        query_select = f'SELECT * FROM {table_name}'

        query_where = 'WHERE 1=1'

        if where:
            query_where += f' AND {where}'

        query_limits = f' LIMIT {limit} OFFSET {offset}'

        # Loop through the table, fetching rows in chunks and writing them to the CSV file
        while offset < rows_to_extract:
            if starting_id:
                query_where += f' AND {primary_key} > {starting_id}'

            query = f'{query_select} {query_where} {query_limits}'

            # Print the SQL query
            logger.log(f"Executing query: {query}", debug=True)
            cursor.execute(query)

            # Loop through the table, fetching rows in chunks and writing them to the CSV file
            while True:

                if args.debug:
                    # Measure the time taken to fetch rows
                    start_time = time.time()

                rows = cursor.fetchmany(args.batch_size)

                if args.debug:
                    end_time = time.time()

                    global TOTAL_QUERY_TIME
                    TOTAL_QUERY_TIME += end_time - start_time

                if not rows:
                    break

                if args.debug:
                    # Set timers for debug mode.
                    start_time = time.time()

                # Write the rows to the CSV file
                rows_list = [list(row) for row in rows]
                csv_writer.writerows(rows_list)

                if args.debug:
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

    logger.log(
        f'⚠️ CSV file contains {csv_rows} rows, but table contains {total_rows} rows.',
        log_type='WARNING')
    logger.log(
        '👉 This could be due to data being added to the table while the script is running.',
        log_type='NOTE')

    return False


def sync_db(db_name, tables=None):
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

            query_where = 'WHERE 1=1'

            if table['where']:
                where = table['where']
                query_where += f' AND {where}'

            if tables and table_name not in tables:
                continue

            # Print the table name
            logger.log(f'📄 Processing table {table_name}...', blank_line=True)

            # Skip the table if the --skip_incremental flag is set and the table is incremental
            if args.skip_incremental and incremental:
                logger.log(
                    '👉 Skipping incremental table because --skip_incremental flag was set.',
                    log_type='NOTE')
                continue

            # Skip the table if the --incremental_only flag is set and the table is not incremental
            if args.incremental_only and not incremental:
                logger.log(
                    '👉 Skipping non-incremental table because --incremental_only flag was set.',
                    log_type='NOTE')
                continue

            # Get the total number of rows in the table
            total_rows = database.run_query(f'SELECT COUNT(*) FROM {table_name} {query_where}').iloc[0, 0]
            logger.log(
                f'👀 Found total of {total_rows} rows in {table_name} table...')

            # Get last id from stored table info
            last_id = incremental and config.get_last_id(db_name, table_name) or None

            # Print the number of rows that will be extracted
            if incremental and last_id:
                # select the max id from the table where the id is greater than the last id
                last_record_in_db = database.run_query(
                    f'SELECT MAX({primary_key}) as id FROM {table_name} {query_where} AND {primary_key} > {last_id}').iloc[0, 0]

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
            fetch_rows_from_table(database, db_name, table, rows_to_extract, where=where)

            if not args.skip_validation:
                # Verify that the CSV file has the correct number of rows
                csv_filename = config.get_output_filename(db_name, table_name, args.output_path)
                verify_csv_file(csv_filename, total_rows)

    except pymysql.Error as error:
        logger.log(error, force=True, blank_line=True, log_type='ERROR')

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

    # final shape of tables should be { db_name: [table_name, table_name] }
    tables = {}

    # If the --tables flag is set, parse the tables into a dict
    if args.tables:
        # split tables into list of db_name.table_name
        tables_list = [table.split('.') for table in args.tables.split(',')]

        # loop through each table and add to tables dict
        for [db_name, table_name] in tables_list:
            if db_name not in tables:
                tables[db_name] = []

            tables[db_name].append(table_name)

    # Loop through each database and table in the configuration
    for [db_name, _db_info] in config.get_db_configs().items():
        # If args.tables, only sync DB if it's in the tables_list dict, pass tables as second arg
        if args.tables:
            if db_name in tables:
                sync_db(db_name, tables[db_name])
        else:
            sync_db(db_name)

    if args.debug:
        # Print the total time elapsed
        logger.log(
            f'✅ DB Tables Successfully Synced. Finished in {script_time_elapsed} seconds.',
            force=True, blank_line=True)
        script_time_elapsed = time.time() - script_start_time
        logger.log(f'📊 Total time taken to fetch rows: {TOTAL_QUERY_TIME} seconds.', debug=True)
        logger.log('📊 Total time taken to write to CSV: {TOTAL_WRITE_TIME} seconds.', debug=True)

    else:
        logger.log('✅ DB Tables Successfully Synced.', force=True, blank_line=True)


if __name__ == '__main__':
    main()
