import pandas as pd
import pymysql
import json
import logging
import sshtunnel
import os
import argparse
import csv
from sshtunnel import SSHTunnelForwarder

# Get the directory where the script is located
script_dir = os.path.dirname(os.path.realpath(__file__))

# Define the command-line arguments
parser = argparse.ArgumentParser(description='Extract data from MySQL tables to CSV files.')
parser.add_argument('--config', type=str, default=os.path.join(script_dir, 'config.json'), help='the configuration file to use')
args = parser.parse_args()

# Open the configuration file and load the database and table information
with open(args.config, 'r') as config_file:
    config = json.load(config_file)

def open_ssh_tunnel(db_info={}, verbose=False):
    """Open an SSH tunnel and connect using a username and password.

    :param db_info: Dictionary containing the SSH host, username, and password
    :param verbose: Set to True to show logging
    :return tunnel: Global SSH tunnel connection
    """

    if verbose:
        sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG

    global tunnel

    tunnel = SSHTunnelForwarder(
        (db_info['ssh_host'], 22),
        ssh_username = db_info['ssh_username'],
        ssh_password = db_info['ssh_password'],
        remote_bind_address = ('127.0.0.1', 3306)
    )

    tunnel.start()

def mysql_connect(db_info={}):
    """Connect to a MySQL server using the SSH tunnel connection

    :param db_info: Dictionary containing the db host, username, password, and database
    :return connection: Global MySQL database connection
    """

    global connection

    connection = pymysql.connect(
        host=db_info['db_host'],
        user=db_info['db_username'],
        passwd=db_info['db_password'],
        db=db_info['db_name'],
        port=tunnel.local_bind_port
    )

def run_query(sql):
    """Runs a given SQL query via the global database connection.

    :param sql: MySQL query
    :return: Pandas dataframe containing results
    """

    return pd.read_sql_query(sql, connection)

def mysql_disconnect():
    """Closes the MySQL database connection.
    """

    connection.close()

def close_ssh_tunnel():
    """Closes the SSH tunnel connection.
    """

    tunnel.close

def save_config():
    """Saves the configuration file.
    """

    with open(args.config, 'w') as config_file:
        json.dump(config, config_file, indent=4)

# Loop through each database and table in the configuration
for db_name, db_info in config.items():

    # If db_info contains ssh_host & its not empty, then we need to open an SSH tunnel
    if 'ssh_host' in db_info and db_info['ssh_host']:
        open_ssh_tunnel(db_info=db_info)

    # Connect to the MySQL database
    mysql_connect(db_info=db_info)

    # Loop through each table in the database and extract the data
    for table_name, table_info in db_info['tables'].items():
        # Name of the CSV file to create for this table
        if 'output' in table_info:
            csv_filename = table_info['output']
        else:
            csv_filename = '{}_{}.csv'.format(db_name, table_name)

        # Get last id from stored table info
        last_id = table_info.get('last_id', 0)

        # Get the total number of rows in the table
        total_rows = run_query('SELECT COUNT(*) FROM {}'.format(table_name)).iloc[0,0]

        print( 'Found {} rows in {} table...'.format(total_rows, table_name) )

        # Print the number of rows that will be extracted
        if last_id:
            print( 'Extracting rows with id > {}...'.format(last_id) )
            rows_to_extract = run_query('SELECT COUNT(*) FROM {} WHERE id > {}'.format(table_name, last_id)).iloc[0,0]
            print( 'Found {} rows to extract...'.format(rows_to_extract) )
        else:
            print( 'Extracting all rows...' )

        print()

        # Check if CSV exists, if not create it with header row by querying table schema
        if not os.path.exists(csv_filename):
            # Get table schema
            table_schema = run_query('DESCRIBE {}'.format(table_name))

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

            # Loop through the table, fetching rows in chunks and writing them to the CSV file
            while offset < total_rows:
                if last_id:
                    rows = run_query('SELECT * FROM {} WHERE id > {} LIMIT {} OFFSET {}'.format(table_name, last_id, limit, offset))
                else:
                    rows = run_query('SELECT * FROM {} LIMIT {} OFFSET {}'.format(table_name, limit, offset))

                if rows.empty:
                    break

                # print the first 5 rows nicely to the console for review
                print( rows.head() )

                # for each row, write it to the CSV file, headers already exist
                for row in rows.itertuples(index=False):
                    csv_writer.writerow(row)

                # Update the last synced id file with the last `id` value in the table
                last_id = rows.iloc[-1]['id']

                # Update the last synced id in the config file
                config[db_name]['tables'][table_name]['last_id'] = int(last_id)

                # Save the config file
                save_config()

                offset += limit

    # Close the database connection
    mysql_disconnect()

    # If db_info contains ssh_host & its not empty, then we need to close the SSH tunnel
    if 'ssh_host' in db_info and db_info['ssh_host']:
        close_ssh_tunnel()
