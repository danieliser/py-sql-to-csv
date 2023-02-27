import pandas as pd
import json
import os
import argparse
import csv
from tqdm import tqdm
from mysqldb import MySQLDB
from config import Config
import time
import inquirer

start_time = time.time()

# Get the directory where the script is located
script_dir = os.path.dirname(os.path.realpath(__file__))

# Define the command-line arguments
parser = argparse.ArgumentParser(description='Extract data from MySQL tables to CSV files.')
# first unnamed argument is the config file
parser.add_argument('-c', '--config', type=str, default=os.path.join(script_dir, 'config.json'), help='the configuration file to use')
parser.add_argument('-o', '--output', type=str, default=os.path.join(script_dir, 'output'), help='the folder to output to')
parser.add_argument('-v', '--verbose', action='store_true', help='should the script print verbose output')
args = parser.parse_args()

# Log messages if verbose is True
def log(message: str, blank_line: bool = False, force: bool = False):
    """Logs a message if verbose is True.

    Args:
        message (str): The message to log.
        verbose (bool): Whether or not to log the message.
    """

    if args.verbose or force:
        if blank_line:
            print()
        print(message)

# Open the configuration file and load the database and table information
config = Config(args.config)

# Ask user if they want to overwrite tables in the config or append to them.
questions = [inquirer.List('overwrite',
                                message='Select if you want to overwrite tables in the config or append to them:',
                                choices=['Overwrite', 'Append'],
                                default='Overwrite')]
overwrite = inquirer.prompt(questions)

# Merge the answers from both prompts
answers = {**overwrite}

def sync_db(db_name):
    """Create config for db tables.

    Args:
        db_name (str): The name of the database to scan.
    """

    # Get the database information
    db_info = config.get_database_info(db_name)

    # Inquire if user needs to add tables from this database to the config
    questions = [inquirer.Confirm('add_tables',
                                    message='Add tables from the {} database to the config?'.format(db_name),
                                    default=True)]
    add_tables = inquirer.prompt(questions)

    answers.update(add_tables)

    if answers['add_tables'] != True:
        return

    # Create a MySQLDB object
    database = MySQLDB(db_info)

    try:
        log( 'Connecting to database {}...'.format(db_name) )

        # Connect to the database
        database.connect()

        # Get the tables in the database
        tables = database.run_query('SHOW TABLES')

        # Display the checkbox list for each table
        print('\nTables in database {}:'.format(db_name))
        # Prompt the user to select tables
        table_questions = [
            inquirer.Checkbox('tables',
                            message='Select tables to add to config:',
                            choices=[(table[0], table[0]) for table in tables.values],
                            default=None)
        ]
        tables = inquirer.prompt(table_questions)
        answers.update(tables)

        # Prompt the user to select columns
        column_questions = [
            inquirer.List('select_columns',
                        message='Select columns to add to config:',
                        choices=['All', 'Selected'],
                        default='All')
        ]
        select_columns = inquirer.prompt(column_questions)
        answers.update(select_columns)


        # set up array of table configs to write back to config file
        if answers['overwrite'] == 'Append':
            db_table_configs = config.get_db_tables(db_name)
        else:
            db_table_configs = []

        # Generate output for each selected table and add to the config file
        for table in answers['tables']:
            # Get the columns in the table & the primary kek and incremental column
            # If user selected All set columns in config for each table to []
            if answers['select_columns'] == 'All':
                columns = []
            else:
                columns = database.run_query('SHOW COLUMNS FROM {}'.format(table))
                columns = [column[0] for column in columns.values]

            primary_key = database.run_query('SHOW KEYS FROM {} WHERE Key_name = \'PRIMARY\''.format(table))
            primary_key = primary_key.values[0][4] if len(primary_key.values) > 0 else ''

            incremental_column = database.run_query('SHOW KEYS FROM {} WHERE Non_unique = 0'.format(table))
            incremental_column = incremental_column.values[0][4] if len(incremental_column.values) > 0 else ''

            db_table_configs.append( {
                "name": table,
                'columns': columns,
                'where': '',
                'primary_key': primary_key,
                'incremental': False,
                'incremental_column': incremental_column,
                'last_id': 0,
                'output': '{}/{}.csv'.format(db_name, table),
            } )

        # Add the table configs to the config file
        config.config['databases'][db_name]['tables'] = db_table_configs

        config.save_config()

    except Exception as e:
        log( 'ERROR: {}'.format(e) )

    finally:
        # Close the database connection
        database.disconnect()

# Loop through each database and table in the configuration
for db_name, db_info in config.get_db_configs().items():
    sync_db(db_name)

# Print the total time elapsed
log( 'Tables successfully added to config file. Finished in {} seconds.'.format(time.time() - start_time), force=True )
