"""Generate a config file for the data-syncing tool.

This script will prompt the user for the database information and generate a
config file for the data-syncing tool.

Usage:
    python generate-config.py -c <config_file> -o <output_folder>

    -c, --config: The config file to modify. Default: config.json
    -o, --output: The folder to output to. Default: output
"""
import os
import argparse
import sys
import pymysql
from mysqldb import MySQLDB
from config import Config
import inquirer
from logger import Logger
from _version import __version__

# Get the directory where the script is located
script_dir = os.path.dirname(os.path.realpath(__file__))

# Define the command-line arguments
parser = argparse.ArgumentParser(description='Extract data from MySQL tables to CSV files.')

parser.add_argument('-c', '--config', type=str, default=os.path.join(script_dir,
                    'config.json'), help='the configuration file to modify')

parser.add_argument('-o', '--output', type=str, default=os.path.join(script_dir,
                    'output'), help='the folder to output to')

parser.add_argument('-v', '--version', action='version', version='py-mysql-sync ' + __version__)

args = parser.parse_args()

# Setup logger.
logger = Logger()

CONFIG_CACHE = None


def get_config(config_file=None):
    """Get the configuration object.

    Args:
        config_file (str): The path to the configuration file.

    Returns:
        Config: The configuration object.
    """

    if config_file is None:
        config_file = args.config

    # pylint: disable=global-statement
    global CONFIG_CACHE
    if CONFIG_CACHE is None:
        CONFIG_CACHE = Config(config_file)

    return CONFIG_CACHE


def build_db_config():
    """Create config for a new database.

    Returns:
        dict: The database information.
    """

    # Prompt the user for the database information
    # 1. SSH needed?
    # If yes, prompt for SSH info
    # 1.a SSH host
    # 1.b SSH port
    # 1.c SSH username
    # 1.d SSH password
    # 1.e SSH key file
    # 1.f SSH key passphrase
    # 2. Database host
    # 3. Database port
    # 4. Database username
    # 5. Database password
    # 6. Database name (query DB for list of databases)
    # 7. Name for this connection? (default to database name), json key compliant.
    # 8. Database tables
    # 9. Database table columns
    # 10. Database table primary key

    ssh_needed = inquirer.confirm('Does the database require SSH?', default=False)

    ssh_questions = [
        inquirer.Text('ssh_host', message='SSH host'),
        inquirer.Text('ssh_port', message='SSH port', default='22'),
        inquirer.Text('ssh_username', message='SSH username'),
        inquirer.Password('ssh_password', message='SSH password')
    ]

    db_questions = [
        inquirer.Text('db_host', message='Database host',
                      default='127.0.0.1' if ssh_needed else None),
        inquirer.Text('db_port', message='Database port', default='3306'),
        inquirer.Text('db_username', message='Database username'),
        inquirer.Password('db_password', message='Database password'),
        inquirer.Text('db_name', message='Database name'),
    ]

    # Get the database information
    db_info = inquirer.prompt(
        ssh_questions + db_questions if ssh_needed else db_questions
    )

    # Establish a connection to the database
    try:
        # Create a MySQLDB object
        database = MySQLDB(db_info)

        # Connect to the database
        database.connect()
        print("Connection established successfully!")

        # Execute a simple test query
        cursor = database.get_cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        print(f"Test query result: {result[0]}")

        db_info.update({'tables': []})

    except pymysql.err.OperationalError as err:
        print(f"Error connecting to the database: {err}")

    except pymysql.err.ProgrammingError as err:
        print(f"Test query failed: {err}")

    finally:
        # Close the database connection
        database.disconnect()

        # Return the database information even if the connection failed. Correct = in file.
        # pylint: disable=lost-exception
        return db_info


def build_db_table_configs(db_name):
    """Create config for db tables.

    Args:
        db_name (str): The name of the database to scan.

    Returns:
        list: The table configurations.
    """

    config = get_config()

    # Get the database information
    db_info = config.get_database_info(db_name)

    # Create a MySQLDB object
    database = MySQLDB(db_info)
    
    # Initialize db_tables outside try/except to prevent UnboundLocalError
    db_tables = []

    try:
        logger.log(f'Connecting to database {db_name}...')

        # Connect to the database
        database.connect()

        # Get the tables in the database
        tables = database.run_query('SHOW TABLES')
        
        if hasattr(tables, 'values'):
            table_list = tables.values
        else:
            table_list = []

        # Display the checkbox list for each table
        print(f'\nTables in database {db_name}:')

        if len(table_list) == 0:
            print("No tables found in database")
            return db_tables

        checkbox_choices = [(table[0], table[0]) for table in table_list]

        # Prompt the user to select tables
        table_qa = inquirer.prompt([
            inquirer.Checkbox('tables',
                              message='Select tables to add to config:',
                              choices=checkbox_choices,
                              default=None),
            inquirer.List('select_columns',
                          message='Select columns to add to config:',
                          choices=['All', 'Selected'],
                          default='All'),
            inquirer.Text('output_path', message='Output path:', default=args.output)
        ])

        # Generate output for each selected table and add to the config file
        for table in table_qa['tables']:
            # Get the columns in the table & the primary kek and incremental column
            # If user selected All set columns in config for each table to []
            if table_qa['select_columns'] == 'All':
                columns = []
            else:
                columns = database.run_query(f'SHOW COLUMNS FROM {table}')
                columns = [column[0] for column in columns.values]

            primary_key = database.run_query(
                f'SHOW KEYS FROM {table} WHERE Key_name = \'PRIMARY\'')
            primary_key = primary_key.values[0][4] if len(primary_key.values) > 0 else ''

            incremental_column = database.run_query(
                f'SHOW KEYS FROM {table} WHERE Non_unique = 0')
            incremental_column = incremental_column.values[0][4] if len(
                incremental_column.values) > 0 else ''

            db_tables.append({
                "name": table,
                'columns': columns,
                'where': '',
                'primary_key': primary_key,
                'incremental': False,
                'incremental_column': incremental_column,
                'last_id': 0,
                'output': f"{table_qa['output_path']}/{db_name}/{table}.csv",
            })

    except Exception as err:
        logger.log(f'ERROR: {err}')

    finally:
        # Close the database connection
        database.disconnect()

        # pylint: disable=lost-exception
        return db_tables

# Actions for setting up configs


def generate_db_config():
    """Create a new database connection for config.

    Returns:
        tuple: The name of the database and the database information.
    """

    # Prompt for the database information
    db_info = build_db_config()

    # Prompt the user for a name for this connection, must only be alphanumeric and - or _
    db_name = inquirer.text('Enter a name for this connection',
                            default=db_info['db_name'] if 'db_name' in db_info else '',
                            validate=lambda _, x: x.isalnum() or '-' in x or '_' in x)

    # Inquire if user needs to add tables from this database to the config
    create_tables = inquirer.confirm('Do you want to add tables for this DB?',
                                     default=True)

    if create_tables is True:
        db_info['tables'] = build_db_table_configs(db_name)

    return db_name, db_info


def update_db_config(db_name):
    """Update an existing database connection for config.

    Args:
        db_name (str): The name of the database to update.

    Returns:
        tuple: The name of the database and the database information.
    """

    while True:

        action = inquirer.list_input('What do you want to do?',
                                     choices=['Update connection info', 'Add tables', 'Done'])

        if action == 'Update connection info':
            db_info = build_db_config()
            # Safely get existing tables or default to empty list
            existing_tables = get_config().get_database_info(db_name).get('tables', [])
            db_info['tables'] = existing_tables

        elif action == 'Add tables':
            db_info = get_config().get_database_info(db_name)
            
            # Ensure tables key exists in db_info
            if 'tables' not in db_info:
                db_info['tables'] = []

            # Inquire if user needs to add tables from this database to the config
            write_mode = inquirer.list_input('Append tables to existing config or overwrite?',
                                             choices=['Overwrite', 'Append'],
                                             default='Append')

            tables = build_db_table_configs(db_name)

            if write_mode == 'Append':
                db_info['tables'] += tables
            else:
                db_info['tables'] = tables

        elif action == 'Done':
            break

    return db_name, db_info


def main():
    """Main function for the program."""

    logger.clear_screen()

    # if config file doesn't exist, create it
    if not os.path.isfile(args.config):
        if not inquirer.confirm('No config file found. Create a new one?', default=True):
            sys.exit(0)

        with open(args.config, 'w', encoding="utf8") as config_file:
            config_file.write('{"databases": {}}')

    # Open the configuration file and load the database and table information
    config = get_config()

    config_version = config.config.get('version', None)

    if config_version is None:
        config.config['version'] = __version__
        config.save_config()

    if config_version is not None and config_version > __version__:
        logger.log('Config file version is newer than this version of the program.', log_type='ERROR')
        sys.exit(1)

    if config_version is not None and config_version < __version__:
        logger.log('Config file version is older than this version of the program.', log_type='WARNING')
        config.config['version'] = __version__
        config.save_config()

    db_configs = config.get_db_configs()

    while True:

        action = inquirer.list_input('What would you like to do?',
                                     choices=['Add a new database connection',
                                              'Update an existing database connection',
                                              'Exit'],
                                     default='Add a new database connection'
                                     if len(db_configs) == 0 else
                                     'Update an existing database connection')

        if action == 'Add a new database connection':
            db_name, db_info = generate_db_config()
            config.add_db_config(db_name, db_info)
        elif action == 'Update an existing database connection':
            db_name = inquirer.prompt([
                inquirer.List('db_name',
                              message='Select a database:',
                              choices=[(db, db) for db in config.get_db_configs().keys()])
            ])['db_name']

            db_name, db_info = update_db_config(db_name)
            config.update_db_config(db_name, db_info)
        elif action == 'Exit':
            break

    # Save the config file
    config.save_config()
    logger.log('Config file updated.')


if __name__ == '__main__':
    main()
