import pandas as pd
import json
import os

script_dir = os.path.dirname(os.path.realpath(__file__))

class Config:
    def __init__(self, config_file_path=os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config.json')):
        self.config_file_path = config_file_path
        self.config = self.load_config()

    def load_config(self):
        """Loads the configuration file into a dictionary.

        Returns:
            dict: The configuration dictionary.
        """
        with open(self.config_file_path, 'r') as config_file:
            config = json.load(config_file)
        return config

    def save_config(self):
        """Saves the configuration dictionary to the configuration file.
        """
        with open(self.config_file_path, 'w') as config_file:
            json.dump(self.config, config_file, indent=4)

    def set_config(self, config):
        """Sets the configuration dictionary.

        Args:
            config (dict): The configuration dictionary.
        """
        self.config = config

    def get_db_configs(self):
        """Returns the database configurations.

        Returns:
            dict: The database configurations.
        """
        return self.config.get('databases', {})

    def get_database_info(self, db_name):
        """Returns the database information for the specified database name.

        Args:
            db_name (str): The name of the database.

        Returns:
            dict: The database information.
        """
        return self.get_db_configs().get(db_name, {})

    def get_db_tables(self, db_name):
        """Returns the table configuration for the specified database name.

        Args:
            db_name (str): The name of the database.

        Returns:
            list: The table configuration.
        """
        return self.get_database_info(db_name).get('tables', [])

    def get_table_info(self, db_name, table_name):
        """Returns the table information for the specified database and table names.

        Args:
            db_name (str): The name of the database.
            table_name (str): The name of the table.

        Returns:
            dict: The table information.
        """
        for table in self.get_db_tables(db_name):
            if table['name'] == table_name:
                return table
        return {}

    def get_last_id(self, db_name, table_name):
        """Returns the last id for the table.

        Args:
            db_name (str): The name of the database.
            table_name (str): The name of the table.

        Returns:
            int: The last id for the table.
        """
        table_info = self.get_table_info(db_name, table_name)
        if 'last_id' in table_info:
            return table_info['last_id']
        elif os.path.exists(self.get_output_filename(db_name, table_name)):
            return pd.read_csv(self.get_output_filename(db_name, table_name))['id'].max()
        else:
            return 0

    def get_output_filename(self, db_name, table_name, output_path=os.path.join(script_dir,'output')):
        """Returns the output filename for the table.

        Args:
            db_name (str): The name of the database.
            table_name (str): The name of the table.

        Returns:
            str: The output filename for the table.
        """
        table_info = self.get_table_info(db_name, table_name)
        if 'output' in table_info:
            return os.path.join(output_path, '{}'.format(table_info['output']))
        else:
            return os.path.join(output_path, '{}_{}.csv'.format(db_name, table_name))

    def set_last_id(self, db_name, table_name, last_id):
        """Sets the last id for the table.

        Args:
            db_name (str): The name of the database.
            table_name (str): The name of the table.
            last_id (int): The last id for the table.
        """
        for table in self.config['databases'][db_name]['tables']:
            if table['name'] == table_name:
                table['last_id'] = last_id
                self.save_config()
                return
        raise ValueError(f"No table named {table_name} in database {db_name}")