# Data Syncing

## Description

This simple python tool incrementally syncs data from configured databases & tables to local csv files. It is designed to be run as a cron job.

Currently only MySQL databases are supported, and exported to CSV. If there is interest in other database types or export formats, please [open an issue](https://github.com/danieliser/py-sql-to-csv/issues).

## Features

* Full or incremental syncs of data from MySQL databases to CSV files.
* Configurable via JSON file.
* Can be run from CLI or cron.
* Supports multiple databases and tables.
* Supports syncing same table multiple times with different filters.
* Supports SSH tunneling to remote databases.
* Post sync validation of row counts.
* GUI tool to generate & update configs.

## Future Features

* Support for other database types.
*

## Installation

1. Clone this repository
2. Install python dependencies: `pip install -r requirements.txt` or `conda install --file requirements.txt`.
3. Create a `config.json` file.
   a. Use the GUI to generate a config file by running `python ./config_editor.py`
   b. Or copy the `config.example.json` file to `config.json` and edit it to configure your database connections.
4. Run the tool by calling  `python ./sync-db-tables.py`.

## Configuration

The configuration file is a JSON file with the following structure:

```json
{
    "databases": {
        "db1": {
            "ssh_host": "1.2.3.4",
            "ssh_username": "sshuser",
            "ssh_password": "sshpass",
            "db_host": "127.0.0.1",
            "db_username": "dbuser",
            "db_password": "dbpass",
            "db_name": "dbname",
            "tables": [
                {
                    "name": "table1",
                    "columns": [],
                    "where": "",
                    "primary_key": "id",
                    "incremental": false,
                    "incremental_column": "id",
                    "last_id": 0,
                    "output": "db1/table1.csv",
                }
            }
        }
    }
}
```

## Usage

The tool is designed to be run via CLI or as a cron job. It will sync all tables configured in the config file. It will also update the `last_id` field in the config file to the last id of the table. This is used to only sync new rows in the next run if incremetnal is enabled.

### Arguments

The tool accepts the following arguments:

* `-c, --config` - The path to the config file. Defaults to `config.json`.
* `-o, --output-path` - The path to the output directory. Defaults to `./output`.
* `-d, --debug` - Enable debug logging.
* `-l, --log` - Path to log file.
* `--verbose` - Enable verbose logging.
* `--batch-size` - Batch size, default 1000.
* `--skip-validation` - Skip validation of row counts.
* `--incremental-only` - Only sync incremental tables.
* `--skip-incremental` - Only sync full tables.
* `--tables` - Sync specific tables, accpets comma separated list of `db_name.table_name` format.
* `-h, --help` - Show help message and exit.
* `-V, --version` - Show version and exit.

## License

MIT

## Author

[Daniel Iser](https://github.com/danieliser)
