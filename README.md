# Data Syncing

## Description

This simple python tool incrementally syncs data from configured databases & tables to local csv files. It is designed to be run as a cron job.

Currently only MySQL databases are supported, and exported to CSV. If there is interest in other database types or export formats, please [open an issue](https://github.com/danieliser/py-sql-to-csv/issues).

## Installation

1. Clone this repository
2. Install python dependencies: `pip install -r requirements.txt` or `conda install --file requirements.txt`.
3. Copy the `config.example.json` file to `config.json` and edit it to configure your databases and tables.
4. Run the tool by calling  `python sync-db-tables.py`.

## Configuration

The configuration file is a JSON file with the following structure:

```json
{
    "db1": {
        "ssh_host": "1.2.3.4",
        "ssh_username": "sshuser",
        "ssh_password": "sshpass",
        "db_host": "127.0.0.1",
        "db_username": "dbuser",
        "db_password": "dbpass",
        "db_name": "dbname",
        "tables": {
            "table1": {
                "last_id": 0,
                "output": "output/table1.csv"
            }
        }
    }
}
```

## Usage

The tool is designed to be run as a cron job. It will sync all tables configured in the config file. It will also update the `last_id` field in the config file to the last id of the table. This is used to only sync new rows in the next run.

### Arguments

The tool accepts the following arguments:

* `-c, --config` - The path to the config file. Defaults to `config.json`.
* `-o, --output-path` - The path to the output directory. Defaults to `./output`.
* `-v, --verbose` - Enable verbose logging.

## License

MIT

## Author

[Daniel Iser](https://github.com/danieliser)
