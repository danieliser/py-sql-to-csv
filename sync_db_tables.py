"""Extracts data from MySQL tables to CSV files."""
import time
import os
import argparse
import csv
import signal
import sys
import threading
import pandas as pd
import pymysql
from mysqldb import MySQLDB
from config import Config
from logger import Logger
from _version import __version__

# pylint: disable=global-statement

# SIMPLE SIGNAL HANDLING - Set handler in main thread only
# Global cancellation flag for graceful shutdown  
CANCEL_REQUESTED = False
CANCEL_EVENT = threading.Event()

def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully - MAIN THREAD ONLY"""
    global CANCEL_REQUESTED
    
    if not CANCEL_REQUESTED:
        # First Ctrl+C - graceful shutdown
        CANCEL_REQUESTED = True
        CANCEL_EVENT.set()
        
        # Use sys.stderr for immediate display
        sys.stderr.write(f"\n🛑 SIGNAL DETECTED - Cancelling export gracefully...\n")
        sys.stderr.flush()
    else:
        # Second Ctrl+C - force exit
        sys.stderr.write(f"\n❌ FORCE QUIT\n")
        sys.stderr.flush()
        
        try:
            # Try to save config one last time
            if 'config' in globals():
                config.save_config()
        except:
            pass
        
        # Force kill
        os._exit(1)

def setup_signal_handling():
    """Set up signal handling in main thread"""
    # Reset to default first, then set our handler
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal_handler)

# Get the directory where the script is located
script_dir = os.path.dirname(os.path.realpath(__file__))

# Define the command-line arguments
parser = argparse.ArgumentParser(description='Extract data from MySQL tables to CSV files.',
                                 prog='MySQL Sync')

parser.add_argument('-c', '--config', type=str, default=os.path.join(script_dir,
                    'config.json'), help='the configuration file to use')

parser.add_argument('-o', '--output-path', type=str,
                    default=os.path.join(script_dir, 'output'), help='the folder to output to')

parser.add_argument('-v', '--verbose', action='count', default=0,
                    help='increase output verbosity (use -v, -vv, or -vvv for more detail)')
parser.add_argument('--quiet', action='store_true',
                    help='suppress all non-error output')

parser.add_argument('--no-progress', action='store_true',
                    help='disable progress bar (fixes Ctrl+C issues on some systems)')

parser.add_argument('-d', '--debug', action='store_true', default=bool(os.environ.get(
    'PYTHONDEBUG')), help='should the script print verbose output')

parser.add_argument('--batch-size', type=int, default=1000,
                    help='number of records to process per database query batch')

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

parser.add_argument('--adaptive', action='store_true', default=False,
                    help='enable adaptive performance management (batch size and pause adjustments)')

parser.add_argument('--degradation-threshold', type=float, default=0.7,
                    help='performance degradation threshold (0.7 = 30%% degradation triggers adaptation)')

parser.add_argument('--max-pause', type=int, default=1000,
                    help='maximum pause duration between batches in milliseconds')

parser.add_argument('--min-pause', type=int, default=0,
                    help='minimum pause duration between batches in milliseconds')
parser.add_argument('--max-retries', type=int, default=3,
                    help='maximum number of retries for database operations')
parser.add_argument('--retry-delay', type=float, default=2.0,
                    help='initial delay in seconds between retries (exponential backoff)')
parser.add_argument('--connection-timeout', type=int, default=30,
                    help='database connection timeout in seconds')
parser.add_argument('--query-timeout', type=int, default=300,
                    help='database query timeout in seconds')
parser.add_argument('--fast-count', action='store_true', default=False,
                    help='use fast table row count estimation (SHOW TABLE STATUS) instead of exact COUNT(*)')

parser.add_argument('--min-batch-size', type=int, default=50,
                    help='minimum batch size for adaptive reduction')

parser.add_argument('--max-batch-multiplier', type=float, default=2.0,
                    help='maximum batch size multiplier for proactive optimization (2.0 = 2x original)')

# ML-based adaptive parameters
parser.add_argument('--learning-rate', type=float, default=0.1,
                    help='ML learning rate for gradient-based optimization (0.01-0.5)')

parser.add_argument('--momentum', type=float, default=0.9,
                    help='ML momentum factor for smoothing adjustments (0.7-0.95)')

parser.add_argument('--exploration-rate', type=float, default=0.05,
                    help='ML exploration rate for discovering better parameter zones (0.01-0.1)')

parser.add_argument('--smoothing-window', type=int, default=10,
                    help='exponential moving average window size for noise reduction')

parser.add_argument('--version', action='version', version='%(prog)s ' + __version__)

args = parser.parse_args()

if args.debug:
    TOTAL_QUERY_TIME = 0
    TOTAL_WRITE_TIME = 0

# Open the configuration file and load the database and table information
config = Config(args.config)
logger = Logger(verbose=args.verbose, debug=args.debug, file=args.log)

TABLE_SCHEMAS = {}

# Custom progress display that doesn't interfere with signals
class CustomProgressDisplay:
    def __init__(self, total, table_name):
        self.total = total
        self.table_name = table_name
        self.start_time = time.time()
        self.last_update = 0
        self.update_interval = 0.5  # Update every 500ms minimum
        
    def update(self, processed, batch_rate, overall_rate, adaptive_status=None):
        """Update progress display using raw stdout to avoid signal interference"""
        current_time = time.time()
        
        # Throttle updates to avoid spam
        if current_time - self.last_update < self.update_interval:
            return
            
        self.last_update = current_time
        
        # Calculate percentage
        percentage = (processed / self.total * 100) if self.total > 0 else 0
        
        # Build progress bar (simple text version)
        bar_width = 30
        filled = int(bar_width * percentage / 100)
        bar = '█' * filled + '░' * (bar_width - filled)
        
        # Build status parts
        desc_parts = []
        desc_parts.append(f"Batch: {batch_rate:.0f} rec/sec")
        desc_parts.append(f"Overall: {overall_rate:.0f} rec/sec") 
        desc_parts.append(f"Total: {processed:,}")
        
        if adaptive_status:
            desc_parts.append(f"({adaptive_status})")
        
        # Create complete status line
        status = f"\r🤖 {self.table_name} |{bar}| {percentage:5.1f}% | {' | '.join(desc_parts)}"
        
        # Write directly to stdout and flush immediately
        sys.stdout.write(status)
        sys.stdout.flush()
        
        # Check for cancellation after each display update
        if CANCEL_REQUESTED:
            sys.stdout.write(f"\n🛑 Cancellation detected during progress update\n")
            sys.stdout.flush()
            return
    
    def close(self):
        """Finish the progress display"""
        sys.stdout.write('\n')
        sys.stdout.flush()

# Simple adaptive performance tracking - delay adjustment only
class SimpleAdaptivePerformanceTracker:
    def __init__(self, max_pause=1000, min_pause=0, degradation_threshold=0.7, **kwargs):
        # Simple tracking parameters
        self.max_pause = max_pause
        self.min_pause = min_pause
        self.degradation_threshold = degradation_threshold
        
        # Current state - batch size stays FIXED
        self.fixed_batch_size = None
        self.current_pause = self.min_pause  # Only thing that adapts
        
        # Performance tracking
        self.baseline_performance = None
        self.recent_performance = []
        self.window_size = 5  # Small window for responsiveness
        
    def initialize(self, initial_batch_size):
        self.fixed_batch_size = initial_batch_size
        
    def record_query_time(self, query_time, batch_size):
        """Record query performance and apply simple delay-only adaptation"""
        if query_time <= 0:
            return self.fixed_batch_size, self.current_pause
            
        # Calculate performance rate (records per second)
        current_rate = batch_size / query_time
        
        # Track recent performance
        self.recent_performance.append(current_rate)
        if len(self.recent_performance) > self.window_size:
            self.recent_performance.pop(0)
            
        # Set baseline on first samples
        if self.baseline_performance is None and len(self.recent_performance) >= 3:
            self.baseline_performance = sum(self.recent_performance) / len(self.recent_performance)
            
        # Simple adaptation: only adjust pause based on performance vs baseline
        if self.baseline_performance and len(self.recent_performance) >= 3:
            recent_avg = sum(self.recent_performance) / len(self.recent_performance)
            performance_ratio = recent_avg / self.baseline_performance
            
            # If performance drops below threshold, increase pause
            if performance_ratio < self.degradation_threshold:
                self.current_pause = min(self.current_pause + 50, self.max_pause)
            # If performance is good, gradually reduce pause
            elif performance_ratio > 0.9 and self.current_pause > self.min_pause:
                self.current_pause = max(self.min_pause, self.current_pause - 25)
        
        return self.fixed_batch_size, self.current_pause
    
    def get_status_info(self):
        """Get current simple adaptation status for display"""
        status_parts = ["Adaptive"]
        
        # Always show current delay (even if 0)
        delay_info = f"⏱️{self.current_pause}ms"
        if self.current_pause == self.min_pause:
            delay_info += " (min)"
        elif self.current_pause == self.max_pause:
            delay_info += " (MAX)"
        status_parts.append(delay_info)
            
        # Show performance status
        if self.baseline_performance and len(self.recent_performance) >= 3:
            recent_avg = sum(self.recent_performance) / len(self.recent_performance)
            perf_ratio = recent_avg / self.baseline_performance
            
            if perf_ratio > 1.05:
                status_parts.append("📈")  # Better than baseline
            elif perf_ratio < self.degradation_threshold:
                status_parts.append("📉")  # Poor performance
            else:
                status_parts.append("🔥")  # Good stable performance
                
        return " | ".join(status_parts)


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


def fetch_rows_from_table(database: MySQLDB, db_name: str, table, rows_to_extract: int, where: str = None):
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

        # Set up simple adaptive performance tracking (delay only)
        if args.adaptive:
            perf_tracker = SimpleAdaptivePerformanceTracker(
                max_pause=args.max_pause,
                min_pause=args.min_pause,
                degradation_threshold=args.degradation_threshold
            )
            perf_tracker.initialize(args.batch_size)
        else:
            perf_tracker = None

        # Set up the initial offset and other variables
        offset = 0
        starting_id = last_id

        # Get a cursor object
        cursor = database.get_cursor()

        # Track overall performance
        script_start_time = time.time()
        total_records_processed = 0

        # Use custom progress display that doesn't interfere with signals
        if args.verbose and not args.no_progress:
            progress_display = CustomProgressDisplay(rows_to_extract, table_name)
        else:
            progress_display = None

        query_select = f'SELECT * FROM {table_name}'

        # Create base WHERE template (rebuilt each loop to prevent accumulation)
        base_where = 'WHERE 1=1'
        if where:
            base_where += f' AND {where}'

        # High-performance ID-based cursor pagination (5-10x faster than OFFSET)
        current_id = starting_id or 0
        batch_count = 0
        total_processed = 0
        
        # Loop through the table using ID cursor instead of OFFSET
        try:
            while total_processed < rows_to_extract and not CANCEL_REQUESTED:
                # Simple cancellation check - background thread will set the flag
                if CANCEL_REQUESTED:
                    sys.stdout.write(f"\n✅ Export cancelled gracefully after {total_processed:,} records\n")
                    sys.stdout.flush()
                    break
                    
                # Batch size is now fixed - only pause adapts
                limit = args.batch_size
                    
                # Build query with ID cursor (always fast O(log n))
                query_where = base_where
                if current_id > 0:
                    query_where += f' AND {primary_key} > {current_id}'

                query = f'{query_select} {query_where} ORDER BY {primary_key} LIMIT {limit}'
                
                # Print the SQL query
                logger.log(f"Executing query: {query}", debug=True)
                
                # Always measure query time for performance tracking
                query_start_time = time.time()

                cursor.execute(query)
                rows = cursor.fetchall()

                query_end_time = time.time()
                query_time = query_end_time - query_start_time

                if args.debug:
                    global TOTAL_QUERY_TIME
                    TOTAL_QUERY_TIME += query_time

                if not rows:
                    break

                # Adaptive performance management
                if perf_tracker:
                    limit, pause_ms = perf_tracker.record_query_time(query_time, len(rows))
                    
                    # Apply pause if needed
                    if pause_ms > 0:
                        time.sleep(pause_ms / 1000.0)  # Convert ms to seconds

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

                # Update cursor to last processed ID (key optimization!)
                last_row = rows[-1]
                primary_key_index = table_schema['Field'].to_list().index(primary_key)
                current_id = last_row[primary_key_index]

                # Update the last synced id in the config file
                config.set_last_id(db_name, table_name, int(current_id))

                # Update progress tracking
                batch_records = len(rows_list)
                total_processed += batch_records
                total_records_processed += batch_records
                batch_count += 1

                # Calculate performance metrics
                current_time = time.time()
                overall_duration = current_time - script_start_time
                batch_rate = batch_records / query_time if query_time > 0 else 0
                overall_rate = total_records_processed / overall_duration if overall_duration > 0 else 0

                # Update the custom progress display with enhanced metrics
                if progress_display is not None:
                    # Get adaptive status when enabled
                    adaptive_status = None
                    if perf_tracker:
                        adaptive_status = perf_tracker.get_status_info()
                    
                    # Update progress display
                    progress_display.update(total_records_processed, batch_rate, overall_rate, adaptive_status)

                    # Save the config file
                    config.save_config()

        except KeyboardInterrupt:
            # Handle Ctrl+C during processing
            if not CANCEL_REQUESTED:
                sys.stdout.write(f"\n🛑 Keyboard interrupt - exiting gracefully after {total_processed:,} records\n")
                sys.stdout.flush()
            config.save_config()

        if progress_display is not None:
            progress_display.close()

        csv_file.close()
        
        # Final status message
        if CANCEL_REQUESTED:
            logger.log(f'🛑 Export cancelled gracefully. Progress saved: {total_processed:,} records exported. Resume from ID: {current_id}')
        else:
            logger.log('⏰ Finished fetching rows from table.')
        
        # Always save config so we can resume from where we left off
        config.save_config()


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

    # Create a MySQLDB object with resilience settings
    database = MySQLDB(db_info, verbose=args.verbose, max_retries=args.max_retries, retry_delay=args.retry_delay, connection_timeout=args.connection_timeout, query_timeout=args.query_timeout)

    try:
        logger.log(f'Connecting to database {db_name}...', blank_line=True)

        # Connect to the database
        database.connect()

        # Loop through each table in the database and extrct the data
        for table in config.get_db_tables(db_name):
            # Check for cancellation before processing each table
            if CANCEL_REQUESTED:
                logger.log(f'🛑 Export cancelled - skipping remaining tables')
                break
                
            table_name = table['name']
            primary_key = table['primary_key'] if 'primary_key' in table else 'id'
            incremental = table['incremental'] if 'incremental' in table else False

            query_where = 'WHERE 1=1'
            where = None

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

            try:
                # Get the total number of rows in the table (with faster estimation for large tables)
                use_fast_estimate = args.fast_count or (args.adaptive and query_where.strip() == 'WHERE 1=1')
                total_rows = database.get_table_row_count(table_name, query_where, use_fast_estimate)
                logger.log(
                    f'👀 Found total of {total_rows} rows in {table_name} table...')
            except Exception as e:
                logger.log(f'❌ Failed to get row count for table {table_name}: {e}', log_type='ERROR')
                logger.log(f'👉 Skipping table {table_name} due to error', log_type='NOTE')
                continue

            # Get last id from stored table info
            last_id = incremental and config.get_last_id(db_name, table_name) or None

            try:
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
            except Exception as e:
                logger.log(f'❌ Failed to determine rows to extract for table {table_name}: {e}', log_type='ERROR')
                logger.log(f'👉 Skipping table {table_name} due to error', log_type='NOTE')
                continue

            try:
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
                    
            except Exception as e:
                logger.log(f'❌ Failed to process table {table_name}: {e}', log_type='ERROR')
                logger.log(f'👉 Continuing with next table...', log_type='NOTE')
                continue

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

    # Set up signal handling in main thread
    setup_signal_handling()

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
