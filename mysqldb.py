"""MySQL database connection class.

This class is used to connect to a MySQL database via an SSH tunnel. It

can be used to run queries and return the results as a Pandas dataframe.

Example:

    db = MySQLDB(db_info)

    db.connect()

    sql = "SELECT * FROM table"
    df = db.run_query(sql)

    db.disconnect()

    print(df)

    # Output:
    #
    #   id  name
    # 0  1  John
    # 1  2  Jane

Attributes:
    db_info (dict): Dictionary containing database connection information.
    tunnel (SSHTunnelForwarder): SSH tunnel object.
    connection (pymysql.Connection): MySQL database connection object.
    verbose (bool): Whether to print verbose output.
    cursor (pymysql.Cursor): MySQL cursor object.

Todo:
    * Add support for SSH key authentication
"""

import logging
import time
import random
import sshtunnel
import pandas as pd
import pymysql
from pymysql import connect
from sshtunnel import SSHTunnelForwarder

class MySQLDB:
    """MySQL database connection class with resilience features."""
    def __init__(self, db_info, verbose=0, max_retries=3, retry_delay=1.0, connection_timeout=30, query_timeout=300):
        self.db_info = db_info
        self.tunnel = None
        self.connection = None
        self.verbose = verbose  # 0=quiet, 1=basic, 2=detailed, 3=debug
        self.cursor = None
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.connection_timeout = connection_timeout
        self.query_timeout = query_timeout

    def open_ssh_tunnel(self):
        """Open an SSH tunnel and connect using a username and password."""

        db_info = self.db_info

        ssh_host = db_info.get('ssh_host')

        if ssh_host:
            if self.verbose >= 3:
                sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG

            # Use the configured MySQL host and port for the remote bind address
            remote_mysql_host = db_info.get('db_host', '127.0.0.1')
            remote_mysql_port = int(db_info.get('db_port', 3306))
            ssh_port = int(db_info.get('ssh_port', 22))

            self.tunnel = SSHTunnelForwarder(
                (ssh_host, ssh_port),
                ssh_username=db_info['ssh_username'],
                ssh_password=db_info['ssh_password'],
                remote_bind_address=(remote_mysql_host, remote_mysql_port),
            )

            self.tunnel.start()

    def mysql_connect(self):
        """Connect to a MySQL server using the SSH tunnel connection
        """

        db_info = self.db_info

        if self.tunnel:
            # When using SSH tunnel, connect to localhost on the tunnel's local port
            host = '127.0.0.1'
            port = self.tunnel.local_bind_port
        else:
            # Direct connection without SSH tunnel
            host = db_info['db_host']
            port = int(db_info.get('db_port', 3306))

        self.connection = connect(
            host=host,
            user=db_info['db_username'],
            passwd=db_info['db_password'],
            db=db_info['db_name'],
            port=port,
            connect_timeout=self.connection_timeout,
            read_timeout=self.query_timeout,
            write_timeout=self.query_timeout,
            autocommit=True,
            charset='utf8mb4'
        )

    def get_cursor(self):
        """Get a cursor object to execute SQL queries on.

        :return: Cursor object
        """

        if self.cursor is None:
            self.cursor = self.connection.cursor()

        return self.cursor

    def is_connection_alive(self):
        """Check if the database connection is still alive.
        
        :return: True if connection is alive, False otherwise
        """
        try:
            if not self.connection:
                return False
            self.connection.ping(reconnect=False)
            return True
        except Exception:
            return False
    
    def reconnect(self):
        """Reconnect to the database with retry logic.
        
        :return: True if reconnection successful, False otherwise
        """
        for attempt in range(self.max_retries):
            try:
                if self.verbose >= 2:
                    print(f"Reconnection attempt {attempt + 1}/{self.max_retries}")
                
                # Close existing connections
                self.disconnect()
                
                # Wait with exponential backoff
                if attempt > 0:
                    delay = self.retry_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
                    if self.verbose >= 3:
                        print(f"Waiting {delay:.1f} seconds before retry...")
                    time.sleep(delay)
                
                # Reconnect
                self.connect()
                
                if self.is_connection_alive():
                    if self.verbose >= 1:
                        print("Reconnection successful")
                    return True
                    
            except Exception as e:
                if self.verbose >= 2:
                    print(f"Reconnection attempt {attempt + 1} failed: {e}")
                continue
        
        if self.verbose >= 1:
            print("All reconnection attempts failed")
        return False
    
    def run_query_with_retry(self, sql: str, max_retries=None):
        """Runs a SQL query with automatic retry on connection failure.
        
        :param sql: MySQL query
        :param max_retries: Override default retry count
        :return: Pandas dataframe containing results
        """
        if max_retries is None:
            max_retries = self.max_retries
            
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                # Check connection health before query
                if not self.is_connection_alive():
                    if self.verbose >= 1:
                        print("Connection lost, attempting to reconnect...")
                    if not self.reconnect():
                        raise pymysql.OperationalError(2006, "MySQL server has gone away")
                
                # Execute query
                return pd.read_sql_query(sql, self.connection)
                
            except (pymysql.OperationalError, pymysql.InterfaceError, pymysql.InternalError) as e:
                last_exception = e
                error_code = e.args[0] if e.args else 0
                
                # Retry on connection-related errors
                if error_code in (2006, 2013, 2014, 2003) and attempt < max_retries:
                    if self.verbose >= 1:
                        print(f"Query attempt {attempt + 1} failed with error {error_code}: {e}")
                    if self.verbose >= 2:
                        print("Attempting to reconnect and retry...")
                    
                    if not self.reconnect():
                        continue
                    
                    # Wait before retry
                    delay = self.retry_delay * (2 ** attempt) + random.uniform(0, 0.5)
                    time.sleep(delay)
                    continue
                else:
                    # Re-raise non-connection errors or final attempt
                    raise
                    
            except Exception as e:
                # Re-raise non-MySQL errors immediately
                raise
        
        # If we get here, all retries failed
        raise last_exception if last_exception else RuntimeError("Query failed after all retries")
    
    def run_query(self, sql: str):
        """Runs a given SQL query via the global database connection.

        :param sql: MySQL query
        :return: Pandas dataframe containing results
        """
        
        return self.run_query_with_retry(sql)

    def run_query_with_cursor(self, sql: str):
        """Runs a given SQL query via the global database connection with cursor.

        :param sql: MySQL query
        :return: Pandas dataframe containing results
        """
        
        return self.run_query_with_retry_cursor(sql)
    
    def run_query_with_retry_cursor(self, sql: str, max_retries=None):
        """Runs a SQL query using cursor with automatic retry on connection failure.
        
        :param sql: MySQL query
        :param max_retries: Override default retry count
        :return: Pandas dataframe containing results
        """
        if max_retries is None:
            max_retries = self.max_retries
            
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                # Check connection health before query
                if not self.is_connection_alive():
                    if self.verbose >= 1:
                        print("Connection lost, attempting to reconnect...")
                    if not self.reconnect():
                        raise pymysql.OperationalError(2006, "MySQL server has gone away")
                
                # Reset cursor after reconnection
                self.cursor = None
                cursor = self.get_cursor()
                cursor.execute(sql)
                
                results = cursor.fetchall()
                cols = [desc[0] for desc in cursor.description]
                return pd.DataFrame(results, columns=cols)
                
            except (pymysql.OperationalError, pymysql.InterfaceError, pymysql.InternalError) as e:
                last_exception = e
                error_code = e.args[0] if e.args else 0
                
                # Retry on connection-related errors
                if error_code in (2006, 2013, 2014, 2003) and attempt < max_retries:
                    if self.verbose >= 1:
                        print(f"Cursor query attempt {attempt + 1} failed with error {error_code}: {e}")
                    if self.verbose >= 2:
                        print("Attempting to reconnect and retry...")
                    
                    # Reset cursor
                    self.cursor = None
                    
                    if not self.reconnect():
                        continue
                    
                    # Wait before retry
                    delay = self.retry_delay * (2 ** attempt) + random.uniform(0, 0.5)
                    time.sleep(delay)
                    continue
                else:
                    # Re-raise non-connection errors or final attempt
                    raise
                    
            except Exception as e:
                # Re-raise non-MySQL errors immediately
                raise
        
        # If we get here, all retries failed
        raise last_exception if last_exception else RuntimeError("Cursor query failed after all retries")
    
    def get_table_row_count(self, table_name, where_clause="", use_fast_estimate=False):
        """Get row count for a table with options for faster estimation.
        
        :param table_name: Name of the table
        :param where_clause: Optional WHERE clause (empty string if none)
        :param use_fast_estimate: If True, use SHOW TABLE STATUS for faster estimate
        :return: Row count (int)
        """
        
        if use_fast_estimate and not where_clause:
            # Use SHOW TABLE STATUS for fast estimation (not exact but much faster)
            try:
                sql = f"SHOW TABLE STATUS LIKE '{table_name}'"
                result = self.run_query_with_retry(sql)
                if not result.empty and 'Rows' in result.columns:
                    estimated_rows = result.iloc[0]['Rows']
                    if self.verbose >= 2:
                        print(f"Using fast estimate of {estimated_rows} rows for {table_name}")
                    return int(estimated_rows) if estimated_rows is not None else 0
            except Exception as e:
                if self.verbose >= 2:
                    print(f"Fast estimate failed, falling back to COUNT(*): {e}")
        
        # Fall back to exact COUNT(*)
        where_sql = f" {where_clause}" if where_clause else ""
        sql = f"SELECT COUNT(*) FROM {table_name}{where_sql}"
        
        if self.verbose >= 3:
            print(f"Running exact count query: {sql}")
            
        result = self.run_query_with_retry(sql)
        return int(result.iloc[0, 0])
    
    def test_connection(self):
        """Test the database connection with a simple query.
        
        :return: True if connection works, False otherwise
        """
        try:
            result = self.run_query_with_retry("SELECT 1 as test")
            return len(result) == 1 and result.iloc[0, 0] == 1
        except Exception:
            return False

    def mysql_disconnect(self):
        """Closes the MySQL database connection.
        """
        
        try:
            if self.cursor:
                self.cursor.close()
                self.cursor = None
        except Exception:
            pass  # Ignore cursor close errors
            
        try:
            if self.connection:
                self.connection.close()
                self.connection = None
        except Exception:
            pass  # Ignore connection close errors

    def close_ssh_tunnel(self):
        """Closes the SSH tunnel connection.
        """

        if self.tunnel:
            self.tunnel.close()

    def connect(self):
        """Connects to the database and opens an SSH tunnel if needed.
        """

        self.open_ssh_tunnel()
        self.mysql_connect()

    def disconnect(self):
        """Closes the database connection and SSH tunnel if needed.
        """

        self.mysql_disconnect()
        self.close_ssh_tunnel()
