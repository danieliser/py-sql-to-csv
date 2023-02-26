import pandas as pd
import pymysql
import logging
import sshtunnel
from sshtunnel import SSHTunnelForwarder

class MySQLDB:
    def __init__(self, db_info, verbose=False):
        self.db_info = db_info
        self.tunnel = None
        self.connection = None
        self.verbose = verbose

    def open_ssh_tunnel(self):
        """Open an SSH tunnel and connect using a username and password.
        """

        db_info = self.db_info

        ssh_host = db_info.get('ssh_host')

        if ssh_host:
            if self.verbose:
                sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG

            self.tunnel = SSHTunnelForwarder(
                (ssh_host, 22),
                ssh_username=db_info['ssh_username'],
                ssh_password=db_info['ssh_password'],
                remote_bind_address=('127.0.0.1', 3306),
            )

            self.tunnel.start()

    def mysql_connect(self):
        """Connect to a MySQL server using the SSH tunnel connection
        """

        db_info = self.db_info

        local_port = self.tunnel.local_bind_port if self.tunnel else None

        self.connection = pymysql.connect(
            host=db_info['db_host'],
            user=db_info['db_username'],
            passwd=db_info['db_password'],
            db=db_info['db_name'],
            port=local_port
        )

    def run_query(self, sql):
        """Runs a given SQL query via the global database connection.

        :param sql: MySQL query
        :return: Pandas dataframe containing results
        """

        return pd.read_sql_query(sql, self.connection)

    def mysql_disconnect(self):
        """Closes the MySQL database connection.
        """

        if self.connection:
            self.connection.close()

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
