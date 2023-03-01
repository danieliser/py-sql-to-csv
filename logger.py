"""Defines a logger function for use in the data-syncing scripts.

This script is meant to be imported into other scripts.

Example:
    >>> from logger import Logger
    >>> logger = Logger(verbose=True)
    >>> logger.log('Hello, world!')
    Hello, world!

Attributes:
    verbose (bool): Whether or not to log messages.
    debug (bool): Whether or not to log debug messages.
    file (str): The file to log to.

Methods:
    log(message: str, blank_line: bool = False, force: bool = False, debug: bool = False):
        Logs a message if verbose is True.
    clear_line():
        Clears the current line.
    clear_screen():
        Clears the screen.

Todo:
    * Add a way to log to a file.
"""
from datetime import datetime
import os

# ensure file gets written even if script is interrupted
os.system('stty intr ^-')


class Logger:
    """A logger class for use in the data-syncing scripts."""

    def __init__(self, verbose: bool = False, debug: bool = False, file: str = None):
        self.verbose = verbose
        self.debug = debug
        self.file = file
        self.logs = []

    def write_to_log_file(self, message: str):
        """Writes a message to a log file.

        Args:
            message (str): The message to write.
            file (str): The file to write to.
        """

        if not self.file:
            return

        with open(self.file, 'a+', encoding="utf8") as logfile:
            logfile.write(message)

    def log(self, message: str, blank_line: bool = False, force: bool = False, debug: bool = False, log_type: str = None):
        """Logs a message if verbose is True.

        Args:
            message (str): The message to log.
            verbose (bool): Whether or not to log the message.
        """

        # WARNING/ERROR -> DEBUG -> INFO
        message_type = 'DEBUG' if debug else log_type if log_type else 'INFO'

        if debug and not self.debug:
            return

        try:
            if self.verbose or force or (self.debug and debug):
                if blank_line:
                    print()
                print(message)
        finally:

            # If file logging is enabled, build a message with the current time in log format.
            if self.file:
                log_header = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} {message_type}'
                if blank_line:
                    self.logs.append('\n')
                self.write_to_log_file(f'{log_header}: {message}\n')

    def clear_line(self):
        """Clears the current line."""
        print('\r', end='')

    def clear_screen(self):
        """Clears the screen."""
        print('\033c', end='')
