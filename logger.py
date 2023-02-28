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

class Logger:
    """A logger class for use in the data-syncing scripts."""

    def __init__(self, verbose: bool = False, debug: bool = False):
        self.verbose = verbose
        self.debug = debug

    def log(self, message: str, blank_line: bool = False, force: bool = False, debug: bool = False):
        """Logs a message if verbose is True.

        Args:
            message (str): The message to log.
            verbose (bool): Whether or not to log the message.
        """

        if debug and not self.debug:
            return

        if self.verbose or force or (self.debug and debug):
            if blank_line:
                print()
            print(message)

    def clear_line(self):
        """Clears the current line."""
        print('\r', end='')

    def clear_screen(self):
        """Clears the screen."""
        print('\033c', end='')
