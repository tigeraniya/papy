""" 
:mod:`papy.utils.logger`
========================

Provides and setups logging facilities for *PaPy*.
"""
import logging
from logging import handlers, Formatter
import time


def start_logger(log_to_file=False, \
                 log_to_stream=False, \
                 log_to_file_level=logging.INFO, \
                 log_to_stream_level=logging.INFO, \
                 log_filename=None, \
                 log_stream=None, \
                 log_rotate=True, \
                 log_size=524288, \
                 log_number=3):
    """
    Function to setup logger to monitor the execution of a *PaPy* pipeline.

    Arguments:

          * log_to_file(bool) [default: ``True``]
            
                Should we save logging messages in a file?

          * log_to_screen(bool) [default: ``False``]

                Should we print logging messages to stdout? 

          * log_to_file_level(int) [default: ``INFO``]

                The minimum logging level of messages to be saved. 
          
          * log_to_screen_level(int) [default: ``ERROR``]

                The minimum logging level of messages to be printed.

          * log_filename(str) [default: ``PaPy_log`` or ``PaPy_log_$TIME$``]

                Name of the log file.

          * log_rotate(bool) [default: ``True``]

                Should we limit the number of logs?

          * log_size(int) [default: 524288]

                Maximum number of bytes saved in a single log file. 
                (only if log_rotate is true)

          * log_number(int) [default: 3] 

                Maximum number of rotatedlogs.
                (only if log_rotate is true)
    """
    if log_rotate:
        log_filename = log_filename or 'PaPy_log'
    else:
        run_time = "_".join(map(str, time.localtime()[0:5]))
        log_filename = 'PaPy_log_%s' % run_time

    root_log = logging.getLogger()
    formatter = Formatter(
            "%(levelname)s %(asctime)s,%(msecs).3d [%(name)s] - %(message)s", \
            datefmt='%H:%M:%S')
    root_log.setLevel(logging.DEBUG)
    if log_to_file:
        if log_rotate:
            file_handler = handlers.RotatingFileHandler(log_filename, \
                                    maxBytes=log_size, backupCount=log_number)
        else:
            file_handler = logging.FileHandler(log_filename, 'w')
        file_handler.setLevel(log_to_file_level)
        file_handler.setFormatter(formatter)
        root_log.addHandler(file_handler)
    if log_to_stream:
        stream_handler = logging.StreamHandler(log_stream)
        stream_handler.setLevel(log_to_stream_level)
        stream_handler.setFormatter(formatter)
        root_log.addHandler(stream_handler)



#EOF
