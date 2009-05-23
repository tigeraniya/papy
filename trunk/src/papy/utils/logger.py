""" loggin facilities for papy.
"""
import logging
from logging import handlers, Formatter
import time

def start_logger(log_level =None, log_to_file =True, log_to_screen =False, log_file =None, log_rotate =False):
    """
    """
    log_level = (log_level or logging.INFO)
    root_log = logging.getLogger()
    formatter = Formatter("%(levelname)s %(asctime)s,%(msecs).3d [%(name)s] - %(message)s", datefmt='%H:%M:%S')
    root_log.setLevel(log_level)
    if log_to_file:
        if log_rotate:
            log_file = (log_file or 'papy_run.log')
            file_handler = handlers.RotatingFileHandler(log_file, mode ='w', backupCount =3)                      
        else:
            run_time  = "_".join(map(str, time.localtime()[0:5]))
            log_file  = "papy_run_%s.log" % (run_time)
            file_handler = logging.FileHandler(log_file, 'w')
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        root_log.addHandler(file_handler)           
    if log_to_screen:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.DEBUG)
        stream_handler.setFormatter(formatter)
        root_log.addHandler(stream_handler)



        
        
        
        
    
   
