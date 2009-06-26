"""
"""
from IMap import imports

@imports([['multiprocessing', []], ['os', []], ['collections', []]])
def get_runtime():
    PAPY_RUNTIME = {}
    cp =  multiprocessing.current_process()
    PAPY_RUNTIME['PID'] = cp.pid
    PAPY_RUNTIME['IS_MAIN'] = not cp._parent_pid
    PAPY_RUNTIME['FORKS'] = def

    
# dictionary to hold forks per process.
# If the main process forks its id look-up
# will be atomic defaultdict['x'] is not.
DEFAULTS['PIDS'] = collections.defaultdict(list)
DEFAULTS['PID_MAIN'] = os.getpid()
DEFAULTS['PIDS'][DEFAULTS['PID_MAIN']]
return DEFAULTS

