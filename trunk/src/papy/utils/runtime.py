""" Functions to gather run-time information about a process.
"""
from IMap import imports

@imports([['os', []], ['collections', []]])
def get_runtime():
    PAPY_RUNTIME = {}
    PAPY_RUNTIME['PID'] = os.getpid()
    PAPY_RUNTIME['PPID'] = os.getppid()
    PAPY_RUNTIME['FORKS'] = [] 
    # collections.defaultdict(list)
    # PAPY_RUNTIME['FORKS'][PAPY_RUNTIME['PID']]
    # defaultdict assign is not atomic value needs to be 
    # created
    return PAPY_RUNTIME
