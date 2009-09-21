#!/usr/bin/env python
"""
"""
# Part 0: import the PaPy infrastructure.
# interface of the API: 
from papy import Plumber, Piper, Worker
# the parallel IMap function and imports wrapper
from IMap import IMap, imports
from papy import workers
# logging support
import logging

# Part 1: Define user functions
@imports(['socket', 'os', 'threading'])
def where(inbox):
    """
    This function identifies the host/process/thread.
    """
    result = "input: %s, host:%s, parent %s, process:%s, thread:%s" % \
    (inbox[0], \
     socket.gethostname(), \
     os.getppid(), \
     os.getpid(), \
     threading._get_ident())
    return result

# Part 2: Define the topology
def pipeline(workers):
    imap_ = IMap(worker_num =0, worker_remote=workers)
    if not use_tcp:
        w_where = Worker(where)
        w_print = Worker(workers.io.print_)
    else:
        w_where = Worker((where, workers.io.dump)
    p_where = Piper(w_where, parallel =imap_)
    p_print = Piper(workers.io.print_, debug =True)
    pipes = Plumber()
    pipes.add_pipe((p_where, p_print))
    return pipes

# Part 3: execute the pipeline
if __name__ == '__main__':
    # get command-line arguments using getopt 
    import sys
    from getopt import getopt
    args = dict(getopt(sys.argv[1:], '', ['use_tcp=', 'workers='])[0])

    # parse arguments    
    use_tcp = eval(args['--use_tcp'])
    host_numbers = args['--workers']
    host_numbers = host_number_commas.split(',')
    workers = [hn.split('#') for hn in host_numbers]
    
    pipes = pipeline(workers, use_tcp)
    pipes.start([range(4)])
    print pipes.get_inputs()[0].imap.pool
    pipes.run()
    pipes.wait()
    pipes.pause()
    pipes.stop()
    print pipes.stats

