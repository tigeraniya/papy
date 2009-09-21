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
def pipeline(worker_remote, use_tcp):
    imap_ = IMap(worker_num=0, worker_remote=worker_remote)
    if not use_tcp:
        w_where = Worker(where)
        w_print = Worker(workers.io.print_)
    else:
        w_where = Worker((where, workers.io.dump_item), kwargs=(
                                                       {},
                                                       {'type':'tcp'}
                                                       ))
        w_print = Worker((workers.io.load_item, workers.io.print_))
    p_where = Piper(w_where, parallel=imap_)
    p_print = Piper(w_print, debug=True)
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
    use_tcp = eval(args['--use_tcp']) # bool
    worker_remote = args['--workers']
    worker_remote = worker_remote.split(',')
    worker_remote = [hn.split('#') for hn in worker_remote]
    worker_remote = [(h, int(n)) for h, n in worker_remote]

    pipes = pipeline(worker_remote, use_tcp)
    pipes.start([range(100)])
    pipes.run()
    pipes.wait()
    pipes.pause()
    pipes.stop()
    print pipes.stats

