#!/usr/bin/env python
from papy import Plumber, Piper, Worker
# parallel IMap function: 
from IMap import IMap, imports
# all example workers
from papy import workers

# Part 2: Define the topology
def pipeline(runtime_):
    typ = runtime_
    dumper = Worker((workers.io.pickle_dumps, workers.io.dump_item), ((),('tcp',)))
    loader = Worker(workers.io.load_item)
    unpickler = Worker(workers.io.pickle_loads)

    imap_ = IMap(worker_num =0, worker_remote =[['localhost:18814', 4]])
    p_dumper = Piper(dumper, parallel =imap_, debug =False)
    p_loader = Piper(loader, debug =False)
    p_unpickler = Piper(unpickler)
    p_printer = Piper(workers.io.print_)

    pipes = Plumber()
    pipes.add_pipe((p_dumper, p_loader, p_unpickler, p_printer))
    return pipes

# Execute:
if __name__ == '__main__':
    from numpy import random
    data = random.random(6)
    print data
    pipes = pipeline(('tcp'))
    pipes.plunge([data])
    
