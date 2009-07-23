#!/usr/bin/env python
# Part 0: import the PaPy infrastructure.
# interface of the API: 
from papy import Plumber, Dagger, Piper, Worker
# parallel IMap function: 
from IMap import IMap, imports
# all example workers
from papy import workers
from papy.utils import logger
import logging
logger.start_logger(log_level=logging.DEBUG, log_to_screen =False, log_rotate =True)


@imports([['re',[]]])
def search(inbox, regexp):
    chunk = inbox[0]
    search = re.compile(regexp).search
    output = []
    while True:
        line = chunk.readline()
        if not line:
            break
        matched = search(line)
        if matched:
            result = matched.groups()[0]
            output.append(result)
    print len(output)
    return output

# Part 2: Define the topology
def pipeline(runtime_):
    fg, imap_, ofh = runtime_
    mapper = Worker((workers.io.mmap_item, search, workers.io.json_dumps, workers.io.dump_shm_item),\
                   ((False,), ('ID=SNP:([^;]*)',),(),()))
    reducer = Worker((workers.io.load_shm_item, workers.io.read_item, workers.io.dump_stream),\
                   ((),(),(ofh,'DELIMITER')))
    mapper_p = Piper(mapper, parallel =imap_)
    reducer_p = Piper(reducer)

    pipes = Plumber()
    pipes.add_pipe((mapper_p, reducer_p))
    pipes.set_inputs([fg])
    return pipes

# Part 3: Define the run-time
def runtime(options):
    ifn = options['--ifile']                            # file-name
    ofn = options['--ofile']                            # file-name
    ifh = open(ifn, 'rb')                               # file-handle
    ofh = open(ofn, 'wb')                               # file-handle
    chunk = float(options['--chunk'])                   # chunk-size
    # this splits a file into chunks of given size.
    chunker = workers.io.make_items(ifh, int(chunk * (2**20))) # 2**20 is 1M
    worker_num = int(options['--worker_num'])           # worker number
    if worker_num > 1:
        imap_ = IMap(worker_num =worker_num, ordered=False, stride=20)
    else:
        imap_ = None
    return chunker, imap_, ofh

# Execute:
if __name__ == '__main__':
    # get command-line options using getopt 
    import sys
    import time
    from getopt import getopt
    options = dict(getopt(sys.argv[1:], '', ['ifile=', 'ofile=', 'worker_num=', 'chunk='])[0])
    # initialize runtime
    runtime_ = runtime(options)
    # initialize the pipeline
    pipes = pipeline(runtime_)
    # start the pipeline 
    start = time.time()
    pipes.plunge()
    pipes._is_finished.wait()
    print time.time() - start
    
