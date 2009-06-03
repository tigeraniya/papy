#!/usr/bin/env python
""" This is a prototype of a pipeline, use it as a start-point to construct your
    own. The construction of a pipeline is split into parts, this has several
    reasons. First it makes your code modular as it detaches the definition of a
    work-flow from the runtime i.e. the real data and computational resources
    and it allows to group all the elements into a single-file executable
    script.

    All the steps are as explicit as possible. If you prefer the less flexible
    but shorter implicit API features please refer to the documentation.
    # Add links 
"""
# Part 0: import the PaPy infrastructure.
# interface of the API: 
from papy import Plumber, Dagger, Piper, Worker
# parallel IMap function: 
from IMap import IMap
# all built-in workers
from papy import workers
# logging facility
from papy.utils import logger

# Part 1: Define user functions
def pow_(inbox, arg):
    """ This function wraps the built-in function pow.
    """
    return pow(inbox[0], arg)

# Part 2: Define the topology
def pipeline(runtime_):
    input_data, imap_ = runtime_
    # initialize Worker instances (i.e. wrap the functions).
    pow2 = Worker(pow_, (2,))
    mul3 = Worker(workers.maths.mul_, (3,))
    prnt = Worker(workers.io.print_)
    # initialize Piper instances (i.e. attach functions to runtime)
    pow2_piper = Piper(pow2, parallel =imap_)
    mul3_piper = Piper(mul3, parallel =imap_)
    prnt_piper = Piper(prnt)
    pipes = Plumber()
    pipes.add_pipe((pow2_piper, mul3_piper, prnt_piper))
    pipes.set_inputs([input_data])
    return pipes

# Part 3: Define the run-time
def runtime(options):
    size = int(options['--size'])
    input_data = xrange(size)
    logger.start_logger(log_to_screen =False, log_rotate =True)
    return input_data

# Execute:
if __name__ == '__main__':
    # get command-line options using getopt 
    import sys
    from getopt import getopt
    options = dict(getopt(sys.argv[1:], '',['size=', 'worker_num='])[0])
    # initialize runtime
    input_data = runtime(options)
    worker_num = int(options['--worker_num'])
    imap_ = IMap(worker_num =worker_num)
    # initialize the pipeline
    pipes = pipeline((input_data, imap_))
    # start the pipeline 
    pipes.plunge()
    
