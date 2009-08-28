#!/usr/bin/env python
"""
This is a prototype of a pipeline, use it as a start-point to construct your 
own. The construction of a pipeline is split into parts, this has several
reasons. First it makes your code modular as it detaches the definition of a
workflow from the runtime i.e. the real data and computational resources and it
allows to group all the elements into a single-file executable script.

All the steps are as explicit as possible. If you prefer the less flexible but 
shorter implicit API features please refer to the documentation.
"""
# Part 0: import the PaPy infrastructure.
# interface of the API: 
from papy import Plumber, Piper, Worker
# the parallel IMap function and imports wrapper: 
from IMap import IMap, imports
# all example workers
from papy import workers
# logging support
from papy.utils import logger
logger.start_logger(log_rotate=False)


# Part 1: Define user functions
@imports(['socket', 'os', 'threading'])
def who(inbox):
    """ This function identifies the host/process/thread.
    """
    return "input: %s, host:%s, parent %s, process:%s, thread:%s" % \
    (inbox[0], socket.gethostname(), os.getppid(), \
     os.getpid(), threading._get_ident())

# Part 2: Define the topology
def pipeline(resources):
    imap1, imap2, imap3 = resources
    # initialize Worker instances (i.e. wrap the functions).
    w_mul = Worker(workers.maths.mul, (3,))
    w_who = Worker(who)
    w_prn = Worker(workers.io.print_)
    # initialize Piper instances (i.e. attach functions to runtime)
    p_mul = Piper(w_mul, parallel=imap1, track=True, name='mul')
    p_who = Piper(w_who, parallel=imap2)
    p_prn = Piper(w_prn, parallel=imap3)
    # create the pipeline and connect pipers
    pipes = Plumber()
    pipes.add_pipe((p_mul, p_who, p_prn))
    return pipes

# Part 3: parse the arguments
def options(args):
    size = int(args['--size'])
    worker_num = int(args['--worker_num'])
    return (size, worker_num)

# Part 4: define the resources
def resources(args):
    size, worker_num = args
    imap1 = IMap(worker_num=worker_num)
    imap2 = IMap(worker_num=worker_num)
    imap3 = None
    return imap1, imap2, imap3

# Part 5: define the input
def data(args):
    size, worker_num = args
    return xrange(size)

# Part 6: Execute
if __name__ == '__main__':
    # get command-line arguments using getopt 
    import sys
    from getopt import getopt
    args = dict(getopt(sys.argv[1:], '', ['size=', 'worker_num='])[0])
    # parse options
    opts = options(args)
    # definie/initialize resources
    rsrc = resources(opts)
    # define/create input data
    inpt = data(opts)
    # attach resources to pipeline
    pipes = pipeline(rsrc)
    # connect and start pipeline
    pipes.start([inpt])
    # run and wait until pipeline is finished
    pipes.run()
    pipes.wait()
    pipes.pause()
    pipes.stop()
    print pipes.stats


