# This pipeline finds prime numbers within
from papy import *
from IMap import IMap

#1. function definitions
def is_prime(number):
    """ Chaeck if number is a prime.
    """
    divisor = 1
    while True:
        divisor += 1
        if number[0] == divisor:
            return (True, number[0])
        elif not (number[0] % divisor):
            return (False, number[0])

def if_print(inp):
    """ Print if prime.
    """ 
    result = inp[0]
    if result[0]:
        print result[1]


#2. parallelism and topology of pipeline
def pipeline(Imap):
    # wrap the functions into pipers
    primer = Piper(is_prime, parallel =Imap)
    printer = Piper(if_print)
    # define the topology
    pipes = Plumber()
    pipes.add_pipe((primer, printer))
    return pipes

#3. run-time
if __name__ == '__main__':
    # run it twice in as a parallel process and linear
    for imap_ in (IMap(ordered =False), IMap(ordered =False, worker_type ='thread')):
        # make input input 
        numbers = xrange(int(1e8),int(1e8+50))
        # get pipeline instance
        pipes = pipeline(imap_)
        # connect input data
        pipes.set_inputs([numbers])
        # start calculations/processing in background
        pipes.plunge()
        # wait until it is finished
        print imap_.pool
        pipes._is_finished.wait() 
        print "Calculation using %s took %fs" % (imap_, pipes.stats['run_time'])

