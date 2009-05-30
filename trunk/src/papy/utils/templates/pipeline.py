""" This is a prototype of a pipeline, use it as a start-point to construct
    your own pipeline. It shows you how to define your own functions and how
    to use built-in workerf functions. It also uses layout, which allows
    function definitions and pipeline code to be contained in the same file.
"""
## Part 0 Explicit import of all needed classes and the workers sub-module
# In most pipelines a "from papy import *" is appropriate.
from papy import Plumber, Piper, Worker
# papy comes with some built-in workers
from papy import workers

## Part 1 function definitions
# We define a function which returns the arg-th power of the
# first input in the inbox. 
def pow_(inbox, arg):
    """ This function wraps the built-in function pow.
    """
    return pow(inbox[0], arg)

## Part 2 topology definitions.
def pipeline(imap_):
    # Part 2a) Initialize Worker instances (i.e. wrap the functions).
    # the first worker uses a user-define function.
    # the second worker a built-in function.
    pow2 = Worker(pow_, (2,))    
    # first arg: a list of functions (or single function),
    # second arg: a list of lists of arguments (or single list of arguments)
    mul3 = Worker(workers.math.mul, (3,))

    # Part 2b. Initialize Piper instances (i.e. how to run the functions).
    # We define both functions to be run using the same IMap instance which will
    # be created at run-time and is an argument to the pipeline function.
    pow2_piper = Piper(pow2, parallel =imap_)
    mul3_piper = Piper(mul3, parallel =imap_)

    # Part 2c. Define the topology (i.e. how are pipers connected).
    pipes = Plumber()

    # Part 2d. Define the pipeline (i.e. how are the pipers connected)
    pipes.add_pipe((pow2_piper, mul3_piper))
    return pipes


## Part 3. Run-time
# at this stage we have a pipeline definition which only need to be 
# connected to some input and some computational resources.
if __name__ == '__main__':

    imap_ = IMap



    # Part 7. Start the pipeline
    pipes.plunge()                      # a little bit of magic
    pull_me = pipes.get_output()[0]     # this pipeline has only one output
    for i,j in zip(pull_me, data):
        # do something with the data (e.g. print it)
        print '%s -> is:%5s   should be:%5s' % (j,i, ((j*j)*3))

# run time.
if __name__ == '__main__':
        # Part 2. Define input data (any iterator)
    data = xrange(10)               # e.g. a generator


    pipeline()
