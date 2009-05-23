""" This is a prototype of a pipeline, use it as a start-point
"""
# Part 0
# This is almost "from papy import *"
from papy import Plumber, Piper, Worker
from papy import PlumberError, DaggerError, PiperError, WorkerError
# papy comes with some bundled workers
from papy import workers

# Part 1 function definitions
# e.g.:
def pow_(inbox, arg):
    """ This function wraps the built-in function pow.
    """
    return pow(inbox[0], arg)

def pipeline():
    # Part 2. Define input data (any iterator)
    data = xrange(10)               # e.g. a generator

    # Part 3. Initialize Worker instances (i.e. wrap the functions)
    pwr = Worker(pow_, (2,))    # 1 arg: a list of functions (or single function),
                                # 2 arg: a list of lists of arguments (or single list of arguments)
    mul3 = Worker(workers.math.mul, (3,))

    # Part 4. Initialize Piper instances (i.e. define worker behaviour: parallelis, timeouts)
    p_pwr = Piper(pwr, parallel =1) # a parallel but order preserving piper
    p_mul3 = Piper(mul3, parallel =0)

    # Part 5. Initialize the pipeline (e.g.: what and where to log)
    pipes = Plumber(log_file ='papy_pipeline_run.log')

    # Part 6. Define the pipeline (i.e. how are the pipers connected)
    pipes.add_pipe((p_pwr, p_mul3))
    pipes.set_input([data],p_pwr)

    # Part 7. Start the pipeline
    pipes.plunge()                      # a little bit of magic
    pull_me = pipes.get_output()[0]     # this pipeline has only one output
    for i,j in zip(pull_me, data):
        # do something with the data (e.g. print it)
        print '%s -> is:%5s   should be:%5s' % (j,i, ((j*j)*3))

if __name__ == '__main__':
    pipeline()