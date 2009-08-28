#!/usr/bin/env python
"""
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
@imports(['MMTK.PDB'])
def create_model(inbox, strip):
    filename = inbox[0]
    return True

@imports(['MMTK', 'MMTK.ForceFields', 'MMTK.Minimization'])
def minimize_model(inbox, steps):
    model = inbox[0]
    return model



# Part 2: Define the topology
def pipeline():
    imap_md = IMap()
    pipes = Plumber()

    # initialize Worker instances (i.e. wrap the functions).
    w_create_model = Worker(create_model, kwargs={'strip': True})
    w_minimize_model = Worker(minimize_model, kwargs={'steps': 100})

    # initialize Piper instances (i.e. attach functions to runtime)
    p_create_model = Piper(w_create_model, debug=True)
    p_minimize_model = Piper(w_minimize_model, debug=True)

    # create the pipeline and connect pipers
    pipes.add_pipe((p_create_model, p_minimize_model))


    return pipes


# Part 3: execute the pipeline
if __name__ == '__main__':
    pipes = pipeline()
    pipes.start([range(10)])
    pipes.run()
    pipes.wait()
    pipes.pause()
    pipes.stop()
    print pipes.stats


