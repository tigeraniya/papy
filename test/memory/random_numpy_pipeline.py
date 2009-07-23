#!/usr/bin/env python
from papy import Plumber, Piper
# parallel IMap function: 
from IMap import IMap, imports
# all example workers
from papy import workers

@imports([['numpy.random', ['random']]])
def make_random():
    while True:
        a = random(100000)
        yield a

def min_(inbox):
    return inbox[0]

# Part 2: Define the topology
def pipeline(runtime_):
    randoms_, imap_ = runtime_
    min1 = Piper(min_, parallel =imap_)
    prnt = Piper(workers.io.print_)
    pipes = Plumber()
    pipes.add_pipe((min1, prnt))
    pipes.set_inputs([randoms_])
    return pipes

# Execute:
if __name__ == '__main__':
    randoms_ = make_random()
    imap_ = IMap()
    pipes = pipeline((randoms_, imap_))
    pipes.plunge()
    
