"""
:mod:`papy`
===========
   :platform: Python 2.5/2.6
   :synopsis: Create a data processing pipeline.

Papy - Parallel pipelines in Python
"""
from papy import Worker, Piper, Dagger, Plumber,\
                 PiperError, WorkerError, DaggerError, PlumberError

from graph import Graph, Node
import workers
import utils

#EOF
