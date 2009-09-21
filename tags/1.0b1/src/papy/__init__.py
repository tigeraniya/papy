"""
Papy - Parallel pipelines in Python

Copyright (c) 2009, Marcin Cieslik
All rights reserved.


This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

:mod:`papy`
===========
   :platform: Python 2.6
   :synopsis: Create and parallel/ distributed data processing pipeline.
"""

__author__ = 'Marcin Cieslik <mpc4p@virginia.edu>'
__version__ = '1.0.0'

from papy import Worker, Piper, Dagger, Plumber, \
                 PiperError, WorkerError, DaggerError, PlumberError, \
                 imports

from graph import Graph, Node
import tkgui
import workers
import utils

#EOF
