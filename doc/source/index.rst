=======================================
**PaPy** - Parallel Pipelines in Python
=======================================

This documentation covers the implementation and usage of *PaPy*. It consists of
a hand-written manual and an API-reference. Please refer also to the comments in
the source code. You will find *PaPy* useful if you need to establish a  data 
processing pipeline using Python libraries or external tools.

    * project page: 
        http://code.google.com/p/papy/
    * repository lives at: 
        http://papy.googlecode.com/svn/trunk/
    * most recent documentation: 
        http://papy.googlecode.com/svn/trunk/doc/sources/
    * most recent html documentation: 
        http://papy.googlecode.com/svn/trunk/html/index.html
    * author email: 
        marcin.cieslik@gmail.com
  
.. warning::

    **parallel features of** *PaPy* **do not work in the interactive 
    interpreter**
  
    Code snippets, examples and use cases are not meant to be typed into the 
    interactive interpreter console. They should be run from the command line
    (this is a limitation of the ``multiprocessing`` module)::
  
        $ python example_file.py

    The relevant files are available in the doc directory of the source-code
    repository.

    Functions defined in the interactive interpreter will not work (and will 
    hang Python) if used with *IMap* instances!  A python function can only be
    communicated to multiple processes if it can be serialized(i.e. pickled) 
    this is not possible if the function is in the same namespace as a child 
    process (created using the multiprocessing library).
 
    **The interpreter will hang on exit if a pipeline does not finish or is 
    halted abnormaly**
  
    The Python interpreter exits (returns) if all spawned threads or forked
    processes return. *PaPy* uses multiple threads to manage the pipeline and
    evaluates functions in seperate threads or processes. All of them need to be
    stopped before the parent python process can return. This is done
    automatically whenever a pipeline finishes or some expected exception 
    occurs, in all other cases it is required that the user stops the pipeline
    manually.


Manual
======

Written documentation.

.. toctree::
   :maxdepth: 2

   Introduction <introduction>
   Installation <install>
   Architecture <architecture>
   A barebones example <barebones>
   Parallelism <parallel>
   Communication <communication>
   Tutorials <tutorials>
   Examples <examples>
   Use cases <cases>
   Benchmarks <benchmark>
   Terms and Definitions <dictionary>
   Known bugs <bugs>
   TODO <todo>


Papy API
========

This part of the documentation is generated automatically from the source code
documentation strings. It should be the most up-to-date version. If there is a
conflict between the hand-written and and generated documentation, please
contact the author e.g. by adding an issue on the project page. 

.. toctree::
   :maxdepth: 2

   papyapi


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

