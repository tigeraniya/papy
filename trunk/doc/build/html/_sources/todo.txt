TODO
====

This TODO is a roadmap for papy development. The Milestone 1.0 corresponds to
the hopefully published manuscript. All known bugs should be fixed by 0.9. The
transition form 0.9 -> 1.0 will be based on the ability to get papy to run
unmodified on the cross-product of the following::
[Linux 2.6 (Fedora, Gentoo, Ubuntu), Mac OSX, Windows XP], 
[Python 2.6, Python 2.5 + multiprocessing], [32bit, 64bit]

FIXME
---------

These issues need to be addressed before any offcial release:

  * The piper consume argument shoud affect the default and minimal buffer_size for
    the IMap instance

Milestones
----------

1.0
+++

Library functionality
^^^^^^^^^^^^^^^^^^^^^

  * Implement a parallel, buffered, layzy implementation of the imap function.
  * Implement a modular message passing framework from core classes

    * Worker
      
      * deal correctly with Exceptions

    * Piper

      * deal correctly with Timeouts
      * deal correctly with produce/spawn/consume
      * report status (frame/exception)

    * Dagger

      * validate topology
      * report piper stats (frame/exception(s)/timeout(s))

    * Plumber

      * stop/restart pipeline
      * run pipeline concurrently (non-blocking)
      * load/save 


  * Implement functions for basic and common tasks in a pipeline:

    * wrap around operator and math from the Python stdlib
    * serialize data:
     
      * pickle (done)
      * JSON (simplejson no multiple objects per file)
      * YAML (terrible performance, no multiple objects per file)

    * store data

      * file-handle (done)

    * print data (done)


Gui functionality
^^^^^^^^^^^^^^^^^

  * load/save pipelines in code format
  * automatically layout nodes/pipers on canvas using spring/kamada-kawai
  * run and monitor pipelines
  * benchmark pipelines
  * interact with the pipleline via the built in interpreter.

Additional functionality
^^^^^^^^^^^^^^^^^^^^^^^^

  * A module to benchmark a pipeline, indicate bottlenecks and recommend the strategy.


Documentation
^^^^^^^^^^^^^

  * The hand-written documentation should build automatically into several formats
    (Sphinx) and be inculded in the repository (tar-balls).
  * the documentation should cover installation of dependancies
  * A very simple 10-line example is needed!
  * all API classes and functions should be documented using reStructuredText and Sphinx.
  * documentation is: manual, examples, use-cases, faq and fuq
  * use-cases, should be complete, non-trivial but concise and
    highlight the features of papy.

    #. DracUlysses: find two most similar lines in dracula and ulysses
    #. CrossRMSD: calculate all-vs-all RMSD distances
    #. deltaG Z-Score: calculate the empirical Z-score of the free enthalpy of
       binding for all transcripts of a small genome.   

Unit-tests
^^^^^^^^^^
  
  * write a complete unit-test suite using unittest.


