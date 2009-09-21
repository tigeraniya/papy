Introduction
============

Many computational tasks require sequential processing of data i.e. the global
task is split into sub-tasks which are evaluated in some order on the data to be
processed.

*PaPy* allows to create a data-processing pipeline from components of isolated
functionality:

  * the function wrapping *Workers*
  * the connection capable *Pipers*
  * the topology defining *Dagger*
  * the parallel executors *IMaps*

The *Dagger* connects *Pipers* via pipes into a directed acyclic graph, while
the *IMaps* are assigned to *Pipers* and evaluate their *Workers* either locally
using threads or processes or on remote hosts. The *Workers* allow to compose 
multiple functions while the *Pipers* allow to connect the inputs and outputs of
*Workers* as defined by the *Dagger* topology.

The input data can be processed in parallel if it is a collection (or it can be 
split into one) of data-items: files, messages, sequences, arrays etc. (in
general all picklable python objects are valid input items).

*PaPy* is written in and for Python this means that the user is expected to
write Python functions with defined call/return semantics, but the function code
is largely arbitrary e.g. they can call a perl script or import a library.
*PaPy* focuses on modularity, functions should be re-used and composed within
pipelines and *Workers*.

The *PaPy* pipeline automatically logs it's execution is resistant to exceptions
and timeouts should work on all platforms where ``multiprocessing`` is available
and allows to utilize a cross-platform ad-hoc grid wherever *RPyC* is supported.


Where/When should *PaPy* be used?
---------------------------------

It is likely that you will benefit from using *PaPy* if any of the following is
true:

  * you need to process large collections of data items.
  * your data collection is to large to fit into memory.
  * you want to utilize an ad-hoc grid.
  * you have to construct a complex data-flow.
  * you are likely to deal with timeouts or bogus data.
  * the execution of your workflow needs to be logged.
  * you want to refactor existing code.
  * you want to reuse(wrap) existing code.


Where/When should *PaPy* **not** be used.
-----------------------------------------

  * The parallel features improve performance only if the functions have
    sufficient granularity i.e. computation to communication ratio.
  * Your input is not a collection and it does not allow for data parallelism.


What is a pipeline?
-------------------

*PaPy* understands a pipeline as any directed acyclic graph. The direction of
the graph is defined by the data flow (edges, pipes) between the data-processing
units (nodes, *Pipers*).  To be precise the direction of the edges is opposite
to the direction of the data stream (pipes). Up-stream *Pipers* have incomming
edges from down-stream *Pipers* this is represented as a pipe with a opposite
orientation i.e. up-stream -> down-stream. *PaPy* pipelines can be branched i.e.
two down-stream *Pipers* might consume input from the same up-stream *Piper* or
one down-stream *Piper* consumes data from several up-stream *Pipers*. *Pipers*
which consume (are connected to) data from outside the directed acyclic graph
are input *Pipers*, while *Pipers* to which no other *Pipers* are connected to are
output *Pipers*. *PaPy* supports pipelines with multiple inputs and outputs, also
several input nodes can consume the same external data. You should think of a
pipeline as an ``imap`` function composed ``imap`` functions i.e.::

  # nested imaps as pipelines
  pipeline = imap(h, izip([imap(f, input_for_f), imap(g, input_for_g)]))

This is a pipeline of three functions f, g, h. Functions f and g are up-stream
relative to h. Because of the ``izip`` function input_for_f and input_for_g have
to be of the same lenght. In *PaPy* the lazy `imap` functions is replaced with
a pool implementation *IMap*, which allows for a parallelism vs. laziness
trade-off.


What does an *IMap* do?
-----------------------

The ``IMap.IMap`` object provides a method to evaluate a functions on a sequence 
of changing arguments provided with optional positional and keyworded arguments
to modify the behaviour of the function. Just like ``multiprocessing.Pool.imap`` 
or ``itertools.imap`` with the key differences that unlike ``itertools.IMap`` it 
evaluates results in parallel. Compared to ``multiprocessing.Pool.imap`` it 
supports multiple functions (called tasks), which are evaluated not one after
another, but in an alternating fashion. *IMap* is completely independent from 
*PaPy* although they are boundled in a single Python package.


Feature summary:
----------------

This is a list of features of a pipeline constructed using the *PaPy* module components.

    * construction of arbitrarily complex pipelines
    * evaluation is lazy (buffered)
    * flexible local and remote parallelism
    * shared local and remote resources
    * robustness to exceptions
    * support for time-outs
    * real-time logging
    * os-independent (really a feature of ``multiprocessing``)
    * cross-platform (really a feature of *RPyC*)
    * small code-base
    * a preliminary GUI based on ``Tkinter``
    * tested & documented.

