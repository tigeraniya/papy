Dictionary of terms and definitions
===================================

A dictionary of terms used within the documentation.

map
---

Higher-order map function. A function which evaluates another function on all
elements of the input collection.

imap
----

Iterated higher-order map function. A function which evaluates another function
on all elements of the input collection returning and evaluating the restuls 
iteratively and lazily. *PaPy* depends on the imap implementation provided by 
the standard Python imap from the itertoos module (itertools.imap) and the 
custom *IMap*.

*IMap*
------

A parallel implementation of a multi-task imap function, which is used within 
*PaPy*. It uses a pool of worker-threads or worker-processes and evaluates 
functions in parallel either locally or remotely.

worker-function
---------------

A function with a standarized input written to be used by a *Worker* class
instance. All functionality of a *PaPy* pipeline has to be coded as 
worker-functions.


worker-process/thread
---------------------

A thread or process inside an IMap instance evaluating a tasklet remotely or
locally.
(related to: IMap, RPyC, thread, process)


Worker
------

An object-oriented wrapper for worker-functions, corresponds to
"function with partially applied arguments"


Piper
-----

An object-oriented wrapper for Worker instances, corresponds to
"worker with defined mode of evaluation"
(related to: IMap, Worker)

Dagger
------

An directed acyclic graph (DAG) to store and connect piper instances.
(related to: Plumber, pipeline)


Plumber
-------

A wrapper for the Dagger designed to run and interact with a running pipeline.
(related to: Dagger, pipeline)

stream
------

In Python terminology a stream is a file-like I/O object.


input stream
------------

The input stream is the data which which enters a *PaPy* pipeline. The data is
assumed to be a collection of items expressed as a Python iterator (or any 
object which has the next method). 

Any sequence (e.g. a ``list`` or a ``tuple``) can be made into an iterator using
the Python built-in ``iter`` function e.g::

   sample_sequence = [data_point1, data_point2, data_point3]
   sample_iterator = iter(sample_sequence)

Files are by default line-iterators i.e.::

   sample_file = open('sample_file.txt')
   sample_file.next() # returns the first line
   sample_file.next() # returns the second line

output stream
-------------

Input item saved and returned by an output *Piper*. By default the output
*Piper* should return a None for every input item, but save the result
persistently.

input item
----------

A single element of the input strea.


output item
-----------

A single element of the returned or saved output stream.


input *Piper*
-------------

A *Piper*, which is connected to a input stream (or multiple input streams) is a 
input *Piper*. Such a *Piper* corresponds to a node in the graph which has no 
upstream nodes within the *PaPy* pipeline or in other words has no outgoing edges
in the directed acyclic graph. An input *Piper* is an input node in the graph
representing the pipeline.

input *Worker*
--------------

The *Worker* instance used to create the input *Piper*. The first *Worker*
function might depending on the type of the input stream have to deserialize the
data.

output *Piper*
--------------

A *Piper*, which generates the output stream is a output *Piper*. A *PaPy*
pipeline might have multiple output *Pipers* in different places of the
pipeline. An output *Piper* corresponds to a node in the graph which has not
downstream nodes within the *PaPy* pipeline or in other words has no incoming
edges in the directed acyclic graph. An output *Piper* is an output node in the
graph representing the pipeline.


output *Worker*
---------------

The *Worker* instance used to create the output *Piper*. The last worker function
should by convention return None and save the data persistently.


input node
----------

A node, which has no outgoing edges. (note *not* incoming edges)


output node
-----------

A node, which has no incoming edges.  (note *not* outgoing edges)


lazy evaluation
---------------

Is the technique of delaying a computation until the result is required.

task
----

A task is an ordered tuple of objects added to the IMap instance it consists of:

  * a function, which will be evaulated on the input element-wise
  * an input (a list, tuple or any iterator object like a numpy array)
  * a tuple of arguments e.g. (arg1, arg2, arg3)
  * a dictionary of keyword arguments i.e. {'arg1': value_1, 'arg2': value_2}

The optional arguments and keyworded arguments have to match the signature of the
function. The task will be iteratively split into jobs in the following way::

  tasklet = (func, element_from_iterable, arguments, keyworded_arguments)

(related to: IMap, Tasklet)


tasklet
-------
A task is the unit of evaluation of a worker-thread/process within an IMap
instance. It is constructed from the task arguments with an input element the
tasklet::

    tasklet = (func, element_from_iterable, argumentns, keyworded_arguments)

Is evaluated as::

    result = func(element_from_iterable, arguments, keyworded_arguments)


inbox
-----

The first argument of any worker function. The elements of the function
correspond to the outputs of the upstream function in the *Worker* instance or
to outputs of other *Pipers*. These outputs are defined by the pipeline
topology. The contents of the inbox depend on a specific input item to the
pipeline. All other arguments of a worker function are predetermined.

inbox element
-------------

The number of elements in the inbox of a worker function is one if the function
is not the first function of a *Worker* instance or is equal to the number of
*Pipers* the worker function, *Worker* and *Piper* is connected to within a
*PaPy* pipeline.




