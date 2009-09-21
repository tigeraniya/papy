Tutorials
=========

In this section practical aspects of *PaPy* usage are covered especially
creating, running and monitoring a *PaPy* pipeline. The tutorials cover:

  * writing functions for Workers
  * working with input data
  * writing functions for output Workers
  * creating a pipeline skeleton
  * optimizing parallelism
  * logging

Writing functions for Workers
-----------------------------

A worker is an instance of the class Worker. Worker instaces are created by
calling the Worker class with a function or several functions as the argument.
optionally an argument set (for the function) or argument sets (for multiple
functions) can be supplied i.e.::

  worker_instance = Worker(function, argument)

or::

  worker_instance = Worker(list_of_functions, list_of_arguments)

A worker instance is therefore defined by two elements: the function or list of
functions and the argument or list of arguments. This means that two different
instances which have been initialized using the same functions *and* respecitve 
arguments are functionally equal. You should think of worker instances as nested
curried functions (search for "partial application").

Writing functions suitable for workers is very easy and adapting existing
functions should be the same. The idea is that any function is valid if it
conforms to a defined input/output scheme. There are only few rules which need to
be followed:

     #. The first input argument: each function in a worker will be given a n-tuple
        of objects, where n is the number input iterators to the Worker. For example 
        a function which sums two numbers should expect a tuple of lenght 2. 
        Remember python uses 0-based counting. If the Worker has only one input
        stream the input to the function will still be a tuple i.e. a 1-tuple.

     #. The additional (and optional) input arguments: a function can
        be given additional arguments.

     #. The output: a function should return a single object _not_ enclosed in a 
        wrapping 1-tuple. If a python function has no explicit return value it 
        implicitly returns None.

Examples:

single input, single ouput::

    def water_to_water(inp):
      result = inp[0]
      return result

single input, no explicit output::

    def water_to_null(inp):
      null = inp[0]

multiple input, single output::

    def water_and_wine(inp):
      juice = inp[0] + inp[1]
      return juice

multiple input, single output, parameters::

    def water_and_wine_dilute(inp, dilute =1):
      juice = inp[0] * dilute + inp[1]
      return juice

Note that in the last exemples inp is a 2-tuple i.e. the Piper based on such a 
worker/function will expect two input streams or in other words will have two 
incoming pipes. If on the other hand we would like to combine elements in the 
input/object from a single pipe we have to define a function like the
following::
        
    def sum2elements(inp):
        unwrapped_inp = inp[0]
        result = unwrapped_inp[0] + unwrapped_inp[1]
        return result

In other words the function receives a wrapped object but returns an unwrapped. 
All python objects can be used as results except Excptions. This is because 
Exceptions are not evaluated down-stream but are passively propagated.


Writing functions for output workers
------------------------------------

An output worker is a worker, which is used in a piper instance at the end of
a pipeline i.e. in the last piper.  Any valid worker function is also a valid
output worker function, but it is recommended for the last piper to persistently
safe the output of the pipeline. The output worker function should therefore
store it's input in a file, database or eventually print it on screen. The
function should not return data. The reason for this recommendation are related
to the implementation details of the IMap and Plumber objects.

    #. The Plumber instance runs a pipeline by retrieving results from output
       pipers *without* saving or returning those results

    #. The IMap instance will retrieve results from the output pieprs *without*
       saving whenever it is told to stop *before* it consumed all input.

The latter point requires some explanation. When the stop method of a running
IMap instance is called the IMap does not stop immediately, but is schedeuled to
stop after the current stride is finished for all tasks. To do this the output
of the pipeline has to be 'cleared' which means that results from output pipers
are retrieved, but not stored. Therefore the 'storage' should be a built-in
function of the last piper. An output worker function might therefore require an
argument which is a connection to some persistent storage e.g. a file-handle. 


Picklability
------------
Objects are submitted to the worker-threads by means of queues to
worker-processes by pipes and to remote processes using sockets. This requires
serialization, which is internally done by the cPickle module. Additionally RPyC
uses it's own 'brine' for serialization. The submitted objects include the
functions, data, arguments and keyworded arguments all of which need to be
picklable! 

Worker-methods are impossible
+++++++++++++++++++++++++++++

Class instances (i.e. Worker instances) are picklable only if all of their
attributes are.  On the other hand class instance methods are not picklable
(because they are not top-level module functions) therefore class methods
(either static, class, bound or unbound) will not work for a parallel piper.

File-handles make remotely no sense
+++++++++++++++++++++++++++++++++++

Function arguments should be picklable, but file-handles are not. It is
recommeded that output pipers store data persistently, therfore output workers
should be run locally and not use a parallel IMap, circumventing the requirement
for picklable attributes.















Parallel execution
==================
The throughput of a pipeline will be most significantly limited by the slowest piper. A piper might be slow either because it does a CPU-intensive task (cpu-bound piper), IO-intensive task (io-bound piper) or it layzily waits for some data(waiting piper). Currently papy allows you to parallelize cpu-bound and waiting pipers easily. In general cpu-bound tasks should be split into a number of processes which is equal to the number of availble cpu-cores. Additional processes are likely to decrese the overall performance. Waiting tasks do not use the computers resources and therfore their parallelism is quite cheap and often can be solved using threads. IO-bound tasks like writing and reading from disk a

When not to use parallel pipers.
--------------------------------
By default a piper is not parallel i.e.

linear_piper = Piper(some_worker)

This has good reason as most of your pipers will not limit the throughput of the pipeline while the creation of process pools is quite expensive (more so on Windows). You should parallelize the bottleneck(s) only. If your pipeline has no obvious bottleneck it's probably fast enough. If not you might be able to use a shared pool.




Optimizing cpu-bound pipers.
----------------------------
If the throughput of your pipeline is limited by a cpu-intensive tasks you should parallelise this piper. The recommended number of processes is the number of availble processors it rarely helps to have more (even +1)

recommended (defaults to creating a new pool with cpu number of processes):
parallel_piper = Piper(cpu_bound_worker, parallel =1)



Optimizing waiting pipers.
--------------------------

Optimizing io-bound pipers.
---------------------------
You should probably know way more about python multiprocessing then I do.


When to use a shared pool.
--------------------------
First of all you *likely* should not use a shared pool.


When to use unordered pipers (parallel =2).
-------------------------------------------
Unordered pipers return results in arbitrary order i.e. for the input iterator [3,2,1] a parallel unordered piper which doubles the input might return [9,2,4] or any other permutation of the results. In theory unordered pipers are quicker then simple parallel pipers if the computational (or waiting) time varies for different inputs simply the result which arrives first is returned first and the process can work on another input.






