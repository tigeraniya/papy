Architecture
============

The architecture of *PaPy* is remarkably simple and intuitive yet flexible. It
consists of only four core components (classes) to construct a data processing
pipeline. Each component provides an isolated subset of the functionality, which
includes: arbitrary directed pipeline topology, parallelism and distributed
computing, user function wrapping, and run-time interactions (e.g. logging).
*PaPy* is intrinsically modular, any function can be used in several places in a
pipeline or re-used in another pipeline.

In this chapter we first introduce object-otiented programming in the context
of papy, explain briefly the core components (building blocks) and introduce a
conceptual outline of pipeline creation. In later sections we revisit each
component and explain the how and why. 


Understanding the object-oriented model
---------------------------------------

Papy is written in an object-oriented(OO) way. The main components: Plumber,
Dagger, Pipers and Workers are in fact class objects. For the end-user it is
important to distinguish between classes and class instances. In Python both
classes and class instances are objects. When you import the module in your
script::

  import papy

A new object (a module) will be availble i.e. you will be able to access classes
and functions provided by papy e.g.::

  papy.some_function
  papy.SomeClass

The name of the new object will be papy. This object has several attributes
which correspond to the components and interface of papy e.g.::

  papy.Plumber
  papy.Dagger
  papy.Piper
  papy.Worker

Attributes are accessed in python using the object.attribute notation. These
components are classes not class instances. They are used to construct class
instances which correspond to the run-time of the program. A single class can in
general have multiple instances. A class instance is constructed by "calling"
(in fact initializing) the class.::

  class_instance = Class(parameters)

The important part is that using papy involves constructing many classes.::

  worker_instance = Worker(custom_function(s), argument(s))
  piper_instance = Piper(worker_instance, options)
  your_interface = Plumber(options)


The core components
-------------------

The core components form the end-user interface i.e. the classes which the user
is expected use directly.

  * The IMap - An implementation of an iterated map function which can 'process'
               multiple tasks (function-sequence tuples) in parallel using
               either threads or processes on the local machine or on remote
               RPyC servers.
  * The Pipers(Workers) - combined provide the functionality by wrapping 
                          user-defined functions handling exceptions and
                          reporting.
  * The Dagger - defines the topology of the pipeline in the form of a directed
                 acyclic graph i.e. the connectivity of the flow (pipes).
  * The Plumber - provides the interface to set-up run and monitor a pipeline
                  (run-time).

.. note::

  These terms are also covered briefly in the dictionary section.


Creating a pipeline
-------------------

This documentation is all about creating a valid papy pipeline. In the process
of design we tried to make papy as idiosyncrasy-free as possible, relying on
familiar concepts of map functions and directed acyclic graphs. A pipeline has
two very distinct states *before* and *after* it is started. Those states
correspond to the "creation time" and "run-time". At creation time functions are
defined (written or imported) and the the data-flow is defined by connecting
functions by directed pipes. At run time data is actually pumped through the
pipeline. This can be summarized by the following outline:

  #. Write worker functions (the functions in the nodes)
  #. Create Worker instances (specify worker function parameters)
  #. Create Piper instances (specify how/where to evaluate the functions)
  #. Create a Dagger (specify the connections pipeline)
  #. Connect the pipeline to the input
  #. Run the pipeline.

The first 4 steps correspond to the "creation time" the last two to the "run
time" of the pipeline. A pipeline can be stored and loaded as a python script.
In the following sections the building blocks of a pipeline are explained. Papy
imposes restrictions on the inputs and outputs of a worker function and on
recommends a generic way to construct a pipeline:

input_iterator -> input_piper -> ... processing pipers ... -> output_piper

The output piper should be used to store the output of the pipeline
persistently i.e. it should return None. Papy provides useful functions to
create output pipers.


The IMap
--------

The IMap class is described extensively in the section about parallelism and in
the API documentation. Here it suffices to say that it is an object which allows
to execute *multiple* functions using a shared pool of worker processes. This
class is independent of papy (in fact it is a seperate module) and can be used
in any python code as an alternative to multiprocessing.Pool imap, map, 
unordered_imap or itertools.imap.::

    # doc/examples/imap_01.py

The next examples illustrates how IMap can be used to share a pool of worker
processes among two tasks.

    # doc/examples/imap_02.py 


The Worker
----------

The Worker is a class which is created with a function or multiple functions
(and the functions arguments) as arguments. It is therefore a function wrapper.
If multiple functions are supplied they are assumed to be nested with the last
function being the outer most i.e.::

    (f,g,h) is h(g(f()))

If a Worker instance is called this compsite function is evaluated on the
supplied argument.::

    from papy import Worker
    from math import radians, degrees
    def papy_radians(input):
        return radians(input[0])
    def papy_degrees(input):
        return degrees(input[0])
    worker_instance = Worker((papy_radians, papy_degrees))
    worker_instance([90.])
    90.0

In this example we create a composite worker from two functions papy_radians and
papy_degrees. The first function converts degrees to radians the second converts
radians to degrees. Obviously if those two functions are nested their result is
identical to their input. papy_radians is evaluated first and papy_degrees last
so the result is in degrees.

The Worker performs several functions:

  * standarizes the inputs and outputs of nodes.
  * allows to reuse and combine multiple functions into as single node
  * catches and wraps exceptions raised within functions.
  * allows functions to be evaluated on remote hosts.

A Worker expects that the wrapped function has a defined input and output. The
input is expected to be boxed in a tuple relative to the output, which should
not be boxed. The worker instance expects [float], but returns just float. Any
function which conforms to this is a valid Worker function. Most built-in
functions need to be wrapped. Please refer to the documentation on how to write
Worker functions. 

If an exception is raised within any of the user-supplied functions it is cought
by the Worker, but is *not raised* instead it is wrapped as a WorkerError
exception and returned i.e.::

    worker = Worker(sqrt) # the math.sqrt function does not conform.
    # raises because exception outside sqrt
    worker(10.) # this is not a valid worker input
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      File "/home/marcin/projects/papy/src/papy/papy.py", line 814, in __call__
        exceptions = [e for e in inbox if isinstance(e, PiperError)]
    TypeError: 'float' object is not iterable 
    # does not raise exception inside sqrt
    worker([10.]) # this is a valid worker input
    WorkerError(TypeError('a float is required',), <built-in function sqrt>, [10.0])
    # the WorkerError is returned not raised. 

The functionality of a Worker instance is defined by the functions it is
composed of and their arguments. Two workers which are composed of the same
functions *and* called with the same arguments are functionally identical
and a single worker instance can be used in multiple places of a pipeline or in
other words in multiple pipers.

The functions within a worker instance need not to be evaluated by the same
process as the worker instance itself. This is accomplished by the
open-source RPyC module. A worker knows how to inject its functions into a RPyC
connection object, after this the worker method will run in the local process,
but the functions wrapped functions on the remote host.

    import rpyc # import the RPyC module
    from papy import Worker
    from papy.workers.maths import pow
    power = Worker(pow, (2,)) # power of two
    power([2]) # evaluated locally
    4
    conn = rpyc.classic.connect("some_host") 
    power._incject(conn) # replace pow with remot pow
    power([3]) # evaluated remotely
    9

A function can run on the remote host i.e. remote python process/thread only if
the modules on which this function depends are availble on that host and those
modules are imported. IMap provides means to attach import statements to
function definitions using the imports decorator. In this way code sent to the
remote host will work if the imported module is availble remotely.::

   @imports([['re', []]])
   def match_string(input, string):
       unboxed = input[0]
       return re.match(string, unboxed)

The above example shows a valid worker function with the equivalent of the
import statment attached.::

    import re

The re module will be availble remotely in the namespace of this
function i.e. other injected functions might not have access to re.


Built-in worker functions
-------------------------

Several classes of worker functions are already part of papy. This collection is
expected to grow, currently the following types of workers are included.

  * core - basic data-flow
  * maths - functions on numbers
  * bool - boolean algebra
  * io - serialisation, printing and file operations

The core modules includes the family of passer functions. They do not alter the
incoming data, but are used to pass only streams from certain imput pipes. For
example a piper connected to 3 other pipers might propagate input from only one.

  * ipasser - propagates the i'th input pipe
  * npasser - propagates the n-first input pipes
  * spasser - propagetes the pipes with numbers in s

For example::

  from papy.workers.core import *
  worker = Worker(ipasser, (0,)) # passes only the first pipe
  worker = Worker(ipasser, (1,)) # passes only the second pipe
  worker = Worker(npasser, (2,)) # passes the first two pipes
  worker = Worker(spasser, ((0,1),) # passes pipes 0 and 1
  worker = Worker(spasser, ((1,0),) # passes pipes 1 and 0

The output of the passes is a *single* tuple of the passed pipes::

  input0 = [0,1,2,3,4,5]
  input1 = [6,7,8,9,10,11]

  worker = Worker(spasser, (1,0))
  # will produce output
  [(6,0), (7,1), ...]

The io sub-module contains functions dealing with input/output relations i.e.
data storage and serialization. It currently supports serialization using the
pickle and JSON protocols and file-based data storage. Generic data-base
backends will be added in future.

Data serialization is a way to conver objects (and in Python almost everything
is an object) into a sequence, which can be stored or transmitted. Papy uses the
pickle serialization format to transmit data between local processes and brine
(an internal serialization protocol from RPyC) to transmit data between hosts.

The user might however want to save and load


Example::
  # PH


The *Piper*
-----------

A *Piper* class instance represents a node in the directed graph of the 
pipeline. It defines what function(s) should at this place be evaluated 
(via the supplied *Worker* instance) and how it should be evaluated (via the 
optional *IMap* instance, which defines the uses computational resources). 
Besides that it performs additional functions which include:

  * logging and reporting
  * exception handling
  * timeouts
  * produce/spawn/consume schemes

To use a *Piper* outside a pipeline three steps are required:

  * creation - requires a *Worker* instance, optional arguments e.g. an *IMap* 
    instance. (``__init__`` method)
  * connection - connects the *Piper* to the input. (``connect`` method)
  * start - allows the *Piper* to return results, starts the *IMap*. (``start`` method)

In the first step we define the *Worker* which will be evaluated by the *Piper*
and the resources to do this computation. Computational resources are 
represented by *IMap* instances. An *IMap* instance can utilize local or remote
threads or processes. If no *IMap* instance is given to the constructor the
``itertools.imap`` function will be used instead. This function will be called
by the Python process used to construct and start the *PaPy* pipeline.

*PaPy* has been designed to log the execution of a workflow at multiple levels 
and with a level of detail which can be specified it uses built-in Python 
logging (the ``logging`` module). The *IMap* function, which should at this 
stage be bug free logs only debug statements. Exceptions within worker-functions
are wrapped as ``WorkerError`` exceptions, these errors are logged by the 
*Piper* instance, which wraps this *Worker* (a single *Worker* instance can be 
used by multiple *Pipers*). By default the pipeline is robust to 
``WorkerErrors`` and these exceptions are logged, but they do not stop the flow.
In this mode if the called *Worker* instance returnes a ``WorkerError`` the 
calling *Piper* instance wraps this error as a ``PiperError`` and
**returns** (not raises) it down-stream into the pipeline. On the other end if a
*Worker* receives a *PiperError* as input it just propagates it further 
down-stream i.e. it does not try meaningless calculations on exceptions. 
In this way errors in the pipeline propagate down-stream as place-holder 
PiperErrors.

A Piper instance evaluates the Worker either by the supplied IMap instance
(described elswhere) or by the builtin itertools.imap function (default). In
reality after a piper is connected to the input it creates a task i.e. function,
data, arguments tuples which is added to the IMap instance used to call the imap
function.

IMap instances support timeouts via the optional timeout argument supplied to the
next method. If the IMap is not able to return a result within the specified
time it raises a TimeoutError. This exception is cought by the piper instance
which expects the result, wrapped into a PiperError exception and propagated
down-stream exactly like WorkerErrors. If the piper is used within a pipeline
and a timeout argument given the skipping argument should be set to true
otherwise the number of results from a piper will be bigger then the number of
tasklets, which will hang the pipeline.::

   # valid with or without timeouts
   universal_piper = Piper(worker_instance, parallel =imap_instance, skipping =True)
   # valid only with timeouts
   nontimeout_piper = Piper(worker_instance, parallel =imap_instance, skipping =False)

Note that the timeouts specified here are 'computation time' timeouts. If for
example a worker function waits for a server response and the server response
does not arrive within some timeout (which can be an argument for the Worker)
then if this exception is raise within the function it will be wrapped into a
WorkerError and raturned not raised as TimeoutErrors.

A single Piper instance can only be used once within a pipeline (this is unlike
Worker instances). 

Pipers are created first and connected to the input data later. The latter is
accomplished by the connect method.

    piper_instance.connect(input_data)

If the piper is used within a papy pipeline i.e. a Dagger or Plumber instance
the user does not have to care about connecting individual pipers. After a piper
has be either started or disconnected, obviously a piper can only be started if
it has been connected before.::

    piper_instance.connect(input_data)
    piper_instance.disconnect()
    # or
    piper_instance.start()

After starting a piper tasks are submitted to the thread/process workers in
the IMap instance and they are evaluated. This is a process which continues
until either the buffer is filled or the input is consumed. Therefore a piper
cannot be simply disconnected when it is 'running'. A special method is needed
to tell the IMap instance to stop input consumption. Because IMap instances
are shared among pipers such a stop can only occur at stride boundaries. The
piper stop method will eventually stop the IMap instance and put the piper in a
stopped state which allows the piper to be disconnected.::

    piper_instance.start()
    piper_instance.stop()
    piper_instance.disconnect() # can be connected and started

Because the stop happens at stride boundary data is not lost during a stop. This
can be illustraded as follows::

    #           plus2            plus1
    # [1,2,3,4] -----> [3,4,5,6] -----> [4,5,6,7]
    # which is equivalent to the following:
    # plus1(plus2([1,2,3,4]) 

If the pipers plus2 and plus1 share a single IMap and the stride is two then the
order of evaluation can be (if the results are retrieved)::

    temp1 = plus2(1)
    temp2 = plus2(2)
    plus1(temp1)
    plus1(temp2)
    <<return>>
    <<return>>
    temp1 = plus2(3)
    temp2 = plus2(4)
    plus1(temp1)
    plus1(temp2)
    <<return>>
    <<return>>

Now let's assume the the stop method has been called just after plus2(1). We do
not want to loose the temp1 result (as 1 has been already consumed from the 
input iterator and iterators cannot rewind), but we can achieve this only if
plus1(temp1) is evaluated this in turn (due to the order of tasklet submission)
can happen only after plus2(2) has been evaluated (i.e. 2 consumed from the
input iterator). To not loose temp2 plus1(temp2) has to be evaluated and finally
the evaluation can stop.::

    temp1 = plus2(1)
    temp2 = plus2(2)
    plus1(temp1)
    plus1(temp2)
    (stopped)

After the stop method returns all worker processes/threads and helper threads
return and the user can close the python interpreter. It is *very* important to
realise what happens with the two calculated results. As has been already
mentioned a proper papy pipeline should have an output piper i.e. a piper which
persistently stores the result.


The *Dagger*
------------

The *Dagger* is an object to connect *Piper* instances into a directed acyclic
graph (DAG). It inherits most methods of the *Graph* object, which is a concise
implementation of the *Graph* data-structure. The *Graph* instance is a
dictionary of arbitary hashable objects "real nodes" e.g. a *Piper* (the keys of
the dictionary) and instances of the Node class "topological nodes" (the values 
of the dictionary). A "topological node" instance is a also dictionary of 
"real nodes" and their corresponding "topological nodes". A "real node"(A) of the 
*Graph* is contained in a "topological node" for another "real node"(B) if there
exist an edge from (A) to (B). A and B might be the same "real node". 
A "topological node" is therefore a sub-graph of the *Graph* object around a 
hashable object and the whole *Graph* is a recursively nested dictionary. The 
*Dagger* is designed to store *Piper* instances as "real nodes" and provides 
additional methods, whereas the  *Graph* makes no assumptions about the object 
type. 


Edges vs. pipes
+++++++++++++++

A *Piper* instance is created by specifiying a *Worker* (and optionally *IMap*
instance) and needs to be connected to an input. The input might be another 
*Piper* or any Python iterator. The output of a *Piper* (up-stream) can be 
consumed by several *Pipers* (down-stream), while a *Piper* (down-stream) might
consume the results of multiple *Pipers* (up-stream). This allows *Pipers* to be
used as any nodes in a directed acyclic graph the *Dagger*

As a result of the above it is much more natural to think of connections between
*Pipers* in terms of data-flow up-stream --> down-stream (data flows from 
up-stream to down-stream) then dependency down-stream --> up-stream (down-stream
depends on up-stream). The *Graph* represents dependancy information as directed
edges (down-stream --> up-stream), while the *Dagger* class introduces the 
concept of pipes to ease the understanding of *PaPy* and make mistakes less 
common. A pipe is nothing else then a reversed edge. To make this explicit::

    input -> piper0 -> piper1 -> output # -> represents a pipe (data-flow)
    input <- piper0 <- piper1 <- output # <- represents an edge (dependancy)

The data is stored internally as edges, but the interface uses pipes. Method
names are explicit.::

    dagger_instance.add_edge() # (inherited from Graph) expects and edge as input 
    dagger_instance.add_pipe() # expecs a pipe as input 

.. note::

    Although all *Graph* methods are availble from the *Dagger* the end-user 
    should use *Dagger* specific methods only. For example the *Graph* method 
    ``add_edge`` will allow to add any edge to the instance, whereas 
    ``add_pipe`` method will not allow to introduce cycles.


Working with the *Dagger*
+++++++++++++++++++++++++

Creation of the a *Dagger* instance is very easy. An empty *Dagger* instance is
created without any arguments to the constructor.::

    dagger_instance = Dagger()

Optionally a set of *Pipers* and/or pipes can be given:: 

    dagger_instance = Dagger(sequence_of_pipers, sequence_of_pipes)
    # which is equivalent to: 
    dagger_instance.add_pipers(sequence_of_pipers)
    dagger_instance.add_pipes(sequence_of_pipes)
    # a sequence of pipers allows to easily add branches
    dagger_instance.add_pipers([1, 2a, 3a, 4])
    dagger_instance.add_pipers([1, 2b, 3b, 4])
    # in this example a Dagger will have 6 pipers (1, 2a, 2b, 3a, 3b, 4), one 
    # branch point 1, one merge point 4, and two branches (2a, 3a) and (2b, 3b).

The *Dagger* allows to add/delete *Pipers* and pipes::

    dagger_instance.add_piper(piper) 
    dagger_instance.del_piper(piper or piper_id)
    dagger_instance.add_pipers(pipers)
    dagger_instance.del_pipers(pipers or piper_ids)

The id of a *Piper* is a run-time specific number associated with a given 
*Piper* instance. This number can be obtained by calling the built-in function
id::

    id(piper)

This number is also shown when a *Piper* instance is printed.::

    print piper_instance

or represented::

    repr(piper_instance)

The representation of a *Dagger* instance also shows the id of the *Pipers*
which are contained in the pipeline.::

    print dagger_instance

The id of a *Piper* instance is define at run-time (it corresponds to the memory
address of the object) therefore it should not be used in scripts or seved in 
any way. Note that the lenght of this number is platform-specific and that no 
guarantee is made that two *Pipers* with non-overlapping will not have the same 
id. The resolve method::

   dagger_instance.resolve(piper or piper_id)

returns a *Piper* instance if the supplied *Piper* or a *Piper* with the 
supplied id is contained in the dagger_instance. This method by default raises a
``DaggerError`` if the *Piper* is not found. If the argument forgive is ``True``
the method returns ``None`` instead::

   dagger_instance.resolve(missing_piper) # raise DaggerError
   dagger_instance.resolve(missing_piper, forgive =True) # returns None


The *Dagger* run-time
+++++++++++++++++++++

The run-time of a *Dagger* instance begins when it's start method is called.
A *Dagger* can only be started if it is connected. Connecting a *Dagger* means
to connect all *Pipers* which it contains as defined by the pipes in the 
*Dagger*. After the *Dagger* is connected it can be started, starting a *Dagger
means to start all it's *Pipers*. *Pipers* have to be started in the order of 
the data-flow i.e. a *Piper* can only be started after all it's up-stream 
*Pipers* have been started. An ordering of nodes/*Pipers* of a graph/*Dagger* 
which has this property is called a postorder. There are possibly more then one
postorder per graph/*Dagger*. The exact postorder used to connect the *Pipers*
has some additional properties

    - all down-stream *Pipers* for a *Piper* (A) come before the next *Piper* 
      (B) for which no such relationship can be established. This can be thought
      as maintaining branch contiguity.
      
    - such branches can additionally be sorted according to the branch argument
      passed to the *Piper* constructor.

Another aspect of order of a *Dagger* is the sequence by which a down-stream 
*Piper* connects multiple up-stream *Pipers*. The inputs cannot be sorted 
based solely on their postorder because the down-stream *Piper* might be 
connected directly to a *Piper* to which one of it's other inputs has been 
connected before. The inputs of a *Piper* are additionaly sorted so that all 
down-stream *Pipers* come before up-stream *Pipers*, while *Pipers* for which no
such relation can be established are still sorted according to their index in 
the postorder. This can be thought of as sorting branches by their "generation".

A started *Dagger* is able to process input data. The simplest way to process 
all inputs is to zip it's output *Pipers*::

    output_pipers = dagger_instance.get_outputs()
    final_results = zip(output_pipers)
    
If any of the *Pipers* used within a *Dagger* uses an *IMap* instance and the 
*Dagger* is started. The Python process can only be exited cleanly if the 
*Dagger* instance is stopped by calling it's `stop` method. 
    

The *Plumber*
-------------

The *Plumber* is an easy to use interface to *PaPy*. It inherits from the 
*Dagger* object and can be used like a *Dagger*, but the *Plumber* class adds 
methods related to the "run time" of a pipeline. A *Plumber* can 
start/run/pause/stop a pipeline.

  #. loading/saving a pipeline.

  #. starting/stopping a pipeline.
  
  #. running/pausing a pipeline.

A *PaPy* pipeline is loaded and saved as executable Python code, which has the 
same priviliges as the Python process. Please keep this in mind starting when 
pipelines from untrusted sources!


The additional components
-------------------------

Those classes and functions are used by the core components, but are general and
might find application in your code.

  * *Graph*(*Node*) - Two classes which implement a graph data-structure using a 
                  recursively nested dictionary. This allows for simplicity of 
                  algorithms/methods i.e. there are no edge objects because 
                  edges are the keys of the *Node* dictionary which in turn is 
                  the value in the dictionary for the arbitrary object in the 
                  *Graph* object i.e.::

                    from papy import Graph
                    graph = Graph()
                    object1 = '1'
                    object2 = '2'
                    graph.add_edge((object1, object2))
                    node_for_object1 = graph[object1]
                    node_for_object2 = graph[object2]

                  The *Dagger* is a *Graph* object with directed edges only and 
                  no cycles.

  * imports    - a function-wrapper, which allows to inject import statments to
                 a functions local namespace at creation (code execution) 
                 e.g. on a remote Python process.

  * inject     - injects a function(builtin or user) into a *RPyC* remote 
                 connection namespace. 
  
