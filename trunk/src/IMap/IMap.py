"""
:mod:`IMap.IMap`
================

This module provides a parallel, buffered, multi-task, imap function.
It evaluates results as they are needed, where the need is relative to
the buffer size. It can use threads and processes.
"""
# Multiprocessing requires Python 2.6 or the backport of this package to the 2.5 line
# get it from http://pypi.python.org/pypi/multiprocessing/
PARALLEL = 0
try:
    from multiprocessing import Process, Pipe, cpu_count, TimeoutError
    from multiprocessing.synchronize import Lock
    from multiprocessing.forking import assert_spawning
    from multiprocessing.queues import SimpleQueue
    PARALLEL += 1
except ImportError:
    print 'mutliprocessing not available get it from:' +\
          'http://pypi.python.org/pypi/multiprocessing/'
try:
    import rpyc
    PARALLEL += 1
except ImportError:
    print 'RPyC not available get it from:' +\
          'http://rpyc.wikidot.com/'

# Threading and Queues
from threading import Thread, Semaphore, Event
from threading import Lock as tLock
from Queue import Queue, Empty
# for PriorityQueue
from heapq import heappush, heappop
# Misc.
from itertools import izip, repeat, cycle, chain
from inspect import getsource, isbuiltin, isfunction
import traceback
from collections import defaultdict, deque
import time
# sets-up logging
from logging import getLogger
log = getLogger(__name__)

class IMap(object):
    """
    Parallel (thread- or process-based), buffered, multi-task, itertools.imap or Pool.imap
    function replacment. Like imap it evaluates a function on elements of an iterable, and
    it does so layzily(with adjusted buffer via the stride and buffer arguments).

    optional arguments:

      * func, iterable, args, kwargs

        Defines th the first and only task, it *starts* the IMap pool and returns an IMap
        iterator. For a description of the args, kwargs and iterable input please see
        the add_task function. Either *both* or *none* func **and** iterable have to be
        specified. Arguments and keyworded arguments are optional.

      * worker_type('process' or 'thread') [default: 'process']

        Defines the type of internally spawned pool workers. For multiprocessing Process
        based worker choose 'process' for threading Thread workers choose 'thread'.

        .. note::

          This choice has *fundamental* impact on the performance of the function please
          understand the difference between processes and threads and refer to the manual
          documentation. As a general rule use 'process' if you have multiple CPUs or
          cores and your functions(tasks) are cpu-bound. Use 'thread' if your function is
          IO-bound e.g. retrieves data from the Web.

          If you specify any remote workers via worker_remote, worker_type has
          to be the default 'process'. This limitation might go away in future
          versions.

      * worker_num(int) [default: number of CPUs, min: 1]

        The number of workers to spawn locally. Defaults to the number of availble CPUs, 
        which is a reasonable choice for process-based IMaps.

        .. note::

          Increasing the number of workers above the number of CPUs makes sense only if
          these are Thread-based workers and the evaluated functions are IO-bound.

      * worker_remote(sequence or None) [default: None]

         Specify the rpyc hosts and number of workers as a sequence of tuples
         ("host_ip_etc", worker_num). For example::

           [['localhost', 2], ['127.0.0.1', 2]]

         the TCP port can also be specified::

           [['localhost:6666']]

      * stride(int) [default: worker_num + sum of remote worker_num, min: 1]

        The stride argument defines the number of consecutive tasklets which are
        submitted to the process/thread pool for a task. This defines the degree
        of parallelism. It should not be smaller than the size of the pool, which
        is equal to the sum of local and remote threads/processes.

      * buffer(int) [default: stride * (tasks * spawn), min: variable]

        The buffer argument limits the maximum memory consumption in tasklet units, by
        limiting the number of tasklets submitted to the pool. By default each task
        should receive it's own buffer of stride size.

        .. note::

          A tasklet is considered submitted until it is returned by the next method.
          The minimum stride is 1 this means that starting the IMap causes one tasklet
          to be submitted to the pool input queue. The following tasklet from the next task
          can enter the queue only if the first one is calculated and returned by the next
          method. A completely lazy evaluation (i.e. submitting the first tasklet *after*
          the next method for the first task has been called is not supported).

          If the buffer is n then n tasklets can enter the pool. Depending on the number of
          tasks and the stride size these can be tasklets from one or several tasks. If the
          tasks are chained i.e. the output from one is consumed by another then at most one
          i-th tasklet is at a given moment in the pool. In those cases the minimum required
          buffer is lower.

      * ordered(bool) [default: True]

        If True the output will be ordered i.e. the result of job_17 from task_1 will be
        returned after job_16 from any task *and* job_17 from task_0 **but** before job_18
        from any task *and* job_17 of task_n (for any n > 1).

      * skip(bool) [default: False]

        Should we skip a result if trying to retrieve it rasied a TimeoutError? If the
        results are ordered and skip =True then the calling the next method after a
        TimeoutError will skip the timeouted result and try to get the next. If
        skip =False it will try to get the same result once more. If the results are not
        ordered then the next result calculated will be skipped.

    """
    @staticmethod
    def _pool_put(pool_semaphore, tasks, put_to_pool_in, pool_size, id_self,\
                  is_stopping):
        """ (internal) Intended to be run in a seperate thread. Feeds tasks into to the
            pool whenever semaphore permits. Finishes if self._stopping is set.
        """
        log.debug('IMap(%s) started pool_putter.' % id_self)
        last_tasks = {}
        for task in xrange(tasks.lenght):
            last_tasks[task] = -1
        stop_tasks = []
        while True:
            # are we stopping the Weaver?
            if is_stopping():
                log.debug('IMap(%s) pool_putter has been told to stop.' % id_self)
                tasks.stop()
            # try to get a task
            try:
                log.debug('IMap(%s) pool_putter waits for next task.' % id_self)
                task = tasks.next()
                log.debug('IMap(%s) pool_putter received next task.' % id_self)
            except StopIteration:
                # Weaver raised a StopIteration
                stop_task = tasks.i # current task
                log.debug('IMap(%s) pool_putter catched StopIteration from task %s.' %\
                                                                  (id_self, stop_task))
                if stop_task not in stop_tasks:
                    # task raised stop for the first time.
                    log.debug('IMap(%s) pool_putter task %s first-time finished.' %\
                                                                (id_self, stop_task))
                    stop_tasks.append(stop_task)
                    pool_semaphore.acquire()
                    log.debug('IMap(%s) pool_putter sends a sentinel for task %s.' %\
                                                                 (id_self, stop_task))
                    put_to_pool_in((stop_task, None, last_tasks[stop_task]))
                if len(stop_tasks) == tasks.lenght:
                    log.debug('IMap(%s) pool_putter sent sentinels for all tasks.' %\
                                                                              id_self)
                    # all tasks have been stopped
                    for worker in xrange(pool_size):
                        put_to_pool_in(None)
                    log.debug('IMap(%s) pool_putter sent sentinel for %s workers' %\
                                                               (id_self,  pool_size))
                    # this kills the pool_putter
                    break
                # multiple StopIterations for a tasks are ignored. This is for stride.
                continue

            # got task
            last_tasks[tasks.i] = task[-1][0] # last valid result
            log.debug('IMap(%s) pool_putter waits for semaphore for task %s' % (id_self, task))
            pool_semaphore.acquire()
            log.debug('IMap(%s) pool_putter gets semaphore for task %s' % (id_self, task))
            put_to_pool_in(task)
            log.debug('IMap(%s) pool_putter submits task %s to worker.' % (id_self, task))
        log.debug('IMap(%s) pool_putter returns' % id_self)

    @staticmethod
    def _pool_get(get, results, next_available, task_next_lock, to_skip, task_num,\
                  pool_size, id_self):
        """ (internal) Intended to be run in a seperate thread and take results from the
            pool and put them into queues depending on the task of the result. It finishes
            if it receives termination-sentinels from all pool workers.
        """
        log.debug('IMap(%s) started pool_getter' % id_self)
        # should return when all workers have returned, each worker sends a sentinel
        # before returning. Before returning it should send sentinels to all tasks
        # but the next availble queue should be released only if we now that no new
        # results will arrive.
        sentinels = 0
        result_ids, last_result_id, very_last_result_id = {}, {}, {}
        for i in xrange(task_num):
            last_result_id[i] = -1
            very_last_result_id[i] = -2
            result_ids[i] = set()

        while True:
            try:
                log.debug('IMap(%s) pool_getter waits for a result.' % id_self)
                result = get()
            except (IOError, EOFError):
                log.error('IMap(%s) pool_getter has a pipe problem.' % id_self)
                break

            # got a sentinel?
            if result is None:
                sentinels += 1
                log.debug('IMap(%s) pool_getter got a sentinel.' % id_self)
                if sentinels == pool_size:
                    log.debug('IMap(%s) pool_getter got all sentinels.' % id_self)
                    # here we are escaping.
                    break
                else:
                    # waiting for more sentinels or results to come.
                    continue

            # got a sentinel for a task?
            # only one sentinel per task will be received
            if result[1] is None:
                task = result[0]
                very_last_result_id[task] = result[2]
                if last_result_id[task] == very_last_result_id[task]:
                    results[task].put(('stop', False, 'stop'))
                    next_available[task].put(True)
                    log.debug('IMap(%s) pool_getter sent sentinel for task %s.'\
                                                               % (id_self, task))
                continue

            # got some result for some task, which might be an exception
            task, i, is_valid, real_result = result
            # locked if next for this task is in the process of raising a TimeoutError
            task_next_lock[task].acquire()
            log.debug('IMap(%s) pool_getter received result %s for task %s)' %\
                      (id_self, i, task))

            if to_skip[task]:
                log.debug('IMap(%s) pool_getter skips results: %s' % (id_self,\
                range(last_result_id[task] +1, last_result_id[task] + to_skip[task] + 1)))
                last_result_id[task] += to_skip[task]
                to_skip[task] = 0

            if i > last_result_id[task]:
                result_ids[task].add(i)
                results[task].put((i, is_valid, real_result))
                log.debug('IMap(%s) pool_getter put result %s for task %s to queue' %\
                          (id_self, i, task))
            else:
                log.debug('IMap(%s) pool_getter skips result %s for task %s' %\
                          (id_self, i, task))

            # this releases the next method for each ordered result in the queue
            # if the IMap instance is ordered =False this information is ommited.
            while last_result_id[task] + 1 in result_ids[task]:
                next_available[task].put(True)
                last_result_id[task] += 1
                log.debug('IMap(%s) pool_getter released task: %s' % (id_self, task))
            if last_result_id[task] == very_last_result_id[task]:
                results[task].put(('stop', False, 'stop'))
                next_available[task].put(True)
            # release the next method
            task_next_lock[task].release()
        log.debug('IMap(%s) pool_getter returns' % id_self)

    def __init__(self, func =None, iterable =None, args =None, kwargs =None,\
                 worker_type =None, worker_num =None, worker_remote =None,\
                 stride =None, buffer =None, ordered =True, skip =False,\
                 name =None):

        log.debug('%s %s starts initializing' % (self, (name or '')))
        if not PARALLEL:
            if worker_type == 'process':
                log.error('worker_type ="process" requires multiprocessing')
                raise ImportError('worker_type ="process" requires multiprocessing')
            else:
                self.worker_type = 'thread'
        else:
            self.worker_type = (worker_type or 'process') # 'thread' or 'process'
        if worker_remote and not PARALLEL == 2:
            log.error('worker_remote requires RPyC')
            raise ImportError('worker_remote requires RPyC')

        self._tasks = []
        self._tasks_tracked = {}
        self._started = Event()               # (if not raise TimeoutError on next)
        self._stopping = Event()              # (starting stopping procedure see stop)
        # pool options
        self.worker_num = cpu_count() if worker_num is None else worker_num
        self.worker_remote = (worker_remote or [])    # [('host', #workers)]
        self.stride = (stride or self.worker_num + sum([i[1] for i in self.worker_remote]))
        self.buffer  = buffer                         # defines the maximum number
        self.name =  (name or 'imap_%s' % id(self))
        # of jobs which are in the input queue, pool and output queues and next method

        # next method options
        self.ordered = ordered
        self.skip = skip

        # make pool input and output queues based on worker type.
        if self.worker_type == 'process':
            self._inqueue = SimpleQueue()
            self._outqueue = SimpleQueue()
            self._putin = self._inqueue._writer.send
            self._getout = self._outqueue._reader.recv
            self._getin = self._inqueue.get
            self._putout = self._outqueue.put
        elif self.worker_type == 'thread':
            self._inqueue = Queue()
            self._outqueue = Queue()
            self._putin = self._inqueue.put
            self._getout = self._outqueue.get
            self._getin = self._inqueue.get
            self._putout = self._outqueue.put
        log.debug('%s finished initializing' % self)

        if bool(func) ^ bool(iterable):
            log.error('%s either, both *or* none func *and* iterable' % self +\
                      'have to be specified.')
            raise ValueError('%s either, both *or* none func *and* iterable' % self +\
                             'have to be specified.')
        elif bool(func) and bool(iterable):
            self.add_task(func, iterable, args, kwargs)
            self.start()

    def _start_managers(self):
        # combine tasks into a weaved queue
        self._next_available = {}   # per-task boolean queue releases next to get a result
        self._next_skipped = {}     # per-task int, number of results to skip (locked)
        self._task_next_lock = {}   # per-task lock around _next_skipped
        self._task_finished = {}    # a per-task is finished variable
        self._task_results = {}     # a per-task queue for results

        self._task_queue = Weave(self._tasks, self.stride)

        # here we determine the size of the maximum memory consumption
        self._semaphore_value = (self.buffer or (len(self._tasks) * self.stride))
        self._pool_semaphore = Semaphore(self._semaphore_value)

        for id_ in range(len(self._tasks)):
            self._next_available[id_] = Queue()
            self._next_skipped[id_] = 0
            self._task_finished[id_] = Event()
            self._task_next_lock[id_] = tLock() # this locks threads not processes
            self._task_results[id_] = PriorityQueue() if self.ordered else Queue()

        # start the pool putter thread
        self._pool_putter = Thread(target =self._pool_put,\
                                   args = (self._pool_semaphore, self._task_queue, self._putin,\
                                           len(self.pool), id(self), self._stopping.isSet))
        self._pool_putter.deamon = True
        self._pool_putter.start()

        # start the pool getter thread
        self._pool_getter = Thread(target =self._pool_get,\
                                   args = (self._getout, self._task_results, self._next_available,\
                                           self._task_next_lock, self._next_skipped,
                                           len(self._tasks), len(self.pool), id(self)))
        self._pool_getter.deamon = True
        self._pool_getter.start()

    def _start_workers(self):
        # creating the pool of worker process or threads
        log.debug('%s starts a %s-pool of %s workers.' % (self, self.worker_type, self.worker_num))
        self.pool = []
        for host, worker_num in [(None, self.worker_num)] + list(self.worker_remote):
            for i in range(worker_num):
                w = Thread(target=worker,  args=(self._inqueue, self._outqueue, host))\
                  if self.worker_type == 'thread' else\
                    Process(target=worker, args=(self._inqueue, self._outqueue, host))
                self.pool.append(w)
        for w in self.pool:    
            w.daemon = True
            w.start()
        log.debug('%s started the pool' % self)

    def add_task(self, func, iterable, args =None, kwargs =None, timeout =None,\
                block =True, track =False):
        """ Adds a task to evaluate. A task is made of a function an iterable and optional
            arguments and keyworded arguments. The iterable can be the result iterator of
            a previously added task. A tasklet is a (func, iterable.next(), args, kwargs).

              * func(callable)

                Will be called with the elements of the iterable, args and kwargs.

              * iterable(iterable)

                The elements of the iterable will be the first arguments passed to the
                func.

              * args(tuple) [default =None]

                A tuple of optional constant arguments passed to the function after the
                argument from the iterable.

              * kwargs(dict) [default =None]

                A dictionary of constant keyworded arguments passed to the function after
                the variable argument from the iterable and the constant arguments in the
                args tuple.

              * track(bool) [default =False]

                If true the results (or exceptions) of task are saved withing
                self._tasks_tracked[%task_id%] as a {index:result} dictionary.
                This is only useful if the task function involves creation of
                persistant data. The dictionary can be used to restore the
                correct order of the data.

            .. note::

               The order in which tasks are added to the IMap instance is
               important. It affects the order in which tasks are submited to
               the pool and consequently the order in which results should be
               retrieved. If the tasks are chained then the order should be a
               valid topological sort (reverse topological oreder).
        """

        if not self._started.isSet():
            task = izip(repeat(len(self._tasks)), repeat(func), repeat((args or ())),\
                        repeat((kwargs or {})), enumerate(iterable))
            task_id = len(self._tasks)
            self._tasks.append(task)
            if track:
                self._tasks_tracked[task_id] = {} # result:index
            return self.get_task(task =task_id, timeout =timeout, block =block)
        else:
            log.error('%s cannot add tasks (is started).' % self)
            raise RuntimeError('%s cannot add tasks (is started).' % self)

    def __call__(self, *args, **kwargs):
        return self.add_task(*args, **kwargs)

    def start(self):
        """ Starts the processes or threads in the pool, the threads which manage
            the pools input and output queues respectively. These processes/threads are
            killed either when all the results are calculated and retrieved or the stop
            method is called.
        """
        if not self._started.isSet():
            self._started.set()
            self._start_workers()
            self._start_managers()
        else:
            log.error('%s is already started.' % self)
            raise RuntimeError('%s is already started.' % self)

    def stop(self, ends =None):
        """ Stops the worker pool threads/processes and the threads which manage the
            input and output queues of the pool respectively. It stops IMap instances
            which did not consume the input and/or calculate the results. Blocks the
            calling thread until all threads and/or processes returned.

             * ends(list) [default: None]

               A list of task ids which are not consumed within the IMap instance. All
               buffered results will be lost and up to 2*stride of inputs consumed. If no
               list is given the end tasks will need to be manually consumed if this is
               not done threads/processes might not terminate and the Python interpreter
               will not exit cleanly.
        """
        if self._started.isSet():
            self._stopping.set()
            # if _stopping is set the pool putter will notify the weave generator
            # that no more new results are needed. The weave generator will
            # stop _before_ getting the first result from task 0 in the next round.
            log.debug('%s begins stopping routine' % self)
            to_do = ends[:] if ends else ends
            # We continue this loop until all end tasks have raised StopIteration
            while to_do:
                for task in to_do:
                    try:
                        #for i in xrange(self.stride):
                        self.next(task =task)
                    except StopIteration:
                        to_do.remove(task)
                        log.debug('%s stopped task %s' % (self, task))
                        continue
                    except Exception, e:
                        log.debug('%s task %s raised exception %s' % (self, task, e))
                        pass
            # for now no pause/resume
            self._tasks = []
            self._pool_putter.join()
            self._pool_getter.join()
            self._stopping.clear()
            self._started.clear()
            log.debug('%s finished stopping routine' % self)
        else:
            log.error('%s has not yet been started.' % self)
            raise RuntimeError('%s has not yet been started.' % self)

    def __str__(self):
        return "IMap(%s)" % id(self)

    def __iter__(self):
        """ A single IMap instance supports iteration over results from many tasks.
            The default is to iterate over the results from the first task. i.e.::

              for i in imap_instance:
                  do sth with i

            will be equivalnet to::

              while True
                  try:
                      result = imap_instance.next(timeout =None, task =0)
                  except StopIteration:
                      pass

            A custom iterator can be obtained by calling get_task.
        """
        return self

    def get_task(self, task =0, timeout =None, block =True):
        """ Returns an iterator which results are bound to one task. The default iterator
            the one which e.g. will be used by default in for loops is the iterator for
            the first task (task =0). Compare::

              for result_from_task_0 in imap_instance:
                  pass

            with::

              for result_from_task_1 in imap_instance.get_task(task_id =1):
                  pass

            a typical use case is::

              task_0_iterator = imap_instance.get_task(task_id =0)
              task_1_iterator = imap_instance.get_task(task_id =1)

              for (task_1_res, task_0_res) in izip(task_0_iterator, task_1_iterator):
                  pass

        """
        return IMapTask(self, task =task, timeout =timeout, block =block)

    def next(self, timeout =None, task =0, block =True):
        """ Returns the next result for the given task (default 0).

            .. note::

               the next result for task n can regardless of the buffersize be evaluated
               only if the result for task n-1 has left the buffer.

               If multiple tasks are evaluated then those tasks share not only the process
               or thread pool but also the buffer, the minimal buffer size is one and
               therefore the results from the buffer have to be removed in the same order
               as tasks are submitted to the pool. The tasks are submited in a topological
               order which allows to chain them.

            .. warning::

               If multiple chained tasks are evaluated then the next method of only the
               last should be called directly. Otherwise the pool might dead-lock depending
               on the buffer-size. This is a consequenceof the fact that tasks are
               submitted to the pool in a next-needed order. Calling the next method of an
               up-stream task changes this topological evaluation order.

        """
        # check if any result is expected (started and not finished)
        if not self._started.isSet():
            log.debug('%s has not yet been started' % self)
            raise RuntimeError('%s has not yet been started' % self)
        elif self._task_finished[task].isSet():
            log.debug('%s has finished task %s' % (self, task))
            raise StopIteration

        # try to get a result
        try:
            log.debug('%s waits for a result: ordered %s, task %s' % (self, self.ordered, task))
            if self.ordered:
                got_next = self._next_available[task].get(timeout =timeout, block =block)
                log.debug('%s has been released for task: %s' % (self, task))
                result = self._task_results[task].get()
            else:
                result = self._task_results[task].get(timeout =timeout, block =block)
        except Empty:
            self._task_next_lock[task].acquire()
            log.debug('%s timeout for result: ordered %s, task %s' % (self, self.ordered, task))
            # the threads might have switched between the exception and the lock.acquire
            # during this switch several items could have been submited to the queue
            # if one of them is the one we are waiting for we get it here immediately, but
            # lock the pool getter not to submit more results.
            try:
                if self.ordered:
                    got_next = self._next_available[task].get(block =False)
                    log.debug('%s has been released for task: %s' % (self, task))
                    result = self._task_results[task].get()
                else:
                    result = self._task_results[task].get(block =False)
            except Empty:
                if self.skip:
                    self._next_skipped[task] += 1
                    self._pool_semaphore.release()
                self._task_next_lock[task].release()
                raise TimeoutError('%s timeout for result: ordered %s, task %s' %\
                                   (self, self.ordered, task))
        log.debug('%s got a result: ordered %s, task %s, %s' % (self, self.ordered, task, result))
        # return or raise the result
        index, is_valid, real_result = result
        if index == 'stop':
            # got the stop sentinel
            self._task_finished[task].set()
            log.debug('%s releases semaphore after StopIteration of task %s' % (self, task))
            self._pool_semaphore.release()
            log.debug('%s has finished task %s for the first time' % (self, task))
            raise StopIteration
        if task in self._tasks_tracked:
            self._tasks_tracked[task][index] = real_result
        if is_valid:
            log.debug('%s returns %s for task %s' % (self, index, task))
            self._pool_semaphore.release()
            return real_result
        else:
            log.debug('%s raises %s for task %s' % (self, index, task))
            self._pool_semaphore.release()
            raise real_result


def worker(inqueue, outqueue, host =None):
    """ Function which is executed by processes/threads within the pool.
        It waits for tasks (function, data, arguments) at the input queue
        evaluates the result and passes it to the output queue.
    """
    put = outqueue.put
    get = inqueue.get
    if hasattr(inqueue, '_writer'):
        inqueue._writer.close()
        outqueue._reader.close()
    if host:
        conn = rpyc.classic.connect(*host.split(':'))
        conn.execute(getsource(imports)) # provide @imports on server

    while True:
        try:
            task = get()
        except (EOFError, IOError):
            break

        if task is None:
            put(None)
            break

        if task[1] is None:
            put(task)
            continue

        job, func, args, kwargs, (i, data) = task 
        if host:
            func = func._inject(conn) if hasattr(func, '_inject') else\
                   inject_func(func, conn)
        try:
            ok, result = (True, func(data, *args, **kwargs))
        except Exception, e:
            ok, result = (False, e)         
        put((job, i, ok, result))
        
        
def inject_func(func, conn, i):
    """ Injects a function object into a rpyc connection object
    """
    name = func.__name__
    if not name in conn.namespace:
        if isbuiltin(func):
            inject_code = '%s = %s' % (name, name)
        elif isfunction(func):
            inject_code = getsource(func)
        conn.execute(inject_code)
    return conn.namespace[name]

def imports(modules, forgive =False):
    """ Should be used as a decorator to attach import statments to function
        definitions. And import them in the in the global namespace of the 
        function. 
        
        Two forms of import statements are supported::

         import module                    # e.q. to ['module', []]
         from module import sub1, func2   # e.q. to ['module', ['sub1', func2]]

        The imported modules should be considered as availble only inside the 
        decorated functions. 

        Arguments:

          * modules(list)

            A list of modules in the following forms::

              ['module',['sub_module1', ... ,'sub_module2']]

            or::

              ['module',[]]

            If a list of sub-modules is specified they will be availble in the
            globals of the function i.e::

              # re module availble in the namespace
              @imports([['re',[]]])
                def need_re(some_string):
                    res = re.search('pattern',some_string)
                    return res.group()

              # search function availble in the namespace
              @imports([['re',['search']]])
                def need_re(some_string):
                    res = search('pattern',some_string) #!
                    # but re.search will also work.
                    return res.group()
          
          * forgive(bool)

            If True will not raise exception on ImportError.
    """
    def wrap(f):
        if modules:
            setattr(f, 'imports', modules)
            try:
                for mod, sub in modules:
                    module = __import__(mod, fromlist=sub)
                    f.func_globals[mod] = module
                    for submod in sub:
                        f.func_globals[submod] = getattr(module, submod)
            except ImportError:
                if not forgive:
                    raise
        return f
    return wrap


class Weave(object):
    """ Weaves a sequence of iterators, which can be stopped if the same number
        of results has been consumed from all iterators. 

        Arguments:

        TODO: refactor iterables -> iterators

    """
    def __init__(self, iterables, repeats =1):
        self.iterables = iterables          # sequence of iterables
        self.lenght = len(self.iterables)
        self.i = 0                          # index of current iterable
        self.repeats = repeats              # number of repeats from an iterable (stride)
        self.r = 0                          # current repeat
        self.stopping = False               # if True stop at i==0, r==0
        self.stopped = False

    def __iter__(self):
        # support for the iterator protocol
        return self

    def stop(self):
        # stopping is a one-way process
        # log.debug('Weave(%s) told to stop.' % id(self))
        self.stopping = True

    def next(self):
        # need new iterable?
        if self.r == self.repeats:
            self.i = (self.i + 1) % self.lenght
            self.r = 0

        self.r += 1
        if self.stopping and self.i ==0 and self.r ==1:
            self.stopped = True
        if self.i == 0 and self.stopped:
            raise StopIteration
        else:
            iterable = self.iterables[self.i]
            return iterable.next()


class IMapTask(object):
    """ Object returned by the get_task method of IMap instances.
        It is a wrapper around an *IMap* instance which returns
        results only for specified arguments: task, timeout, block.

        Arguments:

          * iterator(IMap instance)

            *IMap* instance to wrap, usually this is called by the get_task method
            of the *IMap* instance itself.

          * task(integer)

            Id of the task from the *IMap* instance.

          * timeout
            
            see documentation for: *IMap.next*

          * block

            see documentation for: *IMap.next*
    """
    def __init__(self, iterator, task, timeout, block):
        self.timeout = timeout
        self.block = block
        self.task = task
        self.iterator = iterator

    def __iter__(self):
        return self

    def next(self):
        """ Returns a result if availble within timeout else raises
            a TimeoutError. See documentation for *IMap.next*
        """
        return self.iterator.next(task =self.task, timeout =self.timeout,
                                                     block =self.block)

class PriorityQueue(Queue):
    """ A priority queue using a heap on a list.
        This Queue is thread but not process safe.
    """
    def _init(self, maxsize):
        self.maxsize = maxsize
        self.queue = []

    def _put(self, item):
        return heappush(self.queue, item)

    def _get(self):
        return heappop(self.queue)

#EOF
