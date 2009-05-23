"""
:mod:`IMap`
================

This module provides a parallel, buffered, multi-task, imap function.
It evaluates results as they are needed, where the need is relative to
the buffer size. It can use threads and processes.
"""

# Multiprocessing requires Python 2.6 or the backport of this package to the 2.5 line
# get it from http://pypi.python.org/pypi/multiprocessing/
from multiprocessing import Process, Pipe, cpu_count, TimeoutError
from multiprocessing.synchronize import Lock
from multiprocessing.forking import assert_spawning
from multiprocessing.queues import SimpleQueue
# Threading and Queues
from threading import Thread, Semaphore, Event
from threading import Lock as tLock
from Queue import Queue, Empty
# for PriorityQueue
from heapq import heappush, heappop
# Misc.
from itertools import izip, repeat, cycle
from collections import defaultdict, deque
import time
# sets-up logging
from logging import getLogger
log = getLogger(__name__)

class IMap(object):
    """
    Parallel (thread- or process-based), buffered, multi-task, itertools.imap or Pool.imap
    function replacment. Like imap it evaluates a function on elements of an iterable, and
    it does so layzily.

    optional arguments:

      * func, iterable, args, kwargs

        Defines the the first and only tasks, starts the IMap pool and returns an IMap
        iterator. For a description of the args, kwargs and iterable input please see
        the add_task function. Either both or none func *and* iterable have to be
        given. Arguments are optional.

      * worker_type('process' or 'thread') [default: 'process']

        Defines the type of internally spawned pool workers. For multiprocessing Process
        based worker choose 'process' for threading Thread workers choose 'thread'.

        .. note::

          This choice has *fundamental* impact on the performance of the function please
          understand the difference between processes and threads and refer to the manual
          documentation. As a general rule use 'process' if you have multiple CPUs or
          cores and your functions(tasks) are cpu-bound. Use 'thread' if your function is
          IO-bound e.g. retrieves data from the Web.

      * worker_num(int) [default: number of CPUs, min: 1]

        The number of workers to spawn default to the number of availble CPUs, which is a
        reasonable choice for process-based IMaps.

        .. note::

          Increasing the number of workers above the number of CPUs makes sense only if
          these are Thread-based workers and the evaluated functions are IO-bound.

      * stride(int) [default: worker_num, min: 1]

        The stride argument defines the number of consecutive tasklets which are
        submitted to the process/thread pool for a task. This defines the degree
        of parallelism.

      * buffer(int) [default: stride per task, min: variable]

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
        log.debug('IMap(%s) started pool_putter' % id_self)
        task = None
        while True:
            try:
                log.debug('IMap(%s) pool_putter waits for next task' % id_self)
                if is_stopping():
                    log.debug('IMap(%s) pool_putter has been hi-jacked' % id_self)
                    if task:
                        task = tasks.send(True)
                    else:
                        raise StopIteration
                else:
                    task = tasks.send(None)
            except StopIteration:
                pool_semaphore.acquire()
                log.debug('IMap(%s) pool_putter sends %s sentinels to workers.'\
                          % (id_self, pool_size))
                for worker in xrange(pool_size):
                    put_to_pool_in(None)
                break
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
        sentinels = 0
        result_ids, last_result_id = {}, {}
        for i in xrange(task_num):
            last_result_id[i] = -1
            result_ids[i] = set()

        while True:
            try:
                log.debug('IMap(%s) pool_getter waits for a result' % id_self)
                result = get()
            except (IOError, EOFError):
                log.error('IMap(%s) pool_getter has a pipe problem' % id_self)
                break

            if result is None:
                sentinels += 1
                log.debug('IMap(%s) pool_getter got sentinel #%s' % (id_self, sentinels))
                if sentinels == pool_size:
                    for task in xrange(task_num):
                        log.debug('IMap(%s) pool_getter puts stop for task %s' % (id_self, task))
                        results[task].put(('stop', False, 'stop'))
                        next_available[task].put(True)
                    log.debug('IMap(%s) pool_getter got all results' % id_self)
                    break
                else:
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

            # release the next method
            task_next_lock[task].release()
        log.debug('IMap(%s) pool_getter returns' % id_self)

    def __init__(self, func =None, iterable =None, args =None, kwargs =None,\
                 worker_type =None, worker_num =None, stride =None, buffer =None,\
                 ordered =True, skip =False, name =None):

        log.debug('%s %s starts initializing' % (self, (name or '')))
        self._tasks = []
        self._started = Event()               # (if not raise TimeoutError on next)
        self._stopping = Event()              # (starting stopping procedure see stop)
        # pool options
        self.worker_type = (worker_type or 'process') # 'thread' or 'process'
        self.worker_num = (worker_num or cpu_count()) # number of thread/processes
        self.stride = (stride or self.worker_num)     #
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

        self._task_queue = weave(self._tasks, self.stride)

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
        for i in range(self.worker_num):
            w = Thread(target=worker, args=(self._inqueue, self._outqueue))\
              if self.worker_type == 'thread' else\
              Process(target=worker, args=(self._inqueue, self._outqueue))
            self.pool.append(w)
            w.daemon = True
            w.start()
        log.debug('%s started the pool' % self)

    def add_task(self, func, iterable, args =None, kwargs =None, timeout =None, block =True):
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

            .. note::

               The order in which tasks are added to the IMap instance is important. It
               affects the order in which tasks are submited to the pool and consequently
               the order in which results should be retrieved. If the tasks are chained
               then the order should be a valid topological sort.
        """

        if not self._started.isSet():
            task = izip(repeat(len(self._tasks)), repeat(func), repeat((args or ())),\
                        repeat((kwargs or {})), enumerate(iterable))
            task_id = len(self._tasks)
            self._tasks.append(task)
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
                        self.next(task =task)
                    except StopIteration:
                        to_do.remove(task)
                        log.debug('%s stopped task %s' % (self, task))
                        continue
                    except Exception, e:
                        log.debug('%s task %s raised exception %s' % (self, task, e))
                        pass
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

            A custom iterator can be obtained by calling imap_task.
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
            del self._tasks[task]
            return self.next(task =task)
        if is_valid:
            log.debug('%s returns %s for task %s' % (self, index, task))
            self._pool_semaphore.release()
            return real_result
        else:
            log.debug('%s raises %s for task %s' % (self, index, task))
            self._pool_semaphore.release()
            raise real_result


def worker(inqueue, outqueue):
    """ Function which is executed by processes/threads within the pool.
        It waits for tasks (function, data, arguments) at the input queue
        evaluates the result and passes it to the output queue.
    """
    put = outqueue.put
    get = inqueue.get
    if hasattr(inqueue, '_writer'):
        inqueue._writer.close()
        outqueue._reader.close()

    while True:
        try:
            task = get()
        except (EOFError, IOError):
            break

        if task is None:
            put(None)
            break

        job, func, args, kwargs, (i, data) = task
        try:
            ok, result = (True, func(data, *args, **kwargs))
        except Exception, e:
            ok, result = (False, e)
        put((job, i, ok, result))

def weave(iterables, repeats =1):
    """ Weaves (merges) an iterable (e.g. list) of iterators (e.g. generators).
        Optionally stops at cycle boundaries (*after* the result from the last iterator)
        if the generator receives True using the send method.
    """
    stopping = None
    for i in cycle(iterables):
        if i is iterables[0] and stopping:
                raise StopIteration
        for x in xrange(repeats):
            try:
                stopping = ((yield i.next()) or stopping)
            except StopIteration:
                if i is iterables[0] and x:
                    repeats = x
                    break
                raise StopIteration


class IMapTask(object):
    """ Is returned by the get_task method of IMap instances.
        It is a wrapper around an IMap instance which returns
        results only for a specified task_id.
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
            a TimeoutError.
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
