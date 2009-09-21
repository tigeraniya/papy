"""
:mod:`IMap.IMap`
================

This module provides a parallel, buffered, multi-task, imap function. It 
evaluates results as they are needed, where the need is relative to the buffer
size. It can use threads and processes.
"""
# Multiprocessing requires Python 2.6 or the backport of this package to 
# Python 2.5  get it from http://pypi.python.org/pypi/multiprocessing/
PARALLEL = 0
try:
    from multiprocessing import Process, cpu_count, TimeoutError
    from multiprocessing.queues import SimpleQueue
    PARALLEL += 1
except ImportError:
    print 'mutliprocessing not available get it from:' + \
          'http://pypi.python.org/pypi/multiprocessing/' + \
          'or via su -c "easy_install multiprocessing"'
try:
    import rpyc
    PARALLEL += 1
except ImportError:
    print 'RPyC not available get it from:' + \
          'http://rpyc.wikidot.com/'

# Threading and Queues
from threading import Thread, Semaphore, Event
from threading import Lock as tLock
from Queue import Queue, Empty
# for PriorityQueue
from heapq import heappush, heappop
# Misc.
from itertools import izip, repeat
from inspect import getsource, isbuiltin, isfunction
# sets-up logging
from logging import getLogger
log = getLogger(__name__)
import warnings


class IMap(object):
    """
    Parallel (thread- or process-based, local or remote), buffered, multi-task, 
    ``itertools.imap`` or ``Pool.imap`` function replacment. Like ``imap`` it 
    evaluates a function on elements of a sequence or iterator, and it does so 
    lazily with an adjustable buffer. This is accomplished  via the stride and
    buffer arguments. All sequences or iterators are **required** to be of the
    same lenght. The tasks can be interdependent, the result from one task 
    being the input to a second taks if the tasks are added to the ``IMap`` in 
    the right order.
    
    A completely lazy evaluation, i.e. submitting the first tasklet *after* the
    next method for the first task has been called, is not supported.
    
    Arguments:

        * func, iterable, args, kwargs [default: None]
        
            If the IMap with those arguments they are treated to define the 
            first and only task of the ``IMap``, it *starts* the ``IMap`` pool 
            and returns an ``IMap`` iterator. For a description of the args, 
            kwargs and iterable input please see the add_task method. Either 
            *both* or *none* func **and** se have to be specified. 
            Positional and keyworded arguments are optional.
        
        * worker_type('process' or 'thread') [default: 'process']
        
            Defines the type of internally spawned pool workers. For 
            ``multiprocessing.Process`` based worker choose 'process' for 
            ``threading.Thread`` workers choose 'thread'.
            
            .. note::
            
                This choice has *fundamental* impact on the performance of the
                function. Please understand the difference between processes 
                and threads and refer to the manual documentation. As a 
                general rule use 'process' if you have multiple CPUs or 
                CPU-cores and your task functions are cpu-bound. Use 'thread' 
                if your function is IO-bound e.g. retrieves data from the Web.
                
                If you specify any remote workers via worker_remote, worker_type
                has to be the default 'process'. This limitation might go away 
                in future versions.
        
        * worker_num(int) [default: number of CPUs, min: 1]
        
            The number of workers to spawn locally. Defaults to the number of 
            availble CPUs, which is a reasonable choice for process-based  
            ``IMaps``. For thread-based ``IMaps`` a larger number of might 
            improve performance. This number does *not* include workers needed
            to run remote processes and can be =0 for a purely remote *IMap*.
            
            .. note::
        
                Increasing the number of workers above the number of CPUs makes
                sense only if these are Thread-based workers and the evaluated 
                functions are IO-bound. Some CPU-bound tasks might evaluate 
                faster if the number of worker processes equals the number of 
                CPUs + 1.
        
        * worker_remote(sequence or None) [default: None]
        
            A sequence of remote host identifiers, and remote worker number 
            pairs. Specifies the number of workers per *RPyC* host in the form 
            of ("host:port", worker_num). For example::
            
                  [['localhost', 2], ['127.0.0.1', 2]]
            
            with a custom TCP port::
            
                  [['localhost:6666'], ['remotehost:8888', 4]]
        
        * stride(int) [default: worker_num + sum of remote worker_num, min: 1]
        
            Defines the number of tasklets which are submitted to the process 
            thread, consecutively from a single task. See the documentation for
            ``add_task`` and the manual for an explanation of a task and 
            tasklet. The stride defines the laziness of the pipeline. A long 
            stride improves parallelism, but increases memory consumption. It 
            should not be smaller than the size of the pool, because of idle
            processes or threads.
        
        * buffer(int) [default: stride * (tasks * spawn), min: variable]
        
            The buffer argument limits the maximum memory consumption, by 
            limiting the number of tasklets submitted to the pool. This number 
            is larger then stride because a task might depend on results from
            multiple tasks. The minimum buffer is the maximum possible number
            of queued results. This number depends on the interdependencies 
            between tasks, the produce/spawn/consume numbers and the stride.
            The default is conservative and equals will always be enough 
            regardless of the task interdependencies. Please consult the manual
            before adjusting this setting.
            
            .. note::
            
                A tasklet is considered submitted until the result is returned 
                by the next method. The minimum stride is 1 this means that 
                starting the IMap will cause one tasklet (the first from the 
                first task) to be submitted to the pool input queue. The first 
                tasklet from the second task can enter the queue only if either
                the result from the first tasklet is returned or the buffer size
                is larger then the stride size.
                If the buffer is n then n tasklets can enter the pool. A Stride
                of n requires n tasklets to enter the pool, therefore buffer 
                can never be smaller then stride. If the tasks are chained i.e. 
                the output from one is consumed by another then the first at 
                most one tasklet i-th tasklet from each chained task is at a 
                given moment in the pool. In those cases the minimum buffer to 
                satisfy the worst case number of queued results is lower then 
                the safe default.
       
        * ordered(bool) [default: True]
        
            If True the output of all tasks will be ordered. This means that for
            specific task the result from the n-th tasklet will be return before
            the result from the n+1-th tasklet. If false the results will be 
            returned in the order they are calculated.
        
        * skip(bool) [default: False]
        
            Should we skip a result if trying to retrieve it raised a 
            ``TimeoutError``? If ordered =True and skip =True then the calling 
            the next method after a TimeoutError will skip the result, which did
            not arrive on time and try to get the next. If skip =False it will 
            try to get the same result once more. If the results are not
            ordered then the next result calculated will be skipped. If tasks
            are chained a TimeoutError will collapse the *IMap* evaluation. Do 
            *not* use specify timeouts (this argument becomes irrelevant). 
        
        * name(string) [default: 'imap_id(object)']
        
            An optional name to associate with this *IMap* instance. Should be 
            unique. Useful for nicer code generation.
    """
    @staticmethod
    def _pool_put(pool_semaphore, tasks, put_to_pool_in, pool_size, id_self, \
                  is_stopping):
        """ 
        (internal) Intended tiuo be run in a seperate thread. Feeds tasks into 
        to the pool whenever semaphore permits. Finishes if self._stopping is 
        set.
        """
        log.debug('IMap(%s) started pool_putter.' % id_self)
        last_tasks = {}
        for task in xrange(tasks.lenght):
            last_tasks[task] = -1
        stop_tasks = []
        while True:
            # are we stopping the Weaver?
            if is_stopping():
                log.debug('IMap(%s) pool_putter has been told to stop.' % \
                           id_self)
                tasks.stop()
            # try to get a task
            try:
                log.debug('IMap(%s) pool_putter waits for next task.' % \
                           id_self)
                task = tasks.next()
                log.debug('IMap(%s) pool_putter received next task.' % id_self)
            except StopIteration:
                # Weaver raised a StopIteration
                stop_task = tasks.i # current task
                log.debug('IMap(%s) pool_putter caught StopIteration from task %s.' % \
                                                                  (id_self, stop_task))
                if stop_task not in stop_tasks:
                    # task raised stop for the first time.
                    log.debug('IMap(%s) pool_putter task %s first-time finished.' % \
                                                           (id_self, stop_task))
                    stop_tasks.append(stop_task)
                    pool_semaphore.acquire()
                    log.debug('IMap(%s) pool_putter sends a sentinel for task %s.' % \
                                                           (id_self, stop_task))
                    put_to_pool_in((stop_task, None, last_tasks[stop_task]))
                if len(stop_tasks) == tasks.lenght:
                    log.debug('IMap(%s) pool_putter sent sentinels for all tasks.' % \
                                                                        id_self)
                    # all tasks have been stopped
                    for _worker in xrange(pool_size):
                        put_to_pool_in(None)
                    log.debug('IMap(%s) pool_putter sent sentinel for %s workers' % \
                                                           (id_self, pool_size))
                    # this kills the pool_putter
                    break
                # multiple StopIterations for a tasks are ignored. 
                # This is for stride.
                continue

            # got task
            last_tasks[tasks.i] = task[-1][0] # last valid result
            log.debug('IMap(%s) pool_putter waits for semaphore for task %s' % \
                       (id_self, task))
            pool_semaphore.acquire()
            log.debug('IMap(%s) pool_putter gets semaphore for task %s' % \
                       (id_self, task))
            #gc.disable()
            put_to_pool_in(task)
            #gc.enable()
            log.debug('IMap(%s) pool_putter submits task %s to worker.' % \
                       (id_self, task))
        log.debug('IMap(%s) pool_putter returns' % id_self)

    @staticmethod
    def _pool_get(get, results, next_available, task_next_lock, to_skip, \
                  task_num, pool_size, id_self):
        """ 
        (internal) Intended to be run in a separate thread and take results from
        the pool and put them into queues depending on the task of the result. 
        It finishes if it receives termination-sentinels from all pool workers.
        """
        log.debug('IMap(%s) started pool_getter' % id_self)
        # should return when all workers have returned, each worker sends a 
        #sentinel before returning. Before returning it should send sentinels to
        # all tasks but the next available queue should be released only if we 
        # now that no new results will arrive.
        sentinels = 0
        result_ids, last_result_id, very_last_result_id = {}, {}, {}
        for i in xrange(task_num):
            last_result_id[i] = -1
            very_last_result_id[i] = -2
            result_ids[i] = set()

        while True:
            try:
                log.debug('IMap(%s) pool_getter waits for a result.' % id_self)
                #gc.disable()
                result = get()
                #gc.enable()
            except (IOError, EOFError):
                log.error('IMap(%s) pool_getter has a pipe problem.' % id_self)
                break

            # got a sentinel?
            if result is None:
                sentinels += 1
                log.debug('IMap(%s) pool_getter got a sentinel.' % id_self)
                if sentinels == pool_size:
                    log.debug('IMap(%s) pool_getter got all sentinels.' % \
                              id_self)
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
            # locked if next for this task is in 
            # the process of raising a TimeoutError
            task_next_lock[task].acquire()
            log.debug('IMap(%s) pool_getter received result %s for task %s)' % \
                      (id_self, i, task))

            if to_skip[task]:
                log.debug('IMap(%s) pool_getter skips results: %s' % (id_self, \
                range(last_result_id[task] + 1, last_result_id[task] + \
                      to_skip[task] + 1)))
                last_result_id[task] += to_skip[task]
                to_skip[task] = 0

            if i > last_result_id[task]:
                result_ids[task].add(i)
                results[task].put((i, is_valid, real_result))
                log.debug('IMap(%s) pool_getter put result %s for task %s to queue' % \
                          (id_self, i, task))
            else:
                log.debug('IMap(%s) pool_getter skips result %s for task %s' % \
                          (id_self, i, task))

            # this releases the next method for each ordered result in the queue
            # if the IMap instance is ordered =False this information is 
            # ommitted.
            while last_result_id[task] + 1 in result_ids[task]:
                next_available[task].put(True)
                last_result_id[task] += 1
                log.debug('IMap(%s) pool_getter released task: %s' % \
                          (id_self, task))
            if last_result_id[task] == very_last_result_id[task]:
                results[task].put(('stop', False, 'stop'))
                next_available[task].put(True)
            # release the next method
            task_next_lock[task].release()
        log.debug('IMap(%s) pool_getter returns' % id_self)

    def __init__(self, func=None, iterable=None, args=None, kwargs=None, \
                 worker_type=None, worker_num=None, worker_remote=None, \
                 stride=None, buffer=None, ordered=True, skip=False, \
                 name=None):

        self.name = (name or 'imap_%s' % id(self))
        log.debug('%s %s starts initializing' % (self, self.name))
        # TODO: refactor run-time checks in IMap.__init__
        if not PARALLEL:
            if worker_type == 'process':
                log.error('worker_type ="process" requires multiprocessing')
                raise ImportError('worker_type ="process" requires multiprocessing')
            else:
                self.worker_type = 'thread'
        else:
            self.worker_type = (worker_type or 'process')
            # 'thread' or 'process'
        if worker_remote and not PARALLEL == 2:
            log.error('worker_remote requires RPyC')
            raise ImportError('worker_remote requires RPyC')

        self._tasks = []
        self._tasks_tracked = {}
        self._started = Event()         # (if not raise TimeoutError on next)
        self._stopping = Event()        # (starting stopping procedure see stop)
        # pool options
        if worker_num is None:
            self.worker_num = stride or cpu_count()
        else:
            self.worker_num = worker_num
        self.worker_remote = (worker_remote or [])    # [('host', #workers)]
        self.stride = stride or \
                      self.worker_num + sum([i[1] for i in self.worker_remote])
        self.buffer = buffer            # defines the maximum number
        # of jobs which are in the input queue, pool and output queues
        # and next method

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


        # combine tasks into a weaved queue
        self._next_available = {}   # per-task boolean queue 
                                    # releases next to get a result
        self._next_skipped = {}     # per-task int, number of results
                                    # to skip (locked)
        self._task_next_lock = {}   # per-task lock around _next_skipped
        self._task_finished = {}    # a per-task is finished variable
        self._task_results = {}     # a per-task queue for results

        log.debug('%s finished initializing' % self)

        if bool(func) ^ bool(iterable):
            log.error('%s either, both *or* none func *and* iterable' % self + \
                      'have to be specified.')
            raise ValueError('%s either, both *or* none func *and* iterable' % self + \
                             'have to be specified.')
        elif bool(func) and bool(iterable):
            self.add_task(func, iterable, args, kwargs)
            self.start()

    def _start_managers(self):
        """
        (internal) Starts input and output pool queue managers.
        """
        self._task_queue = Weave(self._tasks, self.stride)
        # here we determine the size of the maximum memory consumption
        self._semaphore_value = (self.buffer or (len(self._tasks) * self.stride))
        self._pool_semaphore = Semaphore(self._semaphore_value)


        # start the pool getter thread
        self._pool_getter = Thread(target=self._pool_get, args=(self._getout, \
                self._task_results, self._next_available, \
                self._task_next_lock, self._next_skipped, len(self._tasks), \
                len(self.pool), id(self)))
        self._pool_getter.deamon = True
        self._pool_getter.start()

        # start the pool putter thread
        self._pool_putter = Thread(target=self._pool_put, args=\
                (self._pool_semaphore, self._task_queue, self._putin, \
                len(self.pool), id(self), self._stopping.isSet))
        self._pool_putter.deamon = True
        self._pool_putter.start()

    def _start_workers(self):
        """
        (internal) Start the thread/process pool workers.
        """
        # creating the pool of worker process or threads
        log.debug('%s starts a %s-pool of %s workers.' % \
                  (self, self.worker_type, self.worker_num))
        self.pool = []
        for host, worker_num in \
                           [(None, self.worker_num)] + list(self.worker_remote):
            for _worker in range(worker_num):
                __worker = Thread(target=worker, args=\
                                  (self._inqueue, self._outqueue, host)) \
                                  if self.worker_type == 'thread' else \
                           Process(target=worker, args=\
                                  (self._inqueue, self._outqueue, host))
                self.pool.append(__worker)
        for __worker in self.pool:
            __worker.daemon = True
            __worker.start()
        log.debug('%s started the pool' % self)

    def _stop(self):
        """
        (internal) Stop the input/output pool queue managers.
        """
        if self._started.isSet():
            # join threads
            self._pool_getter.join()
            self._pool_putter.join()
            for worker in self.pool:
                worker.join()
            # remove threads  
            del self._pool_putter
            del self._pool_getter
            del self.pool
            # remove results
            self._tasks = []
            self._tasks_tracked = {}
            # virgin variables
            self._stopping.clear()
            self._started.clear()

    def add_task(self, func, iterable, args=None, kwargs=None, timeout=None, \
                block=True, track=False):
        """ 
        Adds a task to evaluate. A task is made of a function an iterable and optional
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
            task = izip(repeat(len(self._tasks)), repeat(func), \
                        repeat((args or ())), repeat((kwargs or {})), \
                        enumerate(iterable))
            task_id = len(self._tasks)
            self._tasks.append(task)
            if track:
                self._tasks_tracked[task_id] = {} # result:index
            self._next_available[task_id] = Queue()
            self._next_skipped[task_id] = 0
            self._task_finished[task_id] = Event()
            self._task_next_lock[task_id] = tLock()
            # this locks threads not processes
            self._task_results[task_id] = PriorityQueue() if self.ordered \
                                                          else Queue()
            return self.get_task(task=task_id, timeout=timeout, block=block)
        else:
            log.error('%s cannot add tasks (is started).' % self)
            raise RuntimeError('%s cannot add tasks (is started).' % self)

    def pop_task(self, number):
        """
        Removes a previously added task from the *IMap* instance.
        
        Arguments
        
            * number(int or True)
            
                A positive integer specifying the number of tasks to pop. If 
                number is set ``True`` all tasks will be popped.
        """
        if not self._started.isSet():
            if number is True:
                self._tasks = []
                self._tasks_tracked = {}
            elif number > 0:
                last_task_id = len(self._tasks) - 1
                for i in xrange(number):
                    self._tasks.pop()
                    self._tasks_tracked.pop(last_task_id - i, None)
        else:
            log.error('%s cannot delete tasks (is started).' % self)
            raise RuntimeError('%s cannot delete tasks (is started).' % self)

    def __call__(self, *args, **kwargs):
        return self.add_task(*args, **kwargs)

    def start(self, stages=(1, 2)):
        """
        Starts the processes or threads in the pool (stage 1) and the threads 
        which manage the pools input and output queues respectively (stage 2). 
        
        Arguments:
        
            * stages(tuple) [default: (1,2)]
            
                Specifies which stages of the start process to execute.
                After the first stage the pool worker processes/threads are
                started and the ``IMap._started`` event is set ``True``. A call
                to the next method of the *IMap* instance will block. After the
                second stage the ``IMap._pool_putter`` and ``IMap._pool_getter``
                threads will be started.
        """
        if 1 in stages:
            if not self._started.isSet():
                self._start_workers()
                self._started.set()
        if 2 in stages:
            if not hasattr(self, '_pool_getter'):
                self._start_managers()


    def stop(self, ends=None, forced=False):
        """
        Stops an *IMap* instance. If the list of end taks is specified via the 
        ends argument it blocks the calling thread, retrieves (and discards)
        a maximum of 2 * stride of results, stops the worker pool threads or 
        processes and stops the threads which manage the input and output queues
        of the pool respectively. If the ends argument is not specified, but
        the forced argument is the method does not block and the 
        ``IMap._stop_managers`` has to be called after all pending results have 
        been retrieved. Either ends or forced has to be specified.
        
        Arguments:

            * ends(list) [default: None]
            
                A list of task ids which are not consumed within the IMap 
                instance. All buffered results will be lost and up to 2 * stride 
                of inputs consumed. If no list is given the end tasks will need 
                to be manually consumed or the threads/processes might not 
                terminate and the Python interpreter will not exit cleanly.
                
            * forced(bool) [default: False]
            
                If ends is not ``None`` this argument is ignored. If ends is 
                ``None`` and forced is ``True`` the *IMap* instance will trigger
                stopping mode.
        """
        if self._started.isSet():
            if ends:
                self._stopping.set()
                # if _stopping is set the pool putter will notify the weave 
                # generator that no more new results are needed. The weave 
                # generator will stop _before_ getting the first result from 
                # task 0 in the next stride.
                log.debug('%s begins stopping routine' % self)
                to_do = ends[:] # if ends else ends
                # We continue this loop until all end tasks
                # have raised StopIteration this stops the pool
                while to_do:
                    for task in to_do:
                        try:
                            #for i in xrange(self.stride):
                            self.next(task=task)
                        except StopIteration:
                            to_do.remove(task)
                            log.debug('%s stopped task %s' % (self, task))
                            continue
                        except Exception, excp:
                            log.debug('%s task %s raised exception %s' % \
                                      (self, task, excp))
                # stop threads remove queues 
                self._stop()
                log.debug('%s finished stopping routine' % self)
            elif forced:
                self._stopping.set()
                log.debug('%s begins triggers stopping' % self)
                # someone has to retrieve results and call the _stop_managers
            else:
                # this is the default
                msg = '%s is started, but neither ends nor forced was set.' % \
                        self
                log.error(msg)
                raise RuntimeError(msg)

    def __str__(self):
        return "IMap(%s)" % id(self)

    def __iter__(self):
        """
        A single IMap instance supports iteration over results from many tasks.
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

    def get_task(self, task=0, timeout=None, block=True):
        """
        Returns an iterator which results are bound to one task. The default 
        iterator the one which e.g. will be used by default in for loops is the
        iterator for the first task (task =0). Compare::

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
        return IMapTask(self, task=task, timeout=timeout, block=block)

    def next(self, timeout=None, task=0, block=True):
        """
        Returns the next result for the given task (default 0).

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
            log.debug('%s waits for a result: ordered %s, task %s' % \
                      (self, self.ordered, task))
            if self.ordered:
                _got_next = \
                    self._next_available[task].get(timeout=timeout, block=block)
                log.debug('%s has been released for task: %s' % (self, task))
                result = self._task_results[task].get()
            else:
                result = \
                    self._task_results[task].get(timeout=timeout, block=block)
        except Empty:
            self._task_next_lock[task].acquire()
            log.debug('%s timeout for result: ordered %s, task %s' % \
                      (self, self.ordered, task))
            # the threads might have switched between the exception and the 
            # lock.acquire during this switch several items could have been 
            # submited to the queue if one of them is the one we are waiting 
            # for we get it here immediately, but lock the pool getter not to
            # submit more results.
            try:
                if self.ordered:
                    _got_next = self._next_available[task].get(block=False)
                    log.debug('%s has been released for task: %s' % \
                              (self, task))
                    result = self._task_results[task].get()
                else:
                    result = self._task_results[task].get(block=False)
            except Empty:
                if self.skip:
                    self._next_skipped[task] += 1
                    self._pool_semaphore.release()
                self._task_next_lock[task].release()
                raise TimeoutError('%s timeout for result: ordered %s, task %s' % \
                                   (self, self.ordered, task))
        log.debug('%s got a result: ordered %s, task %s, %s' % \
                  (self, self.ordered, task, result))
        # return or raise the result
        index, is_valid, real_result = result
        if index == 'stop':
            # got the stop sentinel
            self._task_finished[task].set()
            log.debug('%s releases semaphore after StopIteration of task %s' % \
                      (self, task))
            self._pool_semaphore.release()
            log.debug('%s has finished task %s for the first time' % \
                      (self, task))
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


def worker(inqueue, outqueue, host=None):
    """
    Function which is executed by processes/threads within the pool.
    It waits for tasks (function, data, arguments) at the input queue
    evaluates the result and passes it to the output queue.
    """
    put = outqueue.put
    get = inqueue.get
    if hasattr(inqueue, '_writer'):
        inqueue._writer.close()
        outqueue._reader.close()
    if host:
        host_port = host.split(':')
        try:
            host_port = [host_port[0], int(host_port[1])]
        except IndexError:
            pass
        conn = rpyc.classic.connect(*host_port)
        conn.execute(getsource(imports)) # provide @imports on server

    while True:
        try:
            #gc.disable()
            task = get()
            #gc.enable()
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
        except Exception, excp:
            ok, result = (False, excp)
        #gc.disable()
        put((job, i, ok, result))
        #gc.enable()

def inject_func(func, conn):
    """
    Injects a function object into a rpyc connection object.
    """
    name = func.__name__
    if not name in conn.namespace:
        if isbuiltin(func):
            inject_code = '%s = %s' % (name, name)
        elif isfunction(func):
            inject_code = getsource(func)
        conn.execute(inject_code)
    return conn.namespace[name]

def imports(modules, forgive=False):
    """
    Should be used as a decorator to attach import statments to function
    definitions. These imports are added to the global (in Python module level) 
    namespace of the decorated function.
        
    Two forms of import statements are supported (in the following examples
    ``foo``, ``bar``, ``oof, and ``rab`` are modules not classes or functions)::

        import foo, bar              # e.q. to @imports(['foo', 'bar'])
        import foo.oof as oof            
        import bar.rab as rab        # e.g. to @imports(['foo.oof', 'bar.rab'])
        
    Supports alternatives::
    
        try:
            import foo
        except ImportError:
            import bar
            
    becomes::
    
        @imports(['foo,bar'])
        
    and::
    
        try:
            import foo.oof as oof
        except ImportError:
            import bar.rab as oof
            
    becomes::
    
        @imports(['foo.oof,bar.rab'])
        
    .. note::
    
        This import is available in the body of the function as ``oof`` 
            
    .. note::
    
        imports should be exhaustive for every decorated funcion even if two 
        function have the same globals.

    Arguments:

        * modules(sequence)
          
            A list of modules in the following forms::

                ['foo', 'bar', ..., 'baz']

            or::

                ['foo.oof', 'bar.rab', ..., 'baz.zab']
          
        * forgive(bool) [default: ``False``]

            If ``True`` will not raise exception on ``ImportError``.
    """
    def wrap(f):
        if modules:
            # attach import to function
            setattr(f, 'imports', modules)
            for alternatives in modules:
                # alternatives are comma seperated
                alternatives = alternatives.split(',')
                # we import the part of the import X.Y.Z -> Z
                mod_name = alternatives[0].split('.')[-1]
                for mod in alternatives:
                    mod = mod.strip().split('.')

                    try:
                        if len(mod) == 1:
                            module = __import__(mod[0])
                        else:
                            module = getattr(__import__('.'.join(mod[:-1]), \
                                            fromlist=[mod[-1]]), mod[-1])
                        f.func_globals[mod_name] = module
                        break # import only one
                    except ImportError:
                        pass
                else:
                    if forgive: # no break -> no import
                        warnings.warn('Failed to import %s' % alternatives)
                    else:
                        raise ImportError('Failed to import %s' % alternatives)
        return f
    return wrap


class Weave(object):
    """
    Weaves a sequence of iterators, which can be stopped if the same number of 
    results has been consumed from all iterators. 

    Arguments:
    
        * iterators(sequence of iterators)
        
            A sequence of objects supporting the iterator protocol.
            
        * repeats(int) [default: 1]
        
            A positive integer defining the number of results return from an 
            iterator in a stride i.e. before a result from the next iterator is
            returned. 
    """
    def __init__(self, iterators, repeats=1):
        self.iterators = iterators          # sequence of iterators
        self.lenght = len(self.iterators)
        self.i = 0                          # index of current iterators
        self.repeats = repeats     # number of repeats from an iterator (stride)
        self.r = 0                 # current repeat
        self.stopping = False      # if True stop at i==0, r==0
        self.stopped = False

    def __iter__(self):
        # support for the iterator protocol
        return self

    def stop(self):
        """
        If called the Weave will stop at repeats boundaries.
        """
        # stopping is a one-way process
        # log.debug('Weave(%s) told to stop.' % id(self))
        self.stopping = True

    def next(self):
        """
        Returns the next element.
        """
        # need new iterable?
        if self.r == self.repeats:
            self.i = (self.i + 1) % self.lenght
            self.r = 0

        self.r += 1
        if self.stopping and self.i == 0 and self.r == 1:
            self.stopped = True
        if self.i == 0 and self.stopped:
            raise StopIteration
        else:
            iterator = self.iterators[self.i]
            return iterator.next()


class IMapTask(object):
    """
    The ``IMapTask`` is an object-wrapper of ``IMap`` instaces. It is an 
    iterator, which returns results only for a single task. It's next method 
    does not take any arguments. ``IMap.next`` arguments are defined during
    initialization. 
    
    Arguments:

        * iterator(``IMap`` instance)

            ``IMap`` instance to wrap, usually initialization is done by the 
            ``IMap.get_task`` method of the corresponding ``IMap`` instance.

        * task(integer)

            Id of the task from the ``IMap`` instance.

        * timeout
            
            see documentation for: ``IMap.next``.

        * block
            
            see documentation for: ``IMap.next``.
    """
    def __init__(self, iterator, task, timeout, block):
        self.timeout = timeout
        self.block = block
        self.task = task
        self.iterator = iterator

    def __iter__(self):
        return self

    def next(self):
        """
        Returns a result if availble within timeout else raises a TimeoutError.
        See documentation for ``IMap.next``.
        """
        return self.iterator.next(task=self.task, timeout=self.timeout,
                                                    block=self.block)


class PriorityQueue(Queue):
    """
    A priority queue using a heap on a list. This Queue is thread but not 
    process safe.
    """
    def _init(self, maxsize):
        self.maxsize = maxsize
        self.queue = []

    def _put(self, item):
        return heappush(self.queue, item)

    def _get(self):
        return heappop(self.queue)


#EOF
