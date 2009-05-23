"""
:mod:`papy.papy`
================

This module provides classes and functions to construct and run a papy pipeline.
"""
# self-imports
from IMap import IMap
from graph import Graph
from utils import logger
# python imports
from multiprocessing import TimeoutError, cpu_count
from itertools import izip, tee, imap, chain
from types import FunctionType
from inspect import isbuiltin, getsource
from logging import getLogger

class WorkerError(Exception):
    """ Exceptions raised or related to Worker instances.
    """
    pass


class PiperError(Exception):
    """ Exceptions raised or related to Piper instances.
    """
    pass


class DaggerError(Exception):
    """ Exceptions raised or related to Dagger instances.
    """
    pass


class PlumberError(Exception):
    """ Exceptions raised or related to Plumber instances.
    """
    pass


class Dagger(Graph):
    """ The Dagger is a Directed Acyclic Graph.

        Arguments:

          * pipers(sequence) [default: ()]

            A sequence of valid add_piper inputs

          * pipes(sequence) [default: ()]

            A sequence of valid add_pipe inputs
    """

    def __init__(self, pipers =(), pipes =()):
        self.log = getLogger('papy')
        self.log.info('Creating %s from %s and %s' % (repr(self), pipers, pipes))
        self.add_pipers(pipers)
        self.add_pipes(pipes)


    def __repr__(self):
        """ Short representation.
        """
        return 'Dagger(%s)' % id(self)

    def __str__(self):
        """ Long representation.
        """
        return repr(self) + "\n"+\
               "\tPipers:\n" +\
               "\n".join(('\t\t'+repr(p)+' ' for p in self.nodes())) + '\n'\
               "\tPipes:\n" +\
               "\n".join(('\t\t'+repr(p[1])+'>>>'+repr(p[0]) for p in self.edges()))

    def resolve(self, piper, forgive =False):
        """Given a piper or piper id returns the identical piper in the graph.

           Arguments:

             * piper(Piper instance, id(Piper instance))

               Object to find in the graph.

             * forgive(bool) [default =False]

               If forgive is False a DaggerError is raised whenever a piper cannot
               be resolved in the graph. If forgive is True False is returned
        """
        try:
            if piper in self:
                resolved = piper
            else:
                resolved = [p for p in self if id(p) == piper][0]
        except (TypeError, IndexError):
            resolved = False
        if resolved:
            self.log.info('%s resolved a piper from %s' % (repr(self), piper))
        else:
            self.log.info('%s could not resolve a piper from %s' % (repr(self), repr(piper)))
            if not forgive:
                raise DaggerError('%s could not resolve a Piper from %s' % (repr(self), repr(piper)))
            resolved = False
        return resolved

    def connect(self):
        """ Connects pipers in the correct order.
        """
        postorder = self.postorder()
        self.log.info('%s trying to connect in order %s' % (repr(self), repr(postorder)))
        for piper in postorder:
            inbox = self[piper].keys()
            if inbox:
                # omit start-pipers
                piper.connect(self[piper].keys())
        self.log.info('%s succesfuly connected' % repr(self))


    def get_inputs(self):
        start_pipers = [p for p in self if not self.outgoing_edges(p)]
        self.log.info('%s got input pipers %s' % (repr(self), start_pipers))
        return start_pipers

    def get_outputs(self):
        end_pipers = [p for p in self if not self.incoming_edges(p)]
        self.log.info('%s got output pipers %s' % (repr(self), end_pipers))
        return end_pipers

    def add_piper(self, piper, create =True):
        """Adds a piper to the graph (only if the piper is not already in the graph).

           Arguments:

             * piper(Piper instance, Worker instance or id(Piper instance)

               Piper instance or object which will be converted to a piper instance.

             * create(bool) [default: True]

               Should a new piper be created if necessary? If False and piper is not al

        """
        self.log.info('%s trying to add piper %s' % (repr(self), piper))
        piper = (self.resolve(piper, forgive =True) or piper)
        if not isinstance(piper, Piper):
            if create:
                try:
                    piper = Piper(piper)
                except PiperError:
                    self.log.error('%s cannot resolve or create a piper from %s' % (repr(self), repr(piper)))
                    raise DaggerError('%s cannot resolve or create a piper from %s' % (repr(self), repr(piper)))
            else:
                self.log.error('%s cannot resolve a piper from %s' % (repr(self), repr(piper)))
                raise DaggerError('%s cannot resolve a piper from %s' % (repr(self), repr(piper)))
        self.add_node(piper)
        self.log.info('%s added piper %s' % (repr(self), piper))
        return piper

    def del_piper(self, piper, forced =False):
        """Removes a piper from the graph.

           Arguments:

             * piper(Piper instance, Worker instance or id(Piper instance)

               Piper instance or object which will be converted to a piper instance.

             * forced(bool) [default: False]

               If forced is False pipers with down-stream connections will not be removed
               and will raise a DaggerError.
        """
        self.log.info('%s trying to delete piper %s' % (repr(self), repr(piper)))
        try:
            piper = self.resolve(piper, forgive =False)
        except DaggerError:
            self.log.error('%s cannot resolve piper from %s' % (repr(self), repr(piper)))
            raise DaggerError('%s cannot resolve piper from %s' % (repr(self), repr(piper)))
        if self.incoming_edges(piper) and not forced:
            self.log.error('%s piper %s has down-stream pipers (use forced =True to override)' % (repr(self), piper))
            raise DaggerError('%s piper %s has down-stream pipers (use forced =True to override)' % (repr(self), piper))
        self.del_node(piper)
        self.log.info('%s deleted piper %s' % (repr(self), piper))


    def add_pipe(self, pipe):
        """Adds a pipe (A, ..., N) which is an N-tuple tuple of pipers. Adding a pipe
           means to add all the pipers and connect them in the specified order. If a

           Arguments:

             * pipe(sequence)

               N-tuple of Piper instances or objects which can be resolved in the graph
               (see: resolve). The pipers are added in the specified order

           .. note::

              The direction of the edges in the graph is reversed compared to the data
              flow  in a pipe i.e. the target node points to the source node.
        """
        self.log.info('%s adding pipe: %s' % (repr(self), repr(pipe)))
        for i in xrange(len(pipe)-1):
            edge = (pipe[i+1], pipe[i])
            edge = (self.add_piper(edge[0], create =True),\
                    self.add_piper(edge[1], create =True))
            if edge[0] in self.dfs(edge[1], []):
                self.log.error('%s cannot add the %s>>>%s edge (introduces a cycle)' %\
                                (repr(self), edge[0], edge[1]))
                raise DaggerError('%s cannot add the %s>>>%s edge (introduces a cycle)' %\
                                (repr(self), edge[0], edge[1]))
            self.add_edge(edge)
            self.clear_nodes() #dfs
            self.log.info('%s added the %s>>>%s edge' % (repr(self), edge[0], edge[1]))

    def del_pipe(self, pipe, forced =False):
        """Deletes a pipe (A, ..., N) which is an N-tuple of pipers. Deleting a pipe means
           to delete all the connections between pipers and to delete all the pipers.

           Arguments:

             * pipe(sequence)

               N-tuple of Piper instances or objects which can be resolved in the graph
               (see: resolve). The pipers are removed in the reversed order

             * forced(bool) [default: False]

               The forced argument will be given to the del_piper method. If forced is
               False only pipers with no down-stream connections will be deleted

           .. note::

              The direction of the edges in the graph is reversed compared to the data
              flow  in a pipe i.e. the target node points to the source node.
        """
        self.log.info('%s removes pipe%s forced: %s' % (repr(self), repr(pipe), forced))
        pipe = list(reversed(pipe))
        for i in xrange(len(pipe)-1):
            edge = (self.resolve(pipe[i]), self.resolve(pipe[i+1]))
            self.del_edge(edge)
            self.log.info('%s removed the %s>>>%s edge' % (repr(self), edge[0], edge[1]))
            try:
                self.del_piper(edge[0], forced)
                self.del_piper(edge[1], forced)
            except DaggerError:
                pass

    def add_pipers(self, pipers, *args, **kwargs):
        """Adds sequence of pipers in specified oreder.
        """
        for piper in pipers:
            self.add_piper(piper, *args, **kwargs)

    def del_pipers(self, pipers, *args, **kwargs):
        """Deletes sequence of pipers in specified order.
        """
        for piper in pipers:
            self.del_piper(piper, *args, **kwargs)

    def add_pipes(self, pipes, *args, **kwargs):
        """Adds sequecne of pipes in specified order.
        """
        for pipe in pipes:
            self.add_pipe(pipe, *args, **kwargs)

    def del_pipes(self, pipes, *args, **kwargs):
        """Deletes sequence of pipes in specified order.
        """
        for pipe in pipes:
            self.del_pipe(pipe *args, **kwargs)


I_SIG = '%s = IMap(worker_type =%s, worker_num =%s, stride =%s, buffer =%s, ordered =%s, skip =%s, name =%s)\n'
P_SIG = '%s = Piper(%s, parallel =%s, consume =%s, produce =%s, timeout =%s, sort =%s, ornament =%s, debug =%s, name =%s\n'
W_SIG = 'Worker((%s,), %s)'

class Plumber(Dagger):
    """ The Plumber.
    """

    def __init__(self):
        # initialize logging
        logger.start_logger()
        # initialize IMap
        self.imaps = {}
        self.imaps['default'] = IMap()
        Dagger.__init__(self)

    def _code(self):
        """  imports code and runtime calls
        """
        icode, tcode = '', '' # imports, task code
        icall, pcall = '', '' # imap calls, piper calls
        tdone, idone = [], [] # task done, imap done

        for piper in self:
            p = piper
            w = piper.worker
            i = piper.imap
            in_ = i.name if hasattr(i, 'name') else False
            if in_ and in_ not in idone:
                icall += I_SIG % (in_, i.worker_type, i.worker_num, i.stride,\
                                  i.buffer, i.ordered, i.skip, in_)
                idone.append(in_)
            ws =  W_SIG % (",".join([t.__name__ for t in w.task]), w.args)
            pcall += P_SIG % (p.name, ws, in_, p.consume, p.produce, p.timeout,\
                               p.sort.__name__, p.ornament, p.debug, p.name)
            for t in chain(w.task, [p.sort]):
                if t in tdone:
                    continue
                tm, tn = t.__module__, t.__name__
                if tm == '__main__':
                    tcode += getsource(t)
                elif tm != '__builtin__':
                    icode += 'from %s import %s\n' % (tm, tn)
                tdone.append(t)
        return (icode, tcode, icall, pcall)


    def __repr__(self):
        return "Plumber(%s)" % super(Plumber, self).__repr__()

    def __str__(self):
        return super(Plumber, self).__str__()

    def load(self, filename):
        """
        """
        pass

    def save(self, filename):
        """
        """
        pass

    def plunge(self):
        """
        """
        pass


class Piper(object):
    """Creates a new Piper instance.

       arguments:

         * worker(Worker, Piper, sequence):

           Can be a worker or piper instance or a sequence of workers or functions.

         * parallel(False or IMap instance) [default: 0]

           The type of parallelism:
             False - linear using built-in imap
             IMap instance - parallel using a specified IMap instance.

             A custom IMap instance might also be suplied.

         * consume(int) [default: 1]

           The number of input items consumed and sent to the worker as a batch.

         * produce(int) [default: 1]

           The number of repetitions of each calculated result.

         * timeout(int) [default: 1]

           Time to wait till a result is received otherwise a PiperError is *returned*

         * sort(func) [default: None]

           Custom function to sort the randomly ordered list of upstream pipers.

         * ornament(object) [default: None]

           Anything which can be used by sort functions of other pipers

         * debug(bool) [default: False]

           Debug-mode. Raise PiperError on WorkerErrors
    """

    def __init__(self, worker, parallel =False, consume =1, produce =1,\
                 timeout =None, sort =None, ornament =None, debug =False, name =None):
        self.inbox = None
        self.outbox = None
        self.connected = False
        self.finished = False

        self.consume = consume
        self.produce = produce
        self.timeout = timeout
        self.ornament = ornament
        self.debug = debug
        self.name =  (name or 'piper_%s' % id(self))

        self.log = getLogger('papy')
        self.log.info('Creating a new Piper from %s' % worker)

        self.imap = parallel if parallel else imap
        self.sort = sort if sort else cmp

        is_p, is_w, is_f, is_ip, is_iw, is_if = inspect(worker)
        if is_p:
            self.worker = worker.worker
        elif is_w:
            self.worker = worker
        elif is_f or is_if or is_iw:
            self.log.info('Creating new worker from %s' % worker)
            try:
                self.worker = Worker(worker)
                self.log.info('Created a new worker from %s' % worker)
            except Exception, e:
                self.log.error('Could not create a new Worker from %s' % worker)
                raise PiperError('Could not create a new Worker from %s' % worker, e)
        else:
            self.log.error('Do not know how to create a Piper from %s' % repr(worker))
            raise PiperError('Do not know how to create a Piper from %s' % repr(worker))

        self.log.info('Created Piper %s' % self)

    def __iter__(self):
        return self

    def __repr__(self):
        return "=%s(%s)=" % (self.worker, id(self))

    def __hash__(self):
        return self.worker.__hash__()

    def start(self, forced =False):
        """Start the
        """
        if not hasattr(self.imap, '_started'):
            self.log.info('Piper %s does not need to be started' % self)
        elif self.imap._started.isSet():
            self.log.error('Piper %s has already been started' % self)
            raise PiperError('Piper %s has already been started' % self)
        elif self.connected and (len(self.imap._tasks) == 1 or forced):
            self.imap.start()
            self.log.info('Piper %s has been started' % self)
        else:
            self.log.error('Piper %s cannot start. connected: %s, shared: %s' %\
                           (self, self.connected, len(self.imap._tasks)))
            raise PiperError('Piper %s cannot start. connected: %s, shared: %s' %\
                           (self, self.connected, len(self.imap._tasks)))

    def connect(self, inbox):
        """Connects the input(s)[inbox] with the output[outbox, next] via the supplied
           worker instance and IMap instance.
        """
        if hasattr(self.imap, '_started') and self.imap._started.isSet():
            self.log.error('Piper %s is started and cannot connect to %s' % (self, inbox))
            raise PiperError('Piper %s is started and cannot connect to %s' % (self, inbox))
        elif self.connected:
            self.log.error('Piper %s is connected and cannot connect to %s' % (self, inbox))
            raise PiperError('Piper %s is connected and cannot connect to %s' % (self, inbox))
        elif inbox:
            self.log.info('Piper %s connects to %s' % (self, inbox))
            # Make input
            self.inbox  = izip(*[tee(i,1)[0] for i in inbox]) if self.consume == 1 else\
                  consume(izip(*[tee(i,1)[0] for i in inbox]), self.consume)
            # Calculate result
            outbox = self.imap(self.worker, self.inbox) if self.imap is imap else\
                     self.imap(self.worker, self.inbox, timeout =self.timeout)
            # Make output
            self.outbox = outbox if self.produce == 1 else\
                  produce(outbox, self.produce)
            self.connected = True
        return self # this is for __call__

    def stop(self, forced =False):
        """Stops the piper. A piper is started if it's IMap instance is started and linear
           pipers need not to be started or stoped. A piper can be safely stoped if it
           either finished or it does not share the IMap instance.

           Arguments:

             * forced(sequence) [default =False]

               The piper will be forced to stop the IMap instance. A sequence of IMap
               task ids needs to be given e.g.::

                 end_task_ids = [0, 1]       # A list of IMap task ids
                 piper_instance.stop([0,1])

               results in::

                 piper_instance.imap.stop(ends =[0,1])
        """
        if not hasattr(self.imap, '_started'):
            self.log.info('Piper %s does not need to be stoped' % self)
        elif not self.imap._started.isSet():
            self.log.error('Piper %s has not started and cannot be stopped' % self)
        elif self.finished or (len(self.imap._tasks) == 1 or forced):
            self.imap.stop((forced or [0]))
            self.log.info('Piper %s stops (finished: %s)' % (self, self.finished))
        else:
            m = 'Piper %s has not finished is shared and will ' % self +\
                'not be stopped (use forced =end_task_ids)'
            self.log.error(m)
            raise PiperError(m)

    def disconnect(self, forced =False):
        """ Disconnects the outbox from the inbox.
        """

        if not self.connected:
            self.log.error('Piper %s is not connected and cannot be disconnected' % self)
            raise PiperError('Piper %s is not connected and cannot be disconnected' % self)
        elif hasattr(self.imap, '_started') and self.imap._started.isSet():
            self.log.error('Piper %s is started and cannot be disconnected (stop first)' % self)
            raise PiperError('Piper %s is started and cannot be disconnected (stop first)' % self)
        elif len(self.imap._tasks) == 1 or forced:
            # not started but connected either not shared or forced
            self.log.info('Piper %s disconnects from %s' % (self, self.inbox))
            try:
                del self.imap._tasks[self.outbox.task]
            except AttributeError:
                pass
            self.inbox = None
            self.outbox = None
            self.connected = False
        else:
            m = 'Piper %s is connected but is shared and will ' % self +\
                'not be disconnected (use forced =True)'
            self.log.error(m)
            raise PiperError(m)


    def __call__(self, *args, **kwargs):
        """ This is just a convenience mapping to the connect method.
        """
        return self.connect(*args, **kwargs)

    def next(self):
        """ Returns the next result.

            If no result is availble within the specified (at initialization) timeout then
            a PiperError wrapped TimeoutError is returned.

            If the result is a WorkerError it is wrapped in a PiperError and returned or
            raised if debug mode was specified at initialization

            If the result is a PiperError it is propagated
        """
        try:
            next = self.outbox.next()
        except StopIteration, e:
            self.log.info('Piper %s has processed all jobs (finished)' % self)
            self.finished = True
            # We re-raise StopIteration as part of the iterator protocol.
            # And the outbox should do the same.
            raise e
        except (AttributeError, RuntimeError), e:
            # probably self.outbox.next() is self.None.next()
            self.log.error('Piper %s has not yet been started' % self)
            raise PiperError('Piper %s has not yet been started' % self, e)
        except TimeoutError, e:
            self.log.error('Piper %s timed out waited %ss' % (self, self.timeout))
            next = PiperError(e) # we do not raise TimeoutErrors they can be skipped.
        if isinstance(next, WorkerError):
            # return the WorkerError instance returned (not raised) by the
            # worker Process.
            self.log.error('Piper %s generated %s"%s" in func. %s on argument %s' %\
                     (self, type(next[0]), next[0], next[1], next[2]))
            if self.debug:
                # This makes only sense if you are debugging a piper as it will most
                # probably crash papy and python IMap worker processes/threads will hang.
                raise PiperError('Piper %s generated %s"%s" in func. %s on argument %s' %\
                                (self, type(next[0]), next[0], next[1], next[2]))
            next = PiperError(next)
        elif isinstance(next, PiperError):
            # Worker/PiperErrors are wrapped by workers
            self.log.info('Piper %s propagates %s' % (self, next[0]))
        return next


class Worker(object):
    """The Worker is an object which wraps tuples of functions and their arguments and
       and evaluates them when called with a tuple of input data. All exceptions raised
       by the functions are cought, wrapped and returned.

       The Worker can be initialized in a variety of ways:

         * with a tuple of functions and an (optional)
           tuple of tuples of their arguments. e.g.::

             Worker((func1, func2, func3), ((arg1_func1, arg2_func1),(), ()))

         * with another worker instance, which results in their functional equivalence
           e.g.::

             Worker(worker_instance)

         * with multiple worker instances, where the functions and arguments of the
           workers are combined e.g.::

             Worker((worker1, worker2))

           this is equivalent to::

             Worker(worker1.task + worker2.task, worker1.args + worker2.args)

         * with a single function and its arguments in a tuple e.g.::

             Worker(function,(arg1, arg2, arg3))

           which is equivalent to::

             Worker((function,),((arg1, arg2, arg3),))
    """
    def __init__(self, functions, arguments =None):
        is_p, is_w, is_f, is_ip, is_iw, is_if = inspect(functions)
        if is_f:
            self.task = (functions,)
            self.args = ((arguments or ()),)
        elif is_w:
            self.task = functions.task
            self.args = functions.args
        elif is_if:
            self.task = tuple(functions)
            self.args = (arguments or tuple([() for i in self.task]))
        elif is_iw:
            self.task = tuple(chain(*[w.task for w in functions]))
            self.args = tuple(chain(*[w.args for w in functions]))
        else:
            raise TypeError("The Worker expects an iterable of functions or workers " +\
            "got: %s' % functions")
        if len(self.task) != len(self.args):
            raise TypeError("The Worker expects the arguents as ((args1) ... (argsN)) " +\
            "got: %s" % arguments)

    def __repr__(self):
        """Functions within a worker e.g. (f, g, h) are evaluated from left to right
           i.e.: h(g(f(x))) thus their representation f>g>h.
        """
        return ">".join([f.__name__ for f in self.task])

    def __hash__(self):
        """Two workers have the same hash if they are equal, see __eq__.
        """
        return hash((self.task, self.args))

    def __eq__(self, other):
        """Custom worker comparison. Workers are functionally equal if they do the same
           functions and have the same arguments. Two different worker instances can be
           equal if they have been initialized with the same arguments.
        """
        return  (self.task == getattr(other, 'task', None) and
                 self.args == getattr(other, 'args', None))

    def __call__(self, inbox):
        """ Evaluates the function(s) and argument(s) with which the worker has been
            initialized on the given the input data (inbox).

            arguments:

              * inbox(sequence)

                A sequence of items to be evaluated by the function i.e.::

                  f(sequence) is f((data1, data2, ..., data2))

            If an exception is raised by the function the worker returns a WorkerError.
            Typically a raised WorkerError should be wrapped into a PiperError by the
            piper which contains this worker. If any of the data in the inbox is a
            PiperError then the function is not called and the worker propagates a
            PiperError. The originial exception travels along as the first argument of
            the innermost exception.
        """
        outbox = inbox          # we save the input to raise a better exception.
        exceptions = [e for e in inbox if isinstance(e, PiperError)]
        if not exceptions:
            # upstream did not raise exception, running functions
            try:
                for f, a in zip(self.task, self.args):
                    outbox = (f(outbox, *a),)
                outbox = outbox[0]
            except Exception, e:
                # an exception occured in one of the f's do not raise it
                # instead return it
                outbox = WorkerError(e, f, inbox)
        else:
            # if any of the inputs is a PiperError just propagate it.
            outbox = PiperError(*exceptions)
        return outbox


def inspect(piper):
    """ This function determines the instance (Piper, Worker, FunctionType, Iterable).
        It returns a tuple of boolean variables. (is_piper, is_worker, is_function, is_iterable).
    """
    is_piper = isinstance(piper, Piper)
    is_function = isinstance(piper, FunctionType) or isbuiltin(piper)
    is_worker = isinstance(piper, Worker)
    is_iterable = getattr(piper, '__iter__', False) and not (is_piper or is_function or is_worker)
    is_iterable_p = is_iterable and isinstance(piper, Piper)
    is_iterable_f = is_iterable and (isinstance(piper[0], FunctionType) or isbuiltin(piper[0]))
    is_iterable_w = is_iterable and isinstance(piper[0], Worker)
    return (is_piper, is_worker, is_function, is_iterable_p, is_iterable_w, is_iterable_f)


def consume(iterable, n):
    """ This generator takes n-values from the input and yields an n-tuple.
        i.e. n =2 and the singular input is (x,y,y) -> [(x1,x2),(y1,y2),(z1,z2)]
    """
    while True:
        batch = []
        for i in xrange(n):
            batch.append(iterable.next())
        yield zip(*batch)

def produce(iterable, n):
    """ This generator yields n-times each variable in the input.
        i.e. n =3 and input is (1,2) -> [1,1,1,2,2,2]
    """
    while True:
        try:
            result = iterable.next()
            for i in xrange(n):
                yield result
        except Exception, e:
            for i in xrange(n):
                raise e

def imports(modules):
    """
    """
    def wrap(f):
        if modules:
            setattr(f, 'imports', modules)
            for mod, sub in modules:
                module = __import__(mod)
                for submod in sub:
                    f.func_globals[submod] = getattr(module, submod)
                else:
                    f.func_globals[mod] = module
        return f
    return wrap




