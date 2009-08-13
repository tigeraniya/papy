"""
:mod:`papy.papy`
================

This module provides classes and functions to construct and run a *PaPy* 
pipeline.
"""
# self-imports
from IMap import Weave, imports, inject_func
from graph import Graph
from utils import logger
from utils.codefile import *
from utils.defaults import get_defaults
from utils.runtime import get_runtime
# python imports
from multiprocessing import TimeoutError
from itertools import izip, tee, imap, chain, repeat
from threading import Thread, Event
from collections import defaultdict
from types import FunctionType
from inspect import isbuiltin, getsource
from logging import getLogger
from time import time


class WorkerError(Exception):
    """
    Exceptions raised or related to *Worker* instances.
    """
    pass


class PiperError(Exception):
    """
    Exceptions raised or related to *Piper* instances.
    """
    pass


class DaggerError(Exception):
    """
    Exceptions raised or related to *Dagger* instances.
    """
    pass


class PlumberError(Exception):
    """
    Exceptions raised or related to *Plumber* instances.
    """
    pass


class Dagger(Graph):
    """
    The *Dagger* is a directed acyclic graph. It defines the topology of a 
    *PaPy* pipeline/workflow. It is a subclass from *Graph* inverting the 
    direction of edges called within the *Dagger* pipes. Edges can be regarded 
    as dependencies, while pipes as data-flow between *Pipers* or *Nodes* of the 
    *Graph*.

    Arguments:
    
        * pipers(sequence) [default: ``()``]
        
            A sequence of valid ``add_piper`` inputs (see the documentation for 
            the ``add_piper`` method).
        
        * pipes(sequence) [default: ``()``]
        
            A sequence of valid ``add_pipe`` inputs  (see the documentation for 
            the ``add_piper`` method).
    """

    def __init__(self, pipers=(), pipes=(), xtras=None):
        self.log = getLogger('papy')
        self.log.info('Creating %s from %s and %s' % \
                      (repr(self), pipers, pipes))
        self.add_pipers(pipers, xtras)
        self.add_pipes(pipes)


    def __repr__(self):
        """
        Short but unique representation.
        """
        return 'Dagger(%s)' % id(self)

    def __str__(self):
        """
        Long descriptive representation.
        """
        return repr(self) + "\n" + \
               "\tPipers:\n" + \
               "\n".join(('\t\t' + repr(p) + ' ' for p in self.nodes())) + '\n'\
               "\tPipes:\n" + \
               "\n".join(('\t\t' + repr(p[1]) + '>>>' + \
                          repr(p[0]) for p in self.edges()))

    @staticmethod
    def _cmp(x, y):
        """
        A compare function like ``cmp``, which compares *Pipers* by ornament. To 
        be used when sorting upstream *Pipers*.
        """
        return cmp(x.ornament, y.ornament)

    def resolve(self, piper, forgive=False):
        """
        Given a *Piper* or the ``id`` of the *Piper*. Returns this *Piper* if it 
        can be resolved else raises a *DaggerError* or returns ``False`` 
        depending on the forgive argument. 
        
        Arguments:
    
            * piper(*Piper* instance, id(*Piper* instance))
            
                *Piper* instance or its id to be found in the *Dagger*. 
            
            * forgive(bool) [default =``False``]
            
                If forgive is ``False`` a ``DaggerError`` is raised whenever a 
                *Piper*  cannot be resolved in the *Dagger*. If forgive is 
                ``True``: ``False`` is returned.
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
            self.log.info('%s could not resolve a piper from %s' % \
                          (repr(self), repr(piper)))
            if not forgive:
                raise DaggerError('%s could not resolve a Piper from %s' % \
                                  (repr(self), repr(piper)))
            resolved = False
        return resolved

    def connect(self):
        """
        Given the pipeline topology connects *Pipers* in the order input -> 
        output. See ``Piper.connect``.
        """
        postorder = self.postorder()
        self.log.info('%s trying to connect in the order %s' % \
                      (repr(self), repr(postorder)))
        for piper in postorder:
            if not piper.connected and self[piper].keys(): # skip input pipers
                piper.connect(self[piper].keys())   # what if []?
        self.log.info('%s succesfuly connected' % repr(self))

    def connect_inputs(self, datas):
        """
        Connects input *Pipers* to input data in the correct order determined,
        by the ``Piper.ornament`` attribute and the ``Dagger._cmp`` function.
        
        .. note::
        
            It is assumed that the input data is in the form of an iterator and
            that all inputs have the same number of input items. A pipeline will
            deadlock otherwise. 
        
        Arguments:
        
            * datas (sequence of iterators)
            
                Ordered sequence of inputs for all input *Pipers*.
        """
        start_pipers = self.get_inputs()
        start_pipers.sort(self._cmp)
        for piper, data in izip(start_pipers, datas):
            piper.connect([data])

    def disconnect(self):
        """
        Given the pipeline topology disconnects *Pipers* in the order output -> 
        input. See ``Piper.connect``.
        """
        postorder = self.postorder()
        self.log.info('%s trying to disconnect in the order %s' % \
                      (repr(self), repr(postorder)))
        for piper in postorder:
            if piper.connected and self[piper].keys(): # skip input pipers
                piper.disconnect()
        self.log.info('%s succesfuly disconnected' % repr(self))

    def start(self):
        """
        Given the pipeline topology starts *Pipers* in the order input -> 
        output. See ``Piper.start``. The forced =`True` argument is passed to 
        the ``Piper.start`` method, allowing *Pipers* to share *IMaps*.
        """
        postorder = self.postorder()
        for piper in postorder:
            piper.start(forced=True)

    def get_inputs(self):
        """
        Returns *Pipers* which are inputs to the pipeline i.e. have no 
        incoming pipes (outgoing dependency edges). 
        """
        start_p = [p for p in self.postorder() if not self.outgoing_edges(p)]
        self.log.info('%s got input pipers %s' % (repr(self), start_p))
        return start_p

    def get_outputs(self):
        """
        Returns *Pipers* which are outputs to the pipeline i.e. have no 
        outgoing pipes (incoming dependency edges). 
        """
        end_p = [p for p in self.postorder() if not self.incoming_edges(p)]
        self.log.info('%s got output pipers %s' % (repr(self), end_p))
        return end_p

    def add_piper(self, piper, xtra=None, create=True):
        """
        Adds a *Piper* to the *Dagger*, only if the *Piper* is not already in 
        a *Node*. Optionally creates a new *Piper* if the piper argument is 
        valid for the *Piper* constructor. Returns a tuple: (new_piper_created,
        piper_instance) indicating whether a new *Piper* has been created
        and the instance of the added *Piper*.
        
        Arguments:
        
            * piper(*Piper* instance, *Worker* instance or id(*Piper* instance))
            
                *Piper* instance or object which will be converted to a *Piper* 
                instance.
            
            * create(bool) [default: ``True``]
            
                Should a new *Piper* be created if the piper cannot be resolved 
                in the *Dagger*?
            
            * xtra(dict) [default: ``None``]
            
                Dictionary of *Graph* *Node* properties.
        """
        self.log.info('%s trying to add piper %s' % (repr(self), piper))
        piper = (self.resolve(piper, forgive=True) or piper)
        if not isinstance(piper, Piper):
            if create:
                try:
                    piper = Piper(piper)
                except PiperError:
                    self.log.error('%s cannot resolve or create a piper from %s' % \
                                   (repr(self), repr(piper)))
                    raise DaggerError('%s cannot resolve or create a piper from %s' % \
                                      (repr(self), repr(piper)))
            else:
                self.log.error('%s cannot resolve a piper from %s' % \
                               (repr(self), repr(piper)))
                raise DaggerError('%s cannot resolve a piper from %s' % \
                                  (repr(self), repr(piper)))
        new_piper_created = self.add_node(piper, xtra)
        if new_piper_created:
            self.log.info('%s added piper %s' % (repr(self), piper))
        return (new_piper_created, piper)

    def del_piper(self, piper, forced=False):
        """
        Removes a *Piper* from the *Dagger*.

        Arguments:
        
            * piper(*Piper* instance, *Worker* instance or id(*Piper* instance))
            
                  *Piper* instance or object from which a *Piper* instance can 
                  be constructed.
            
            * forced(bool) [default: ``False``]
            
                  If forced =``False *Pipers* with outgoing pipes (incoming 
                  edges) will not be removed and will raise a ``DaggerError``.
        """
        self.log.info('%s trying to delete piper %s' % \
                      (repr(self), repr(piper)))
        try:
            piper = self.resolve(piper, forgive=False)
        except DaggerError:
            self.log.error('%s cannot resolve piper from %s' % \
                           (repr(self), repr(piper)))
            raise DaggerError('%s cannot resolve piper from %s' % \
                              (repr(self), repr(piper)))
        if self.incoming_edges(piper) and not forced:
            self.log.error('%s piper %s has down-stream pipers (use forced =True to override)' % \
                           (repr(self), piper))
            raise DaggerError('%s piper %s has down-stream pipers (use forced =True to override)' % \
                              (repr(self), piper))
        self.del_node(piper)
        self.log.info('%s deleted piper %s' % (repr(self), piper))


    def add_pipe(self, pipe):
        """
        Adds a pipe (A, ..., N) which is an N-tuple tuple of *Pipers*. Adding a 
        pipe means to add all the *Pipers* and connect them in the specified 
        left to right order.

        Arguments:
        
            * pipe(sequence)
            
                  N-tuple of *Piper* instances or objects which are valid 
                  ``add_piper`` arguments. See: ``Dagger.add_piper`` and 
                  ``Dagger.resolve``.
        
        .. note::
        
            The direction of the edges in the graph is reversed compared to the
            left to right data-flow in a pipe.
        """
        #TODO: Check if consume/spawn/produce is right!
        self.log.info('%s adding pipe: %s' % (repr(self), repr(pipe)))
        for i in xrange(len(pipe) - 1):
            edge = (pipe[i + 1], pipe[i])
            edge = (self.add_piper(edge[0], create=True)[1], \
                    self.add_piper(edge[1], create=True)[1])
            if edge[0] in self.dfs(edge[1], []):
                self.log.error('%s cannot add the %s>>>%s edge (introduces a cycle)' % \
                                (repr(self), edge[0], edge[1]))
                raise DaggerError('%s cannot add the %s>>>%s edge (introduces a cycle)' % \
                                (repr(self), edge[0], edge[1]))
            self.add_edge(edge)
            self.clear_nodes() #dfs
            self.log.info('%s added the %s>>>%s edge' % \
                          (repr(self), edge[0], edge[1]))

    def del_pipe(self, pipe, forced=False):
        """
        Deletes a pipe (A, ..., N) which is an N-tuple of pipers. Deleting a 
        pipe means to delete all the connections between pipers and to delete
        all the *Pipers*. If forced =``False`` only *Pipers* which are not 
        needed anymore (i.e. have not downstream *Pipers*) are deleted.

        Arguments:

            * pipe(sequence)

                N-tuple of *Piper* instances or objects which can be resolved in 
                the *Dagger* (see: ``Dagger.resolve``). The *Pipers* are removed
                from right to left.
                
            * forced(bool) [default: ``False``]

               The forced argument will be forwarded to the ``Dagger.del_piper``
               method. If forced is ``False`` only *Pipers* with no outgoing 
               pipes will be deleted.

        .. note::

            The direction of the edges in the *Graph* is reversed compared to 
            the  left to right data-flow in a pipe.
        """
        self.log.info('%s removes pipe%s forced: %s' % \
                      (repr(self), repr(pipe), forced))
        pipe = list(reversed(pipe))
        for i in xrange(len(pipe) - 1):
            edge = (self.resolve(pipe[i]), self.resolve(pipe[i + 1]))
            self.del_edge(edge)
            self.log.info('%s removed the %s>>>%s edge' % \
                          (repr(self), edge[0], edge[1]))
            try:
                self.del_piper(edge[0], forced)
                self.del_piper(edge[1], forced)
            except DaggerError:
                pass

    def add_pipers(self, pipers, *args, **kwargs):
        """
        Adds a sequence of *Pipers* to the *Dagger* in specified order. Takes 
        optional arguments for ``Dagger.add_piper``.
        
        Arguments:
        
            * pipers (sequence of valid ``add_piper`` arguments)
            
                Sequence of *Pipers* or valid ``Dagger.add_piper`` arguments 
                to be added to the *Dagger* in the left to right order of the 
                sequence.
        """
        for piper in pipers:
            self.add_piper(piper, *args, **kwargs)

    def del_pipers(self, pipers, *args, **kwargs):
        """
        Deletes a sequence of *Pipers* from the *Dagger* in reverse of the 
        specified order. Takes optional arguments for ``Dagger.del_piper``.
        
        Arguments:
        
            * pipes (sequence of valid ``del_pipe`` arguments)
            
                Sequence of *Pipers* or valid ``Dagger.del_piper`` arguments 
                to be removed from the *Dagger* in the right to left order of 
                the sequence.
        """
        pipers.reverse()
        for piper in pipers:
            self.del_piper(piper, *args, **kwargs)

    def add_pipes(self, pipes, *args, **kwargs):
        """
        Adds a sequence of pipes to the *Dagger* in the specified order. 
        Takes optional arguments for ``Dagger.add_pipe``.
        
        Arguments:
        
            * pipes (sequence of valid ``add_pipe`` arguments)
            
                Sequence of pipes or valid ``Dagger.add_pipe`` arguments to be 
                added to the *Dagger* in the left to right order of the 
                sequence.
        """
        for pipe in pipes:
            self.add_pipe(pipe, *args, **kwargs)

    def del_pipes(self, pipes, *args, **kwargs):
        """
        Deletes a sequence of pipes from the *Dagger* in the specified order. 
        Takes optional arguments for ``Dagger.del_pipe``.
        
        Arguments:
        
            * pipes (sequence of valid ``del_pipe`` arguments)
            
                Sequence of pipes or valid ``Dagger.del_pipe`` arguments to be 
                removed from the *Dagger* in left to right order of the 
                sequence.
        """
        for pipe in pipes:
            self.del_pipe(pipe * args, **kwargs)


class Plumber(Dagger):
    """
    The *Plumber* is a subclass of *Dagger* and *Graph* with added run-time 
    methods and a high-level interface for working with *PaPy* pipelines. 
    
    Arguments:
    
        * dagger(*Dagger* instance) [default: ``None``]
        
            An optional *Dagger* instance.
    """

    def _finish(self, isstopped):
        """
        (internal) Executes when last output piper raises ``StopIteration``.
        """
        self.stats['run_time'] = time() - self.stats['start_time']
        self.log.info('%s finished, stopped: %s.' % \
        (repr(self), isstopped))
        self._is_finished.set()

    def _track(self, frame_finished):
        """
        (internal) Executes when last output *Piper* returns something.
        """
        # this should be fixed to monitor not only the last!
        if frame_finished:
            self.stats['last_frame'] += 1
            self.log.info('%s finished tasklet %s' % \
            (repr(self), self.stats['last_frame']))

    @staticmethod
    def _plunge(tasks, is_stopping, track, finish):
        """
        (internal) Calls the next method of weaved tasks until they are finished
        or The *Plumber* instance is stopped see ``Dagger.chinkup``.
        """
        # If no result received either not started or start & stop
        # could have been called before the plunger thread
        while True:
            if is_stopping():
                tasks.stop()
            try:
                tasks.next()
                frame_finished = (tasks.i == (tasks.lenght - 1))
                track(frame_finished)
            except StopIteration:
                finish(is_stopping())
                break

    def __init__(self, logger_options={}, **kwargs):
        self._is_stopping = Event()
        self._is_finished = Event()

        # Plumber statistics
        self.stats = {}
        self.stats['last_frame'] = -1
        self.stats['start_time'] = None
        self.stats['run_time'] = None

        logger.start_logger(**logger_options)
        self.log = getLogger('papy')

        # init
        #TODO: check if this works with and the stats attributes are correctly
        # set for a predefined dagger.
        Dagger.__init__(self, **kwargs)

    def _code(self):
        """
        (internal) Generates imports, code and runtime calls.
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
                icall += I_SIG % (in_, i.worker_type, i.worker_num, i.stride, \
                                  i.buffer, i.ordered, i.skip, in_)
                idone.append(in_)
            ws = W_SIG % (",".join([t.__name__ for t in w.task]), w.args, w.kwargs)
            cmp_ = p.cmp__name__ if p.cmp else None
            pcall += P_SIG % (p.name, ws, in_, p.consume, p.produce, p.spawn, \
                              p.produce_from_sequence, p.timeout, cmp_, \
                              p.ornament, p.debug, p.name, p.track)
            for t in chain(w.task, [p.cmp]):
                if (t in tdone) or not t:
                    continue
                tm, tn = t.__module__, t.__name__
                if (tm == '__builtin__') or hasattr(p, tn):
                    continue
                if tm == '__main__':
                    tcode += getsource(t)
                else:
                    icode += 'from %s import %s\n' % (tm, tn)
                tdone.append(t)

        pipers = [p.name for p in self]
        pipers = '[%s]' % ", ".join(pipers)
        pipes = [L_SIG % (d.name, s.name) for s, d in self.edges()]
        pipes = '[%s]' % ", ".join(pipes)                           # pipes
        xtras = [str(self[p].xtra) for p in self]
        xtras = '[%s]' % ",".join(xtras)                            # node xtra
        return (icode, tcode, icall, pcall, pipers, xtras, pipes)

    def __repr__(self):
        """
        Short but unique representation.
        """
        return "Plumber(%s)" % super(Plumber, self).__repr__()

    def __str__(self):
        """
        Long descriptive representation.
        """
        return super(Plumber, self).__str__()

    def load(self, filename):
        """
        Load pipeline from source file.
        
        Arguments:
        
            * filename(path)
            
                Location of the pipeline source code.
        """
        namespace = {}
        execfile(filename, namespace)
        pipers, xtras, pipes = namespace['pipeline']()
        self.add_pipers(pipers, xtras)
        self.add_pipes(pipes)

    def save(self, filename):
        """
        Save pipeline as source file.
        
        Arguments:
        
            * filename(path)
            
                Path to save pipeline source code.
        """
        handle = open(filename, 'wb')
        handle.write(P_LAY % self._code())
        handle.close()

    def plunge(self, data, tasks=None, stride=1):
        """
        Executes the pipeline by connecting the input *Pipers* of the pipeline 
        to the input data, connecting the pipeline, starting the *IMaps* and 
        pulling results from output *Pipers*. A stride number of results is 
        pulled from a *Piper* is requested before the next next output *Piper*.
        *Pipers* with the ``track`` attribute set ``True`` will have their 
        results stored within ``Dagger.stats['pipers_tracked']``.
        
        Arguments:

            .. note::
            
                Warning! If you change the defaults of these arguments you 
                should better know what you are doing. The order of the tasks
                in the sequence and the stride specified here should be 
                compatible with the buffer and stride attributes of the *IMap*
                instances used by the *Pipers* and the pipelines topology. 
                Please refer to the manual.

            * tasks(sequence of *Piper* instances) [default: ``None``]

                If no sequence is given all output *Pipers* are plunged in 
                correct topological order. If a sequence is given the *Pipers*
                are plunged in the left to right order.

            * stride(int) [default: 1]

                By default take only one result from each output *Piper*. As a 
                general rule the stride cannot be bigger then the stride of the
                *IMap* instances. The default will change in future versions.
        """
        #TODO: change the default stride for Plumber.plunge
        # connect pipers
        self.connect_inputs(data)
        self.connect()

        # collect results for tracked tasks
        self.stats['pipers_tracked'] = {}
        for ppr in self.postorder():
            if hasattr(ppr.imap, '_tasks_tracked') and ppr.track:
                self.stats['pipers_tracked'][ppr.name] = \
                [ppr.imap._tasks_tracked[t.task] for t in ppr.imap_tasks]

        # start IMaps
        self.stats['start_time'] = time()
        self.start()    # forced =True

        # remove non-block results for end tasks
        tasks = (tasks or self.get_outputs())
        wtasks = Weave(tasks, repeats=stride)
        self._plunger = Thread(target=self._plunge, args=(wtasks, \
                        self._is_stopping.isSet, self._track, self._finish))
        self._plunger.deamon = True
        self._plunger.start()


    def chinkup(self):
        """
        Cleanly stops a running pipeline. Blocks until stopped.
        """
        self._is_stopping.set()
        self._plunger.join()


class Piper(object):
    """
    Creates a new Piper instance.
    
    .. note::
    
        The (produce * spawn) of the upstream *Piper* has to equal the (consume 
        * spawn) of the downstream *Piper*. for each pair of *pipers* connected
        by a pipe. This will be in future enforced by the *Dagger*.
        
    Arguments:

        * worker(*Worker* instance, *Piper* instance or sequence of 
          worker-functions or *Worker* instances)

            A *Piper* can be created from a *Worker* instance another *Piper* 
            instance or a sequence of worker-functions or *Worker* instances
            in every case a new instance is created.

        * parallel(``False`` or *IMap* instance) [default: ``False``]
        
            If parallel =``False`` *Piper* will not evaluate the *Worker* in 
            parallel but use the "manager" process and the ``itertools.imap`` 
            function. Otherwise the specified *IMap* instance will be used. 

        * consume(int) [default: 1]

            The number of input items consumed from *all* directly connected 
            upstream *Pipers* per one *Worker* evaluation. Results will be 
            passed to the worker-function as a sequence of individual results.


        * produce(int) [default: 1]

            The number of results to generate for each *Worker* evaluation 
            result. Results will be either repetitions of the single *Worker* 
            return value or will be elements of the returned sequence if 
            produce_from_sequence =``True``. 
           
        * produce_from_sequence(bool) [default: ``False``]
    
            If ``True`` and produce > 1 the results are produced from the 
            sequence returned by the *Worker*. If ``False`` the result returned 
            by the *Worker* is repeated. If produce =1 this option is ignored.
        
        * spawn(int) [default: 1]
        
            The number of times this *Piper* is implicitly added to the pipeline
            to consume the specified number of results.
            
        * timeout(int) [default: ``None``]

            Time to wait till a result is available. Otherwise a ``PiperError``
            is **returned** not raised.

        * cmp(func) [default: Piper._cmp]

            Compare function to sort the randomly ordered list of upstream
            *Pipers*. By convention using the ornament argument/attribute. By 
            default the ``Piper._cmp`` method is used for sorting.

        * ornament(object) [default: self]

            Anything which can be used by compare functions of downstream
            *Pipers*. By default the ornament is the same as the instance 
            object, i.e. the default behavior is equivalent to.::
          
               cmp(piper_instance1, piper_instance2)

        * debug(bool) [default: False]

            Verbose debugging mode. Raises a ``PiperError`` on ``WorkerErrors``.

            .. warning:: 
             
                this will most likely hang the Python interpreter after the 
                error occurs. Use during development only!
    """
    @staticmethod
    def _cmp(x, y):
        """
        Compares pipers by ornament.
        """
        return cmp(x.ornament, y.ornament)

    def __init__(self, worker, parallel=False, consume=1, produce=1, \
                 spawn=1, produce_from_sequence=False, timeout=None, cmp=None,
                 ornament=None, debug=False, name=None, track=False):
        self.inbox = None
        self.outbox = None
        self.connected = False
        self.finished = False
        self.imap_tasks = []

        self.consume = consume
        self.spawn = spawn
        self.produce = produce
        self.produce_from_sequence = produce_from_sequence
        self.timeout = timeout
        self.debug = debug
        self.track = track

        self.log = getLogger('papy')
        self.log.info('Creating a new Piper from %s' % worker)

        self.imap = parallel if parallel else imap # this is itetools.imap

        self.cmp = cmp if cmp else None
        self.ornament = ornament if ornament else None

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
            except Exception, excp:
                self.log.error('Could not create a new Worker from %s' % \
                                worker)
                raise PiperError('Could not create a new Worker from %s' % \
                                 worker, excp)
        else:
            self.log.error('Do not know how to create a Piper from %s' % \
                           repr(worker))
            raise PiperError('Do not know how to create a Piper from %s' % \
                             repr(worker))

        # initially return self by __iter__
        self._iter = self
        self.name = name or "piper_%s" % id(self)
        self.log.info('Created Piper %s' % self)

    def __iter__(self):
        """
        (internal) returns copied ``Piper._iter``, which should be overwritten
        after each ``itertools.tee``.
        """
        return self._iter

    def __repr__(self):
        return "%s(%s)" % (self.name, repr(self.worker))

    def start(self, forced=False):
        """
        Makes the *Piper* ready to return results. This involves starting the 
        the provided *IMap* instance. If multiple *Pipers* share an *IMap* 
        instance the order in which the *Pipers* are started is important. The
        valid order is upstream before downstream. The forced argument has to be
        ``True`` if this *Piper* shares the *IMap* instance.
        
        Arguments:
        
            * forced(bool) [default =``False``]
            
                Starts the *IMap* instance if it is shared by multiple *Pipers* 
                instead of raising a ``PiperError``.
        """
        if not hasattr(self.imap, '_started'):
            self.log.info('Piper %s does not need to be started' % self)
        elif self.imap._started.isSet():
            self.log.info('Piper %s has already been started' % self)
        elif self.connected and (len(self.imap._tasks) == 1 or forced):
            self.imap.start()
            self.log.info('Piper %s has been started' % self)
        else:
            self.log.error('Piper %s cannot start. connected: %s, shared: %s' % \
                           (self, self.connected, len(self.imap._tasks)))
            raise PiperError('Piper %s cannot start. connected: %s, shared: %s' % \
                             (self, self.connected, len(self.imap._tasks)))

    def connect(self, inbox):
        """
        Connects the *Piper* to its upstream *Pipers*. Upstream *Pipers* should
        be passed as a sequence. This connects the ``Piper.inbox`` with the 
        ``Piper.outbox`` respecting the consume, spawn and produce arguments. 
        """
        if hasattr(self.imap, '_started') and self.imap._started.isSet():
            self.log.error('Piper %s is started and cannot connect to %s' % \
                           (self, inbox))
            raise PiperError('Piper %s is started and cannot connect to %s' % \
                             (self, inbox))
        elif self.connected:
            self.log.error('Piper %s is connected and cannot connect to %s' % \
                           (self, inbox))
            raise PiperError('Piper %s is connected and cannot connect to %s' % \
                             (self, inbox))
        else:
            # sort input
            inbox.sort((self.cmp or self._cmp))
            self.log.info('Piper %s connects to %s' % (self, inbox))

            # Make input
            stride = self.imap.stride if hasattr(self.imap, 'stride') else 1

            # copy input iterators
            teed = []
            for piper in inbox:
                if hasattr(piper, '_iter'):
                    piper._iter, piper = tee(piper, 2)
                teed.append(piper)

            # set how much to consume from input iterators 
            self.inbox = izip(*teed) if self.consume == 1 else\
                  Consume(izip(*teed), n=self.consume, \
                  stride=stride)

            # set how much to 
            for i in xrange(self.spawn):
                self.imap_tasks.append(\
                    self.imap(self.worker, self.inbox) \
                        if self.imap is imap else \
                    self.imap(self.worker, self.inbox, timeout=self.timeout, \
                              track=self.track))
            # chain the results together.
            outbox = Chain(self.imap_tasks, stride=stride)
            # Make output
            prd = ProduceFromSequence if self.produce_from_sequence else Produce
            self.outbox = outbox if self.produce == 1 else\
                  prd(outbox, n=self.produce, stride=stride)
            self.connected = True

        return self # this is for __call__

    def stop(self, forced=False):
        """
        Tries to cleanly stop the *Piper*. A *Piper* is "started" if it's 
        *IMap* instance is "started". Non-parallel *Pipers* need not to be 
        started or stopped. A *Piper* can be safely stopped if it either 
        finished or it does not share the *IMap* instance. Else a the 
        forced =`True` has to be specified. This argument is passed to the 
        ``IMap.stop`` method. See ``IMap.stop`` from the ``IMap`` module.

        Arguments:

            * forced(sequence) [default =False]

                The *Piper* will be forced to stop the *IMap* instance. A 
                sequence of *IMap* task ids needs to be given e.g.::

                    end_task_ids = [0, 1]    # A list of IMap task ids
                    piper_instance.stop(end_task_ids)

                results in::

                    IMap_instance.stop([0,1])
        """

        if not hasattr(self.imap, '_started'):
            self.log.info('Piper %s does not need to be stoped' % self)
        elif not self.imap._started.isSet():
            self.log.error('Piper %s has not started and cannot be stopped' % \
                           self)
        elif self.finished or (len(self.imap._tasks) == 1 or forced):
            self.imap.stop((forced or [0]))
            self.log.info('Piper %s stops (finished: %s)' % \
                          (self, self.finished))
        else:
            m = 'Piper %s has not finished is shared and will ' % self + \
                'not be stopped (use forced =end_task_ids)'
            self.log.error(m)
            raise PiperError(m)

    def disconnect(self, forced=False):
        """
        Disconnects the *Piper* from its upstream *Pipers* or input data. If the
        *Piper* 
        
        Arguments:
        
            * forced(bool) [default: ``False``]
            
                If forced is ``True`` tries to forcefully remove all tasks 
                (including the spawned ones) from the *IMap* instance 
        """

        if not self.connected:
            self.log.error('Piper %s is not connected and cannot be disconnected' % self)
            raise PiperError('Piper %s is not connected and cannot be disconnected' % self)
        elif hasattr(self.imap, '_started') and self.imap._started.isSet():
            self.log.error('Piper %s is started and cannot be disconnected (stop first)' % self)
            raise PiperError('Piper %s is started and cannot be disconnected (stop first)' % self)
        #TODO: what if self.imap._tasks does not exist?
        elif len(self.imap._tasks) == 1 or forced:
            # not started but connected either not shared or forced
            self.log.info('Piper %s disconnects from %s' % (self, self.inbox))
            try:
                #TODO: figure out if taks removal from imap._tasks should be 
                #done in reverse.
                for imap_task in self.imap_tasks:
                    del self.imap._tasks[imap_task.task]
            except AttributeError:
                # this handle the case when using itertools.imap
                pass
            self.imap_tasks = []
            self.inbox = None
            self.outbox = None
            self.connected = False
        else:
            mess = 'Piper %s is connected but is shared and will ' % self + \
                'not be disconnected (use forced =True)'
            self.log.error(mess)
            raise PiperError(mess)


    def __call__(self, *args, **kwargs):
        """ 
        This is just a convenience mapping to the ``Worker.connect`` method.
        """
        return self.connect(*args, **kwargs)

    def next(self):
        """
        Returns the next result. If no result is availble within the specified 
        (at initialization) timeout then a ``PiperError`` wrapped TimeoutError 
        is returned.

        If the result is a ``WorkerError`` it is wrapped in a ``PiperError`` and 
        returned or raised if debug mode was specified at initialization. If 
        the result is a ``PiperError`` it is propagated.
        """
        try:
            next = self.outbox.next()
        except StopIteration, excp:
            self.log.info('Piper %s has processed all jobs (finished)' % self)
            self.finished = True
            # We re-raise StopIteration as part of the iterator protocol.
            # And the outbox should do the same.
            raise excp
        except (AttributeError, RuntimeError), excp:
            # probably self.outbox.next() is self.None.next()
            self.log.error('Piper %s has not yet been started' % self)
            raise PiperError('Piper %s has not yet been started' % self, excp)
        except TimeoutError, excp:
            self.log.error('Piper %s timed out waited %ss' % \
                           (self, self.timeout))
            next = PiperError(excp)
            # we do not raise TimeoutErrors so they can be skipped.
        if isinstance(next, WorkerError):
            # return the WorkerError instance returned (not raised) by the
            # worker Process.
            self.log.error('Piper %s generated %s"%s" in func. %s on argument %s' % \
                     (self, type(next[0]), next[0], next[1], next[2]))
            if self.debug:
                # This makes only sense if you are debugging a piper as it will 
                # most probably crash papy and python IMap worker processes 
                # threads will hang.
                raise PiperError('Piper %s generated %s"%s" in func %s on argument %s' % \
                            (self, type(next[0]), next[0], next[1], next[2]))
            next = PiperError(next)
        elif isinstance(next, PiperError):
            # Worker/PiperErrors are wrapped by workers
            self.log.info('Piper %s propagates %s' % (self, next[0]))
        return next


class Worker(object):
    """
    The *Worker* is an object which composes sequences of functions. When called
    the functions are evaluated from left to right. The function on the right 
    will receive the return value from the function on the left. Optionally 
    takes sequences of positional and keyworded arguments for none or
    all of the composed functions. Positional arguments should be given in a 
    tuple. Each element of this tuple should be a tuple of positional arguments
    for the corresponding function. If a function does not take positional 
    arguments its corresponding element in the arguments tuple should be an 
    empty tuple i.e. ``()``. Keyworded  arguments should be given in a tuple. 
    Each  element of this tuple should be a dictionary of arguments for the 
    corresponding function. If a function does not take any keyworded arguments
    its corresponding element in the keyworded arguments tuple should be an 
    empty dictionary i.e. ``{}``. If none of the functions takes arguments of a 
    given type the positional and/or keyworded arguments tuple can be omitted.
    
    All exceptions raised by the worker-functions are caught, wrapped and 
    returned *not* raised. If the *Worker* is called with a sequence which 
    contains an exception no worker-function is evaluated and the exception is
    wrapped and returned.

    The *Worker* can be initialized in a variety of ways:
    
    * with a sequence of functions and a optional sequences of positional and
      keyworded arguments e.g.::
        
        Worker((func1,         func2,    func3), 
              ((arg11, arg21), (arg21,), ()),
              ({},             {},       {'arg31':arg31}))
        
    * with another *Worker* instance, which results in their functional 
      equivalence e.g.::
        
        Worker(worker_instance)
        
    * With multiple *Worker* instances, where the functions and arguments of the
      *Workers* are combined e.g.::
        
        Worker((worker1, worker2))
        
      this is equivalent to::
        
        Worker(worker1.task + worker2.task, \
               worker1.args + worker2.args, \
               worker1.kwargs + worker2.kwargs)
        
    * with a single function and its arguments in a tuple e.g.::
        
        Worker(function,(arg1, arg2, arg3))
        
      which is equivalent to::
        
        Worker((function,),((arg1, arg2, arg3),))
    """
    def __init__(self, functions, arguments=None, kwargs=None, name=None):
        is_p, is_w, is_f, is_ip, is_iw, is_if = inspect(functions)
        if is_f:
            self.task = (functions,)
            if arguments is not None:
                self.args = (arguments,)
            else:
                self.args = ((),)
            if kwargs is not None:
                self.kwargs = (kwargs,)
            else:
                self.kwargs = ({},)
        elif is_w: # copy from other
            self.task = functions.task
            self.args = functions.args
            self.kwargs = functions.kwargs
        elif is_if:
            self.task = tuple(functions)
            if arguments is not None:
                self.args = arguments
            else:
                self.args = tuple([() for i in self.task])
            if kwargs is not None:
                self.kwargs = kwargs
            else:
                self.kwargs = tuple([{} for i in self.task])
        elif is_iw:
            self.task = tuple(chain(*[w.task for w in functions]))
            self.args = tuple(chain(*[w.args for w in functions]))
            self.kwargs = tuple(chain(*[w.kwargs for w in functions]))
        else:
            # e.g. is piper
            raise TypeError("The Worker expects an iterable of functions or" + \
                            " workers got: %s" % functions)
        if len(self.task) != len(self.args) or len(self.task) != len(self.args):
            raise TypeError("The Worker expects the arguents as ((args1) " + \
                            "... argsN)) and keyword arguments as " + \
                            "({kwargs}, ... ,{kwargs.}) got: %s" % \
                            repr(arguments))
        # for representation
        self.__name__ = ">".join([f.__name__ for f in self.task])
        # for identification
        self.name = "%s_%s" % (self.__name__, id(self))

    def __repr__(self):
        """
        Functions within a worker e.g. (f, g, h) are evaluated from left to 
        right i.e.: h(g(f(x))) thus their representation f>g>h.
        """
        return "%s(%s)" % (self.name, self.__name__)

    def __hash__(self):
        """
        *Worker* instances are not hashable.
        """
        raise TypeError('Worker instances are not hashable')

    def __eq__(self, other):
        """
        Custom *Worker* equality comparison. *Workers* are functionally 
        equivalent if they evaluate the same worker-functions, in the same order
        and have the same positional and keyworded arguments. Two different 
        *Worker* instances (objects with different ids) can be equivalent if 
        their functions have been initialized with the same arguments.
        """
        return  (self.task == getattr(other, 'task', None) and
                 self.args == getattr(other, 'args', None) and
                 self.kwargs == getattr(other, 'kwargs', None))

    def _inject(self, conn):
        """
        (internal) Inject/replace all functions into a rpyc connection object.
        """
        # provide PAPY_DEFAULTS remotely
        # provide PAPY_RUNTIME remotely
        if not 'PAPY_INJECTED' in conn.namespace:
            inject_func(get_defaults, conn)
            inject_func(get_runtime, conn)
            conn.execute('PAPY_DEFAULTS = get_defaults()')
            conn.execute('PAPY_RUNTIME = get_runtime()')
            conn.execute('PAPY_INJECTED = True')
        # inject all functions
        for func in self.task:
            inject_func(func, conn)
        # create list of functions called TASK
        # and inject a function comp_task which 
        inject_func(comp_task, conn)
        conn.execute('TASK = %s' % \
                   str(tuple([i.__name__ for i in self.task])).replace("'", ""))
                    # ['func1', 'func2'] -> "(func1, func2)"
        # inject compose function, wil

        self.task = [conn.namespace['comp_task']]
        self.args = [[self.args]]
        self.kwargs = [[self.kwargs]]
        # instead of multiple remote back and the combined functions is
        # evaluated remotely.
        return self

    def __call__(self, inbox):
        """
        Evaluates the worker-function(s) and argument(s) with which the *Worker*
        has been initialized given the input data i.e. inbox.

        Arguments:

            * inbox(sequence)

                A sequence of items to be evaluated by the function i.e.::

                    f(sequence) is f((data1, data2, ..., data2))

                If an exception is raised by the worker-function the *Worker* 
                returns a ``WorkerError``. Typically a raised ``WorkerError``
                should be wrapped into a ``PiperError`` by the *Piper* instance
                which wraps this *Worker* instance. If any of the data in the 
                inbox is a *PiperError* then the worker-function is not called
                at all and the *Worker* instance propagates the exception 
                (``PiperError``) from the upstream *Piper* to the wrapping 
                *Piper*. The originial exception travels along as the first 
                argument of the innermost exception.
        """
        outbox = inbox          # we save the input to raise a better exception.
        exceptions = [e for e in inbox if isinstance(e, PiperError)]
        if not exceptions:
            # upstream did not raise exception, running functions
            try:
                for func, args, kwargs in \
                zip(self.task, self.args, self.kwargs):
                    outbox = (func(outbox, *args, **kwargs),)
                outbox = outbox[0]
            except Exception, excp:
                # an exception occured in one of the f's do not raise it
                # instead return it.
                outbox = WorkerError(excp, func.__name__, inbox)
        else:
            # if any of the inputs is a PiperError just propagate it.
            outbox = PiperError(*exceptions)
        return outbox


def inspect(piper):
    """
    Determines the instance (Piper, Worker, FunctionType, Iterable). It returns 
    a tuple of boolean variables i.e: (is_piper, is_worker, is_function, 
    is_iterable_of_pipers, is_iterable_of_workers, is_iterable_of_functions).
    """
    is_piper = isinstance(piper, Piper)
    is_function = isinstance(piper, FunctionType) or isbuiltin(piper)
    is_worker = isinstance(piper, Worker)
    is_iterable = getattr(piper, '__iter__', False) and not \
                 (is_piper or is_function or is_worker)
    is_iterable_p = is_iterable and isinstance(piper, Piper)
    is_iterable_f = is_iterable and (isinstance(piper[0], FunctionType) or \
                                     isbuiltin(piper[0]))
    is_iterable_w = is_iterable and isinstance(piper[0], Worker)
    return (is_piper, is_worker, is_function, is_iterable_p, is_iterable_w, \
            is_iterable_f)

@imports(['itertools'])
def comp_task(inbox, args, kwargs):
    """
    Composes functions in the global sequence variable TASK and evaluates the
    composition given input (inbox) and arguments (args, kwargs).
    """
    # Note. this function uses a global variable which must be defined on the 
    # remote host.
    for func, args, kwargs in itertools.izip(TASK, args, kwargs):
        inbox = (func(inbox, *args, **kwargs),)
    return inbox[0]


class Consume(object):
    """
    This iterator-wrapper consumes n results from the input iterator and weaves
    the results together in strides. If the result is an exception it is *not*
    raised.
    """
    def __init__(self, iterable, n=1, stride=1):
        self.iterable = iterable
        self.stride = stride
        self._stride_buffer = None
        self.n = n

    def __iter__(self):
        return self

    def _rebuffer(self):
        batch_buffer = defaultdict(list)
        self._stride_buffer = []
        for i in xrange(self.n):                        # number of consumed 
            for stride in xrange(self.stride):               # results
                try:
                    res = self.iterable.next()
                except StopIteration:
                    continue
                except Exception, res:
                    pass
                batch_buffer[stride].append(res)

        for stride in xrange(self.stride):
            batch = batch_buffer[stride]
            self._stride_buffer.append(batch)
        self._stride_buffer.reverse()

    def next(self):
        """
        Returns the next sequence of results, given stride and n.
        """
        try:
            results = self._stride_buffer.pop()
        except (IndexError, AttributeError):
            self._rebuffer()
            results = self._stride_buffer.pop()
        if not results:
            raise StopIteration
        return results


class Chain(object):
    """ 
    This is a generalization of the ``zip`` and ``chain`` functions. 
    If stride =1 it behaves like ``itertools.zip``, if stride =len(iterable) it
    behaves like ``itertools.chain`` in any other case it zips iterables in 
    strides e.g::

        a = Chain([iter([1,2,3]), iter([4,5,6], stride =2)
        list(a)
        >>> [1,2,4,5,3,6]
        
    It is further resistant to exceptions i.e. if one of the iterables
    raises an exception the ``Chain`` does not end in a ``StopIteration``, but 
    continues with other iterables.
    """
    def __init__(self, iterables, stride=1):
        self.iterables = iterables
        self.stride = stride
        self.l = len(self.iterables)
        self.s = self.stride
        self.i = 0

    def __iter__(self):
        return self

    def next(self):
        """
        Returns the next result from the chained iterables given strid.
        """
        if self.s:
            self.s -= 1
        else:
            self.s = self.stride - 1
            self.i = (self.i + 1) % self.l # new iterable
        return self.iterables[self.i].next()


class Produce(object):
    """ 
    This iterator-wrapper returns n-times each result from the wrapped iterator.
    i.e. if n =2 and the input iterators results are (1, Exception, 2) then the 
    ``Produce`` instance will return 6 (i.e. 2*3) results in the order [1, 1, 
    Exception, Exception, 2, 2] if the stride =1. If stride =2 the output will 
    look like this: [1, Exception, 1, Exception, 2, 2]. Note that 
    ``StopIteration`` is also an exception, and the Produce iterator might 
    return values after a ``StopIteration`` is raised. 
    """
    def __init__(self, iterable, n=1, stride=1):
        self.iterable = iterable
        self.stride = stride
        self._stride_buffer = None
        self._repeat_buffer = None
        self.n = n             # times the results in the buffer are repeated

    def __iter__(self):
        return self

    def _rebuffer(self):
        """
        (internal) refill the repeat buffer
        """
        results = []
        exceptions = []
        for i in xrange(self.stride):
            try:
                results.append(self.iterable.next())
                exceptions.append(False)
            except Exception, excp:
                results.append(excp)
                exceptions.append(True)
        self._repeat_buffer = repeat((results, exceptions), self.n)

    def next(self):
        """
        Returnes the next result, given stride and n.
        """
        try:
            res, excp = self._stride_buffer.next()
        except (StopIteration, AttributeError):
            try:
                self._stride_buffer = izip(*self._repeat_buffer.next())
            except (StopIteration, AttributeError):
                self._rebuffer()
                self._stride_buffer = izip(*self._repeat_buffer.next())
            res, excp = self._stride_buffer.next()
        if excp:
            raise res
        else:
            return res


class ProduceFromSequence(Produce):
    """
    This iterator wrapper is an iterator, but it returns elements from the 
    sequence returned by the wrapped iterator. The number of returned elements
    is defined by n and should not be smaller then the sequence returned by the 
    wrapped iterator. 
    
    For example if the wrapped iterator results are ((11, 12), (21, 22), 
    (31, 32)) then n *should* equal 2. For stride =1 the result will be: 
    [11, 12, 21, 22, 31, 32]. For stride =2 [11, 21, 12, 22, 31, 32]. Note that
    StopIteration is also an exception!
    """
    def _rebuffer(self):
        """
        (internal) refill the repeat buffer
        """
        # collect a stride worth of results(result lists) or exceptions
        results = []
        exceptions = []
        for i in xrange(self.stride):
            try:
                results.append(self.iterable.next())
                exceptions.append(False)
            except Exception, excp:
                results.append(excp)
                exceptions.append(True)
        # un-roll the result lists
        res_exc = []
        for rep in xrange(self.n):
            flat_results = []
            for i in xrange(self.stride):
                result_list, exception = results[i], exceptions[i]
                if not exception:
                    flat_results.append(result_list[rep])
                else:
                    flat_results.append(result_list)
            res_exc.append((flat_results, exceptions))
        # make an iterator (like repeat)
        self._repeat_buffer = iter(res_exc)


#EOF
