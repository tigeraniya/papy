"""
:mod:`papy.papy`
================

This module provides classes and functions to construct and run a papy pipeline.
"""
# self-imports
from IMap import IMap, Weave, imports, inject_func 
from graph import Graph
from utils import logger
from utils.defaults import get_defaults
from utils.runtime import get_runtime
# python imports
from multiprocessing import TimeoutError, cpu_count
from itertools import izip, tee, imap, chain, cycle, repeat
from threading import Thread, Event
from collections import defaultdict
from types import FunctionType
from inspect import isbuiltin, getsource
from logging import getLogger
from time import time


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

    def __init__(self, pipers =(), pipes =(), xtras =None):
        self.log = getLogger('papy')
        self.log.info('Creating %s from %s and %s' % (repr(self), pipers, pipes))
        self.add_pipers(pipers, xtras)
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
        self.log.info('%s trying to connect in the order %s' % (repr(self), repr(postorder)))
        for piper in postorder:
            if not piper.connected and self[piper].keys(): # skip input pipers
                piper.connect(self[piper].keys())   # what if []?
        self.log.info('%s succesfuly connected' % repr(self))

    def disconnect(self):
        """ Disconnects pipers in the correct order.
        """ 
        postorder = self.postorder()
        self.log.info('%s trying to disconnect in the order %s' % (repr(self), repr(postorder)))
        for piper in postorder:
            if piper.connected and self[piper].keys(): # skip input pipers
                piper.disconnect()
        self.log.info('%s succesfuly disconnected' % repr(self))

    def start(self):
        """ Starts all pipers in the correct order.
        """
        postorder = self.postorder()
        for piper in postorder:
            piper.start(forced =True)

    def get_inputs(self):
        start_pipers = [p for p in self.postorder() if not self.outgoing_edges(p)]
        self.log.info('%s got input pipers %s' % (repr(self), start_pipers))
        return start_pipers

    def get_outputs(self):
        end_pipers = [p for p in self.postorder() if not self.incoming_edges(p)]
        self.log.info('%s got output pipers %s' % (repr(self), end_pipers))
        return end_pipers

    def add_piper(self, piper, create =True, xtra =None):
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
        self.add_node(piper, xtra)
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


# imap call signature
I_SIG = '    %s = IMap(worker_type ="%s", worker_num =%s, stride =%s, buffer =%s,'  +\
                  'ordered =%s, skip =%s, name ="%s")\n'
# piper call signature
P_SIG = '    %s = Piper(%s, parallel =%s, consume =%s, produce =%s, timeout =%s, '  +\
                   'cmp =%s, ornament =%s, debug =%s, name ="%s")\n'
# worker call signature
W_SIG = 'Worker((%s,), %s)'
# list signature
L_SIG = '(%s, %s)'
# papy pipeline source-file layout
P_LAY =  \
    'from papy import *'                                                  + '\n'   +\
    'from IMap import IMap'                                               + '\n\n' +\
    '%s'                                                                  + '\n\n' +\
    '%s'                                                                  + '\n\n' +\
    'def pipeline():'                                                     + '\n'   +\
             '%s'                                                         + '\n\n' +\
             '%s'                                                         + '\n\n' +\
    '    ' + 'pipers = %s'                                                + '\n'   +\
    '    ' + 'xtras = %s'                                                 + '\n'   +\
    '    ' + 'pipes  = %s'                                                + '\n'   +\
    '    ' + 'return Dagger(pipers =pipers, pipes =pipes, xtras =xtras)'  + '\n\n' +\
    'if __name__ == "__main__":'                                          + '\n'   +\
    '    ' + 'pipeline()'                                                 + '\n'   +\
    ''                                                                    + '\n'


class Plumber(Dagger):
    """ The Plumber.
    """

    def _serve(self):
        """ 
        """
        pass
        

    def _finish(self, isstopped):
        """ Executes when last output piper raises StopIteration.
        """
        self.stats['run_time'] = time() - self.stats['start_time'] 
        self.log.info('%s finished, stopped: %s.' %\
        (repr(self), isstopped))
        self._is_finished.set()

    def _track(self, frame_finished):
        """ Executes when last output piper returns something.
        """
        # this should be fixed to monitor not only the last!
        if frame_finished:
            self.stats['last_frame'] += 1
            self.log.info('%s finished tasklet %s' %\
            (repr(self), self.stats['last_frame']))

    @staticmethod
    def _plunge(tasks, is_stopping, track, finish):
        """ Calls the next method of weaved tasks until they are finished or
            The Plumber instance is chinkedup.
        """
        started = False    # If no result received either not started or start & stop
        while True:        # could have been called before the plunger thread
            if is_stopping():
                tasks.stop()
            try:
                tasks.next()
                frame_finished = (tasks.i == (tasks.lenght - 1))
                track(frame_finished)
            except StopIteration:
                finish(is_stopping())
                break

    def __init__(self, dagger =None, **kwargs):
        self._is_stopping = Event()
        self._is_finished = Event()

        # Plumber statistics
        self.stats = {}
        self.stats['last_frame'] = -1
        self.stats['start_time'] = None
        self.stats['run_time'] = None

        # init
        Dagger.__init__((dagger or self), **kwargs)

    def _code(self):
        """ Generates imports, code and runtime calls.
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
            cmp_ = p.cmp__name__ if p.cmp else None
            pcall += P_SIG % (p.name, ws, in_, p.consume, p.produce, p.timeout,\
                              cmp_, p.ornament, p.debug, p.name)
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
        pipes  = [L_SIG % (d.name, s.name) for s, d in self.edges()]
        pipes  = '[%s]' % ", ".join(pipes)                             # pipes
        xtras  = [str(self[p].xtra) for p in self]
        xtras  = '[%s]' % ",".join(xtras)                              # node xtra
        return (icode, tcode, icall, pcall, pipers, xtras, pipes)


    def __repr__(self):
        return "Plumber(%s)" % super(Plumber, self).__repr__()

    def __str__(self):
        return super(Plumber, self).__str__()

    def load(self, filename):
        """ Load pipeline.
        """
        execfile(filename)
        self.__init__(pipeline())

    def save(self, filename):
        """ Save pipeline.
        """
        h = open(filename, 'wb')
        h.write(P_LAY % self._code())
        h.close()

    def plunge(self, tasks =None, stride =1):
        """ Runs the plumber which means that the next methods of each output piper are
            called in cycles and the results discarded.

            Arguments:

            Warning! If you change those arguments you should better know what you are
            doing. The order of the tasks in the sequence and the stride size defined
            here must be compatible with the buffer_size and stride of the IMap
            instances used by the pipers.

              * tasks(sequence of IMapTask instances) [default: None]

                If no sequence is give all output pipers are plunged in correct order.

              * stride(int) [default: 1]

                By default take only one result from each output piper. As a general rule
                the stride cannot be bigger then the stride of the IMap instances.
        """
        # connect pipers
        self.connect()
        # collect results for tracked tasks
        self.stats['pipers_tracked'] = {}
        for p in self.postorder():
            if hasattr(p.imap, '_tasks_tracked') and p.track:
                self.stats['pipers_tracked'][p.name] =\
                [p.imap._tasks_tracked[t.task] for t in p.imap_tasks]

        # start IMaps
        self.stats['start_time'] = time()
        self.start()

        # remove non-block results for end tasks
        tasks = (tasks or self.get_outputs())
        wtasks = Weave(tasks, repeats =stride)
        self._plunger = Thread(target =self._plunge, args =(wtasks,\
                        self._is_stopping.isSet, self._track, self._finish))
        self._plunger.deamon = True
        self._plunger.start()


    def chinkup(self):
        """ Stop a running pipeline. Blocks until stopped.
        """
        self._is_stopping.set()
        self._plunger.join()



class Piper(object):
    """Creates a new Piper instance.

       arguments:

         * worker(Worker, Piper or sequence)

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

         * spawn(int) [default: 1]

           The number of repeats of this piper to add to the IMap.

         * timeout(int) [default: 1]

           Time to wait till a result is received otherwise a PiperError
           is *returned*.

         * cmp(func) [default: None -> cmp_ornament]

           Compare function to sort the randomly ordered list of upstream
           pipers. Preferrably using the ornament attribute, which is the
           default. (see ornament)

         * ornament(object) [default: None -> self]

           Anything which can be used by compare functions of down-stream
           pipers. By default the ornament is the same as the instance object,
           i.e. the default beheviour is equivalent to.::
          
               cmp(piper_instance1, piper_instance2)

         * debug(bool) [default: False]

           Debug-mode. Raise PiperError on WorkerErrors.

           .. warning:: 
             
             this will most-likely hang the python interpreter.
    """
    @staticmethod
    def _cmp(x, y):
        """ Compares pipers by ornament.
        """
        return cmp(x.ornament, y.ornament)

    def __init__(self, worker, parallel =False, consume =1, produce =1,\
                 spawn =1, timeout =None, cmp =None, ornament =None,\
                 debug =False, name =None, track =False):
        self.inbox = None
        self.outbox = None
        self.connected = False
        self.finished = False
        self.imap_tasks = []

        self.consume = consume
        self.spawn = spawn
        self.produce = produce
        self.timeout = timeout
        self.debug = debug
        self.name =  (name or 'piper_%s' % id(self))
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
            self.log.info('Piper %s has already been started' % self)
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
        else:
            # sort input
            inbox.sort((self.cmp or self._cmp))
            self.log.info('Piper %s connects to %s' % (self, inbox))
            # Make input
            stride = self.imap.stride if hasattr(self.imap, 'stride') else 1
            self.inbox  = izip(*[tee(i,1)[0] for i in inbox]) if self.consume == 1 else\
                  Consume(izip(*[tee(i,1)[0] for i in inbox]), n =self.consume,\
                  stride =stride)
            # Calculate result
            for i in xrange(self.spawn):
                self.imap_tasks.append(\
                    self.imap(self.worker, self.inbox) if self.imap is imap else\
                    self.imap(self.worker, self.inbox, timeout =self.timeout,\
                    track =self.track))
            outbox = Chain(self.imap_tasks, stride =stride)
            # Make output
            self.outbox = outbox if self.produce == 1 else\
                  Produce(outbox, n =self.produce, stride =stride)
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
                for imap_task in self.imap_tasks:
                    del self.imap._tasks[imap_task.task]
            except AttributeError:
                pass
            self.imap_tasks = []
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
        self.__name__ =  ">".join([f.__name__ for f in self.task])

    def __repr__(self):
        """Functions within a worker e.g. (f, g, h) are evaluated from left to right
           i.e.: h(g(f(x))) thus their representation f>g>h.
        """
        return self.__name__


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

    def _inject(self, conn):
        """ Inject/replace all functions into a rpyc connection object.
        """
        # provide DEFAULTS remotely
        inject_func(get_defaults, conn)
            conn.execute('PAPY_DEFAULTS = get_defaults()')
        # provide PAPY_RUNTIME remotely
        if not 'PAPY_RUNTIME' in conn.namespace:
            inject_func(get_runtime, conn)
            conn.execute('PAPY_RUNTIME = get_runtime()')
        # provide partial remotely
        conn.execute('from functools import partial')
        # inject compose function
        inject_func(compose, conn)
        # inject all functions
        for f in self.task:
            inject_func(f, conn)
        # make partial of composed function
        conn.execute('comp_func = partial(compose, funcs =%s)' %\
                      str(tuple([i.__name__ for i in self.task])).replace("'",""))
                        # ['func1', 'func2'] -> "(func1, func2)"
        comp_func = conn.namespace['comp_func']
        self.task = [comp_func]
        self.args = [[self.args]]
        # instead of multiple remote back and the combined functions is
        # evaluated remotely.
        return self

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
                # instead return it.
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

@imports([['itertools',['izip']]])
def compose(inbox, args, funcs):
    """ Composes functions.
    """
    for f, a in izip(funcs, args):
        inbox = (f(inbox, *a),)
    return inbox[0]

class Consume(object):
    """ This iterator-wrapper consumes n results from the input iterator and zips the
        results together. If the result is an exception it is *not* raised.
    """
    def __init__(self, iterable, n =1, stride =1):
        self.iterable = iterable
        self.stride = stride
        self.n = n

    def __iter__(self):
        return self

    def _rebuffer(self):
        batch_buffer = defaultdict(list)
        self._stride_buffer = []
        for i in xrange(self.n):                        # number of consumed 
            for s in xrange(self.stride):               # results
                try:
                    r = self.iterable.next()
                except StopIteration:
                    continue
                except Exception, r:
                    pass
                batch_buffer[s].append(r)
        
        for s in xrange(self.stride):
            batch = batch_buffer[s]
            self._stride_buffer.append(batch)
        self._stride_buffer.reverse()
    
    def next(self):
        try:
            results = self._stride_buffer.pop()
        except (IndexError, AttributeError):
            self._rebuffer()
            results = self._stride_buffer.pop()
        if not results:
            raise StopIteration
        return results


class Chain(object):
    """ This is a generalization of the zip and chain functions. If stride =1 it
        behaves like itertools.zip, if stride =len(iterable) it behaves like
        itertools.chain in any other case it zips iterables in strides e.g::

        a = Chain([iter([1,2,3]), iter([4,5,6], stride =2)
        list(a)
        >>> [1,2,4,5,3,6]
        
        it is also resistant to exceptions i.e. if one of the iterables
        raises an exception the Chain does not end in a StopIteration, but continues.
    """
    def __init__(self, iterables, stride =1):
        self.iterables = iterables
        self.stride = stride
        self.l = len(self.iterables)
        self.s = self.stride
        self.i = 0

    def __iter__(self):
        return self

    def next(self):
        if self.s:
            self.s -= 1
        else:
            self.s = self.stride -1
            self.i = (self.i + 1) % self.l # new iterable
        return self.iterables[self.i].next()


class Produce(object):
    """ This iterator-wrapper yields n-times each result of the input. i.e. if n =2 and
        input results are (1, Exception, 2) then the Produce instance will return 2*3
        results in the order [1, 1, Exception, Exception, 2, 2] if the stride
        =1. If stride =2 the output will look like this: [1, Exception, 1,
        Exception, 2, 2].

        Note that StopIteration is also an exception!
    """
    def __init__(self, iterable, n =1, stride =1):
        self.iterable = iterable
        self.stride = stride
        self.n = n             # times the results in the buffer are repeated

    def __iter__(self):
        return self

    def _rebuffer(self):
        results = []
        exceptions = []
        for i in xrange(self.stride):
            try:
                results.append(self.iterable.next())
                exceptions.append(False)
            except Exception, e:
                results.append(e)
                exceptions.append(True)
        self._repeat_buffer = repeat((results, exceptions), self.n)

    def next(self):
        try:
            r, e = self._stride_buffer.next()
        except (StopIteration, AttributeError):
            try:
                self._stride_buffer = izip(*self._repeat_buffer.next())
            except (StopIteration, AttributeError):
                self._rebuffer()
                self._stride_buffer = izip(*self._repeat_buffer.next())
            r, e = self._stride_buffer.next()
        if e:
            raise r
        else:
            return r



