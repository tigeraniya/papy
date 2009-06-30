""" 
:mod:`papy.workers.io`
========================

A collection of input/output worker functions. To connect *Pipers* to
external inputs/outputs (streams) or other *Pipers* (items). Two types of
functions are provided:

  * stream function - load or save the input stream from or into a single file,
    therefore they can only be used at the beginnings or ends of a pipeline.
    Stream loaders are not worker functions, as they are colled once (with the
    input) and create the input collection in the form of a generator of items.

  * item functions - load, save, process or display data items. These are 
    *Worker* functions and should be used within *Pipers*. 

No method of interprocess communication, besides the default inefficient
two-pass ``multiprocessing.Queue`` and temporary files is supported on all 
platforms even among UNIXes as they rely on particular implementation details.
"""
# all imports in this module have to be injected to remote RPyC connections.
# imports is provided remotely by IMap
from IMap import imports
# get_defaults and get_runtime are provided by worker._inject
from papy.utils.defaults import get_defaults
from papy.utils.runtime import get_runtime

#
# LOGGING
# 
def print_(inbox):
    """ Prints the first element of the inbox.
    """
    print inbox[0]
#
# INPUT OUTPUT
#
# STREAMS
@imports([['posix_ipc', ['SharedMemory']], ['mmap',[]], ['os',[]]], forgive =True)
def open_shm(name):
    """ Equivalent to the built in open function but opens a file in shared
        memory. A single file can be opened multiple times. Only the name of the
        file is necessary not the absolute location (most likely /dev/shm/).

        Arguments:

          * name(string)

            The name of the file to open e.g. 'my_file' not /dev/shm/my_file
            


    """
    # The class is defined within a function to allow
    # it to be injected together with the import statments
    # on a remote RPyC host.
    # TODO: closed, ?encoding?, isatty, mode, ?newlines?, next, readlines,
    # xreadlines, writelines, readinto
    # TODO: ??name
    class ShmHandle(SharedMemory):
        """ This is wrapper around memory mapped shared memory provided by
            posix shm. 
        """
        # to avoid recursive mapfile lookup 
        def __init__(self, name):
            # try to make some shared memory
            try:
                # create new file (won't create if exist)
                SharedMemory.__init__(self, name, flags =posix_ipc.O_CREX)
            except posix_ipc.ExistentialError:
                # or open existing file (won't open if not exist)
                SharedMemory.__init__(self, name)
            try:
                self.mapfile = mmap.mmap(self.fd, 0)
            except mmap.error:
                # tried to open empty file
                self.mapfile = None

        def __getattr__(self, name):
            # cannot multiple inheritance using two C-classes.
            if not self.mapfile:
                # if we opened the handle when it was empty 
                # no mapfile was created. 
                self.mapfile = mmap.mmap(self.fd, 0)
            return getattr(self.mapfile, name)

        def write(self, str):
            try:
                bytes = len(str)
                bytes_needed = self.mapfile.tell() - self.size + bytes
                self.mapfile.resize(self.size + bytes_needed)
            except AttributeError:
                os.ftruncate(self.fd, bytes)
                # the default is shared memory, ACCESS_WRITE
                # needs file to be opened as r+ 
                self.mapfile = mmap.mmap(self.fd, 0)
            self.mapfile.write(str)

        def fileno(self):
            return self.fd

        def truncate(self, size =None):
            self.mapfile.resize(size or self.tell()) 
        
    return ShmHandle(name)


def dump_stream(inbox, handle, delimiter =None):
    """ Writes the first element of the inbox to the provided stream (file
        handle) delimiting the input by the optional delimiter string. Returns
        the name of the file being written.

        Note that only a single process can have access to a file handle open
        for writing. Therefore this worker function should only be used by a
        linear piper.

        Arguments:

          * handle(stream)

            File handle open for writing.
          
          * delimiter(string) [default: None]

            A string which will seperate the written items. e.g:
            "END" becomes "\\nEND\\n" in the output stream. The default is an
            empty string which means that items will be seperated by a blank
            line i.e.: '\\n\\n'
    """
    handle.write(inbox[0])
    delimiter = '\n%s\n' % (delimiter or '')
    handle.write(delimiter)
    return handle.name    

def load_stream(handle, delimiter =None):
    """ Creates a string generator from a stream (file handle) containing
        data delimited by the delimiter strings.

        This is a stand-alone function and should be used to feed external data
        into a pipeline.

        Arguments:

          * delimiter(string) [default: None]

            The default means that items will be separated by 
            two new-line characters i.e.: '\\n\\n'
    """
    delimiter = (delimiter or '') + '\n'
    while True:
        temp = []
        while True:
            line = handle.readline()
            if line == delimiter:
                # remove introduced first '\n'
                temp[-1] = temp[-1][:-1]
                break
            elif line == '':
                raise StopIteration
            else:
                temp.append(line)
        yield "".join(temp)

@imports([['cPickle',[]]])
def load_pickle_stream(handle):
    """ Creates an object generator from a stream (file handle) containing data
        in pickles. To be used with the ``dump_pickle_stream``
        .. warning::

            File handles should not be read by different processes.
    """
    while True:
        try:
            yield cPickle.load(handle)
        except EOFError:
            raise StopIteration

def dump_pickle_stream(inbox, handle):
    """ Writes the first element of the inbox to the provided stream (data
        handle) as a pickle. To be used with the ``load_pickle_stream`` worker.
    """
    cPickle.dump(inbox[0], handle, -1)

# ITEMS
@imports([['tempfile',[]], ['os', []], ['errno', []], ['mmap', []],\
          ['signal', []],  ['posix_ipc', []], ['socket', []],\
          ['urllib', []], ['random', []]], forgive = True)
def dump_item(inbox, type ='file', prefix =None, suffix =None, dir =None,\
              timeout =320, buffer =None):
    """ Writes the first element of the inbox as a file of a specified type.
        The type can be 'file', 'fifo', 'shm', 'tcp' or 'udp' corresponding to 
        typical files, named pipes(FIFOs) and posix shared memory. FIFOs and shared
        memory are volatile, but shared memory can exist longer then the python
        process.

        Returns the semi-random name of the file written. By default creates 
        files and fifos in the default temporary directory and shared memory
        in /dev/shm. To use named pipes the operating system has to support
        both forks and fifos (not Windows). To use shared memory the system has
        to be proper posix (not MacOSX) and the posix_ipc module has to be
        installed. Sockets should work on operating systems.

        This worker is useful to efficently communicate parallel pipers without
        the overhead of using queues.

        Arguments:

          * type('file', 'fifo', 'shm', 'tcp', 'udp') [default: 'file']
            
            Type of the created file/socket.

          * prefix(string) [default: tmp_papy_%type%]

            Prefix of the file to be created. Should probably identify the
            worker and piper. 

          * suffix(string) [default: '']

            Suffix of the file to be created. Should probably identify the 
            format of the serialization protocol e.g. 'pickle' or
            de-serialized data e.g. 'numpy'.

          * dir(string) [default: tempfile.gettempdir() or /dev/shm]
            
            Directory to safe the file to. (can be changed only for types
            'file' and 'fifo'

          * timeout(integer) [default: 320]

            Number of seconds to keep the process at the write-end of the
            socket or pipe alive.
    """
    # this determines host specific defaults
    # and runtime information.
    if not 'PAPY_DEFAULTS' in globals():
        global PAPY_DEFAULTS
        PAPY_DEFAULTS = get_defaults()
    if not 'PAPY_RUNTIME' in globals():
        global PAPY_RUNTIME
        PAPY_RUNTIME = get_runtime()

    # get a random filename generator
    names = tempfile._get_candidate_names()
    names.rng.seed() # re-seed rng after the fork
    # try to own the file
    if type in ('file', 'fifo', 'shm'):
        prefix = prefix or 'tmp_papy_%s' % type
        suffix = suffix or ''
        dir = dir or tempfile.gettempdir()
        while True:
            # create a random file name
            file = prefix + names.next() + suffix
            if type in ('file', 'fifo'):
                file = os.path.join(dir, file)
                try:
                    if type == 'file':
                        fd = os.open(file, tempfile._bin_openflags, 0600)
                        tempfile._set_cloexec(fd) # ?, but still open
                    elif type == 'fifo':
                        os.mkfifo(file)
                    file = os.path.abspath(file)
                    break
                except OSError, e:
                    # first try to close the fd
                    try:
                        os.close(fd)
                    except OSError, ee:
                        if ee.errno == errno.EBADF:
                            pass
                        # strange error better raise it
                        raise ee
                    if e.errno == errno.EEXIST:
                        # file exists try another one
                        continue
                    # all other errors should be raise
                    raise e
            elif type == 'shm':
                try:
                    mem = posix_ipc.SharedMemory(file, size =len(inbox[0]),\
                                                      flags =posix_ipc.O_CREX)
                    break
                except posix_ipc.ExistentialError:
                    continue
    # the os will create a random socket for us.
    elif type in ('tcp', 'udp'):
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM 
                                            if type == 'tcp' else 
                                             socket.SOCK_DGRAM)
        # try to bind to a port
        try:
            host = socket.gethostbyaddr(socket.gethostname())[0] # from /etc/hosts
        except socket.gaierror:
            host = urllib.urlopen(PAPY_DEFAULTS['WHATS_MYIP_URL']).read()
        sock.bind(('', 0))           # os-chosen free port on all interfaces 
        port = sock.getsockname()[1] # port of the socket
    else:
        raise ValueError("type: %s not undertood" % type)

    # got a file, fifo or memory
    if type == 'file':
        handle = open(file, 'wb')
        os.close(fd) # no need to own a file twice!
        handle.write(inbox[0])
        handle.close() # close handle
        file = (file, 0)
    elif type == 'shm':
        mapfile = mmap.mmap(mem.fd, mem.size)
        mapfile.write(inbox[0])
        mapfile.close()     # close the memory map
        os.close(mem.fd)    # close the file descriptor
        file = (file, 0)
    else:
        # forking mode. forks should be waited
        if type == 'fifo':
            pid = os.fork()
            if not pid:
                # we set an alarm for 5min if nothing starts to read 
                # within this time the process gets killed.
                signal.alarm(timeout)
                fd = os.open(file, os.O_EXCL & os.O_CREAT | os.O_WRONLY)
                signal.alarm(0)
                os.write(fd, inbox[0])
                os.close(fd)
                os._exit(0)
            file = (file, pid)
        elif type == 'tcp':
            sock.listen(1)
            pid = os.fork()
            if not pid:
                # sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                signal.alarm(timeout)
                rsock, (rhost, rport) = sock.accept() # blocks until client connects
                signal.alarm(0)
                rsock.sendall(inbox[0])               # returns if all data was sent
                # child closes all sockets and exits
                rsock.close() 
                sock.close()
                os._exit(0)
            # parent closes server socket
            sock.close()
            file = (host, port, 'tcp')
        elif type == 'udp':
            BUFFER = (buffer or PAPY_DEFAULTS['UDP_SNDBUF'])
            pid  = os.fork()
            if not pid:
                # first reply
                signal.alarm(timeout)
                data, rhost = sock.recvfrom(BUFFER) # this blocks
                signal.alarm(0)
                i = 0
                while True:
                    # sends an empty '' when data finishes and exits
                    data = inbox[0][i:i+BUFFER]
                    sock.sendto(data, rhost)
                    i += BUFFER
                    if data:
                        continue
                    break
                sock.close()
                os._exit(0)
            # parent closes server socket
            sock.close()
            file = (host, port, 'udp')
        
        # 0. get pid list and methods for atomic operations
        # 1. add the child pid to the pid list
        # 2. try to wait each pid in the list without blocking:
        #    if success remove pid if not ready pass if OSError child not exists
        #    another thread has waited this child.
        # 0.
        pids = PAPY_RUNTIME['FORKS'][os.getpid()] # entry pre-exists 
        add_pid = pids.append   # list methods are atomic
        del_pid = pids.remove 
        # 1.
        add_pid(pid)
        # 2. 
        for pid in pids:
            try:
                killed, status = os.waitpid(pid, os.WNOHANG)
                if killed:
                    del_pid(pid)
            except OSError, e:
                if e.errno == os.errno.ECHILD:
                    continue
                raise

    # filename needs still to be unlinked
    return file

@imports([['mmap', []], ['os',[]], ['stat', []],\
          ['posix_ipc', []], ['warnings', []]], forgive =True)
def load_item(inbox, type ='string', remove =True, buffer =None):
    """ Loads data from a file. Determines the file type automatically ('file',
        'fifo', 'shm', 'tcp', 'udp') but allows to specify the representation 
        type 'string' or 'mmap' for memmory mapped access to the file. Returns
        a the loaded item as a string or mmap object. Internally creates an item
        from a file object

        Arguments:

          * type('string' or 'mmap') [default: string]

            Determines the type of object the worker returns i.e. the file read
            as a string or a memmory map. FIFOs cannot be memory mapped. 

          * remove(boolean) [default: True]

            Should the file be removed from the filesystem? This is mandatory
            for FIFOs and sockets and generally a *very* good idea for shared 
            memory. Files can be used to store data persistantly.
    """
    # determine the input type
    is_file, is_fifo, is_shm, is_socket = False, False, False, False
    name = inbox[0]
    if len(name) == 2 and isinstance(name[0], basestring):
        is_file = True
        
    if is_file:
        try:
            is_fifo = stat.S_ISFIFO(os.stat(name[0]).st_mode)
        except OSError:
            is_shm = os.path.exists(os.path.join('/dev/shm', name[0]))
    else:
        is_item = len(name) == 4
        is_socket = len(name) == 3 
        is_tcp = name[2] == 'tcp'
        is_udp = name[2] == 'udp'

    if (is_fifo or is_socket) and (type == 'mmap'):
        warnings.warn('memory mapping is not supported for FIFOs and sockets',\
                                                                RuntimeWarning)
        type = 'string'
    if (is_fifo or is_socket) and not remove:
        warnings.warn('FIFOs and sockets have to be removed',\
                                               RuntimeWarning)
        remove = True 

    # get a fd and start/stop
    start = 0
    if is_shm:
        memory = posix_ipc.SharedMemory(name[0])
        stop = memory.size - 1
        fd = memory.fd

    elif is_fifo or is_file:
        stop = os.stat(name[0]).st_size - 1
        fd = os.open(name[0], os.O_RDONLY)
        BUFFER = (buffer or PAPY_DEFAULTS['PIPE_BUF'])
    
    elif is_socket:
        host, port = socket.gethostbyname(name[0]), name[1]
        if is_tcp:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            stop = -1
            fd = sock.fileno()
            BUFFER = (buffer or PAPY_DEFAULTS['TCP_RCVBUF'])
        elif is_udp:
            BUFFER = (buffer or PAPY_DEFAULTS['UDP_RCVBUF'])
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.sendto('', (host, port)) # 'greet server'
            stop = -1
        else:
            raise ValueError

    elif is_item:
        (fd, name), start, stop = name

    else:
        raise ValueError('?')

    # get the data
    if type =='mmap':
        offset = start - (start % mmap.ALLOCATIONGRANULARITY)
        start = start - offset
        stop = stop - offset + 1
        data = mmap.mmap(fd, stop, access=mmap.ACCESS_READ, offset =offset)
        data.seek(start)

    elif type == 'string':
        data = []
        if stop == -1:
            while True:
                if is_socket and is_udp:
                    buffer_  = sock.recv(BUFFER)
                else:
                    buffer_ = os.read(fd, BUFFER)                                          
                if not buffer_:
                    break
                data.append(buffer_)
            data = "".join(data)
            # data = sock.recv(socket.MSG_WAITALL) 
            # this would read all the data from a socket
        else:
            os.lseek(fd, start, 0)
            data = os.read(fd, stop - start + 1)
    else:
        raise ValueError('type: %s not understood.' % type)

    # remove the file or close the socket
    if remove:
        if is_shm:
            # closes and unlinks the shared memory
            os.close(fd)
            memory.unlink()
        elif is_socket:
            # closes client socket
            sock.close()
        else:
            # pipes and files are just removed
            os.close(fd)
            os.unlink(name[0])
    else:
        # a file not remove, but close fh
        os.close(fd)

    # returns a string or mmap
    return data



@imports([['papy', []], ['tempfile', []], ['multiprocessing',[]],\
          ['threading', []]], forgive = True)
def dump_manager_item(inbox, address =('127.0.0.1', 46779), authkey ='papy'):
    """ Writes the first element of the inbox as a shared object. The object is
        stored as a value in a shared dictionary served by a *Manager* process.
        Returns the key for the object value the address and the authentication
        key.

        To use this worker a *DictServer* instance has to be running. 
        (see also: Plumber)

        Arguments:

          * address(2-tuple) [default: ('127.0.0.1', 46779)]

            A 2-tuple identifying the server(string) and port(integer).

          * authkey(string) [default: 'papy']

            Authentication string to connect to the server.           
    """
    class DictClient(multiprocessing.managers.BaseManager):
        pass
    DictClient.register('dict')
    # get database 
    manager = DictClient(address, authkey)
    manager.connect()
    kv = manager.dict()
    # identify process/thread
    ptid = hash((threading.current_thread(),\
           multiprocessing.current_process()))
    # get a random process/thread safe key
    names = tempfile._get_candidate_names()
    while True:
        name = names.next()
        k = "%s_%s" % (name, ptid)
        if not k in kv:
            break
        # else ... loop forever
    # update dict
    kv[k] = inbox[0]
    return (k, address, authkey)

@imports([['papy', []], ['multiprocessing', []]], forgive =True)
def load_manager_item(inbox, remove =True):
    """
    """
    class DictClient(multiprocessing.managers.BaseManager):
        pass
    DictClient.register('dict')
    k, address, authkey = inbox[0]
    manager = DictClient(address, authkey)
    manager.connect()
    kv = manager.dict()
    if remove:
        v = kv.pop(k)
    else:
        v = kv[k]
    return v
    

@imports([['sqlite3', []], ['MySQLdb', []], ['warnings', []]], forgive =True)
def dump_db_item(inbox, type='sqlite', table ='temp', **kwargs):
    """ Writes the first element of the inbox as a key/value pair in a database
        of the provided type. Currently supported: "sqlite" and "mysql".
        Returns the information necessary for the load_db_item to retrieve the
        element.
        
        According to the sqlite documentation: You should avoid putting SQLite
        database files on NFS if multiple processes might try to access the file
        at the same time.

        Arguments:

          * type(str) [default: 'sqlite']

            Type of the database to use currently supported 'sqlite' and 'mysql'
            Using MySQL requires a running 

          * db(str) [default: 'papydb']

            Default name of the database, for sqlite it is the name of the
            database file in the current working directory. Databases can be
            shared among pipers. Having multiple SQLite database files improves
            concurrency. A new file will be created if none exists. The MySQL 
            database has to exists (it will not be created).
          
          * table(str) [default: 'temp']  

            Name of the table to store the key/value pairs into. Tables can be
            shared among pipers.

          * host, user, passwd

            Authentication information. Refer to the generic dbapi2
            documentation.
    """
    # connect defaults
    kwargs['db'] = kwargs.get('db') or 'papydb'
    
    # backend specific
    if type =='sqlite':
        dbapi2 = sqlite3.dbapi2
        ai = 'autoincrement'
        kwargs['database']  = kwargs.pop('db')
        kwargs['isolation_level'] ='IMMEDIATE'
    elif type =='mysql':
        dbapi2 = MySQLdb
        ai = 'auto_increment'
        kwargs['host'] = kwargs.get('host') or 'localhost'
        #warnings.simplefilter('ignore')

    else:
        raise ValueError('Database format %s not understood!' % db)

    # Under Unix, you should not carry an open SQLite database across a fork() 
    # system call into the child process. Problems will result if you do.
    while True:
        try:
        # Calling this routine with an argument less than or equal to zero turns
        # off all busy handlers. "isolation_level=None" does not actually mean
        # "provide a lower isolation level", it means "use SQLite's inherent
        # isolation /concurrency primitives rather than those in PySQLite".  By
        # default, SQLite version 3 operates in autocommit mode. In autocommit
        # mode, all changes to the database are committed as soon as all
        # operations associated with the current database connection complete.
        # http://www.sqlite.org/lang_transaction.html
        # After a BEGIN IMMEDIATE, you are guaranteed that no other thread or
        # process will be able to write to the database or do a BEGIN IMMEDIATE
        # or BEGIN EXCLUSIVE. 
            con =dbapi2.connect(**kwargs)
            cur = con.cursor()
            cur.execute("create table if not exists %s (id integer primary key %s, value blob)" % (table, ai))
            break
        except dbapi2.OperationalError, e:
            # if locked wait ... forever
            if not e.args[0] == 'database is locked':
                raise e 
    # inserts are atomic, no locking needed.

    if type in ('mysql',):
        cur.execute("insert into %s (value) values ('%s')" % (table, dbapi2.Binary(inbox[0])))
    elif type in ('sqlite',):
        cur.execute("insert into %s (value) values (?)" % table, (dbapi2.Binary(inbox[0]),))

    #     
    id_ = cur.lastrowid

    #clean-up
    cur.close()
    con.commit()
    #warnings.simplefilter('default')
    # from mysql-python ... no auto-commit mode.  you'll need to do
    # connection.commit() before closing the connection, or else none of your
    # changes will be written to the database.
    return type, id_, table, kwargs

@imports([['sqlite3', []], ])
def load_db_item(inbox, remove =True):
    """ Loads an item from a sqlite database. Returns the stored string.

        Arguments:

          * remove(bool) [default: True]

            Remove the loaded item from the table (temporary storage).
    """
    type, id_, table, kwargs = inbox[0]
    
    if type =='sqlite':
        dbapi2 = sqlite3.dbapi2
    elif type =='mysql':
        dbapi2 = MySQLdb

    while True:
        try:
            con = dbapi2.connect(**kwargs)
            cur = con.cursor()
            break
        except dbapi2.OperationalError, e:
            if not e.args[0] == 'database is locked':
                raise e
    if type =='sqlite':
        get_item = 'select value from %s where id = ?' % table
        item = str(cur.execute(get_item, (id_,)).fetchone()[0])
    elif type =='mysql':
        get_item = 'select value from %s where id = %s' % (table, id_)
        cur.execute(get_item)
        item = str(cur.fetchone()[0])
    
    # remove the retrieved item.
    if remove:
        if type in ('sqlite',):
            del_item = 'delete from %s where id = ?' % table
            cur.execute(del_item, (id_,))
        elif type in ('mysql',):
            del_item = 'delete from %s where id = %s' % (table, id_)
            cur.execute(del_item)
    
    # clean-up connection
    cur.close()
    con.commit()
    return item




# FILES
@imports([['time',[]]])
def make_lines(handle, follow =False, wait =0.1):
    """ Creates a line generator from a stream (file handle) containing data in
        lines.

        Arguments:

          * follow(bool) [default: False]

            If true follows the file after it finishes like 'tail -f'.

          * wait(float) [default: 0.1]

            Time to wait between file polls.
    """
    while True:
        line = handle.readline()
        if line:
            yield line
        elif follow:
            time.sleep(wait)
        else:
            raise StopIteration

@imports([['mmap', []], ['os',['fstat']]])
def make_items(handle, size):
    """ Creates a generator of items from a file handle. The size argument is
        the approximate size of the generated chunks in bytes. The main purpose
        of this worker function is to allow multiple worker processes/threads to
        read from the same file handle.
        
        A chunk is a 3-tuple (file descriptor, first_byte, last_byte), which 
        defines the position of chunk within a file. The size of a chunk i.e. 
        last_byte - first_byte is **approximately** the ``size`` argument. The
        last byte in a chunk is always a '\\n'. The first byte points 
        always to the first character in a line. A chunk can also be a whole
        file i.e. the first byte is 0 and the last byte is 

        Arguments:

          * size(int) [default: mmap.ALLOCATIONGRANULARITY]

            on windows: 64KBytes
            on linux: 4KBytes

            Approximate chunk size in bytes.
    """
    fd = handle.fileno()
    file_size = fstat(fd).st_size
    size = (size or mmap.ALLOCATIONGRANULARITY)
    mmaped = mmap.mmap(fd, file_size, access=mmap.ACCESS_READ)
    # start at the beginning of file
    start, stop = 0, 0
    while True:
        # stop can be 0 if at the beginning or after a chunk
        # or some value if size was to small to contain a new-line 
        stop = (stop or start) + size
        # reached end of file
        if stop >= file_size:
            yield ((fd, None), start, file_size - 1)
            break
        # try to get a chunk
        last_n = mmaped.rfind('\n', start, stop)
        if last_n != -1:
            yield ((fd, None), start, last_n)
            start = last_n + 1
            stop = 0
        # if no chunk the chunk size will be start, start+size+size in next
        # round.

@imports([['glob', []], ['os',[]], ['tempfile',[]]])
def find_items(prefix ='tmp', suffix ='', dir =None):
    """ Creates a file name generator from files matching the supplied
        arguments. Matches the same files as those created by ``dump_chunk``
        for the same arguments.

        Arguments:

          * prefix(string) [default: 'tmp']

            Mandatory first chars of the files to find.

          * suffix(string) [default: '']

            Mandatory last chars of the files to find.

          * dir(string) [default: current working directory]

            Directory where the files should be located.
    """
    dir = dir or tempfile.gettempdir()
    pattern = os.path.join(dir, prefix + '*' + suffix)
    chunk_files = glob.iglob(pattern)
    while True:
        yield chunk_files.next()


#
# SERIALIZATION
#
# cPickle
@imports([['cPickle',[]], ['gc',[]]])
def pickle_dumps(inbox):
    """ Serializes the first element of the input using the pickle protocol.
    """
    # http://bugs.python.org/issue4074
    gc.disable()
    str = cPickle.dumps(inbox[0], cPickle.HIGHEST_PROTOCOL)
    gc.enable()
    return str

@imports([['cPickle',[]], ['gc',[]]])
def pickle_loads(inbox):
    """ De-serializes the first element of the input using the pickle protocol.
    """
    gc.disable()
    obj = cPickle.loads(inbox[0])
    gc.enable()
    return obj 

# MARSHAL
@imports([['marshal',[]], ['gc',[]]])
def marshal_dumps(inbox):
    """ Serializes the first element of the input using the marshal protocol.
    """
    gc.disable()
    str = marshal.dumps(inbox[0], 2)
    gc.enable()
    return str

@imports([['marshal',[]], ['gc',[]]])
def marshal_loads(inbox):
    """ Serializes the first element of the input using the marshal protocol.
    """
    gc.disable()
    obj = marshal.loads(inbox[0])
    gc.enable()
    return obj 

# JSON
@imports([['simplejson',[]], ['gc',[]]], forgive =True)
def json_dumps(inbox):
    """ Serializes the first element of the input using the JSON protocol.
    """ 
    gc.disable()
    str = simplejson.dumps(inbox[0])
    gc.enable()
    return str

@imports([['simplejson',[]], ['gc',[]]], forgive =True)
def json_loads(inbox):
    """ De-serializes the first element of the input using the JSON protocol.
    """
    gc.disable()
    obj = simplejson.loads(inbox[0])
    gc.enable()
    return obj

# CSV
@imports([['csv',[]], ['cStringIO', []]])
def csv_dumps(inbox, **kwargs):
    handle = cStringIO.StringIO()
    csv_writer = csv.writer(handle, **kwargs)
    csv_writer.writerow(inbox[0])
    return handle.getvalue()

@imports([['csv',[]]])
def csv_loads(inbox):
    pass

