""" 
:mod:`papy.workers.io`
========================

A collection of input/output worker functions. Functions dealing with stream
inputs are normal functions i.e. they cannot be used within a worker. 
Four types of functions are provided.

  * logging functions - currently only stdout printing.

  * stream function - load or save the input stream from or into a single file,
    therefore they can only be used at the beginnings or ends of a pipeline.
    Stream loaders are not worker functions.

  * item functions - load, save or process data items.

  * file functions - create streams from the contents of a file or several
    files. These are not worker functions.
"""
import os
if 'PC_PIPE_BUF' in os.pathconf_names:
    # unix
    x, y = os.pipe()
    PIPE_BUF = os.fpathconf(x, "PC_PIPE_BUF")
else:
    # in Jython 16384
    # on windows 512
    # in jython in windows 512
    PIPE_BUF = 512

from IMap import imports
from multiprocessing.managers import BaseManager, DictProxy

SHAREDDICT = {}
class DictServer(BaseManager):
    pass
DictServer.register('dict', lambda: SHAREDDICT, DictProxy)

class DictClient(BaseManager):
    pass
DictClient.register('dict')


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
          ['posix_ipc', []]], forgive = True)
def dump_item(inbox, type ='file', prefix =None, suffix =None, dir =None):
    """ Writes the first element of the inbox as a file of a specified type.
        The type can be 'file', 'fifo' or 'shm' corresponding to typical
        files, named pipes(FIFOs) and posix shared memory. FIFOs and shared
        memory are volatile, but shared memory can exist longer then the python
        process.

        Returns the semi-random name of the file written. By default creates 
        files and fifos in the default temporary directory and shared memory
        in /dev/shm. To use named pipes the operating system has to support
        both forks and fifos (not Windows). To use shared memory the system has
        to be proper posix (not MacOSX) and the posix_ipc module has to be
        installed.

        This worker is useful to efficently communicate parallel pipers without
        the overhead of using queues.

        Arguments:

          * type('file', 'fifo' or 'shm') [default: 'file']
            
            Type of the created file

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
    """
    # get a random filename generator
    names = tempfile._get_candidate_names()
    # try to own the file
    while True:
        prefix = prefix or 'tmp_papy_%s' % type
        suffix = suffix or ''
        file = prefix + names.next() + suffix
        if type in ('file', 'fifo'):
            dir = dir or tempfile.gettempdir()
            file = os.path.join(dir, file)
            try:
                if type == 'file':
                    fd = os.open(file, tempfile._bin_openflags, 0600)
                    tempfile._set_cloexec(fd)
                elif type == 'fifo':
                    os.mkfifo(file)
                file = os.path.abspath(file)
                break
            except OSError, e:
                if e.errno == errno.EEXIST:
                    continue
        elif type == 'shm':
            try:
                mem = posix_ipc.SharedMemory(file, size =len(inbox[0]),\
                                                  flags =posix_ipc.O_CREX)
                break
            except posix_ipc.ExistentialError:
                continue
        else:
            raise ValueError("type: %s not undertood" % type)
    # got a file, fifo or memory
    if type == 'file':
        handle = open(file, 'wb')
        handle.write(inbox[0])
        handle.close() # close handle/fd
    elif type == 'fifo':        
        pid = os.fork()
        if not pid:
            handle = open(file, 'wb')
            handle.write(inbox[0])
            handle.close() # close handle/fd
            os._exit(0)
    elif type == 'shm':
        mapfile = mmap.mmap(mem.fd, mem.size)
        mapfile.write(inbox[0])
        mapfile.close()     # close the memory map
        os.close(mem.fd)    # close the file descriptor
    # filename needs still to be unlinked
    return file

@imports([['mmap', []], ['os',[]], ['papy', []]])
def load_item(inbox, type ='string', close =True, remove =True):
    """ Loads data from a file. Determines the file type automatically ('file',
        'fifo' or 'shm') but allows to specify the representation type 'string' 
        or 'mmap' for memmory mapped access to the file. Returns a the loaded
        item as a string or mmap object.

        Arguments:

          * type('string' or 'mmap') [default: string]

            Determines the type of object the worker returns i.e. the file read
            as a string or a memmory map. FIFOs cannot be memory mapped. 

          * remove(boolean) [default: True]

            Should the file be removed from the filesystem? This is mandatory
            for FIFOs and generally a *very* good idea for shared memory.
    """
    try:
        # did we get an item?
        (fd, name), start, stop = inbox[0]
    except ValueError:
        # if not make one
        (fd, name), start, stop = papy.workers.io.make_item(inbox, remove) 
       
    if type == 'string':
        data = []
        if stop == -1:
            # if file size is 0 it is probably a pipe
            while True:
                buffer = os.read(fd, PIPE_BUF)
                if not buffer:
                    break
                data.append(buffer)
            data = "".join(data)
        else:
            os.lseek(fd, start, 0)
            data = os.read(fd, stop - start + 1)
    
    elif type =='mmap':
        offset = start - (start % mmap.ALLOCATIONGRANULARITY)
        start = start - offset
        stop = stop - offset + 1
        data = mmap.mmap(fd, stop, access=mmap.ACCESS_READ, offset =offset)
        data.seek(start)

    if close:
        os.close(fd)
    if remove and name:
        os.unlink(name)
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
    # get database 
    manager = papy.workers.io.DictClient(address, authkey)
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
    k, address, authkey = inbox[0]
    manager = papy.workers.io.DictClient(address, authkey)
    manager.connect()
    kv = manager.dict()
    if remove:
        v = kv.pop(k)
    else:
        v = kv[k]
    return v
    

@imports([['sqlite3', ['dbapi2']]])
def dump_sqlite_item(inbox, name, table ='papy'):
    """ Writes the first element of the inbox as a value in a sqlite database.
        Returns the name of the database file, table name and row id for the 
        inserted item.

        According to the sqlite documentation: You should avoid putting SQLite
        database files on NFS if multiple processes might try to access the file
        at the same time. 

        Arguments:

          * name(str) [default: 'sqlite.tmp']

            Name of the database file to use. A new file will be created only if
            it does not exist.

          * table(str) [default: 'papy']

            Name of the table to insert the item into. Can be unique per-piper
            or shared.
    """
    # Under Unix, you should not carry an open SQLite database across a fork() 
    # system call into the child process. Problems will result if you do.
    while True:
        try:
        # Calling this routine with an argument less than or equal to zero turns
        # off all busy handlers.  "isolation_level=None" does not actually mean
        # "provide a lower isolation level", it means "use SQLite's inherent
        # isolation /concurrency primitives rather than those in PySQLite".  By
        # default, SQLite version 3 operates in autocommit mode. In autocommit
        # mode, all changes to the database are committed as soon as all
        # operations associated with the current database connection complete.
        # http://www.sqlite.org/lang_transaction.html
        # After a BEGIN IMMEDIATE, you are guaranteed that no other thread or
        # process will be able to write to the database or do a BEGIN IMMEDIATE
        # or BEGIN EXCLUSIVE. 
            con = dbapi2.connect(name, isolation_level ="IMMEDIATE")
            cur = con.cursor()
            cur.execute("create table if not exists %s (id integer primary key\
                         autoincrement, value blob)" % (table,))
            break
        except dbapi2.OperationalError, e:
            # if locked wait ... forever
            if not e.args[0] == 'database is locked':
                raise e 
    # inserts are atomic, no locking needed.
    id_ = con.execute("insert into %s (value) values (?)"
                     % table, (dbapi2.Binary(inbox[0]),)).lastrowid
    con.commit()
    con.close()
    return (name, table, id_)

@imports([['sqlite3', ['dbapi2']]])
def load_sqlite_item(inbox, remove =True):
    """ Loads an item from a sqlite database. Returns the stored string.

        Arguments:

          * remove(bool) [default: True]

            Remove the loaded item from the table (temporary storage).
    """
    name, table, id_ = inbox[0]
    while True:
        try:
             con = dbapi2.connect(name)
             break
        except dbapi2.OperationalError, e:
            if not e.args[0] == 'database is locked':
                raise e
    get_item = 'select value from %s where id = ?' % table
    item = str(con.execute(get_item, (id_,)).fetchone()[0])
    if remove:
        del_item = 'delete from %s where id = ?' % table
        con.execute(del_item, (id_,))
    con.close()
    return item



def dump_redis_item(inbox, name):
    pass

def load_redis_item(inbox, name):
    pass

# FILES
@imports([['os', []], ['posix_ipc', []], ['stat', []]], forgive =True)
def make_item(inbox, remove =True):
    """ Creates an item from a file name. The file has to exist it can be a
        normal file, a named pipe(FIFO) or POSIX shared memory.

        Shared memory files should be unlinked at this stage.

        Arguments:

          * remove(boolean) [default: True]

            Should the file be removed from the filesystem?
    """
    name = inbox[0]
    try:
        # if this succeeds we have pipe or a file in the CWD.
        size = os.stat(name).st_size
        fd = os.open(name, os.O_RDONLY)
        if remove and not stat.S_ISFIFO(os.fstat(fd).st_mode):
            # do not remove if name is a fifo
            os.unlink(name)
            name = None
    except OSError:
        # if this succeeds we have a shared memory fie.
        memory = posix_ipc.SharedMemory(name)
        size = memory.size
        fd = memory.fd
        if remove:
            # this is bettern then os.unlink ... I think.
            memory.unlink()
            name = None
    return ((fd, name), 0, size - 1)

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

