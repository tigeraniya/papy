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
from IMap import imports

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
        in pickles. This is equivalnet to ``load_stream`` and ``pickle_loads``,
        but possibly faster.

        .. warning::

            File handles should not be read by different processes.
    """
    while True:
        try:
            yield cPickle.load(handle)
            handle.read(2) # read '\n\n'
        except EOFError:
            raise StopIteration

# ITEMS
@imports([['tempfile',['mkstemp']], ['os', ['fdopen', 'getcwd']]])
def dump_item(inbox, prefix ='tmp', suffix ='', dir =None):
    """ Writes the first element of the inbox as a file. Returns the name
        of the file written. By default creates the file in the current working
        directory.

        This worker is useful to persistantly store data and communicate
        parallel pipers without the overhead of using queues.

        Arguments:

          * prefix(string) [default: 'tmp']

            Prefix of the file to be created. Should probably identify the
            worker and piper.

          * suffix(string) [default: '']

            Suffix of the file to be created. Should probably identify the file
            format of the chunk.

          * dir(string) [default: current working directory]

            Directory where the file will be created.
    """
    dir = dir or getcwd()
    fd, name = mkstemp(suffix, prefix, dir)
    handle = fdopen(fd, 'wb')
    handle.write(inbox[0])
    handle.close()
    return name

@imports([['os', ['stat', 'open', 'unlink']]])
def load_item(inbox, remove =True):
    """ Creates an item from a on-disk file.
    """
    name = inbox[0]
    size = stat(name).st_size
    fd = open(name, os.O_RDONLY)
    if remove:
        unlink(name)
    return ((fd, name), 0, size - 1)

@imports([['mmap', []], ['os',[]]])
def mmap_item(inbox):
    """ Returns a mmap object (memory mapped file) from a chunk which should be
        the first and only element of the inbox. This can be faster then reading
        the chunk. You should **not** call the ``seek`` method of the mmap
        object as the beginning of the object is **not** the beginning of the
        chunk. The index returned ``tell`` is the first byte of the chunk, the
        last byte is the last byte of the mmap object. 

        For a description of the chunk see ``get_chunks``.
    """
    (fd, name), start, stop = inbox[0]
    offset = start - (start % mmap.ALLOCATIONGRANULARITY)
    start = start - offset
    stop = stop - offset + 1
    mmaped = mmap.mmap(fd, stop, access=mmap.ACCESS_READ, offset =offset)
    mmaped.seek(start)
    return mmaped

@imports([['os', ['read', 'lseek', 'unlink']]])
def read_item(inbox):
    """ Reads out a string from a chunk which should be the first and only
        element in the inbox. This might be slower then memmory mapping the
        chunk. The first and last bytes of the string are the first and last
        bytes of the chunk.

        For a description of the chunk see ``get_chunks``.
    """
    (fd, name), start, stop = inbox[0]
    lseek(fd, start, 0)
    return read(fd, stop - start + 1)


@imports([['tempfile', []], ['mmap', []], ['posix_ipc', []]], forgive =True)
def dumpshm_item(inbox):
    """ Writes the first element of the inbox as POSIX shared memory. Returns
        the name of the file written in /dev/shm/.

        This worker is useful to temporarily communicate parallel pipers without
        the overhead of using queues or on-disk files.
    """
    # get a random filename generator
    names = tempfile._get_candidate_names()
    while True:
        n = names.next()
        try: # try to create new shared memory for filename
            memory = posix_ipc.SharedMemory(n, size =len(inbox[0]), flags =posix_ipc.O_CREX)
            break
        except posix_ipc.ExistentialError:
            pass
    mapfile = mmap.mmap(memory.fd, memory.size)
    mapfile.write(inbox[0])
    mapfile.close() # file-handle needs still to be unlinked
    return n

@imports([['posix_ipc', []]], forgive =True)
def loadshm_item(inbox, remove =True):
    """ Creates an item from a POSIX shared memory file.
    """
    n = inbox[0]
    memory = posix_ipc.SharedMemory(n)
    if remove:
        memory.unlink()
    return ((memory.fd, n), 0, memory.size - 1)
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

@imports([['glob', ['iglob']],['os',['getcwd', 'path']]])
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
    dir = dir or getcwd()
    pattern = path.join(dir, prefix + '*' + suffix)
    chunk_files = iglob(pattern)
    while True:
        yield chunk_files.next()


#
# SERIALIZATION
#
# cPickle
@imports([['cPickle',[]]])
def pickle_dumps(inbox):
    """ Serializes the first element of the input using the pickle protocol.
    """
    return cPickle.dumps(inbox[0], cPickle.HIGHEST_PROTOCOL)

@imports([['cPickle',[]]])
def pickle_loads(inbox):
    """ De-serializes the first element of the input using the pickle protocol.
    """
    return cPickle.loads(inbox[0])

# JSON
@imports([['simplejson',[]]], forgive =True)
def json_dumps(inbox):
    """ Serializes the first element of the input using the JSON protocol.
    """ 
    return simplejson.dumps(inbox[0])

@imports([['simplejson',[]]], forgive =True)
def json_loads(inbox):
    """ De-serializes the first element of the input using the JSON protocol.
    """     
    return simplejson.loads(inbox[0])

# CSV
@imports([['csv',[]]])
def csv_dumps(inbox, handle):
    pass

@imports([['csv',[]]])
def csv_loads(inbox, handle):
    pass

