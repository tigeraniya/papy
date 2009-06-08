""" 
:mod:`papy.workers.io`
========================

A collection of input/output worker functions.
"""
from IMap import imports


# Printing
def print_(inbox):
    """ Prints the first element of the inbox.
    """
    print inbox[0]

# LINEAR 
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

        Arguments:

          * delimiter(string) [default: None]

            The default means that items will be separated by 
            a blank line i.e.: '\\n\\n'
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

# PARALLEL
@imports([['tempfile',['mkstemp']], ['os', ['fdopen', 'getcwd']]])
def dump_chunk(inbox, prefix ='tmp', suffix ='', dir =None):
    """ Writes the first element of the inbox as a chunk file. Returns the name
        of the file written. By default creates the file in the current working
        directory. A chunk file is a file which is a single chunk.

        This worker is useful to persistantly store data and communicate
        parallel pipers without the overhead of using queues.

        For a description of the chunk see ``get_chunks``.

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

@imports([['os', ['stat', 'open']]])
def load_chunk(inbox):
    """ Creates a chunk from a chunk file. The whole file will be a single chunk.

        For a dsecription of the chunk file see ``dump_chunk``.
        For a description of the chunk see ``get_chunks``.
    """
    file_name = inbox[0]
    file_size = stat(file_name)
    handle = open(file_name, 'rb')
    fd = handle.fileno()
    return (fd, 0, file_size - 1)

@imports([['glob', ['glob']],['os',['getcwd', 'path']]])
def find_chunks(prefix ='tmp', suffix ='', dir =None):
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
    chunk_files = glob(pattern)
    while True:
        yield chunk_files.next()
    
@imports([['mmap', []]])
def mmap_chunk(inbox):
    """ Returns a mmap object (memory mapped file) from a chunk which should be
        the first and only element of the inbox. This can be faster then reading
        the chunk. You should **not** call the ``seek`` method of the mmap
        object as the beginning of the object is **not** the beginning of the
        chunk. The index returned ``tell`` is the first byte of the chunk, the
        last byte is the last byte of the mmap object. 

        For a description of the chunk see ``get_chunks``.
    """
    fd, start, stop = inbox[0]
    offset = start - (start % mmap.ALLOCATIONGRANULARITY)
    start = start - offset
    stop = stop - offset + 1
    mmaped = mmap.mmap(fd, stop, access=mmap.ACCESS_READ, offset =offset)
    mmaped.seek(start)
    return mmaped

@imports([['os', ['fdopen']]])
def read_chunk(inbox):
    """ Reads out a string from a chunk which should be the first and only
        element in the inbox. This might be slower then memmory mapping the
        chunk. The first and last bytes of the string are the first and last
        bytes of the chunk.

        For a description of the chunk see ``get_chunks``.
    """
    fd, start, stop = inbox[0]
    handle = fdopen(fd, 'rb')
    handle.seek(start)
    return handle.read(stop - start + 1)

#EXTERNAL
@imports([['time',[]]])
def get_lines(handle, follow =False, wait =0.1):
    """ Creates a line generator from a file handle. This worker is useful if
        working with  

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

@imports([['mmap', []], ['os',[]]])
def get_chunks(handle, size):
    """ Creates a generator of chunks from a file handle. The size argument is
        the approximate size of the generated chunks in bytes.
        
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
    file_size = os.fstat(fd).st_size
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
            yield (fd, start, file_size - 1)
            break
        # try to get a chunk
        last_n = mmaped.rfind('\n', start, stop)
        if last_n != -1:
            yield (fd, start, last_n)
            start = last_n + 1
            stop = 0
        # if no chunk the chunk size will be start, start+size+size in next
        # round.

    
# Pickling
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

@imports([['cPickle',[]]])
def load_pickle(handle):
    """ Creates a generator from a stream (file handle) containing pickles of
        objects. This is faster than load + pickle_loads.

        .. warning::

          The file handle should not be used by any other
          object/thread/process.
    """
    while True:
        try:
            yield cPickle.load(handle)
        except EOFError:
            raise StopIteration

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

