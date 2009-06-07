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

# Dumping to file-handle
def dump(inbox, handle, delimiter =None):
    """ Writes the first element of the inbox to the provided stream (file
        handle) delimiting the input by the optional delimiter string.

        Note that only a single process can have access to a file handle open
        for writing.

        Arguments:

          * handle(stream)

            File handle open for writing.
          
          * delimiter(string) [default: None]

            A string which will seperate the written items. e.g:
            "END" becomes "\nEND\n" in the output stream
    """
    handle.write(inbox[0])
    if delimiter:
        delimiter = '\n' + delimiter + '\n'
        handle.write(delimiter)

def dump_iter(inbox, handle):
    """ Writes the first element of the inbox to the provided stream (file
        handle), the first element is assumed to be a sequence and each item
        of the sequence is written as a separate line.
    """
    for item in inbox[0]:
        handle.write("%s\n" % item)

@imports([['tempfile',['mkstemp']], ['os', ['fdopen', 'getcwd']]])
def dump_chunk(inbox, prefix ='chunk', suffix ='', dir =None):
    """ Saves the first element of the inbox as a new file.
    """
    dir = dir or os.getcwd()
    fd, name = mkstemp(suffix, prefix, dir)
    handle = fdopen(fd, 'wb')
    handle.write(inbox[0])
    return name

@imports([['os', ['stat']]])
def load_chunk(inbox):
    """ Creates a chunk from a file name.
    """
    file_name = inbox[0]
    file_size = os.stat(file_name)
    handle = open(file_name, 'rb')
    fd = handle.fileno()
    return (fd, 0, file_size - 1)

def load(handle, delimiter):
    """ Creates a generator from a stream (file handle) containing data
        delimited by delimiter strings.
    """
    delimiter = delimiter + '\n'
    while True:
        temp = []
        while True:
            line = handle.readline()
            if line == delimiter:
                # remove introduced '\n'
                temp[-1] = temp[-1][:-1]
                break
            elif line == '':
                raise StopIteration
            else:
                temp.append(line)
        yield "".join(temp)

@imports([['time',[]]])
def load_file(handle, follow =False, wait =0.1):
    """ Creates a line generator from a stream (file handle).

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
def chunk_file(handle, size):
    """ Creates a file chunk generator. A chunk is a tuple (handle, first_byte,
        last_byte). The size of each chunk i.e. last_byte - first_byte is
        *approximately* the ``size`` argument. The first byte points always to
        the first character in a line the last byte is a new-line character or
        EOF.
    
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

@imports([['mmap', []]])
def mmap_chunk(inbox):
    """ Given a chunk i.e. (handle, first_byte, last_byte) creates a mmap object
        which contains the chunk. The last byte of the mmap object is the last
        byte of the chunk, but the first byte of the object *is not* the first
        byte of the chunk. The position of the pointer is the start of the
        chunk.
    """
    fd, start, stop = inbox[0]
    offset = start - (start % mmap.ALLOCATIONGRANULARITY)
    start = start - offset
    stop = stop - offset + 1
    mmaped = mmap.mmap(fd, stop, access=mmap.ACCESS_READ, offset =offset)
    mmaped.seek(start)
    return mmaped

@imports([['os', ['fdopen', 'remove']]])
def read_chunk(inbox, lines =False, delete =False):
    """ Reads out data from a chunk either as a string or as a list of lines.
        Optionally deletes the associated file.
    """
    fd, start, stop = inbox[0]
    handle = fdopen(fd, 'rb')
    readout = handle.readlines() if lines else handle.read()
    handle.close()
    if delete:
        remove(handle.name)
    
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

