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
    """ Saves the first element of the inbox to the provided stream (file
        handle) delimiting the input by the optional delimiter string.
    """
    handle.write(inbox[0])
    if delimiter:
        delimiter = '\n' + delimiter + '\n'
        handle.write(delimiter)

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
def mmap_file(handle, chunk):
    fd = handle.fileno()
    file_size = os.fstat(fd).st_size
    mmaped = mmap.mmap(fd, file_size, access=mmap.ACCESS_READ)
    start = 0
    stop = chunk
    while True:
        stop = mmaped.rfind('\n', start, stop)
        yield(fd, start, stop)
        start = stop
        stop = start + chunk




# Pickling
@imports([['cPickle',[]]])
def pickle_dumps(inbox):
    """ Serializes the first element of the input using the pickle protocol.
    """
    return cPickle.dumps(inbox[0])

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

