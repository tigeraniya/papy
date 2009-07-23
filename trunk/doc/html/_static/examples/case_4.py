# to run this example start a the classic_server.py fron another shell
# python classic_server.py -m forking
# Note that a forking server can only be run on *NIX OS.
from IMap import *
from papy import *
from pprint import pprint

#from dummy_module import hid
#from papy import *

# this function reports the hostname, process id, function id, and function caller
# the imports decorator injects import statements into function locals on the remote
# server.
@imports([['os',[]], ['socket',[]], ['sys', []]])
def hid(i):
    caller = sys._getframe(1).f_code.co_name
    return "%s is on host:%s, process:%s, using hid:%s, calledby:%s" %\
    (i[0], socket.gethostname(), os.getpid(), id(hid), caller)

def print_(inp):
    """ Just print.
    """ 
    print inp[0]

# we need to define create IMap instances outside __main__
def remote_imap(d):
    # create an IMap with 2 local and 2 fake-remote workers
    Imap = IMap(hid, d, worker_num =2, worker_remote=[('localhost', 2)])
    return Imap

def remote_pipeline():
    Imap = IMap(worker_num =2, worker_remote=[('localhost', 2)])
    # wrap the functions into pipers
    hider = Piper(hid, parallel =Imap)
    printer = Piper(print_)
    # define the topology
    pipes = Plumber()
    pipes.add_pipe((hider, printer))
    return pipes

if __name__ == '__main__':
    data = [[i] for i in range(20)]
    # mixed IMap with 2 local and 4 remote processes 
    Imap = remote_imap(data)
    pprint(list(Imap))
    # output should look similar to this:
    # (snip)
    # '6 is on host:struc5, process:28824, using hid:140333944953984, calledby:worker',
    # '7 is on host:struc5, process:28823, using hid:140333944953984, calledby:worker',
    # '8 is on host:struc5, process:28829, using hid:140562205420552, calledby:_handle_call',
    # (snip)
    # Note that 6,7 are different local processes and 8 is one of the two remote
    # processes. The host_name does not differ if the server is run on the same
    # host. Note also that remote functions have a different id.
    pipes = remote_pipeline()
    # connect input data
    data = xrange(20)
    pipes.set_inputs([data])
    # start calculations/processing in background
    pipes.plunge()
    pipes._is_finished.wait() 
    # the output should be similar e.g.:
    #17 is on host:struc5, process:29448, using hid:140194183998832, calledby:__call__
    #18 is on host:struc5, process:29452, using hid:140562205420552, calledby:_handle_call
    #19 is on host:struc5, process:29453, using hid:140562205420552, calledby:_handle_call





