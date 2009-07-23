from IMap import *

@imports([['os',[]], ['socket',[]], ['sys', []]])
def hid(i):
    caller = "%s" % repr(sys._getframe(1).f_locals['self'])
    return "%s is on host:%s, process:%s, using hid:%s, calledby:%s(%s)" %\
    (i[0], socket.gethostname(), os.getpid(), id(hid), caller, 1)

