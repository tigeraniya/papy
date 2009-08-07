""" 
:mod:`papy.utils.defaults`
==========================

Provides/discovers OS-dependent defaults for different variables.
"""
from IMap import imports
import os, socket

@imports(['os', 'socket', 'collections'])
def get_defaults():
    """
    Returns a dictionary of variables and their possibly os-dependent defaults.
    """
    DEFAULTS = {}
    # Determine the run-time pipe read/write buffer.
    if 'PC_PIPE_BUF' in os.pathconf_names:
        # unix
        x, y = os.pipe()
        DEFAULTS['PIPE_BUF'] = os.fpathconf(x, "PC_PIPE_BUF")
    else:
        # in Jython 16384
        # on windows 512
        # in jython in windows 512
        DEFAULTS['PIPE_BUF'] = 512

    # Determine the run-time socket buffers.
    # Note that this number is determine on the papy server
    # and inherited by the clients.
    tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    DEFAULTS['TCP_SNDBUF'] = tcp_sock.getsockopt(socket.SOL_SOCKET, \
                                                 socket.SO_SNDBUF)
    DEFAULTS['TCP_RCVBUF'] = tcp_sock.getsockopt(socket.SOL_SOCKET, \
                                                 socket.SO_RCVBUF)
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    DEFAULTS['UDP_SNDBUF'] = udp_sock.getsockopt(socket.SOL_SOCKET, \
                                                 socket.SO_SNDBUF)
    DEFAULTS['UDP_RCVBUF'] = udp_sock.getsockopt(socket.SOL_SOCKET, \
                                                 socket.SO_RCVBUF)

    # check the ip visible from the world.
    DEFAULTS['WHATS_MYIP_URL'] = \
    'http://www.whatismyip.com/automation/n09230945.asp'
    return DEFAULTS



#EOF
