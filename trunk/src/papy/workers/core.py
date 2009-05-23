""" 
:mod:`papy.workers.core`
========================

A collection of core workers-functions to use in Worker instances.
"""


def ipasser(inbox, i =0):
    """ Passes the i-th input from inbox. By default passes the first input.

        Arguments:

          * i(int) [default: 0]
    """
    return inbox[i]

def npasser(inbox, n =None):
    """ Passes n first inputs from inbox. By default passes the whole inbox.

        Arguments:

          * n(int) [default: None]
    """
    return inbox[:n]

def spasser(inbox, s =None):
    """ Passes inputs with indecies in s. By default passes the whole inbox.

        Arguments:

          * s(sequence) [default: None -> range(len(inbox))]
    """
    s = (s or range(len(inbox)))
    return [input for i, input in enumerate(inbox) if i in s] 




