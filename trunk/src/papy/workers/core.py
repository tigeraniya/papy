""" 
:mod:`papy.workers.core`
========================

A collection of core workers-functions to use in Worker instances.
"""

def plugger(inbox):
    """ Returns nothing.
    """
    return None

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

def nzipper(inbox, n =None):
    """ 
    """
    return zip(*inbox[:None])[0]

def ijoiner(inbox, i =0, join =""):
    """ Joins and returns the elements of the input with index i.
    """
    return join.join(inbox[i])

def njoiner(inbox, n =None, join =""):
    """ Joins and returns the first n inputs.
    """
    return join.join(inbox[:n])

def sjoiner(inbox, s =None, join =""):
    """ Joins and returns the first input with indices in s.
    """
    return join.join([input for i, input in enumerate(inbox) if i in s])
