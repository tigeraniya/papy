"""
:mod:`papy.utils.remote`
========================

Provides a namespace (dictionary) server.
"""
from multiprocessing.managers import BaseManager, DictProxy


SHAREDDICT = {}
class DictServer(BaseManager):
    pass
DictServer.register('dict', lambda: SHAREDDICT, DictProxy)



#EOF
