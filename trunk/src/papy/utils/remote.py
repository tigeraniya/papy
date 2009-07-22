""" A dictionary server.
"""
import os
from multiprocessing.managers import BaseManager, DictProxy

from papy.utils.defaults import DEFAULTS

SHAREDDICT = {}
class DictServer(BaseManager):
    pass
DictServer.register('dict', lambda: SHAREDDICT, DictProxy)

    

