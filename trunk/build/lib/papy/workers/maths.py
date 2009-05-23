""" This module contains common math functions to use in papy Worker instances.
"""
import math
import operator as op

def add(inbox, arg):
    return op.add(inbox[0], arg)

def sub(inbox, arg):
    return op.sub(inbox[0], arg)

def mul(inbox, arg):
    return op.mul(inbox[0], arg)

def div(inbox, arg):
    return op.truediv(inbox[0], arg)

def pow(inbox, arg):
    return op.pow(inbox[0], arg)