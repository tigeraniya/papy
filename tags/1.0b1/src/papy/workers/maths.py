""" 
:mod:`papy.workers.maths`
=========================

This module contains worker-functions for common math to use in *PaPy* *Worker*
instances. It wraps around functions from the ``operator`` and ``math`` Python
standard library modules. Any function from ``math`` and ``operator`` is 
included in this library if it is related to evaluations.

If a function accepts multiple arguments two flavours of wrappers are provided:

  1. simple wrapper: math.X(arg1, arg2) should be called from
     papy.workers.maths.X as X([arg1], arg2)

  2. star wrapper: math.X(arg1, arg2) should be called from
     papy.workers.maths.Xstar ars Xstar(arg)

In other words 1. should be used to construct *Workers* which accept arguments 
at construction and 2. should be used to construct *Workers* which accept 
multiple inputs.
"""
import operator, math
from IMap import imports

# operator module
@imports(['operator'])
def abs(inbox):
    return operator.abs(inbox[0])

@imports(['operator'])
def add(inbox, b):
    return operator.add(inbox[0], b)

@imports(['operator'])
def addstar(inbox):
    return operator.add(*inbox)

@imports(['operator'])
def div(inbox, b):
    return operator.div(inbox[0], b)

@imports(['operator'])
def divstar(inbox):
    return operator.div(*inbox)

@imports(['operator'])
def floordiv(inbox, b):
    return operator.floordiv(inbox[0], b)

@imports(['operator'])
def floordivstar(inbox):
    return operator.floordiv(*inbox)

@imports(['operator'])
def lshift(inbox, b):
    return operator.lshift(inbox[0], b)

@imports(['operator'])
def mod(inbox, b):
    return operator.mod(inbox[0], b)

@imports(['operator'])
def modstar(inbox):
    return operator.mod(*inbox)

@imports(['operator'])
def sub(inbox, b):
    return operator.sub(inbox[0], b)

@imports(['operator'])
def substar(inbox):
    return operator.sub(*inbox)

@imports(['operator'])
def mul(inbox, b):
    return operator.mul(inbox[0], b)

@imports(['operator'])
def mulstar(inbox):
    return operator.mul(*inbox)


# math module
@imports(['math'])
def acos(inbox):
    return math.acos(inbox[0])

@imports(['math'])
def asin(inbox):
    return math.asin(inbox[0])

@imports(['math'])
def atan(inbox):
    return math.atan(inbox[0])

@imports(['math'])
def atan2(inbox, x):
    return math.atan2(inbox[0], x)

@imports(['math'])
def atan2star(inbox):
    return math.atan2(*inbox)

@imports(['math'])
def ceil(inbox):
    return math.ceil(inbox[0])

@imports(['math'])
def cos(inbox):
    return math.cos(inbox[0])

@imports(['math'])
def cosh(inbox):
    return math.cosh(inbox[0])

@imports(['math'])
def degrees(inbox):
    return math.degrees(inbox[0])

@imports(['math'])
def exp(inbox):
    return math.exp(inbox[0])

@imports(['math'])
def fabs(inbox):
    return math.fabs(inbox[0])

@imports(['math'])
def floor(inbox):
    return math.floor(inbox[0])

@imports(['math'])
def fmodr(inbox, y):
    return math.fmod(inbox[0], y)

@imports(['math'])
def fmodstar(inbox):
    return math.fmod(*inbox)

@imports(['math'])
def frexp(inbox):
    return math.frexp(inbox[0])

@imports(['math'])
def hypot(inbox):
    return math.hypot(inbox[0])

@imports(['math'])
def ldexp(inbox, i):
    return math.ldexp(inbox[0], i)

@imports(['math'])
def ldexpstar(inbox):
    return math.ldexp(*inbox)

@imports(['math'])
def log(inbox, base=math.e):
    return math.log(inbox[0], base)

@imports(['math'])
def logstar(inbox):
    return math.log(*inbox)

@imports(['math'])
def log10(inbox):
    return math.log10(inbox[0])

@imports(['math'])
def modf(inbox):
    return math.modf(inbox[0])

@imports(['math'])
def powstar(inbox):
    return math.pow(*inbox)

@imports(['math'])
def pow(inbox, y):
    return math.pow(inbox[0], y)

@imports(['math'])
def radians(inbox):
    return math.radians(inbox[0])

@imports(['math'])
def sin(inbox):
    return math.sin(inbox[0])

@imports(['math'])
def sinh(inbox):
    return math.sinh(inbox[0])

@imports(['math'])
def sqrt(inbox):
    return math.sqrt(inbox[0])

@imports(['math'])
def tan(inbox):
    return math.tan(inbox[0])

@imports(['math'])
def tanh(inbox):
    return math.tanh(inbox[0])



#EOF
