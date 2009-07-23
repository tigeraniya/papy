from papy import *

# constructs a worker using the sqrt function
w_sqrt = Worker(workers.maths.sqrt)

# workers accept boxed input
input = [7]

result = w_sqrt(input)
# but return unboxed:
print 'boxed          unboxed'
print '%s  --sqrt-->  %s' % (input, result)


