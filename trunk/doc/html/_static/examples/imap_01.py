# example will not run in the interactive console!
from IMap import IMap
def upcase(string):
   return string.upper()
def imap(input):
   return IMap(upcase, input)
if __name__ == '__main__':
    input = ['a','b','c']
    output = imap(input)
    print "%s -> %s" % (input, list(output))
