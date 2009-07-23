# example will not run in the interactive console!
from IMap import IMap
def upcase(string):
   return string.upper()
def plus2(number):
   return number + 2

def imap(function0, function1, input0, input1):
   imap_instance = IMap()
   imap_instance.add_task(upcase, input0)
   imap_instance.add_task(plus2, input1)
   imap_instance.start()
   return imap_instance

if __name__ == '__main__':
    input0, input1 = ['a','b','c'], [1,2,3]
    imap_instance =  imap(upcase, plus2, input0, input1) 
    results0 = imap_instance.get_task(task =0)
    results1 = imap_instance.get_task(task =1)
    print "%s -> %s " % (zip(input0, input1), zip(results0, results1))


