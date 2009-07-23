# This pipeline follows and greps a file for a specified search pattern.
from papy import *
from IMap import IMap

#1. function definitions
@imports([('re',[])])
def grep(inbox, pattern):
    """ Searches input string for pattern.
        returns None if pattern is not found.
    """
    if re.search(pattern, inbox[0]):
        return inbox[0]

#2. parallelism and topology of pipeline
def pipeline(Imap):
    # wrap the functions into pipers
    # follow =True argumetn like 'tail -f' 
    grepper_w = Worker(grep, ('papy',))
    grepper = Piper(grepper_w, parallel =Imap)
    printer = Piper(workers.io.print_)
    # define the topology
    pipes = Plumber()
    pipes.add_pipe((grepper, printer))
    return pipes

#3. run-time
if __name__ == '__main__':
    # 
    for imap_ in [IMap(worker_type ='process', worker_num =2)]:
        # make input input 
        handle = open('input_file', 'rb')
        handle_generator = workers.io.load_file(handle, follow =True)
        # get pipeline instance
        pipes = pipeline(imap_)
        # connect input data
        pipes.set_inputs([handle_generator])
        # start calculations/processing in background
        pipes.plunge()
        print """ In PaPy a thread pulls at the intput (handle_generator)
                  If the generator reaches the end it raises StopIteration
                  which makes PaPy stop gracefully.

                  The follow =True argument tells the generator to follow the
                  file like 'tail -f'. No StopIteration will ever be raised. We
                  We can tell the pipeline to shut-down manually::

                    pipes.chinkup()

                  but the pulling thread will notice this only after it has
                  received a result, because pipelines are not allowed to miss
                  any data-items (or events). We have to generate a sentinel

                    echo 'papy' >> input_file
              """
