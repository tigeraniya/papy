#!/usr/bin/env python
from papy import Plumber, Dagger, Piper, Worker
from IMap import IMap, imports
from papy import workers
from papy.utils import logger
logger.start_logger(log_to_screen =False, log_rotate =True)

@imports([['random', []]])
def random_list(inbox, list_size):
    random.seed()
    random_list = [random.random() for i in xrange(list_size)]
    return random_list

def sum_list(inbox):
    return sum(inbox[0])

def pipeline(runtime_):
    imap1, imap2, type, list_size = runtime_
    if type == 'queue':
        w1 = Worker(random_list, (list_size,))
        w2 = Worker((sum_list, workers.io.print_))
    else:
        w1 = Worker((random_list, workers.io.pickle_dumps, workers.io.dump_item),\
                   ((list_size,), (),                      (type,)             ))
        w2 = Worker((workers.io.load_item, workers.io.pickle_loads, sum_list, workers.io.print_),\
                   ((),                    (),                      (), ()))
    p1 = Piper(w1, parallel =imap1, debug =False)
    p2 = Piper(w2, parallel =imap2, debug =False)
    pipes = Plumber()
    pipes.add_pipe((p1, p2))
    return pipes

def runtime(options):
    type = options['--type']
    list_size = int(options['--list_size'])
    worker_num = int(options['--worker_num'])
    imap1 = IMap(worker_num =worker_num)
    imap2 = IMap(worker_num =worker_num)
    return (imap1, imap2, type, list_size)

# Execute:
if __name__ == '__main__':
    import sys
    from getopt import getopt
    options = dict(getopt(sys.argv[1:], '', ['list_size=', 'worker_num=', 'type='])[0])
    runtime_ = runtime(options)
    pipes = pipeline(runtime_)
    input_data = xrange(12)
    pipes.plunge([input_data])

    pipes._is_finished.wait()
    print "using: %s took %fs" % (options['--type'], pipes.stats['run_time'])


#./local_ipc.py --worker_num=1 --type=udp --list_size=10000
