# standard python tool to work with iterables
from itertools import tee, izip, imap
# the process pool from multiprocessing
from multiprocessing import Pool
# the process/thread parallel (pool) imap from papy
from papy.imap import IMap
# for basic timing
from time import sleep, time
from numpy import mean
from math import floor

def io_bound(inbox):
    sleep(0.5)
    return inbox

def cpu_bound(inbox):
    for i in xrange(100000):
        '%s and %s' %(i , i)
    return inbox

if __name__ == '__main__':

    # test single
    input_data = xrange(8)
    for tool in (imap, IMap, Pool):
        loop_times = []
        if tool is imap:
            output = imap(io_bound, input_data)

        if tool is IMap:
            imp = IMap()
            output = imp.add_task(io_bound, input_data)
            imp.start()

        if tool is Pool:
            pool = Pool()
            output = pool.imap(io_bound, input_data)

        sleep(0)
        abs_start = time()
        start = time()
        for i in output:
            loop_times.append(time() - start)
            start = time()
        abs_stop = time()
        total_time = abs_stop - abs_start

        print 'Tool: %s' % tool
        print 'total time: %f' % total_time
        print 'loop times: %s' % [floor((l+0.05)*10)/10 for l in loop_times]
        print 'maximum loop: %f' % max(loop_times)
        print 'average loop: %f' % mean(loop_times)
        print 'minimum loop: %f' % min(loop_times)


    # test chained
    input_data = xrange(16)
    for tool in (imap, IMap, Pool):
        abs_start = time()
        loop_times = []
        if tool is imap:
            start = time()
            output0 = imap(cpu_bound, input_data)
            output1 = imap(cpu_bound, output0)
            output2 = imap(cpu_bound, output1)
            output3 = imap(cpu_bound, output2)
            output4 = imap(cpu_bound, output3)
            output5 = imap(cpu_bound, output4)
            output = imap(cpu_bound, output5)

        if tool is IMap:
            start = time()
            imp = IMap()
            output1 = imp.add_task(cpu_bound, input_data)
            output2 = imp.add_task(cpu_bound, output1)
            output3 = imp.add_task(cpu_bound, output2)
            output4 = imp.add_task(cpu_bound, output3)
            output5 = imp.add_task(cpu_bound, output4)
            output6 = imp.add_task(cpu_bound, output5)
            output  = imp.add_task(cpu_bound, output6)
            imp.start()

        if tool is Pool:
            start = time()
            pool = Pool()
            output1 = pool.imap(cpu_bound, input_data)
            output2 = pool.imap(cpu_bound, output1)
            output3 = pool.imap(cpu_bound, output2)
            output4 = pool.imap(cpu_bound, output3)
            output5 = pool.imap(cpu_bound, output4)
            output6 = pool.imap(cpu_bound, output5)
            output  = pool.imap(cpu_bound, output6)

        for i in output:
            loop_times.append(time() - start)
            start = time()
        abs_stop = time()
        total_time = abs_stop - abs_start
        print 'Tool: %s' % tool
        print 'total time: %f' % total_time
        print 'maximum loop: %f' % max(loop_times)
        print 'average loop: %f' % mean(loop_times)
        print 'minimum loop: %f' % min(loop_times)
