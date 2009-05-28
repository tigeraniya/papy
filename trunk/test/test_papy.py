from __future__ import nested_scopes
from itertools import izip, chain, imap
from exceptions import StopIteration
from time import sleep, time
from random import randint
from math import ceil, sqrt
import unittest
import operator
from numpy import mean
from multiprocessing import TimeoutError
from papy import *
from papy.papy import compose, imports, Produce, Consume, Chain
from IMap import *
import logging
from papy.utils import logger
from functools import partial
logger.start_logger(log_level =logging.DEBUG, log_to_screen =False, log_rotate =True)




def powr(inbox, arg):
    return operator.pow(inbox[0], arg)
def times_m_minus_n(inbox, m, n):
    return inbox[0]*m - n
def power(i):
    return i[0]*i[0]

def sqrr(i):
    return sqrt(i[0])

def passer(i):
    return i[0]
def double(i):
    return 2*i[0]
def sleeper(i):
    sleep(i[0])
    return i
def pow2(i):
    return i[0]*i[0]
def sum2(i):
    return i[0]+i[1]
def ss2(i):
    return i[0][0] + i[1][0]

@imports([['math', ['sqrt']]])
def ss(i):
    res = sum([sqrt(j[0]) for j in i])
    return res

@imports([('re',[])])
def retre_yes(i):
    return re

@imports([('sys',['version'])])
def sys_ver(i):
    return version

def retre_no(i):
    return re

class GeneratorTest(unittest.TestCase):

    def setUp(self):
        self.gen4 = (i for i in xrange(1,4))
        self.gen10 = (i for i in xrange(1,10))
        self.gen15 = (i for i in xrange(1,15))
        self.gen20 = (i for i in xrange(1,20))
        self.sleep4a = (i for i in xrange(1,4) if sleeper([i]))
        self.sleep4b = (i for i in xrange(1,4) if sleeper([i]))

class test_Graph(unittest.TestCase):

    def setUp(self):
        self.graph = Graph()
        self.repeats = 20

    def test_Node(self):
        node = Node(1)
        self.assertEqual(node,{1:{}})
        node = Node('1')
        self.assertEqual(node,{'1':{}})
        node = Node()
        self.assertEqual(node,{})
        node = Node(())
        self.assertEqual(node,{():{}})
        self.assertRaises(TypeError,Node,{})
        self.assertRaises(TypeError,Node,[])

    def test_node(self):
        self.graph.add_node(1)
        self.assertTrue(isinstance(self.graph[1], Node))
        self.assertEqual(self.graph[1], {})
        self.graph.del_node(1)
        for i in xrange(self.repeats):
            node = randint(0,100)
            self.graph.add_node(node)
            self.graph.del_node(node)
            self.assertFalse(self.graph)
            self.assertFalse(self.graph.nodes())
            self.assertFalse(self.graph.edges())
        self.assertEqual(self.graph, {})

    def test_nodes(self):
        for i in xrange(self.repeats):
            nodes = set([])
            for i in xrange(self.repeats):
                nodes.add(randint(1,10))
            self.graph.add_nodes(nodes)
            self.assertEqual(nodes,set(self.graph.nodes()))
            self.graph.del_nodes(nodes)
            self.assertFalse(self.graph)

    def test_edge(self):
        for i in xrange(self.repeats):
            edge = (randint(0,49), randint(50,100))
            double = randint(0,1)
            self.graph.add_edge(edge, double)
            self.assertTrue(edge in self.graph.edges())
            if double:
                self.assertTrue((edge[1], edge[0]) in self.graph.edges())
            self.graph.del_edge(edge, double)
            assert bool(self.graph[edge[0]]) is False
            assert bool(self.graph[edge[1]]) is False
            self.assertFalse(edge in self.graph.edges())
            self.assertFalse((edge[1], edge[0]) in self.graph.edges())

    def test_edges_random(self):
        for i in xrange(self.repeats):
            self.graph = Graph()
            edges = set()
            for i in xrange(self.repeats):
                edges.add((randint(0,49), randint(50,100)))
            self.graph.add_edges(edges)
            gotedges = set(self.graph.edges())
            self.assertEqual(edges, gotedges)
            self.graph.del_edges(edges)
            gotedges = set(self.graph.edges())
            self.assertEqual(set([]), gotedges)

        for i in xrange(self.repeats):
            edges = set()
            alledges = set()
            for i in xrange(self.repeats):
                edge = (randint(0,49), randint(50,100))
                edges.add(edge)
                alledges.add(edge)
                alledges.add((edge[1], edge[0]))
            self.graph.add_edges(edges, double =True)
            gotedges = set(self.graph.edges())
            self.assertEqual(alledges, gotedges)
            self.graph.del_edges(edges, double =True)
            gotedges = set(self.graph.edges())
            self.assertEqual(set([]), gotedges)

    def test_edges_manual(self):
        self.graph.add_edge(('sum2','pwr'))
        self.graph.add_edge(('sum2','dbl'))
        self.graph.add_edge(('dbl','pwr'))
        self.graph.del_node('pwr')
        self.assertEqual({'sum2': {'dbl': {}}, 'dbl': {}}, self.graph)
        self.graph.del_node('dbl')
        self.assertRaises(KeyError, self.graph.del_node, 'pwr')
        self.assertEqual({'sum2': {}}, self.graph)
        self.graph.del_node('sum2')
        self.assertFalse(self.graph)
        self.graph.add_edge(('sum2','pwr'))
        self.graph.del_edge(('sum2','pwr'))
        self.assertEqual({'sum2': {}, 'pwr': {}}, self.graph)
        self.assertTrue(self.graph)

    def test_dfs(self):
        edges = [(1,2), (3,4), (5,6), (1,3), (1,5), (1,6), (2,5)]
        self.graph.add_edges([(1,2), (3,4), (5,6), (1,3), (1,5), (1,6), (2,5)])
        self.assertEqual(len(self.graph.dfs(1, [])), 6)
        self.graph.clear_nodes()
        self.assertEqual(len(self.graph.dfs(4, [])), 1)
        self.assertEqual(len(self.graph.dfs(1, [])), 5) # 4 is not clear
        self.graph.clear_nodes()
        self.assertEqual(len(self.graph.dfs(2, [])), 3)
        self.graph.clear_nodes()
        self.graph.add_edge((4,1))
        a = []
        self.assertEqual(len(self.graph.dfs(1, a)), 6)
        self.assertEqual(a, [6, 5, 2, 4, 3, 1])

    def test_postorder(self):
        edges = [(1,2), (3,4), (5,6), (1,5), (1,6), (2,5), (4,6)]
        self.graph.add_edges(edges)
        self.graph.postorder()
        self.graph.clear_nodes()
        self.graph.postorder(reverse =True)
        self.graph.clear_nodes()
        self.graph.preorder(reverse =True)


class test_Worker(GeneratorTest):

    def test_single(self):
        pwr = Worker(power)
        pwr = pwr([2])
        self.assertEqual(pwr, 2*2)
        dbl = Worker(double)
        dbl = dbl([3])
        self.assertEqual(dbl, 2*3)

    def test_eq(self):
        pwr = Worker(power)
        dbl = Worker(double)
        pwr2 = Worker(power)
        self.assertEqual(pwr, pwr2)
        self.assertEqual(hash(pwr), hash(pwr2))
        assert pwr is not pwr2
        self.assertNotEqual(pwr, dbl)
        self.assertNotEqual(pwr, 'abcd')
        self.assertNotEqual('abcd', pwr)
        pwrdbl = Worker([power, double])
        pwrdbl2 = Worker([power, double])
        dblpwr = Worker([double, power])
        self.assertEqual(pwrdbl, pwrdbl2)
        self.assertEqual(hash(pwrdbl), hash(pwrdbl2))
        self.assertNotEqual(pwrdbl, dblpwr)
        self.assertNotEqual(hash(pwrdbl), hash(dblpwr))
        self.assertNotEqual(pwr, pwrdbl)
        assert pwrdbl is not pwrdbl2
        assert pwr is not pwr2

    def test_double(self):
        pwr = Worker(power)
        dbl = Worker(double)
        dblpwr = dbl([pwr([3])])
        self.assertEqual(dblpwr,2*(3*3))
        pwrdbl = pwr([dbl([3])])
        self.assertEqual(pwrdbl,(2*3)*(2*3))

    def test_nested(self):
        dblpwr = Worker([power, double])
        dblpwr = dblpwr([3])
        self.assertEqual(dblpwr,2*(3*3))
        pwrdbl = Worker([double, power])
        pwrdbl = pwrdbl([3])
        self.assertEqual(pwrdbl,(2*3)*(2*3))

    def test_keyword(self):
        pwr = Worker(powr, (2,))
        self.assertEqual(pwr((4,)), 16)
        pwr = Worker(powr, (3,))
        self.assertEqual(pwr((2,)), 8)

    def test_keywords(self):
        mn = Worker(times_m_minus_n, (3,7))
        self.assertEqual(mn((4,)), 5)
        mn = Worker(times_m_minus_n, (2,1))
        self.assertEqual(mn((4,)), 7)
        mn.args = ((1,1),)
        self.assertEqual(mn((4,)), 3)

    def test_multi(self):
        multi0 = Worker((powr, times_m_minus_n), ((2,),(3,7)))
        self.assertEqual(multi0((4,)), 41)
        multi = Worker((double, pow2))
        self.assertEqual(multi((1,)), 4)
        multi2 = Worker(multi0)
        multi3 = Worker(multi)
        self.assertEqual(multi3((1,)), 4)
        self.assertEqual(multi2((4,)), 41)
        self.assertEqual(multi3, multi)
        self.assertEqual(multi2, multi0)

    def test_input(self):
        # good input
        w = Worker(powr)
        w = Worker([powr])
        x = Worker(w)
        x = Worker([x])
        z = Worker([power, double])
        z = Worker([w, x])

    def test_IMap(self):
        imap = IMap()
        pw2 = Worker(pow2)
        imap.add_task(pw2, [[1],[2],[3],[4]])
        imap.start()
        for i,j in izip(imap, [[1],[2],[3],[4]]):
            self.assertEqual(i,j[0] * j[0])

    def test_IMap2(self):
        imap = IMap()
        pw2 = Worker(powr, (2,))
        imap.add_task(pw2, [[1],[2],[3],[4]])
        imap.start()
        for i,j in izip(imap, [[1],[2],[3],[4]]):
            self.assertEqual(i,j[0] * j[0])

    def test_imports(self):
        @imports([('re',[]), ('sys',[])])
        def pr(i):
            return (re, sys)
        pr(1)

    def test_compose(self):
        def plus(i):
            return i+1
        def minus(i):
            return i-1
        assert 0 == compose(0, ((),()), funcs =(plus, minus))
        plus_minus = partial(compose, funcs =(plus, minus))
        assert 0 == plus_minus(0, ((),()))


    def test_sys_ver(self):
        assert [('sys',['version'])] == sys_ver.imports
        assert sys_ver.func_globals.get('version')
        rr = Worker(sys_ver)
        assert rr([1]) == __import__('sys').version

    def test_imports_re(self):
        assert [('re',[])] == retre_yes.imports
        assert not hasattr(retre_no, 'imports')
        assert retre_yes.func_globals.get('re')
        rr = Worker(retre_yes)
        assert rr([1]) == __import__('re')

    def test_exceptions(self):
        self.assertRaises(TypeError, Worker, 1)
        self.assertRaises(TypeError, Worker, [1])

class test_Piper(GeneratorTest):

    def xtest_logger(self):
        from logging import Logger
        pwr = Piper(power, parallel =1)
        self.assertTrue(isinstance(pwr.log, Logger))

    def xtest_pool(self):
        pwr = Piper(power)
        dbl = Piper(double)
        assert pwr is not dbl
        assert pwr.imap == dbl.imap
        poolx = IMap()
        pooly = IMap()
        pwr = Piper(power, parallel = poolx)
        dbl = Piper(double, parallel = pooly)
        assert pwr is not dbl
        assert pwr.imap != dbl.imap
        pwr = Piper(power)
        self.assertEqual(pwr.imap, imap)

    def xtest_eq(self):
        pwr = Piper(power)
        pwrs = [pwr]
        self.assertEqual(pwr, pwrs[0])
        pwr2 = Piper(power)
        self.assertNotEqual(pwr, pwr2)
        self.assertEqual(pwr.worker, pwr2.worker)

    def xtest_basic_call(self):
        pool = IMap()
        for i in range(10):
            ppr_instance = Piper(power, parallel =pool)
            ppr_busy = ppr_instance([[1, 2, 3, 4]])
            assert ppr_instance is ppr_busy
            self.assertRaises(PiperError, ppr_busy.next)
            ppr_busy.start()
            for i in izip(ppr_busy, [1,2,3,4]):
                self.assertEqual(i[0], i[1]*i[1])
            self.assertRaises(StopIteration, ppr_busy.next) # it. protocol
            self.assertRaises(StopIteration, ppr_busy.next) # it. protocol
            assert ppr_busy.imap._started.isSet()
            ppr_busy.stop()
            assert not ppr_busy.imap._started.isSet()
            self.assertRaises(PiperError, ppr_busy.next)

        ppr_instance = Piper(power)
        ppr_busy = ppr_instance([[1, 2, 3, 4]])
        assert ppr_instance is ppr_busy
        for i in ppr_busy:
            pass
        self.assertRaises(StopIteration, ppr_busy.next) # it. protocol
        self.assertRaises(StopIteration, ppr_busy.next) # it. protocol

    def xtest_connects(self):
        pool = IMap()
        ppr_instance = Piper(power, parallel =pool)
        ppr_busy = ppr_instance([[7,2,3]])
        assert ppr_instance is ppr_busy
        self.assertRaises(PiperError, ppr_busy,[[7,2,3]]) # second connect
        self.assertRaises(PiperError, ppr_busy.next)      # not started
        ppr_busy.disconnect()
        self.assertRaises(PiperError, ppr_busy.start)
        assert not ppr_busy.imap._tasks
        ppr_busy.connect([[1,2,3]])
        ppr_busy.start()
        assert ppr_busy.next() == 1
        ppr_busy.stop()
        self.assertRaises(RuntimeError, ppr_busy.imap.next)

    def xtest_connects2(self):
        pool = IMap()
        ppr_1 = Piper(power, parallel =pool)
        ppr_2 = Piper(double, parallel =pool)
        ppr_1busy = ppr_1([[7,2,3]])
        ppr_2busy = ppr_2([[7,2,3]])
        assert (ppr_1busy, ppr_2busy) == (ppr_1, ppr_2)
        self.assertRaises(PiperError,ppr_1busy,[[7,2,3]]) # second connect
        self.assertRaises(PiperError,ppr_1busy,[[7,2,3]]) # second connect

        self.assertRaises(PiperError, ppr_1busy.next)     # not started
        self.assertRaises(PiperError, ppr_2busy.next)     # not started
        self.assertRaises(PiperError, ppr_1busy.disconnect)
        self.assertRaises(PiperError, ppr_2busy.disconnect)

        self.assertRaises(PiperError, ppr_1busy.start)
        self.assertRaises(PiperError, ppr_2busy.start)

        ppr_2busy.disconnect(forced =True)
        ppr_1busy.disconnect()

        assert not pool._tasks

        ppr_2busy.connect([[7,2,3]])
        ppr_1busy.connect([[1,1,1]])

        ppr_1busy.start(forced =True)
        assert ppr_1busy.next() == 1
        assert ppr_2busy.next() == 14
        ppr_2busy.stop(forced =[0,1])
        self.assertRaises(RuntimeError, ppr_1busy.imap.next)
        self.assertRaises(RuntimeError, ppr_2busy.imap.next)

    def xtest_connect_empty(self):
        passer = Piper(workers.core.ipasser)
        passer([[1]])
        passer.next()
        self.assertRaises(StopIteration, passer.next)
        passer = Piper(workers.core.ipasser)
        passer([[]])
        self.assertRaises(StopIteration, passer.next)
        passer = Piper(workers.core.ipasser)
        passer([])
        self.assertRaises(StopIteration, passer.next)

    def xtest_output_pickle(self):
        import os
        handle = os.tmpfile()
        data = [{1:1},{2:2}]
        pickler = Worker(workers.io.pickle_dumps)
        dumper = Worker(workers.io.dump, (handle,))
        pickle_piper = Piper(pickler)
        dump_piper = Piper(dumper)
        pickle_piper([data])
        dump_piper([pickle_piper])     
        list(dump_piper)
        handle.seek(0)
        input = workers.io.load_pickle(handle)
        passer = Piper(workers.core.ipasser)
        passer([input])
        a = list(passer)
        assert a == [{1: 1}, {2: 2}]
        handle.close()


    def xtest_output_simplejson(self):
        import os
        handle = os.tmpfile()
        data = [{'1':1},{'2':2}]
        sj = Worker(workers.io.json_dumps)
        dumper = Worker(workers.io.dump, (handle, '---'))
        sj_piper = Piper(sj, debug =True)
        dump_piper = Piper(dumper)
        sj_piper([data])
        dump_piper([sj_piper])     
        list(dump_piper)
        handle.seek(0)
        input = workers.io.load(handle, '---')
        passer = Piper(workers.io.json_loads)
        passer([input])
        a = list(passer)
        assert a == [{'1': 1}, {'2': 2}]
        handle.close()

    def xtest_connect_pickle(self):
        handle = open('test_pick', 'rb')
        input = workers.io.load_pickle(handle)
        passer = Piper(workers.core.ipasser)
        assert list(passer([input])) == [[1, 2, 3], [1, 2, 3], [1, 2, 3], [1, 2, 3],\
        [4, 5, 6], [4, 5, 6], [4, 5, 6], [4, 5, 6], [4, 5, 6], [4, 5, 6], [4, 5, 6],\
        [4, 5, 6], [4, 5, 6], [4, 5, 6], [4, 5, 6], [4, 5, 6]]

    def xtest_sort(self):
        p2 = Piper(workers.core.ipasser, ornament =2)
        p1 = Piper(workers.core.ipasser, ornament =1)
        a = [p2, p1]
        a.sort(cmp =Piper._cmp_ornament)
        assert a == [p1, p2]

    def xtest_failure(self):
        pwr = Piper(power)
        pwr = pwr([[1,'a',3]])
        pwr.start() # should not raise even if not needed
        self.assertEqual(pwr.next(), 1)
        self.assertTrue(isinstance(pwr.next(), PiperError))
        self.assertEqual(pwr.next(), 9)
        self.assertRaises(StopIteration, pwr.next)
        #pwr = Piper(power, debug =True)
        #pwr = pwr([[1,'a',3]])
        #pwr.start()
        #self.assertEqual(pwr.next(), 1)
        #self.assertRaises(PiperError, pwr.next)
        #self.assertEqual(pwr.next(), 9)
        #self.assertRaises(StopIteration, pwr.next)

        #pool = IMap()
        #pwr = Piper(power, parallel =pool)
        #pwr = pwr([[1,'a',3]])
        #pwr.start() # should work
        #self.assertEqual(pwr.next(), 1)
        #self.assertTrue(isinstance(pwr.next(), PiperError))
        #self.assertEqual(pwr.next(), 9)
        #self.assertRaises(StopIteration, pwr.next)
        #pwr = Piper(power, debug =True)
        #pwr = pwr([[1,'a',3]])
        #pwr.start()
        #self.assertEqual(pwr.next(), 1)
        #self.assertRaises(PiperError, pwr.next)
        #self.assertEqual(pwr.next(), 9)
        #self.assertRaises(StopIteration, pwr.next)

    def xtest_chained_failure(self):
        from exceptions import StopIteration
        pwr = Piper(power)
        dbl = Piper(double)
        pwr = pwr([[1,'a',3]])
        pwr.start()
        dbl = dbl([pwr])
        dbl.start()
        self.assertEqual(dbl.next(), 2)
        a = dbl.next()
        self.assertTrue(isinstance(a, PiperError))  # this is what dbl return (it wrapped what it got)
        self.assertTrue(isinstance(a[0], PiperError))       # wrapped in the workers piper
        self.assertTrue(isinstance(a[0][0], WorkerError))   # wrapped in the worker
        self.assertTrue(isinstance(a[0][0][0], TypeError))  # raised in the worker
        self.assertEqual(dbl.next(), 18)
        self.assertRaises(StopIteration, dbl.next)

        pool =IMap()
        pwr = Piper(power, parallel =pool)
        dbl = Piper(double, parallel =pool)
        pwr = pwr([[1,'a',3]])
        dbl = dbl([pwr])
        self.assertRaises(PiperError, pwr.start)
        self.assertRaises(PiperError, dbl.start)
        dbl.start(forced =True)
        self.assertEqual(dbl.next(), 2)
        a = dbl.next()
        self.assertTrue(isinstance(a, PiperError))  # this is what dbl return (it wrapped what it got)
        self.assertTrue(isinstance(a[0], PiperError))       # wrapped in the workers piper
        self.assertTrue(isinstance(a[0][0], WorkerError))   # wrapped in the worker
        self.assertTrue(isinstance(a[0][0][0], TypeError))  # raised in the worker
        self.assertEqual(dbl.next(), 18)
        self.assertRaises(StopIteration, dbl.next)
        self.assertRaises(StopIteration, pwr.next)
        dbl.stop()
        self.assertRaises(PiperError, pwr.next)


    def xtest_verysimple(self):
        for par in (False, IMap()):
            gen10 = (i for i in xrange(1,10))
            gen15 = (i for i in xrange(1,15))
            ppr = Piper(power, parallel =par)
            ppr = ppr([gen15])
            ppr.start()
            for i,j in izip(ppr, xrange(1,15)):
                self.assertEqual(i,j*j)
            ppr.stop()
            ppr = Piper(power, parallel =par)
            ppr = ppr([gen10])
            ppr.start()
            for i,j in izip(ppr, xrange(1,10)):
                self.assertEqual(i,j*j)

    def xtest_single(self):
        for par in (False, IMap()):
            gen10 = (i for i in xrange(1,10))
            gen15 = (i for i in xrange(1,15))
            gen20 = (i for i in xrange(1,20))
            ppr = Piper(power, parallel =par)
            ppr = ppr([gen10])
            ppr.start()
            for i,j in izip(ppr, xrange(1,20)):
                self.assertEqual(i,j*j)
            ppr.stop()
            ppr = Piper([power], parallel =par)
            ppr = ppr([gen15])
            ppr.start()
            for i,j in izip(ppr, xrange(1,15)):
                self.assertEqual(i,j*j)
            ppr.stop()
            pwr = Worker(power)
            ppr = Piper(pwr,  parallel =par)
            ppr = ppr([gen20])
            ppr.start()
            for i,j in izip(ppr, xrange(1,20)):
                self.assertEqual(i,j*j)
            ppr.stop()

    def xtest_double(self):
        for par in (False, IMap()):
            gen10 = (i for i in xrange(1,10))
            gen15 = (i for i in xrange(1,15))
            gen20 = (i for i in xrange(1,20))
            ppr = Piper([power, double], parallel =par)
            ppr = ppr([gen20])
            ppr.start()
            for i,j in izip(ppr, xrange(1,20)):
                self.assertEqual(i,2*j*j)
            ppr.stop()
            pwrdbl = Worker([power,double])
            ppr = Piper(pwrdbl, parallel =par)
            ppr = ppr([gen15])
            ppr.start()
            for i,j in izip(ppr, xrange(1,15)):
                self.assertEqual(i,2*j*j)
            ppr.stop()
            dblpwr = Worker([double,power])
            ppr = Piper(dblpwr, parallel =par)
            ppr = ppr([gen10])
            ppr.start()
            for i,j in izip(ppr, xrange(1,10)):
                self.assertEqual(i,(2*j)*(2*j))
            ppr.stop()

    def xtest_linked(self):
        for par in (False, IMap()):
            gen10 = (i for i in xrange(10))
            pwr = Piper(power, parallel =par)
            dbl = Piper(double, parallel =par)
            dbl([gen10])
            ppr = pwr([dbl])
            ppr.start(forced =True)
            for i,j in izip(ppr, (i for i in xrange(10))):
                self.assertEqual(i,(2*j)*(2*j))
            ppr.stop()

    def xtest_linked2(self):
        for par in (False, IMap()):
            gen10 = (i for i in xrange(10))
            pwr = Piper(power, parallel =par)
            dbl = Piper(double, parallel =par)
            ppr = pwr([dbl([gen10])])
            ppr.start(forced =True)
            for i,j in izip(ppr, (i for i in xrange(10))):
                self.assertEqual(i,(2*j)*(2*j))
            ppr.stop()

    def xtest_exceptions(self):
        self.assertRaises(PiperError, Piper, 1)
        self.assertRaises(PiperError, Piper, [1])

    def xtest_linear(self):
        for par in (False, IMap()):
            gen20 = (i for i in xrange(1,20))
            piper = Piper(pow2, parallel =par)
            piper([gen20])
            piper.start()
            for i,j in izip(piper,xrange(1,20)):
                self.assertEqual(i,j*j)
            self.assertRaises(StopIteration, piper.next)
            piper.stop()

    def xtest_Produce(self):
        product = Produce(iter([0,1,2,3,4,5,6]), n=2, stride=3)
        result = []
        for i in range(24):
            try:
                result.append(product.next())
            except StopIteration:
                result.append('s')
        self.assertEqual(result,
        [0,1,2,0,1,2,3,4,5,3,4,5,6,'s','s',6,'s','s','s','s','s','s','s','s'])
    def xtest_Consume(self):
        consumpt =\
        Consume(iter([0,1,2,0,1,2,3,4,5,3,4,5,6,'s','s',6,'s','s','s','s','s','s','s','s']), stride =3, n=2)
        result = []
        for i in range(12):
            result.append(consumpt.next())
        self.assertEqual(result,[[0, 0], [1, 1], [2, 2], [3, 3],
                                [4, 4], [5, 5], [6, 6], ['s', 's'], 
                                ['s', 's'], ['s', 's'], ['s', 's'], ['s', 's']]) 
    def xtest_produce(self):
        inp = [0,1,2,3,4,5,6]
        par = IMap(stride =3)
        w_p2 = Worker(pow2)
        p_p2 = Piper(w_p2, parallel =par, produce =2)
        p_p2 = p_p2([inp])
        p_p2.start()
        result = []
        for i in range(25):
            try:
                result.append(p_p2.next())
            except StopIteration:
                result.append('s')
        self.assertEqual(result,
        [0,1,4,0,1,4,9,16,25,9,16,25,36,'s','s',36,'s','s','s','s','s','s','s','s','s'])

    def xtest_produce_error(self):
        inp = [0,1,'z',3,4,5,6]
        par = IMap(stride =3)
        w_p2 = Worker(pow2)
        p_p2 = Piper(w_p2, parallel =par, produce =2, debug =True)
        p_p2 = p_p2([inp])
        p_p2.start()
        result = []
        for i in range(25):
            try:
                result.append(p_p2.next())
            except StopIteration:
                result.append('s')
            except Exception, e:
                result.append('e')
        self.assertEqual(result,
        [0,1,'e',0,1,'e',9,16,25,9,16,25,36,'s','s',36,'s','s','s','s','s','s','s','s','s'])

    def xtest_Chain(self):
        inp1 = [0,1,2,3,4,5,6]
        inp2 = [1,2,3,4,5,6,7]
        par = IMap(stride =3)
        w_p2 = Worker(pow2)
        p_p2 = Piper(w_p2, parallel =par, produce =2)
        p_p3 = Piper(w_p2, parallel =par, produce =2)
        p_p2 = p_p2([inp1])
        p_p3 = p_p3([inp2])
        p_p2.start(forced =True)
        chainer = Chain([p_p2, p_p3], stride =3)
        result = []
        for i in range(36):
            try:
                result.append(chainer.next())
            except StopIteration:
                result.append('s')
        self.assertEqual(result, [0, 1, 4, 1, 4, 9, 0, 1, 4, 1, 4, 9, 9, 16, 25, 16, 25, 36,
                                  9, 16, 25, 16, 25, 36, 36, 's', 's', 49, 's', 's', 36,
                                  's', 's', 49, 's', 's',]) 

    def xtest_consume(self):
        inp = [0,1,4,0,1,4,9,16,25,9,16,25,36,'s','s',36,'s','s','s','s','s','s','s','s']
        par = IMap(stride =3)
        w_s2 = Worker(ss2)
        p_s2 = Piper(w_s2, parallel =par, consume =2)
        p_s2 = p_s2([inp])
        p_s2.start()
        result = list(p_s2)
        self.assertEqual([0, 2, 8, 18, 32, 50, 72, 'ss', 'ss', 'ss', 'ss', 'ss'], result)

    def xtest_produce_consume(self):
        inp = [0,1,2,3,4,5,6]
        par = IMap(stride =3)
        w_p2 = Worker(pow2)
        p_p2 = Piper(w_p2, parallel =par, produce =2)
        p_p2 = p_p2([inp])
        w_s2 = Worker(ss2)
        p_s2 = Piper(w_s2, parallel =par, consume =2)
        p_s2 = p_s2([p_p2])
        p_s2.start(forced =True)
        self.assertEqual(list(p_s2), [0, 2, 8, 18, 32, 50, 72])

    def xtest_produce_spawn_consume(self):
        inp = [0,1,2,3,4,5,6]
        par = IMap(stride =3)
        w_p2 = Worker(pow2)
        p_p2 = Piper(w_p2, parallel =par, produce =2)
        p_p2 = p_p2([inp])
        w_sq = Worker(sqrr)
        p_sq = Piper(w_sq, parallel =par, spawn =2)
        p_sq = p_sq([p_p2]) 
        w_s2 = Worker(ss2)
        p_s2 = Piper(w_s2, parallel =par, consume =2)
        p_s2 = p_s2([p_sq])
        p_s2.start(forced =True)
        self.assertEqual(list(p_s2), [0.0, 2.0, 4.0, 6.0, 8.0, 10.0, 12.0])

    def xtest_produce_consume2(self):
        for st in (1,2,3,4,5):
            for par in (IMap(stride =st),None):
                from math import sqrt
                inp = [1,2,3,4,5]
                w_p2 = Worker(pow2)
                p_p2 = Piper(w_p2, parallel =par, produce =200)
                p_p2 = p_p2([inp])
                w_ss = Worker(ss)
                p_ss = Piper(w_ss, parallel =par, consume =200)
                p_ss([p_p2])
                p_ss.start(forced =True)
                for j in [1,2,3,4,5]:
                    self.assertAlmostEqual(p_ss.next(),200*j)
                p_ss.stop([1])

    def xtest_timeout(self):
        par = IMap(worker_num =1)
        piper = Piper(sleeper, parallel =par, timeout =0.75)
        inp = [0.5,1.0,0.5]
        piper([inp])
        piper.start()
        assert piper.next()[0] == 0.5 # get 1
        a = piper.next() # get timeout
        self.assertTrue(isinstance(a, PiperError))
        self.assertTrue(isinstance(a[0], TimeoutError))
        assert piper.next()[0] == 1.0
        assert piper.next()[0] == 0.5
        piper.stop()



class test_Dagger(unittest.TestCase):

    def setUp(self):
        # pipers
        self.sm2 = Piper(sum2)
        self.pwr = Piper(power)
        self.dbl = Piper(double)
        self.spr = Piper(sleeper)
        self.pwrdbl = Piper([power, double])
        self.dblpwr = Piper([double, power])
        self.dbldbl = Piper([double, double])
        self.pwrpwr = Piper([power, power])
        # workers
        self.w_sm2 = Worker(sum2)
        self.w_pwr = Worker(power)
        self.w_dbl = Worker(double)
        self.w_spr = Worker(sleeper)
        self.w_pwrdbl = Worker([power, double])
        self.w_dblpwr = Worker([double, power])
        self.w_dbldbl = Worker([double, double])
        self.w_pwrpwr = Worker([power, power])
        self.dag = Dagger()

    #def test_init(self):
        #dag1 = Dagger()
        #assert not dag1
        #dag2 = Dagger((self.pwrpwr,self.dbldbl),((self.pwr, self.dbl), (self.dbl, self.pwrdbl)))
        #assert dag2
        #assert dag1 != dag2

    def test_logger(self):
        from logging import Logger
        self.assertTrue(isinstance(self.dag.log, Logger))

    def testresolve(self):
        self.dag = Dagger((self.dbl,self.pwr))
        self.assertEqual(len(self.dag.nodes()), 2)
        assert self.dbl is self.dag.resolve(self.dbl)
        assert self.pwr is self.dag.resolve(id(self.pwr))
        assert self.pwr != self.dag.resolve(id(self.dbl))
        self.assertRaises(DaggerError, self.dag.resolve, self.w_pwr)
        dbl = Piper(double)
        self.assertRaises(DaggerError, self.dag.resolve, dbl)
        self.assertRaises(DaggerError, self.dag.resolve, id(dbl))
        w_dbl = Worker(double)
        self.assertRaises(DaggerError, self.dag.resolve, self.w_dbl)

    def test_make_piper(self):
        assert self.pwr is not Piper(self.pwr)
        assert self.pwr is not Piper(power)
        assert self.pwr is not Piper(self.w_pwr)
        self.dag.add_piper(self.pwr)
        assert self.pwr is self.dag.resolve(self.pwr)

    def test_add_piper(self):
        self.dag.add_piper(self.pwr)
        self.dag.add_piper(self.pwr)
        self.assertEqual(len(self.dag), 1)
        self.assertEqual(len(self.dag.nodes()), 1)
        self.dag.add_pipers([self.dbl])
        self.assertEqual(len(self.dag), 2)
        self.assertRaises(DaggerError,self.dag.add_piper,[1])
        self.assertRaises(DaggerError,self.dag.add_piper,[self.dblpwr])
        self.assertRaises(DaggerError,self.dag.add_piper,1)
        self.assertRaises(DaggerError,self.dag.add_pipers,[1])
        self.assertRaises(TypeError, self.dag.add_pipers,1)
        self.dag.add_pipers([self.dblpwr, self.pwrdbl])
        self.assertEqual(len(self.dag), 4)

    def test_incoming(self):
        self.dag.add_pipe((self.pwr, self.dbl, self.pwrdbl))
        self.assertRaises(DaggerError, self.dag.del_piper, self.pwr)
        self.assertRaises(DaggerError, self.dag.del_piper, self.dbl)
        self.dag.del_piper(self.pwrdbl)
        self.dag.del_piper(self.dbl)
        self.dag.del_piper(self.pwr)


    def test_del_piper(self):
        self.dag.add_piper(self.pwr)
        self.dag.del_piper(self.pwr)
        self.dag.add_piper(self.pwr)
        self.dag.del_piper(id(self.pwr))
        self.assertEqual(len(self.dag), 0)
        self.assertEqual(len(self.dag.nodes()), 0)
        self.assertRaises(DaggerError, self.dag.add_piper, self.w_pwr, create =False)
        self.assertRaises(DaggerError, self.dag.add_piper, [1], create =True)
        self.dag.add_piper(self.w_pwr)
        self.assertEqual(len(self.dag), 1)
        self.dag.add_piper(self.dbl)
        self.dag.add_piper(self.dbl)
        self.assertEqual(len(self.dag), 2)
        dbl = Piper(double)
        self.dag.add_piper(dbl)
        self.assertEqual(len(self.dag), 3)
        self.assertEqual(len(self.dag.nodes()), 3)
        self.dag.del_piper(self.dbl)
        self.assertEqual(len(self.dag), 2)
        self.assertRaises(DaggerError, self.dag.del_piper, self.dbl)
        self.assertRaises(DaggerError, self.dag.del_piper, Piper(self.w_pwr))
        self.assertRaises(DaggerError, self.dag.del_piper, self.w_pwr)
        self.assertRaises(DaggerError, self.dag.del_piper, 77)
        self.assertRaises(DaggerError, self.dag.del_piper, [77])
        self.dag.add_piper(self.pwr)
        self.dag.del_piper(self.pwr)
        self.dag.add_piper(self.pwr)
        self.assertEqual(len(self.dag), 3)
        self.assertRaises(DaggerError, self.dag.del_piper, [self.pwr])
        self.dag.del_pipers([self.pwr])
        self.assertEqual(len(self.dag), 2)
        self.dag.add_piper(self.pwr)
        ids = [id(i) for i in self.dag]
        self.dag.del_pipers(ids)
        self.assertEqual(len(self.dag), 0)
        self.assertEqual(len(self.dag.nodes()), 0)
        self.dag.add_piper(self.pwr)
        pwr = Piper(self.w_pwr)
        self.dag.add_piper(pwr)
        self.assertEqual(len(self.dag), 2)
        pipers = self.dag.nodes()
        self.dag.del_pipers(pipers)
        self.assertEqual(len(self.dag), 0)
        self.assertEqual(len(self.dag.nodes()), 0)
        self.assertRaises(DaggerError, self.dag.del_piper, self.dbl)
        self.assertRaises(DaggerError, self.dag.del_pipers,\
                                               [self.dbl, self.pwrpwr])
        self.assertRaises(DaggerError, self.dag.del_piper, (1,2))

    def test_incoming2(self):
        self.dag.add_pipe((self.pwr, self.dbl, self.pwrdbl))
        self.assertTrue(self.pwr in self.dag)
        self.assertRaises(DaggerError, self.dag.del_piper, self.pwr)
        self.dag.del_piper(self.pwr, forced =True)
        self.assertFalse(self.pwr in self.dag)

    def test_add_workers(self):
        self.dag.add_piper(self.w_pwr)
        self.dag.add_pipers([self.w_pwr])
        self.assertEqual(len(self.dag), 2)
        self.assertEqual(len(self.dag.nodes()), 2)
        self.assertRaises(DaggerError,self.dag.add_piper,[1])
        self.assertRaises(DaggerError,self.dag.add_piper,1)
        self.dag.add_pipers([self.w_pwr, self.w_dbl])
        self.dag.add_piper(self.w_pwrdbl)
        self.assertEqual(len(self.dag), 5)
        self.dag.add_piper(self.w_pwrdbl)
        self.assertRaises(DaggerError,self.dag.add_piper,self.w_pwrdbl, create =False)
        self.assertEqual(len(self.dag), 6)

    def test_add_pipe(self):
        self.dag = Dagger()
        self.dag.add_pipe((self.pwr, self.dbl))
        self.assertEqual(len(self.dag.nodes()),2)
        self.assertEqual(len(self.dag.edges()),1)
        self.dag.add_pipe((self.dbl, self.pwrdbl))
        self.assertEqual(len(self.dag.nodes()),3)
        self.assertEqual(len(self.dag.edges()),2)
        self.assertEqual(len(self.dag.deep_nodes(self.pwrdbl)),2)

    def test_del_pipe(self):
        self.dag.add_pipe((self.pwr, self.sm2))
        self.dag.add_pipe((self.pwr, self.dbl))
        self.dag.add_pipe((self.dbl, self.sm2))
        self.assertEqual(len(self.dag.edges()), 3)
        self.assertEqual(len(self.dag.nodes()), 3)
        self.assertEqual(len(self.dag.incoming_edges(self.pwr)),2)
        self.dag.del_pipe((self.dbl, self.sm2))
        self.assertEqual(len(self.dag.nodes()), 1)

    def test_del_pipe2(self):
        self.dag.add_pipe((self.pwr, self.dbl, self.pwrpwr, self.dblpwr))
        self.dag.add_pipe((self.pwr, self.spr))
        self.dag.del_piper(self.dblpwr)
        self.assertRaises(DaggerError,self.dag.del_piper,self.pwr)
        self.dag.del_pipe((self.pwr, self.spr))
        assert self.pwr in self.dag
        assert self.spr not in self.dag

    def test_circular_prevention(self):
        self.dag.add_pipe((self.dbl, self.dbldbl, self.sm2))
        self.assertRaises(DaggerError, self.dag.add_pipe, (self.sm2, self.dbl))
        self.dag.add_pipe((self.dbl, self.sm2))

    def test_connect_simple(self):
        for par in (False, IMap()):
            self.dag = Dagger()
            inp  = [1,2,3,4]
            pwr = Piper(pow2, parallel =par)
            dbl = Piper(double, parallel =par)
            self.dag.add_pipe((pwr, dbl))
            pwr([inp])
            self.dag.connect()
            pwr.start(forced =True)
            self.assertEqual(list(dbl), [2, 8, 18, 32])
            pwr.stop(forced =[1])

    def test_inputoutput(self):
        for par in (False, IMap()):
            self.dag = Dagger()
            pwr = Piper(pow2, parallel =par)
            dbl = Piper(double, parallel =par)
            pwr2 = Piper(pow2, parallel =par)
            self.dag.add_piper(pwr)
            assert pwr is self.dag.get_inputs()[0]
            assert pwr is self.dag.get_outputs()[0]
            self.dag.add_pipe((pwr, dbl))
            assert len(self.dag) == 2
            assert pwr is self.dag.get_inputs()[0]
            assert dbl is self.dag.get_outputs()[0]
            self.dag.add_pipe((pwr, pwr2))
            assert len(self.dag) == 3
            assert pwr is self.dag.get_inputs()[0]
            assert len(self.dag.get_outputs()) == 2
            assert pwr not in self.dag.get_outputs()
            assert pwr2  in self.dag.get_outputs()
            assert dbl  in self.dag.get_outputs()
            self.assertRaises(DaggerError, self.dag.add_pipe, (pwr2, pwr))



class test_Plumber(GeneratorTest):

    def setUp(self):
        self.sm2 = Piper(sum2)
        self.pwr = Piper(power)
        self.mul = Piper(workers.maths.mul)
        i = IMap()
        self.pwrp = Piper(power, parallel =i)
        self.dblp = Piper(double, parallel =i)
        self.dbl = Piper(double)
        self.spr = Piper(sleeper)
        self.pwrdbl = Piper([power, double])
        self.dblpwr = Piper([double, power])
        self.dbldbl = Piper([double, double])
        self.pwrpwr = Piper([power, power])
        self.plum = Plumber()

    def test_init(self):
        assert isinstance(self.plum, Dagger)

    def test_code(self):
        self.plum.add_piper(self.mul, xtra ={'color':'red'})
        #print self.plum[self.mul].xtra
        self.plum.add_piper(self.pwr)
        rr = Worker(sys_ver)
        pr = Piper(rr, parallel =IMap(name ='illo'))
        self.plum.add_piper(self.pwrp)
        self.plum.add_piper(pr)
        self.plum.add_pipe((self.mul, self.pwr))
        #print self.plum.save('test.py')

    def test_pluge1(self):
        #imap
        self.plum.add_pipe([self.pwr, self.dbl])
        self.pwr([[1,2,3]])
        self.plum.plunge()
        self.plum.chinkup()
        # IMap
        self.plum = Plumber()
        self.plum.add_pipe([self.pwrp, self.dblp])
        self.pwrp([[1,2,3]])
        self.plum.plunge()
        self.plum.chinkup()


suite_Graph = unittest.makeSuite(test_Graph,'test')
suite_Worker = unittest.makeSuite(test_Worker,'test')
suite_Piper = unittest.makeSuite(test_Piper,'xtest')
suite_Dagger = unittest.makeSuite(test_Dagger,'test')
suite_Plumber = unittest.makeSuite(test_Plumber,'test')

if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(suite_Graph)
    runner.run(suite_Worker)
    runner.run(suite_Piper)
    runner.run(suite_Dagger)
    runner.run(suite_Plumber)

#EOF
