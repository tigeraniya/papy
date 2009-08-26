from __future__ import nested_scopes
from itertools import izip, chain, imap
from exceptions import StopIteration
from time import sleep, time
from random import randint, choice
from math import ceil, sqrt
import unittest
import os
import operator
from multiprocessing import TimeoutError
import multiprocessing
from papy import *
from papy.papy import comp_task, imports, _Produce, Produce, Consume, Chain
from IMap import *
import logging
from papy.utils import logger
from functools import partial
logger.start_logger(log_to_file_level=logging.DEBUG)

import threading
def powr(inbox, arg):
    return operator.pow(inbox[0], arg)

def times_m_minus_n(inbox, m, n):
    return inbox[0] * m - n
def power(i):
    #print multiprocessing.current_process(),
    #print i
    return i[0] * i[0]

def sqrr(i):
    return sqrt(i[0])

def args_and_kwargs(inbox, arg1, arg2, frame):
    return frame

def passer(i):
    return i[0]

def double(i):
    #print multiprocessing.current_process(),
    #print 'double', i
    return 2 * i[0]

def sleeper(i):
    sleep(i[0])
    return i

def pow2(i):
    return i[0] * i[0]

def mul2(i):
    return i[0] * 2

def sum2(i):
    return i[0] + i[1]
def ss2(i):
    return i[0][0] + i[1][0]

@imports(['math'])
def ss(i):
    res = sum([math.sqrt(j[0]) for j in i])
    return res

@imports(['re'])
def retre_yes(i):
    return re

@imports(['sys'])
def sys_ver(i):
    return sys.version

@imports(['ysy,sys'])
def ysy_sys(i):
    return ysy

@imports(['multiprocessing.connection'])
def mul_con(i):
    return connection

@imports(['X.Y,multiprocessing.forking'])
def X_Y(i):
    return Y

@imports(['xml.dom.domreg'])
def xml_dom_domreg(i):
    return domreg


def retre_no(i):
    return re

class GeneratorTest(unittest.TestCase):

    def setUp(self):
        self.gen4 = (i for i in xrange(1, 4))
        self.gen10 = (i for i in xrange(1, 10))
        self.gen15 = (i for i in xrange(1, 15))
        self.gen20 = (i for i in xrange(1, 20))
        self.sleep4a = (i for i in xrange(1, 4) if sleeper([i]))
        self.sleep4b = (i for i in xrange(1, 4) if sleeper([i]))

class test_Graph(unittest.TestCase):

    def setUp(self):
        self.graph = Graph()
        self.repeats = 20

    def test_Node(self):
        node = Node(1)
        self.assertEqual(node, {1:{}})
        node = Node('1')
        self.assertEqual(node, {'1':{}})
        node = Node()
        self.assertEqual(node, {})
        node = Node(())
        self.assertEqual(node, {():{}})
        self.assertRaises(TypeError, Node, {})
        self.assertRaises(TypeError, Node, [])

    def test_node(self):
        self.graph.add_node(1)
        self.assertTrue(isinstance(self.graph[1], Node))
        self.assertEqual(self.graph[1], {})
        self.graph.del_node(1)
        for i in xrange(self.repeats):
            node = randint(0, 100)
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
                nodes.add(randint(1, 10))
            self.graph.add_nodes(nodes)
            self.assertEqual(nodes, set(self.graph.nodes()))
            self.graph.del_nodes(nodes)
            self.assertFalse(self.graph)

    def test_edge(self):
        for i in xrange(self.repeats):
            edge = (randint(0, 49), randint(50, 100))
            double = randint(0, 1)
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
                edges.add((randint(0, 49), randint(50, 100)))
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
                edge = (randint(0, 49), randint(50, 100))
                edges.add(edge)
                alledges.add(edge)
                alledges.add((edge[1], edge[0]))
            self.graph.add_edges(edges, double=True)
            gotedges = set(self.graph.edges())
            self.assertEqual(alledges, gotedges)
            self.graph.del_edges(edges, double=True)
            gotedges = set(self.graph.edges())
            self.assertEqual(set([]), gotedges)

    def test_edges_manual(self):
        self.graph.add_edge(('sum2', 'pwr'))
        self.graph.add_edge(('sum2', 'dbl'))
        self.graph.add_edge(('dbl', 'pwr'))
        self.graph.del_node('pwr')
        self.assertEqual({'sum2': {'dbl': {}}, 'dbl': {}}, self.graph)
        self.graph.del_node('dbl')
        self.assertRaises(KeyError, self.graph.del_node, 'pwr')
        self.assertEqual({'sum2': {}}, self.graph)
        self.graph.del_node('sum2')
        self.assertFalse(self.graph)
        self.graph.add_edge(('sum2', 'pwr'))
        self.graph.del_edge(('sum2', 'pwr'))
        self.assertEqual({'sum2': {}, 'pwr': {}}, self.graph)
        self.assertTrue(self.graph)

    def test_dfs(self):
        edges = [(1, 2), (3, 4), (5, 6), (1, 3), (1, 5), (1, 6), (2, 5)]
        self.graph.add_edges([(1, 2), (3, 4), (5, 6), (1, 3), (1, 5), (1, 6), (2, 5)])
        self.assertEqual(len(self.graph.dfs(1, [])), 6)
        self.graph.clear_nodes()
        self.assertEqual(len(self.graph.dfs(4, [])), 1)
        self.assertEqual(len(self.graph.dfs(1, [])), 5) # 4 is not clear
        self.graph.clear_nodes()
        self.assertEqual(len(self.graph.dfs(2, [])), 3)
        self.graph.clear_nodes()
        self.graph.add_edge((4, 1))
        a = []
        self.assertEqual(len(self.graph.dfs(1, a)), 6)
        self.assertEqual(a, [6, 5, 2, 4, 3, 1])

    def test_postorder1(self):
        edges = [(1, 2), (3, 4), (5, 6), (1, 5), (1, 6), (2, 5), (4, 6)]
        self.graph.add_edges(edges)
        self.graph.postorder()
        self.graph.clear_nodes()
        self.graph.postorder(reverse=True)
        self.graph.clear_nodes()

    def test_postorder2(self):
        edges = [(1, 2), (2, 3), (3, 4), (1, 5), (5, 6), (5, 7), (1, 8), (8, 9), (8, 10)]
        self.graph.add_edges(edges)
        assert self.graph.postorder() == list(reversed(self.graph.postorder(True)))

    def test_postorder3(self):
        edges = [(1, 2)]
        self.graph.add_edges(edges)
        self.graph.add_node(3)
        assert self.graph.postorder() == list(reversed(self.graph.postorder(True)))
        self.graph.add_node(4)
        assert self.graph.postorder() == list(reversed(self.graph.postorder(True)))
        edges = [(4, 5)]
        assert self.graph.postorder() == list(reversed(self.graph.postorder(True)))


    def test_node_rank1(self):
        edges = [(1, 2), (3, 4), (5, 6), (1, 5), (1, 6), (2, 5), (4, 6)]
        self.graph.add_edges(edges)
        assert self.graph.node_rank() == {1: 3, 2: 2, 3: 2, 4: 1, 5: 1, 6: 0}

    def test_node_rank2(self):
        edges = [(1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (1, 6)]
        self.graph.add_edges(edges)
        assert self.graph.node_rank() == {1: 5, 2: 4, 3: 3, 4: 2, 5: 1, 6: 0}

    def test_node_width1(self):
        edges = [(1, 2), (1, 3), (1, 4), (2, 5), (3, 5), (3, 6), \
                 (2, 7), (4, 7), (5, 7), (6, 7)]
        self.graph.add_edges(edges)
        self.graph.node_width()

    def test_rank_width1(self):
        edges = [(1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (1, 7), (7, 6)]
        self.graph.add_edges(edges)
        assert self.graph.rank_width() == {0: 1, 1: 2, 2: 1, 3: 1, 4: 1, 5: 1}

    def test_rank_width2(self):
        edges = [(1, 2), (1, 3), (1, 4), (1, 5), (5, 7), \
                 (7, 6), (2, 6), (3, 6), (4, 6)]
        self.graph.add_edges(edges)
        assert self.graph.rank_width() == {0: 1, 1: 4, 2: 1, 3: 1}


class test_Worker(GeneratorTest):

    def testsingle(self):
        pwr = Worker(power)
        pwr = pwr([2])
        self.assertEqual(pwr, 2 * 2)
        dbl = Worker(double)
        dbl = dbl([3])
        self.assertEqual(dbl, 2 * 3)

    def testeq(self):
        pwr = Worker(power)
        dbl = Worker(double)
        pwr2 = Worker(power)
        self.assertEqual(pwr, pwr2)
        self.assertRaises(TypeError, hash, pwr)
        assert pwr is not pwr2
        self.assertNotEqual(pwr, dbl)
        self.assertNotEqual(pwr, 'abcd')
        self.assertNotEqual('abcd', pwr)
        pwrdbl = Worker([power, double])
        pwrdbl2 = Worker([power, double])
        dblpwr = Worker([double, power])
        self.assertEqual(pwrdbl, pwrdbl2)
        self.assertNotEqual(pwrdbl, dblpwr)
        self.assertNotEqual(pwr, pwrdbl)
        assert pwrdbl is not pwrdbl2
        assert pwr is not pwr2

    def testdouble(self):
        pwr = Worker(power)
        dbl = Worker(double)
        dblpwr = dbl([pwr([3])])
        self.assertEqual(dblpwr, 2 * (3 * 3))
        pwrdbl = pwr([dbl([3])])
        self.assertEqual(pwrdbl, (2 * 3) * (2 * 3))

    def testnested(self):
        dblpwr = Worker([power, double])
        dblpwr = dblpwr([3])
        self.assertEqual(dblpwr, 2 * (3 * 3))
        pwrdbl = Worker([double, power])
        pwrdbl = pwrdbl([3])
        self.assertEqual(pwrdbl, (2 * 3) * (2 * 3))

    def testkeyword(self):
        pwr = Worker(powr, (2,))
        self.assertEqual(pwr((4,)), 16)
        pwr = Worker(powr, (3,))
        self.assertEqual(pwr((2,)), 8)
        pwr_ = Worker(powr, (), {'arg':2})
        self.assertEqual(pwr_((4,)), 16)

    def testkeywords(self):
        mn = Worker(times_m_minus_n, (3, 7))
        self.assertEqual(mn((4,)), 5)
        mn = Worker(times_m_minus_n, (2, 1))
        self.assertEqual(mn((4,)), 7)
        mn.args = ((1, 1),)
        self.assertEqual(mn((4,)), 3)

    def testkwargs(self):
        mn = Worker(times_m_minus_n, (), {'m':3, 'n':7})
        self.assertEqual(mn((4,)), 5)

    def testmulti(self):
        multi0 = Worker((powr, times_m_minus_n), ((2,), (3, 7)))
        self.assertEqual(multi0((4,)), 41)
        multi = Worker((double, pow2))
        self.assertEqual(multi((1,)), 4)
        multi2 = Worker(multi0)
        multi3 = Worker(multi)
        self.assertEqual(multi3((1,)), 4)
        self.assertEqual(multi2((4,)), 41)
        self.assertEqual(multi3, multi)
        self.assertEqual(multi2, multi0)

    def testinput(self):
        # good input
        w = Worker(powr)
        w = Worker([powr])
        x = Worker(w)
        x = Worker([x])
        z = Worker([power, double])
        z = Worker([w, x])

    def testIMap(self):
        imap = IMap()
        pw2 = Worker(pow2)
        imap.add_task(pw2, [[1], [2], [3], [4]])
        imap.start()
        for i, j in izip(imap, [[1], [2], [3], [4]]):
            self.assertEqual(i, j[0] * j[0])

    def testIMap2(self):
        imap = IMap()
        pw2 = Worker(powr, (2,))
        imap.add_task(pw2, [[1], [2], [3], [4]])
        imap.start()
        for i, j in izip(imap, [[1], [2], [3], [4]]):
            self.assertEqual(i, j[0] * j[0])

    def testimap_kwargs(self):
        imap = IMap()

    def testimports(self):
        @imports(['re', 'sys'])
        def pr(i):
            return (re, sys)
        pr(1)

    def testcomp_task(self):
        def plus(i):
            return i[0] + 1
        def minus(i):
            return i[0] - 1

        papy.TASK = (plus, minus)
        assert 0 == comp_task([0], ((), ()), ({}, {}))

    def testysy_sys(self):
        assert ['ysy,sys'] == ysy_sys.imports
        assert ysy_sys.func_globals.get('ysy')
        assert ysy_sys.func_globals.get('ysy') is sys

    def testX_Y(self):
        import multiprocessing.forking
        assert multiprocessing.forking is X_Y([1])
        assert multiprocessing.forking is Y

    def test_xml_dom_domreg(self):
        assert domreg is xml_dom_domreg([1])


    def testsys_ver(self):
        assert ['sys'] == sys_ver.imports
        assert sys_ver.func_globals.get('sys')
        rr = Worker(sys_ver)
        assert rr([1]) == __import__('sys').version

    def testimports_re(self):
        assert ['re'] == retre_yes.imports
        assert not hasattr(retre_no, 'imports')
        assert retre_yes.func_globals.get('re')
        rr = Worker(retre_yes)
        assert rr([1]) == __import__('re')

    def testexceptions(self):
        self.assertRaises(TypeError, Worker, 1)
        self.assertRaises(TypeError, Worker, [1])

    def testdump_stream(self):
        fh = open('test_dump_stream', 'wb')
        dump_work = Worker(workers.io.dump_stream, (fh, ''))
        dumper = Piper(dump_work)
        inbox = [['first1\nfirst2\n', 'second1\nsecond2\n', 'third\n']]
        dumper(inbox)
        assert list(dumper) == ['test_dump_stream', 'test_dump_stream', 'test_dump_stream']
        fh.close()
        fh = open('test_dump_stream', 'rb')
        assert fh.read() == "\n\n".join(inbox[0]) + "\n\n"

    def testload_stream(self):
        fh = open('test_load_stream', 'wb')
        dump_work = Worker(workers.io.dump_stream, (fh, 'SOME_STRING'))
        dumper = Piper(dump_work)
        inbox = [['first1\nfirst2\n', 'second1\nsecond2\n', 'third\n']]
        dumper(inbox)
        assert list(dumper)
        fh.close()
        fh = open('test_load_stream', 'rb')
        load_work = workers.io.load_stream(fh, 'SOME_STRING')
        assert list(load_work) == inbox[0]

    def testload_pickle_stream(self):
        import cPickle
        fh = open('pickle_stream', 'wb')
        a = ['aaa\n', (1, 2, 3), 'abc', {}]
        for i in a:
            cPickle.dump(i, fh)
        fh.close()
        fh = open('pickle_stream', 'rb')
        b = workers.io.load_pickle_stream(fh)
        assert a == list(b)

    def testload_pickle_shm_stream(self):
        import cPickle, os
        fh = workers.io.open_shm('stream')
        a = ['aaa\n', (1, 2, 3), 'abc', {}]
        for i in a:
            cPickle.dump(i, fh)
        fh = workers.io.open_shm('stream')
        b = workers.io.load_pickle_stream(fh)
        assert a == list(b)
        fh.unlink()

    def testdump_load_item(self):
        import os
        a = ['aaa\n', 'b_b_b', 'abc\n', 'ddd']
        for i in a:
            file = workers.io.dump_item([i])
            ii = workers.io.load_item([file])
            assert ii == i

    def testdump_fifo_load_item(self):
        import os
        a = ['aaa\n', 'b_b_b', 'abc\n', 'ddd']
        for i in a:
            file = workers.io.dump_item([i], type='fifo')
            ii = workers.io.load_item([file])
            assert ii == i

    def testdump_shm_load_item(self):
        import os
        a = ['aaa\n', 'b_b_b', 'abc\n', 'ddd']
        for i in a:
            file = workers.io.dump_item([i], type='shm')
            ii = workers.io.load_item([file])
            assert ii == i

    def testdump_tcp_load_item(self):
        import os
        a = ['aaa\n', 'b_b_b', 'abc\n', 'ddd']
        for i in a:
            file = workers.io.dump_item([i], type='tcp')
            ii = workers.io.load_item([file])
            assert ii == i

    def testdump_udp_load_item(self):
        import os
        a = ['aaa\n', 'b_b_b', 'abc\n', 'ddd']
        for i in a:
            file = workers.io.dump_item([i], type='udp')
            ii = workers.io.load_item([file])
            assert ii == i



    def testdump_load_sqlite_item(self):
        import os
        a = ['aaa\n', 'b_b_b', 'abc\n', 'ddd']
        for i in a:
            file = workers.io.dump_db_item([i])
            item = workers.io.load_db_item([file], remove=True)
            assert item == i

    def test_dump_load_mysql_item(self):
        a = ['aaa\n', 'b_b_b', 'abc\n', 'ddd']
        for i in a:
            file = workers.io.dump_db_item([i], type='mysql', user='mcieslik', \
            host='localhost', passwd='pinkcream69', db='papy')
            item = workers.io.load_db_item([file], remove=True)
            assert item == i


    def testdump_load_item_mmap(self):
        import os
        a = ['aaa\n', 'b_b_b', 'abc\n', 'ddd']
        for i in a:
            file = workers.io.dump_item([i])
            ii = workers.io.load_item([file], type='mmap')
            assert ii.read(10000) == i

    def testdump_shm_load_item_mmap(self):
        import os
        a = ['aaa\n', 'b_b_b', 'abc\n', 'ddd']
        for i in a:
            file = workers.io.dump_item([i], type='shm')
            ii = workers.io.load_item([file], type='mmap')
            assert ii.read(1000) == i

    def testdump_fifo_load_item_mmap(self):
        import os
        a = ['aaa\n', 'b_b_b', 'abc\n', 'ddd']
        files = []
        for i in a:
            file = workers.io.dump_item([i], type='fifo')
            workers.io.load_item([file], type='mmap')

    def test_dump_load_manager_item(self):
        import os
        manager = utils.remote.DictServer(address=('localhost', 57333),
                                        authkey='abc')
        manager.start()
        a = ['aaa\n', 'b_b_b', 'abc\n', 'ddd']
        for i in a:
            file = workers.io.dump_manager_item([i], ('localhost', 57333), 'abc')
            item = workers.io.load_manager_item([file], remove=False)
            assert item == i
        manager.shutdown()

    def test_find_items(self):
        a = ['aaa\n', 'b_b_b', 'abc\n', 'ddd']
        b = []
        for i in a:
            workers.io.dump_item([i], 'file', 'test', '.string')
        abc = workers.io.find_items('test', '.string')
        for z in abc:
            iii = workers.io.load_item([(z, 0)])
            b.append(iii)
        assert sorted(b) == sorted(a)

    def test_make_items(self):
        fh = open('files/test_make_items', 'rb')
        chunker = workers.io.make_items(fh, 4000)
        output = ""
        for item in chunker:
            mmap = workers.io.load_item([item], type='mmap', remove=False)
            while True:
                line = mmap.readline()
                if line:
                    output += line
                else:
                    break
        assert output == fh.read()

    def testpickle(self):
        a = ['aaaaaa', 'bbbbbbbm\n', 'ccccccccccc']
        for i in a:
            b = workers.io.pickle_dumps([i])
            c = workers.io.pickle_loads([b])
            assert i == c

    def testjson(self):
        a = ['aaaaaa', 'bbbbbbbm\n', 'ccccccccccc']
        for i in a:
            b = workers.io.json_dumps([i])
            c = workers.io.json_loads([b])
            assert i == c

    def testpickle_stream(self):
        a = ['aa\naaaa', 'bbbbbbbm\n', 'cccccc\nccccc']
        fh = open('test_pickle_stream2', 'wb')
        for i in a:
            workers.io.dump_pickle_stream([i], fh)
        fh.close()
        fh = open('test_pickle_stream2', 'rb')
        b = workers.io.load_pickle_stream(fh)
        import os
        os.remove('test_pickle_stream2')
        assert a == list(b)

    def testpickle_stream_shm(self):
        a = ['aa\naaaa', 'bbbbbbbm\n', 'cccccc\nccccc']
        fh = workers.io.open_shm('test_pickle_stream2')
        for i in a:
            workers.io.dump_pickle_stream([i], fh)
        fh.close()
        fh = workers.io.open_shm('test_pickle_stream2')
        b = workers.io.load_pickle_stream(fh)
        fh.unlink()
        assert a == list(b)


class test_Piper(GeneratorTest):


    def test(self):
        piper = Piper(power)
        assert piper.tees == []
        assert piper.tee_locks[0].locked() is False
        assert len(piper.tee_locks) == 1
        assert piper.started is False
        assert piper.connected is False
        assert piper.finished is False
        assert piper.imap is imap

    def test_simple_exceptions(self):
        for par in (None, IMap()):
            piper = Piper(power, parallel=par)

            self.assertRaises(PiperError, piper.start)
            self.assertRaises(PiperError, piper.stop)
            piper.connect([[1, 2, 3]])
            self.assertRaises(PiperError, piper.connect, [[1, 2, 3, 4]])
            self.assertRaises(PiperError, piper.stop)
            if par is not None:
                self.assertRaises(PiperError, piper.start)
            piper.start(forced=True)
            assert piper.finished is False
            assert list(piper) == [1, 4, 9]
            piper.stop(ends=[0])
            assert piper.started is False
            if par is not None:
                assert not piper.imap._started.isSet()
            assert piper.connected is True
            assert piper.finished is True


    def testlogger(self):
        from logging import Logger
        pwr = Piper(power)
        self.assertTrue(isinstance(pwr.log, Logger))

    def testpool(self):
        pwr = Piper(power)
        dbl = Piper(double)
        assert pwr is not dbl
        assert pwr.imap is dbl.imap
        poolx = IMap()
        pooly = IMap()
        pwr = Piper(power, parallel=poolx)
        dbl = Piper(double, parallel=pooly)
        assert pwr is not dbl
        assert pwr.imap != dbl.imap
        pwr = Piper(power)
        assert pwr.imap is imap
        assert dbl.imap is not imap
        assert isinstance(dbl.imap, IMap)

    def testeq(self):
        pwr = Piper(power)
        pwrs = [pwr]
        self.assertEqual(pwr, pwrs[0])
        pwr2 = Piper(power)
        self.assertNotEqual(pwr, pwr2)
        self.assertEqual(pwr.worker, pwr2.worker)

    def testbasic_call(self):
        pool = IMap()
        for i in range(100):
            ppr_instance = Piper(power, parallel=pool)
            ppr_busy = ppr_instance([[1, 2, 3, 4]])
            assert ppr_instance is ppr_busy
            self.assertRaises(PiperError, ppr_busy.next)
            self.assertRaises(PiperError, ppr_busy.start)
            ppr_busy.start(forced=True)
            assert ppr_busy.started is True
            assert ppr_busy.imap._started.isSet()
            for i in izip(ppr_busy, [1, 2, 3, 4]):
                self.assertEqual(i[0], i[1] * i[1])
            self.assertRaises(StopIteration, ppr_busy.next) # it. protocol
            self.assertRaises(StopIteration, ppr_busy.next) # it. protocol
            assert ppr_busy.imap._started.isSet()
            ppr_busy.stop(ends=[0])
            assert ppr_busy.started is False
            assert ppr_busy.finished is True
            assert ppr_busy.connected is True
            ppr_busy.disconnect()
            assert ppr_busy.imap._tasks == []
            assert ppr_busy.connected == False
            assert not ppr_busy.imap._started.isSet()
            self.assertRaises(PiperError, ppr_busy.next)

        ppr_instance = Piper(power)
        ppr_busy = ppr_instance([[1, 2, 3, 4]])
        assert ppr_instance is ppr_busy
        assert ppr_busy.started == False
        assert ppr_busy.connected == True
        assert ppr_busy.finished == False
        ppr_busy.start()
        assert ppr_busy.started == True
        for i in ppr_busy:
            pass
        ppr_busy.stop()
        assert ppr_busy.started == False
        assert ppr_busy.connected == True
        self.assertRaises(StopIteration, ppr_busy.next) # it. protocol
        self.assertRaises(StopIteration, ppr_busy.next) # it. protocol
        ppr_busy.disconnect()
        assert ppr_busy.connected == False
        assert ppr_busy.finished == True
        self.assertRaises(PiperError, ppr_busy.next) # it. protocol
        self.assertRaises(PiperError, ppr_busy.next) # it. protocol

    def testconnects(self):
        pool = IMap()
        ppr_instance = Piper(power, parallel=pool)
        ppr_busy = ppr_instance([[7, 2, 3]])
        assert ppr_instance is ppr_busy
        self.assertRaises(PiperError, ppr_busy, [[7, 2, 3]]) # second connect
        self.assertRaises(PiperError, ppr_busy.next)      # not started
        ppr_busy.disconnect()
        self.assertRaises(PiperError, ppr_busy.start)
        assert not ppr_busy.imap._tasks
        ppr_busy.connect([[1, 2, 3]])
        ppr_busy.start(forced=True)
        assert ppr_busy.next() == 1
        ppr_busy.stop(ends=[0], forced=True) # not finished
        self.assertRaises(RuntimeError, ppr_busy.imap.next)

    def testtrack(self):
        inpt = xrange(100)
        pool = IMap()
        ppr_instance = Piper(power, parallel=pool, track=True)
        ppr_instance([inpt])
        ppr_instance.start(forced=True)
        list(ppr_instance)
        assert ppr_instance.imap._tasks_tracked[0].values() == [i * i for i in
        ppr_instance.imap._tasks_tracked[0].keys()]

    def testconnects2(self):
        pool = IMap()
        ppr_1 = Piper(power, parallel=pool)
        ppr_2 = Piper(double, parallel=pool)
        ppr_1busy = ppr_1([[7, 2, 3]])
        ppr_2busy = ppr_2([[7, 2, 3]])
        assert (ppr_1busy, ppr_2busy) == (ppr_1, ppr_2)
        self.assertRaises(PiperError, ppr_1busy, [[7, 2, 3]]) # second connect
        self.assertRaises(PiperError, ppr_1busy, [[7, 2, 3]]) # second connect
        self.assertRaises(PiperError, ppr_1busy.next)     # not started
        self.assertRaises(PiperError, ppr_2busy.next)     # not started
        self.assertRaises(PiperError, ppr_1busy.disconnect) # not last

        ppr_2busy.disconnect()
        ppr_1busy.disconnect()
        self.assertRaises(PiperError, ppr_1busy.start)
        self.assertRaises(PiperError, ppr_2busy.start)
        assert pool._tasks == []

        ppr_2busy.connect([[7, 2, 3]])
        ppr_1busy.connect([[1, 1, 1]])

        ppr_1busy.start(forced=True)
        ppr_2busy.start()

        assert ppr_1busy.next() == 1
        assert ppr_2busy.next() == 14
        ppr_2busy.stop(ends=[0, 1], forced=True)
        self.assertRaises(RuntimeError, ppr_1busy.imap.next)
        self.assertRaises(RuntimeError, ppr_2busy.imap.next)

    def testconnect_empty(self):
        for i, j in ((None, None), (IMap(), IMap())):
            passer = Piper(workers.core.ipasser, parallel=i)
            passer([[1]])
            passer.start(forced=True)
            passer.next()
            self.assertRaises(StopIteration, passer.next)
            self.assertRaises(StopIteration, passer.next)
            self.assertRaises(StopIteration, passer.next)
            passer.stop(ends=[0])
            assert passer.finished is True
            assert passer.started is False

            passer = Piper(workers.core.ipasser, parallel=j)
            passer([[]])
            assert passer.connected is True
            assert passer.started is False
            passer.start(forced=True)
            assert passer.started is True
            assert passer.connected is True

            self.assertRaises(StopIteration, passer.next)
            self.assertRaises(StopIteration, passer.next)
            self.assertRaises(StopIteration, passer.next)
            passer.stop(ends=[0])
            assert passer.started is False
            assert passer.connected is True
            assert passer.finished is True
            passer.disconnect()
            assert passer.connected is False


    def testconnectnew2(self):
        for inp in ([[1]],): # [[]]):      
            imap = IMap()
            for i, j in ((None, None), (imap, imap), (IMap(), IMap()), \
                         (None, IMap()), (IMap(), None)):
                passer1 = Piper(workers.core.ipasser, parallel=i)
                passer2 = Piper(workers.core.ipasser, parallel=j)
                passer1(inp)
                passer2([passer1])
                passer1.start(forced=True)
                passer2.start(forced=True)
                passer2.next()
                self.assertRaises(StopIteration, passer2.next)
                passer2.stop(ends=[0])


    def testoutput_pickle(self):
        import os
        handle = os.tmpfile()
        data = [{1:1}, {2:2}]
        pickler = Worker(workers.io.pickle_dumps)
        dumper = Worker(workers.io.dump_stream, (handle,))
        pickle_piper = Piper(pickler)
        dump_piper = Piper(dumper)
        pickle_piper([data])
        dump_piper([pickle_piper])
        pickle_piper.start(forced=True)
        dump_piper.start()
        list(dump_piper)
        handle.seek(0)
        input = workers.io.load_stream(handle)
        depickler = Piper(workers.io.pickle_loads)
        depickler([input])
        depickler.start()
        a = list(depickler)
        assert a == [{1: 1}, {2: 2}]
        handle.close()
        dump_piper.stop()
        pickle_piper.stop()
        depickler.stop()
        assert dump_piper.started is False
        assert pickle_piper.started is False
        assert depickler.started is False
        dump_piper.disconnect()
        pickle_piper.disconnect()
        depickler.disconnect()

    def testoutput_simplejson(self):
        import os
        handle = os.tmpfile()
        data = [{'1':1}, {'2':2}]
        sj = Worker(workers.io.json_dumps)
        dumper = Worker(workers.io.dump_stream, (handle, '---'))
        sj_piper = Piper(sj, debug=True)
        dump_piper = Piper(dumper)
        sj_piper([data])
        dump_piper([sj_piper])
        sj_piper.start()
        list(dump_piper)
        handle.seek(0)
        input = workers.io.load_stream(handle, '---')
        passer = Piper(workers.io.json_loads)
        passer([input])
        a = list(passer)
        assert a == [{'1': 1}, {'2': 2}]
        handle.close()

    def testconnect_pickle(self):
        handle = open('test_pick', 'rb')
        input = workers.io.load_pickle_stream(handle)
        passer = Piper(workers.core.ipasser)
        assert list(passer([input])) == [[1, 2, 3], [1, 2, 3], [1, 2, 3], [1, 2, 3], \
        [4, 5, 6], [4, 5, 6], [4, 5, 6], [4, 5, 6], [4, 5, 6], [4, 5, 6], [4, 5, 6], \
        [4, 5, 6], [4, 5, 6], [4, 5, 6], [4, 5, 6], [4, 5, 6]]

    def testposixshm(self):
        data = xrange(100)
        handle1 = workers.io.open_shm('posixshm')
        handle2 = workers.io.open_shm('posixshm')
        dumper = Worker(workers.io.dump_pickle_stream, (handle1,))
        loader = workers.io.load_pickle_stream(handle2)
        p_dumper = Piper(dumper)
        p_dumper([data])
        p_dumper.start()
        list(p_dumper)
        assert list(loader) == list(data)
        handle1.unlink()

    def test_2_running(self):
        p = IMap()
        for pool1, pool2 in ((IMap(), IMap()),
                             (p, p),
                             (IMap(), None),
                             (None, IMap())):

            data = [1, 2, 3]
            piper1 = Piper(passer, parallel=pool1)
            piper2 = Piper(passer, parallel=pool2)
            piper1.connect([data])
            piper2.connect([piper1])
            piper1.start(forced=True)
            piper2.start(forced=True)
            assert list(piper2) == [1, 2, 3]

    def test_fork_join(self):
        for stride in (1, 2, 3, 4, 5):
            for r in range(24):
                after1 = IMap(stride=stride)
                after2 = IMap(stride=stride)
                after3 = IMap(stride=stride)
                before1 = IMap(stride=stride)
                before2 = IMap(stride=stride)

                combinations = [(None, None, None, None),
                                (IMap(stride=stride), IMap(stride=stride), IMap(stride=stride), IMap(stride=stride)),
                                (IMap(stride=stride), None, None, IMap(stride=stride)),
                                (None, IMap(stride=stride), IMap(stride=stride), IMap(stride=stride)),
                                (None, after1, after1, after1),
                                (None, after2, after2, IMap(stride=stride)),
                                (IMap(), after3, after3, after3),
                                (before1, before1, before1, before1),
                                (before2, before2, IMap(stride=stride), before2)
                                ]
                for pool1, pool2, pool3, pool4 in combinations:
                    data = range(r)

                    piper1 = Piper(passer, parallel=pool1)
                    piper2 = Piper(passer, parallel=pool2)
                    piper3 = Piper(passer, parallel=pool3)
                    piper4 = Piper(workers.core.npasser, parallel=pool4)

                    piper1.connect([data])
                    piper2.connect([piper1])
                    piper3.connect([piper1])
                    piper4.connect([piper2, piper3])

                    for piper in (piper1, piper2, piper3, piper4):
                        piper.start(forgive=True)
                        # start workers only once
                        try:
                            if not piper.imap._started.isSet():
                                piper.imap._start_workers()
                                piper.imap._started.set()
                        except AttributeError:
                            pass
                    started_imaps = []
                    for piper in (piper1, piper2, piper3, piper4):
                        # start managers only once
                        # requires _started to be set
                        try:
                            if piper.imap not in started_imaps:
                                piper.imap._start_managers()
                                started_imaps.append(piper.imap)
                        except AttributeError:
                            pass

                    assert list(piper4) == [list(i) for i in zip(range(r), range(r))]


    def test_3_fork(self):
        for stride in (1, 2, 3, 4, 5):
            for r in range(24):
                after1 = IMap(stride=stride)
                after2 = IMap(stride=stride)
                before1 = IMap(stride=stride)
                before2 = IMap(stride=stride)

                combinations = [(None, None, None),
                                (IMap(), IMap(), IMap()),
                                (IMap(), None, None),
                                (None, IMap(), IMap()),
                                (None, after1, after1),
                                (IMap(), after2, after2),
                                (before1, before1, before1)
                                ]
                for pool1, pool2, pool3 in combinations:
                    data = range(r)
                    if hasattr(pool3, 'stride'):
                        stride = pool3.stride
                    else:
                        stride = 1
                    piper1 = Piper(passer, parallel=pool1)
                    piper2 = Piper(passer, parallel=pool2)
                    piper3 = Piper(passer, parallel=pool3)
                    piper1.connect([data])
                    piper2.connect([piper1])
                    piper3.connect([piper1])
                    piper1.start(forced=True)
                    piper2.start(forced=True)
                    piper3.start(forced=True)
                    a = []
                    for result in range((r / stride) + stride):
                        for s in range(stride):
                            try:
                                piper2.next()
                            except Exception, excp:
                                pass
                        for s in range(stride):
                            try:
                                piper3.next()
                            except:
                                pass
                    assert piper1.finished
                    assert piper2.finished
                    assert piper3.finished
                    for p in (pool1, pool2, pool3):
                        if hasattr(p, '_pool_getter'):
                            p._pool_getter.join()

    def test_2_stopping(self):
        p = IMap()
        for pool1, pool2 in ((IMap(), IMap()),
                             (p, p),
                             (IMap(), None),
                             (None, IMap())):

            data = [1, 2, 3]
            piper1 = Piper(passer, parallel=pool1)
            piper2 = Piper(passer, parallel=pool2)
            piper1.connect([data])
            piper2.connect([piper1])
            assert piper1.connected is True
            assert piper2.connected is True
            piper1.start(forced=True)
            piper2.start(forced=True)
            assert piper1.started is True
            assert piper2.started is True
            assert list(piper2) == [1, 2, 3]
            assert piper1.finished is True
            assert piper2.finished is True
            assert piper1.started is True
            assert piper2.started is True

            if not pool1 is None:
                piper1.stop(ends=[len(piper1.imap._tasks) - 1])
            else:
                piper1.stop()
            if not pool2 is None:
                piper2.stop(ends=[len(piper2.imap._tasks) - 1])
            else:
                piper2.stop()
            assert piper1.started is False
            assert piper2.started is False
            if not pool1 is None:
                assert not pool1._started.isSet()
            if not pool2 is None:
                assert not pool2._started.isSet()

    def testdump_items(self):
        for typ in ('tcp', 'udp'):
            for typ2 in ('string',):
                imap1 = IMap()
                imap2 = IMap()
                imap3 = IMap()
                for i1, i2 in ((imap1, imap2), (imap3, imap3), (None, None)):
                    data = xrange(10)
                    pickler = Worker(workers.io.pickle_dumps)
                    dumper = Worker(workers.io.dump_item, (typ,))
                    loader = Worker(workers.io.load_item)
                    unpickler = Worker(workers.io.pickle_loads)

                    p_pickler = Piper(pickler)
                    p_dumper = Piper(dumper, parallel=i1, debug=True)
                    p_loader = Piper(loader, parallel=i2)
                    p_unpickler = Piper(unpickler)

                    p_pickler([data])
                    p_dumper([p_pickler])
                    p_loader([p_dumper])
                    p_unpickler([p_loader])

                    p_pickler.start()
                    p_dumper.start(forced=True)
                    p_loader.start(forced=True)
                    p_unpickler.start()
                    assert list(data) == list(p_unpickler)

    def testdump_itmes_thread(self):
        for typ in ('tcp', 'udp'):
            for typ2 in ('string',):
                imap1 = IMap(worker_type='thread')
                imap2 = IMap(worker_type='thread')
                imap3 = IMap(worker_type='thread')
                for i1, i2 in ((imap1, imap2), (imap3, imap3), (None, None)):
                    data = xrange(10)
                    pickler = Worker(workers.io.pickle_dumps)
                    dumper = Worker(workers.io.dump_item, (typ,))
                    loader = Worker(workers.io.load_item)
                    unpickler = Worker(workers.io.pickle_loads)

                    p_pickler = Piper(pickler)
                    p_dumper = Piper(dumper, parallel=i1, debug=True)
                    p_loader = Piper(loader, parallel=i2)
                    p_unpickler = Piper(unpickler)

                    p_pickler([data])
                    p_dumper([p_pickler])
                    p_loader([p_dumper])
                    p_unpickler([p_loader])

                    p_pickler.start()
                    p_dumper.start(forced=True)
                    p_loader.start(forced=True)
                    p_unpickler.start()
                    assert list(data) == list(p_unpickler)


    def testsort(self):
        p2 = Piper(workers.core.ipasser, ornament=2)
        p1 = Piper(workers.core.ipasser, ornament=1)
        a = [p2, p1]
        a.sort(cmp=Piper._cmp)
        assert a == [p1, p2]

    def testfailure(self):
        pwr = Piper(power)
        pwr = pwr([[1, 'a', 3]])
        pwr.start() # should not raise even if not needed
        self.assertEqual(pwr.next(), 1)
        self.assertTrue(isinstance(pwr.next(), PiperError))
        self.assertEqual(pwr.next(), 9)
        self.assertRaises(StopIteration, pwr.next)
        pwr = Piper(power, debug=True)
        pwr = pwr([[1, 'a', 3]])
        pwr.start()
        self.assertEqual(pwr.next(), 1)
        self.assertRaises(PiperError, pwr.next)
        self.assertEqual(pwr.next(), 9)
        self.assertRaises(StopIteration, pwr.next)
        pool = IMap()
        pwr = Piper(power, parallel=pool)
        pwr = pwr([[1, 'a', 3]])
        pwr.start(forced=True) # should work
        self.assertEqual(pwr.next(), 1)
        self.assertTrue(isinstance(pwr.next(), PiperError))
        self.assertEqual(pwr.next(), 9)
        self.assertRaises(StopIteration, pwr.next)
        pwr = Piper(power, debug=True)
        pwr = pwr([[1, 'a', 3]])
        pwr.start()
        self.assertEqual(pwr.next(), 1)
        self.assertRaises(PiperError, pwr.next)
        self.assertEqual(pwr.next(), 9)
        self.assertRaises(StopIteration, pwr.next)

    def testchained_failure(self):
        from exceptions import StopIteration
        pwr = Piper(power)
        dbl = Piper(double)
        pwr = pwr([[1, 'a', 3]])
        dbl = dbl([pwr])
        pwr.start()
        dbl.start()
        self.assertEqual(dbl.next(), 2)
        a = dbl.next()
        self.assertTrue(isinstance(a, PiperError))  # this is what dbl return (it wrapped what it got)
        self.assertTrue(isinstance(a[0], PiperError))       # wrapped in the workers piper
        self.assertTrue(isinstance(a[0][0], WorkerError))   # wrapped in the worker
        self.assertTrue(isinstance(a[0][0][0], TypeError))  # raised in the worker
        self.assertEqual(dbl.next(), 18)
        self.assertRaises(StopIteration, dbl.next)

        pool = IMap()
        pwr = Piper(power, parallel=pool)
        dbl = Piper(double, parallel=pool)
        pwr = pwr([[1, 'a', 3]])
        dbl = dbl([pwr])
        self.assertRaises(PiperError, pwr.start)
        self.assertRaises(PiperError, dbl.start)
        dbl.start(forced=True)
        self.assertEqual(dbl.next(), 2)
        a = dbl.next()
        self.assertTrue(isinstance(a, PiperError))  # this is what dbl return (it wrapped what it got)
        self.assertTrue(isinstance(a[0], PiperError))       # wrapped in the workers piper
        self.assertTrue(isinstance(a[0][0], WorkerError))   # wrapped in the worker
        self.assertTrue(isinstance(a[0][0][0], TypeError))  # raised in the worker
        self.assertEqual(dbl.next(), 18)
        self.assertRaises(StopIteration, dbl.next)
        self.assertRaises(StopIteration, pwr.next)
        dbl.stop(ends=[1])
        self.assertRaises(PiperError, pwr.next)

    def testverysimple(self):
        for par in (False, IMap()):
            gen10 = (i for i in xrange(1, 10))
            gen15 = (i for i in xrange(1, 15))
            ppr = Piper(power, parallel=par)
            ppr = ppr([gen15])
            ppr.start(forced=True)
            for i, j in izip(ppr, xrange(1, 15)):
                self.assertEqual(i, j * j)
            ppr.stop(ends=[0])
            ppr = Piper(power, parallel=par)
            ppr = ppr([gen10])
            ppr.start(forced=True)
            for i, j in izip(ppr, xrange(1, 10)):
                self.assertEqual(i, j * j)

    def testsingle(self):
        for par in (False, IMap()):
            gen10 = (i for i in xrange(1, 10))
            gen15 = (i for i in xrange(1, 15))
            gen20 = (i for i in xrange(1, 20))
            ppr = Piper(power, parallel=par)
            ppr = ppr([gen10])
            ppr.start(forced=True)
            for i, j in izip(ppr, xrange(1, 20)):
                self.assertEqual(i, j * j)
            ppr.stop(ends=[0])
            ppr = Piper([power], parallel=par)
            ppr = ppr([gen15])
            ppr.start(forced=True)
            for i, j in izip(ppr, xrange(1, 15)):
                self.assertEqual(i, j * j)
            ppr.stop(ends=[0])
            pwr = Worker(power)
            ppr = Piper(pwr, parallel=par)
            ppr = ppr([gen20])
            ppr.start(forced=True)
            for i, j in izip(ppr, xrange(1, 20)):
                self.assertEqual(i, j * j)
            ppr.stop(ends=[0])

    def testdouble(self):
        for par in (False, IMap()):
            gen10 = (i for i in xrange(1, 10))
            gen15 = (i for i in xrange(1, 15))
            gen20 = (i for i in xrange(1, 20))
            ppr = Piper([power, double], parallel=par)
            ppr = ppr([gen20])
            ppr.start(forced=True)
            for i, j in izip(ppr, xrange(1, 20)):
                self.assertEqual(i, 2 * j * j)
            ppr.stop(ends=[0])
            pwrdbl = Worker([power, double])
            ppr = Piper(pwrdbl, parallel=par)
            ppr = ppr([gen15])
            ppr.start(forced=True)
            for i, j in izip(ppr, xrange(1, 15)):
                self.assertEqual(i, 2 * j * j)
            ppr.stop(ends=[0])
            dblpwr = Worker([double, power])
            ppr = Piper(dblpwr, parallel=par)
            ppr = ppr([gen10])
            ppr.start(forced=True)
            for i, j in izip(ppr, xrange(1, 10)):
                self.assertEqual(i, (2 * j) * (2 * j))
            ppr.stop(ends=[0])

    def testlinked(self):
        for par in (False, IMap()):
            gen10 = (i for i in xrange(10))
            pwr = Piper(power, parallel=par)
            dbl = Piper(double, parallel=par)
            dbl([gen10])
            ppr = pwr([dbl])
            dbl.start(forced=True)
            ppr.start(forced=True)
            for i, j in izip(ppr, (i for i in xrange(10))):
                self.assertEqual(i, (2 * j) * (2 * j))
            ppr.stop(ends=[1])

    def testlinked2(self):
        for par in (False, IMap()):
            gen10 = (i for i in xrange(10))
            pwr = Piper(power, parallel=par)
            dbl = Piper(double, parallel=par)
            ppr = pwr([dbl([gen10])])
            dbl.start(forgive=True)
            ppr.start(forced=True)
            for i, j in izip(ppr, (i for i in xrange(10))):
                self.assertEqual(i, (2 * j) * (2 * j))
            ppr.stop(ends=[1])

    def testexceptions(self):
        self.assertRaises(PiperError, Piper, 1)
        self.assertRaises(PiperError, Piper, [1])

    def testlinear(self):
        for par in (False, IMap()):
            gen20 = (i for i in xrange(1, 20))
            piper = Piper(pow2, parallel=par)
            piper([gen20])
            piper.start(forced=True)
            for i, j in izip(piper, xrange(1, 20)):
                self.assertEqual(i, j * j)
            self.assertRaises(StopIteration, piper.next)
            piper.stop(ends=[0])

    def test__Produce(self):
        product = _Produce(iter([0, 1, 2, 3, 4, 5, 6]), n=2, stride=3)
        result = []
        for i in range(24):
            try:
                result.append(product.next())
            except StopIteration:
                result.append('s')
        self.assertEqual(result,
        [0, 1, 2, 0, 1, 2, 3, 4, 5, 3, 4, 5, 6, 's', 's', 6, 's', 's', 's', 's', 's', 's', 's', 's'])

    def test_Produce(self):
        product = Produce(iter([(11, 12), (21, 22), (31, 32), (41, 42), (51, 52), (61, 62), (71, 72)]), n=2, stride=3)
        result = []
        for i in range(24):
            try:
                result.append(product.next())
            except StopIteration:
                result.append('s')
        self.assertEqual(result,
        [11, 21, 31, 12, 22, 32, 41, 51, 61, 42, 52, 62, 71, 's', 's', 72, 's', 's', 's', 's', 's', 's', 's', 's'])

    def test_Consume(self):
        consumpt = \
        Consume(iter([0, 1, 2, 0, 1, 2, 3, 4, 5, 3, 4, 5, 6, 's', 's', 6, 's', 's', 's', 's', 's', 's', 's', 's']), stride=3, n=2)
        result = []
        for i in range(12):
            result.append(consumpt.next())
        self.assertEqual(result, [[0, 0], [1, 1], [2, 2], [3, 3],
                                [4, 4], [5, 5], [6, 6], ['s', 's'],
                                ['s', 's'], ['s', 's'], ['s', 's'], ['s', 's']])

    def test_produce_from_sequence(self):
        inp = [(11, 12), (21, 22), (31, 32), (41, 42), (51, 52), (61, 62), (71, 72)]
        par = IMap(stride=3)
        w_p2 = Worker(workers.core.ipasser)
        p_p2 = Piper(w_p2, parallel=par, produce=2)
        p_p2 = p_p2([inp])
        p_p2.start(forced=True)
        result = []
        for i in range(24):
            try:
                result.append(p_p2.next())
            except StopIteration:
                result.append('s')
        self.assertEqual(result,
        [11, 21, 31, 12, 22, 32, 41, 51, 61, 42,
         52, 62, 71, 's', 's', 72, 's', 's', 's', 's', 's', 's', 's', 's'])

#    def testproduce(self):
#        inp = [0, 1, 2, 3, 4, 5, 6]
#        par = IMap(stride=3)
#        w_p2 = Worker(pow2)
#        p_p2 = Piper(w_p2, parallel=par, produce=2)
#        p_p2 = p_p2([inp])
#        p_p2.start(forced=True)
#        result = []
#        for i in range(25):
#            try:
#                result.append(p_p2.next())
#            except StopIteration:
#                result.append('s')
#        self.assertEqual(result,
#        [0, 1, 4, 0, 1, 4, 9, 16, 25, 9, 16, 25, 36, 's', 's', 36, 's', 's', 's', 's', 's', 's', 's', 's', 's'])
#
#
#    def testproduce_error(self):
#        inp = [0, 1, 'z', 3, 4, 5, 6]
#        par = IMap(stride=3)
#        w_p2 = Worker(pow2)
#        p_p2 = Piper(w_p2, parallel=par, produce=2, debug=True)
#        p_p2 = p_p2([inp])
#        p_p2.start(forced=True)
#        result = []
#        for i in range(25):
#            try:
#                result.append(p_p2.next())
#            except StopIteration:
#                result.append('s')
#            except Exception, e:
#                result.append('e')
#        self.assertEqual(result,
#        [0, 1, 'e', 0, 1, 'e', 9, 16, 25, 9, 16, 25, 36, 's', 's', 36, 's', 's', 's', 's', 's', 's', 's', 's', 's'])

#    def testChain(self):
#        inp1 = [0, 1, 2, 3, 4, 5, 6]
#        inp2 = [1, 2, 3, 4, 5, 6, 7]
#        par = IMap(stride=3)
#        w_p2 = Worker(pow2)
#        p_p2 = Piper(w_p2, parallel=par, produce=2)
#        p_p3 = Piper(w_p2, parallel=par, produce=2)
#        p_p2 = p_p2([inp1])
#        p_p3 = p_p3([inp2])
#        p_p2.start(forced=True)
#        chainer = Chain([p_p2, p_p3], stride=3)
#        result = []
#        for i in range(36):
#            try:
#                result.append(chainer.next())
#            except StopIteration:
#                result.append('s')
#        self.assertEqual(result, [0, 1, 4, 1, 4, 9, 0, 1, 4, 1, 4, 9, 9, 16, 25, 16, 25, 36,
#                                  9, 16, 25, 16, 25, 36, 36, 's', 's', 49, 's', 's', 36,
#                                  's', 's', 49, 's', 's', ])

    def test_consume(self):
        inp = [0, 1, 4, 0, 1, 4, 9, 16, 25, 9, 16, 25, 36, 's', 's', 36, 's', 's', 's', 's', 's', 's', 's', 's']
        par = IMap(stride=3)
        w_s2 = Worker(ss2)
        p_s2 = Piper(w_s2, parallel=par, consume=2)
        p_s2 = p_s2([inp])
        p_s2.start(forced=True)
        result = list(p_s2)
        self.assertEqual([0, 2, 8, 18, 32, 50, 72, 'ss', 'ss', 'ss', 'ss', 'ss'], result)

#    def testproduce_consume(self):
#        inp = [0, 1, 2, 3, 4, 5, 6]
#        #par = IMap(stride=3)
#        par = None
#        w_p2 = Worker(pow2)
#        p_p2 = Piper(w_p2, parallel=par, produce=2, debug=True)
#        p_p2 = p_p2([inp])
#        w_s2 = Worker(ss2)
#        p_s2 = Piper(w_s2, parallel=par, consume=2, debug=True)
#        p_s2 = p_s2([p_p2])
#        p_p2.start(forced=True)
#        p_s2.start(forced=True)
#        self.assertEqual(list(p_s2), [0, 2, 8, 18, 32, 50, 72])

    def test_produce_consume(self):
        inp = [(11, 12), (21, 22), (31, 32), (41, 42), (51, 52), (61, 62)]
        par = IMap(stride=3)
        w_p2 = Worker(workers.core.ipasser)
        p_p2 = Piper(w_p2, parallel=par, produce=2)
        p_p2 = p_p2([inp])
        w_s2 = Worker(ss2)
        p_s2 = Piper(w_s2, parallel=par, consume=2, debug=True)
        p_s2 = p_s2([p_p2])
        p_p2.start(forced=True)
        p_s2.start(forced=True)
        self.assertEqual(list(p_s2), [23, 43, 63, 83, 103, 123])

    def testproduce_from_sequence_spawn_consume(self):
        for i in range(20):
            inp = [(11, 12), (21, 22), (31, 32), (41, 42), (51, 52), (61, 62)]
            par = IMap(stride=3)

            w_p2 = Worker(workers.core.ipasser)
            p_p2 = Piper(w_p2, parallel=par, produce=2)
            p_p2 = p_p2([inp])

            w_m2 = Worker(mul2)
            p_m2 = Piper(w_m2, parallel=par, spawn=2)
            p_m2 = p_m2([p_p2])

            w_s2 = Worker(ss2)
            p_s2 = Piper(w_s2, parallel=par, consume=2)
            p_s2 = p_s2([p_m2])

            p_p2.start(forgive=True)
            p_m2.start(forgive=True)
            p_s2.start(forced=True)
            self.assertEqual(list(p_s2), [46, 86, 126, 166, 206, 246])


#    def xtestproduce2(self):
#        for p in range(1, 5):
#            for s in range(1, 7):
#                for d in range(17):
#                    print 'stride%s, last %s' % (s, d - 1)
#                    inp = range(d)
#                    par = IMap(worker_num=s)
#                    w_p2 = Worker(pow2)
#                    p_p2 = Piper(w_p2, parallel=par, produce=2, debug=True)
#                    p_p2 = p_p2([inp])
#                    p_p2.start(forced=True)
#                    res = []
#                    for i in range(100):
#                        try:
#                            res.append(p_p2.next())
#                        except StopIteration:
#                            res.append('s')
#                    print res


#    def testproduce_spawn_consume(self):
#        for i in range(20):
#            inp = [0, 1, 2, 3, 4, 5, 6]
#            par = IMap(worker_num=3)
#            par = None
#
#            w_p2 = Worker(pow2)
#            p_p2 = Piper(w_p2, parallel=IMap(), produce=2, debug=True)
#            p_p2 = p_p2([inp])
#
#            w_sq = Worker(sqrr)
#            p_sq = Piper(w_sq, parallel=par, spawn=2, debug=True)
#            p_sq = p_sq([p_p2])
#
#            w_ip = Worker(workers.core.ipasser)
#            p_ip = Piper(w_ip, parallel=None, spawn=2, debug=True)
#            p_ip = p_ip([p_sq])
#
#            p_p2.start(forced=True)
#            p_sq.start(forced=True)
#            p_ip.start(forced=True)
#            res = []
#            for i in range(16):
#                try:
#                    res.append(p_ip.next())
#                except StopIteration:
#                    res.append('s')
#            print res
#            return
#
#            w_s2 = Worker(ss2)
#            p_s2 = Piper(w_s2, parallel=par, consume=2, debug=True)
#            p_s2 = p_s2([p_sq])
#
#            p_p2.start(forced=True)
#            p_sq.start(forced=True)
#            p_s2.start(forced=True)
#            self.assertEqual(list(p_s2), [0.0, 2.0, 4.0, 6.0, 8.0, 10.0, 12.0])

#    def testproduce_consume2(self):
#        for st in (1, 2, 3, 4, 5):
#            for par in (IMap(stride=st), None):
#                from math import sqrt
#                inp = [1, 2, 3, 4, 5]
#                w_p2 = Worker(pow2)
#                p_p2 = Piper(w_p2, parallel=par, produce=200)
#                p_p2 = p_p2([inp])
#                w_ss = Worker(ss)
#                p_ss = Piper(w_ss, parallel=par, consume=200)
#                p_ss([p_p2])
#                p_p2.start(forgive=True)
#                p_ss.start(forced=True)
#                for j in [1, 2, 3, 4, 5]:
#                    self.assertAlmostEqual(p_ss.next(), 200 * j)
#                self.assertRaises(StopIteration, p_ss.next)
#                p_ss.stop(ends=[1])

    def testtimeout(self):
        par = IMap(worker_num=1)
        piper = Piper(sleeper, parallel=par, timeout=0.75)
        inp = [0.5, 1.0, 0.5]
        piper([inp])
        piper.start(forced=True)
        assert piper.next()[0] == 0.5 # get 1
        a = piper.next() # get timeout
        self.assertTrue(isinstance(a, PiperError))
        self.assertTrue(isinstance(a[0], TimeoutError))
        assert piper.next()[0] == 1.0
        assert piper.next()[0] == 0.5
        piper.stop(ends=[0], forced=True) # really did not finish

    def test_tee(self):
        for i in range(20):
            inp = iter([1, 2, 3, 4, 5, 6])
            w_ip = Worker(workers.core.ipasser)
            w_p2 = Worker(pow2)
            w_m2 = Worker(mul2)
            p_ip = Piper(w_ip)
            p_p2 = Piper(w_p2)
            p_m2 = Piper(w_m2)
            p_ip([inp])
            p_p2([p_ip])
            p_m2([p_ip])
            p_ip.start()
            p_p2.start()
            p_m2.start()
            assert zip(p_p2, p_m2) == [(1, 2), (4, 4), (9, 6), \
                                       (16, 8), (25, 10), (36, 12)]

#    def test_tee_produce(self):
#        inp = iter([1, 2, 3, 4, 5, 6])
#        w_ip = Worker(workers.core.ipasser)
#        w_p2 = Worker(workers.core.npasser, (2,))
#        w_m2 = Worker(workers.core.npasser, (2,))
#        p_ip = Piper(w_ip, produce=2, debug=True)
#        p_p2 = Piper(w_p2, consume=2, debug=True)
#        p_m2 = Piper(w_m2, consume=2, debug=True)
#        p_ip([inp])
#        p_p2([p_ip])
#        p_m2([p_ip])
#        p_ip.start(forced=True)
#        p_p2.start(forced=True)
#        p_m2.start(forced=True)
#        assert list(p_p2) == [[(1,), (1,)], [(2,), (2,)], [(3,), (3,)], [(4,), \
#                                              (4,)], [(5,), (5,)], [(6,), (6,)]]
#        assert list(p_m2) == [[(1,), (1,)], [(2,), (2,)], [(3,), (3,)], [(4,), \
#                                              (4,)], [(5,), (5,)], [(6,), (6,)]]


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
        self.dag = Dagger((self.dbl, self.pwr))
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
        self.assertRaises(DaggerError, self.dag.add_piper, [1])
        self.assertRaises(DaggerError, self.dag.add_piper, [self.dblpwr])
        self.assertRaises(DaggerError, self.dag.add_piper, 1)
        self.assertRaises(DaggerError, self.dag.add_pipers, [1])
        self.assertRaises(TypeError, self.dag.add_pipers, 1)
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
        self.assertRaises(DaggerError, self.dag.add_piper, self.w_pwr, create=False)
        self.assertRaises(DaggerError, self.dag.add_piper, [1], create=True)
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
        self.assertRaises(DaggerError, self.dag.del_pipers, \
                                               [self.dbl, self.pwrpwr])
        self.assertRaises(DaggerError, self.dag.del_piper, (1, 2))

    def test_incoming2(self):
        self.dag.add_pipe((self.pwr, self.dbl, self.pwrdbl))
        self.assertTrue(self.pwr in self.dag)
        self.assertRaises(DaggerError, self.dag.del_piper, self.pwr)
        self.dag.del_piper(self.pwr, forced=True)
        self.assertFalse(self.pwr in self.dag)

    def test_add_workers(self):
        self.dag.add_piper(self.w_pwr)
        self.dag.add_pipers([self.w_pwr])
        self.assertEqual(len(self.dag), 2)
        self.assertEqual(len(self.dag.nodes()), 2)
        self.assertRaises(DaggerError, self.dag.add_piper, [1])
        self.assertRaises(DaggerError, self.dag.add_piper, 1)
        self.dag.add_pipers([self.w_pwr, self.w_dbl])
        self.dag.add_piper(self.w_pwrdbl)
        self.assertEqual(len(self.dag), 5)
        self.dag.add_piper(self.w_pwrdbl)
        self.assertRaises(DaggerError, self.dag.add_piper, self.w_pwrdbl, create=False)
        self.assertEqual(len(self.dag), 6)

    def test_add_pipe(self):
        self.dag = Dagger()
        self.dag.add_pipe((self.pwr, self.dbl))
        self.assertEqual(len(self.dag.nodes()), 2)
        self.assertEqual(len(self.dag.edges()), 1)
        self.dag.add_pipe((self.dbl, self.pwrdbl))
        self.assertEqual(len(self.dag.nodes()), 3)
        self.assertEqual(len(self.dag.edges()), 2)
        self.assertEqual(len(self.dag.deep_nodes(self.pwrdbl)), 2)

    def test_del_pipe(self):
        self.dag.add_pipe((self.pwr, self.sm2))
        self.dag.add_pipe((self.pwr, self.dbl))
        self.dag.add_pipe((self.dbl, self.sm2))
        self.assertEqual(len(self.dag.edges()), 3)
        self.assertEqual(len(self.dag.nodes()), 3)
        self.assertEqual(len(self.dag.incoming_edges(self.pwr)), 2)
        self.dag.del_pipe((self.dbl, self.sm2))
        self.assertEqual(len(self.dag.nodes()), 1)

    def test_del_pipe2(self):
        self.dag.add_pipe((self.pwr, self.dbl, self.pwrpwr, self.dblpwr))
        self.dag.add_pipe((self.pwr, self.spr))
        self.dag.del_piper(self.dblpwr)
        self.assertRaises(DaggerError, self.dag.del_piper, self.pwr)
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
            inp = [1, 2, 3, 4]
            pwr = Piper(pow2, parallel=par)
            dbl = Piper(double, parallel=par)
            self.dag.add_pipe((pwr, dbl))
            pwr([inp])
            self.dag.connect()
            pwr.start(forced=True)
            self.assertEqual(list(dbl), [2, 8, 18, 32])
            pwr.stop(ends=[1])

    def test_connect_disconnect(self):
        for par in (IMap(), None):
            self.dag = Dagger()
            pwr = Piper(pow2, parallel=par)
            dbl = Piper(double, parallel=par)
            pwrdbl = Piper([power, double])
            self.dag.add_pipe((pwr, dbl))
            self.dag.add_piper(pwrdbl)
            self.dag.connect_inputs([[1, 2, 3], [4, 5, 6]])
            self.dag.connect()
            self.dag.disconnect()
            assert pwr.connected is False
            assert dbl.connected is False
            assert pwrdbl.connected is False
            if pwrdbl.imap is not imap:
                assert pwrdbl.imap._tasks == []

    def test_connect_disconnect2(self):
        for par in (IMap(), None):
            self.dag = Dagger()
            pwr = Piper(pow2, parallel=par)
            dbl = Piper(double, parallel=par)
            pwrdbl = Piper([power, double])
            pwrpwr = Piper([power, power])
            self.dag.add_pipe((pwr, dbl))
            self.dag.add_pipe((pwrdbl, pwrpwr))
            self.dag.connect([[1, 2, 3], [4, 5, 6]])
            self.dag.disconnect()
            assert pwr.connected is False
            assert dbl.connected is False
            assert pwrdbl.connected is False
            assert pwrpwr.connected is False
            if pwrdbl.imap is not imap:
                assert pwrdbl.imap._tasks == []

    def test_startstop(self):
        for rep in range(5):
            for i in range(12):
                for w in (1, 2, 3, 4, 5, 6, 7):
                    d = range(i)
                    print '#',
                    self.dag = Dagger()
                    imaps = [IMap(worker_num=w), IMap(worker_num=w), \
                             IMap(worker_num=w), IMap(worker_num=w)]

                    i1 = randint(0, 3)
                    i2 = randint(0, 3)
                    i3 = randint(0, 3)
                    i4 = randint(0, 3)
                    pwr = Piper(power, parallel=imaps[i1])
                    #dbl_1 = Piper(double, parallel=imaps[1])
                    #dbl_2 = Piper(double, parallel=imaps[2])
                    dbl_3 = Piper(double, parallel=imaps[i2])
                    dbl_4 = Piper(double, parallel=imaps[i3])
                    sum_2 = Piper(sum2, parallel=imaps[i4])




                    self.dag.add_pipe((pwr, dbl_3))
                    self.dag.add_pipe((pwr, dbl_4))
                    #self.dag.add_pipe((dbl_1, dbl_3))
                    #self.dag.add_pipe((dbl_2, dbl_4))
                    self.dag.add_pipe((dbl_3, sum_2))
                    self.dag.add_pipe((dbl_4, sum_2))

                    self.dag.connect([d])
                    self.dag.start()

                    assert list(sum_2) == [(i ** 2) * 4 for i in d]
        print

    def test_inputoutput(self):
        for par in (False, IMap()):
            self.dag = Dagger()
            pwr = Piper(pow2, parallel=par)
            dbl = Piper(double, parallel=par)
            pwr2 = Piper(pow2, parallel=par)
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
        self.pwrp = Piper(power, parallel=i)
        self.dblp = Piper(double, parallel=i)
        self.dbl = Piper(double)
        self.spr = Piper(sleeper)
        self.pwrdbl = Piper([power, double])
        self.dblpwr = Piper([double, power])
        self.dbldbl = Piper([double, double])
        self.pwrpwr = Piper([power, power])
        self.plum = Plumber()

    def test_init(self):
        assert isinstance(self.plum, Dagger)

    def test_round(self):
        self.plum.add_piper(self.mul, xtra={'color':'red'})
        #print self.plum[self.mul].xtra
        self.plum.add_piper(self.pwr)
        rr = Worker(sys_ver)
        pr = Piper(rr, parallel=IMap(name='illo'))
        self.plum.add_piper(self.pwrp)
        self.plum.add_piper(pr)
        self.plum.add_pipe((self.mul, self.pwr))
        self.plum.save('test.py')
        self.plum2 = Plumber()
        self.plum2.load('test.py')

    def test_pluge1(self):
        #imap
        self.plum.add_pipe([self.pwr, self.dbl])
        self.plum.plunge([[1, 2, 3]])
        self.plum.chinkup()
        # IMap
        self.plum = Plumber()
        self.plum.add_pipe([self.pwrp, self.dblp])
        self.plum.plunge([[1, 2, 3]])
        self.plum.chinkup()

    def test_track(self):
        inpt = xrange(100)
        imap_ = IMap()
        wrkr = Worker((power, workers.io.json_dumps, workers.io.dump_shm_item))
        pipr = Piper(wrkr, parallel=imap_, track=True)
        self.plum.add_piper(pipr)
        pipr([inpt])
        self.plum.plunge()
        self.plum._is_finished.wait()
        dct = self.plum.stats['pipers_tracked'].values()[0][0]
        assert len(dct) == 100
        for i in dct.values():
            os.unlink('/dev/shm/%s' % i)


suite_Graph = unittest.makeSuite(test_Graph, 'test')
suite_Worker = unittest.makeSuite(test_Worker, 'test')
suite_Piper = unittest.makeSuite(test_Piper, 'test')
suite_Dagger = unittest.makeSuite(test_Dagger, 'test')
suite_Plumber = unittest.makeSuite(test_Plumber, 'test')

if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(suite_Graph)
    #runner.run(suite_Worker)
    #runner.run(suite_Piper)
    runner.run(suite_Dagger)
    #runner.run(suite_Plumber)
    #runner.run(suite_Plumber)


#EOF
