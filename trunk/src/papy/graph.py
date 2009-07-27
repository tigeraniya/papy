"""
"""
from collections import deque
from itertools import repeat, izip


class Node(dict):
    """ Node of a Graph.
    """
    def __init__(self, entity =None, xtra =None):
        self.clear()
        try:
            if entity is not None:
                dict.__init__(self, {entity:Node(xtra =xtra)})
            else:
                self.xtra = (xtra or {})
        except Exception, e:
            raise e

    def clear(self):
        """ Sets the discovered and examined attributes to False.
        """
        self.discovered = False
        self.examined = False

    def nodes(self):
        """ Returns a list of nodes directly reachable from this node.
        """
        return self.keys()

    def iternodes(self):
        """ Returns an iterator of nodes _directly_ reachable from this node.
        """
        return self.iterkeys()

    def deep_nodes(self, allnodes =None):
        """ A recursive method to return _all_ nodes reachable from this node.
        """
        allnodes = (allnodes or [])
        for (node, Node_) in ((n, N) for (n, N) in\
            self.iteritems() if not n in allnodes):
            allnodes.append(node)
            Node_.deep_nodes(allnodes)
        return allnodes


class Graph(dict):
    """ Dictionary based Graph class.
    """
     #This Graph implementation is a little bit unusual as it does not explicitly
     #hold a list of edges. The graph is a dictionary where the keys of the dictionary
     #are any hashable objects, while the values are Node instances. A Node instance is
     #also a dictionary, where the keys are objects and the values are Node instances.
     #A node instance(value) is basically a dictionary of outgoing edges from the
     #object(key). The edges are indexed by the incoming objects. So we end up with a
     #recursivly nested dictionary which defines the topology of the graph.

    def __init__(self, nodes =(), edges =(), xtras =None):
        """ Accepts a list of nodes and edges as input.
        """
        self.add_nodes(nodes, xtras)
        self.add_edges(edges)

    def dfs(self, node, bucket =None, order ='append'):
        """ Recursive depth first search. By default (order = 'append') this returns the
            nodes in the reverse postorder. To change this into the preorder use a
            collections.deque bucket and order 'appendleft'.

            Arguments:

              * bucket(list or queue) [default: None]

                The user *must* provide the list or queue to store the nodes

              * order(str) [default: 'append']

                Method of the bucket which will be called with the node which has been
                examined. Other valid options might be 'appendleft' for a dequeue.
        """
        if self[node].discovered:
            return bucket
        self[node].discovered = True
        for node_ in self[node].iternodes():
            self.dfs(node_, bucket, order)
        getattr(bucket, order)(node)
        self[node].examined = True
        return bucket

    def postorder(self, reverse =False):
        """ Returns the postorder of nodes if the graph.
            If it is a directed acyclic graph.
        """
        nodes = []
        for node in self.nodes():
            self.dfs(node, nodes)
            if reverse: nodes.reverse()
        if reverse: nodes.reverse()
        self.clear_nodes()
        return nodes

    def preorder(self, reverse =False):
        """ Returns the preorder of nodes if the graph.
            If it is a directed acyclic graph.
        """
        nodes = deque([])
        for node in self.nodes():
            self.dfs(node, nodes, order ='appendleft')
            if not reverse: nodes.rotate()
        if not reverse: nodes.rotate()
        self.clear_nodes()
        return list(nodes)

    def maxdepth(self):
        nodes = self.postorder()
        node_depth = {}
        for node in nodes:
            max_depth = 0
            for child in self[node].nodes():
                some_depth = node_depth[child] + 1
                max_depth = max(max_depth, some_depth)
            node_depth[node] = max_depth
        return node_depth

    def maxwidth(self):
        nodes = self.postorder()
        node_width = {}
        for node in nodes:
            max_width = 1
            for child in self[node].nodes():
                some_width = node_
        max_rank = max(node_rank.values())
        for rank in xrange(max_rank):
            pass

    def add_node(self, node, xtra =None):
        """ Adds a node to the graph. Returns True if a new node has been added.
        """
        if not node in self:
            n = Node(node, xtra)
            self.update(n)
            return True
        return False


    def del_node(self, node):
        for node_ in self.values():
            if node in node_:
                node_.pop(node)
        self.pop(node)

    def add_edge(self, edge, double =False):
        (left_entity, right_entity) = edge
        self.add_node(left_entity)
        self.add_node(right_entity)
        self[left_entity].update({right_entity:self[right_entity]})
        if double:
            self.add_edge((edge[1], edge[0]))

    def del_edge(self, edge, double =False):
        (left_entity, right_entity) = edge
        self[left_entity].pop(right_entity)
        if double:
            self.del_edge((edge[1], edge[0]))

    def add_nodes(self, nodes, xtras =None):
        for node, xtra in izip(nodes, (xtras or repeat(None))):
            self.add_node(node, xtra)

    def del_nodes(self, nodes):
        for node in nodes:
            self.del_node(node)

    def add_edges(self, edges, *args, **kwargs):
        for edge in edges:
            self.add_edge(edge, *args, **kwargs)

    def del_edges(self, edges, *args, **kwargs):
        for edge in edges:
            self.del_edge(edge, *args, **kwargs)

    def nodes(self):
        return self.keys()

    def iternodes(self):
        return self.iterkeys()

    def clear_nodes(self):
        for node in self.itervalues():
            node.clear()

    def deep_nodes(self, node):
        return self[node].deep_nodes()

    def edges(self, nodes =None):
        """ Returns all edges in the Graph.
        """
        # If a Node has been directly updated (__not__ recommended)
        # then the Graph will not know the added nodes and therefore will
        # miss half of their edges.
        edges = set()
        for node in (nodes or self.iterkeys()):
            ends = self[node].nodes()
            edges.update([(node, end) for end in ends])
        return tuple(edges)

    def incoming_edges(self, node):
        edges = self.edges()
        in_edges = []
        for out_node, in_node in edges:
            if node is in_node:
                in_edges.append((out_node, in_node))
        return tuple(in_edges)

    def outgoing_edges(self, node):
        edges = self.edges()
        out_edges = []
        for out_node, in_node in edges:
            if node is out_node:
                out_edges.append((out_node, in_node))
        return tuple(out_edges)
