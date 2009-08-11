"""
:mod:`papy.graph`
=================

This module implements a graph data structure without explicit edges, using 
nested Python dictionaries.
"""
from collections import deque, defaultdict
from itertools import repeat, izip


class Node(dict):
    """ 
    *Node* is the topological node of a *Graph*. Please note that the 
    node object is not the same as the topological node. The node object is any
    hashable Python object. The topological node is is defined for each node 
    object and is a dictionary of other node objects with incoming edges from 
    this node object.
    """
    def __init__(self, entity=None, xtra=None):
        self.discovered = False
        self.examined = False
        try:
            if entity is not None:
                dict.__init__(self, {entity:Node(xtra=xtra)})
            else:
                self.xtra = (xtra or {})
        except Exception, excp:
            raise excp

    def clear(self):
        """
        Sets the discovered and examined attributes to ``False``.
        """
        self.discovered = False
        self.examined = False

    def nodes(self):
        """
        Returns a list of node objects directly reachable from this *Node*.
        """
        return self.keys()

    def iternodes(self):
        """
        Returns an iterator of node objects directly reachable from this *Node*.
        """
        return self.iterkeys()

    def deep_nodes(self, allnodes=None):
        """
        A recursive method to return **all** node objects reachable from this 
        *Node*.
        """
        allnodes = (allnodes or [])
        for (node, node_) in ((node, node_) for (node, node_) in\
            self.iteritems() if not node in allnodes):
            allnodes.append(node)
            node_.deep_nodes(allnodes)
        return allnodes


class Graph(dict):
    """
    Dictionary based graph data structure.

    This *Graph* implementation is a little bit unusual as it does not 
    explicitly hold a list of edges. The *Graph* is a dictionary where the keys
    of the dictionary are any hashable objects (node objects), while the values 
    are *Node* instances. A *Node* instance is also a dictionary, where the keys
    are node objects and the values are *Node* instances. A *Node* 
    instance(value) is basically a dictionary of outgoing edges from the node 
    object(key). The edges are indexed by the incoming objects. So we end up 
    with a recursivly nested dictionary which defines the topology of the 
    *Graph*.
    
    Arguments:
        
        * nodes(sequence of nodes) [default: ``()``]
        
            A sequence of nodes to be added to the graph. 
            See: ``Graph.add_nodes``
        
        * edges(sequence of edges) [default: ``()``]
        
            A sequence of edges to be added to the graph.
            See: ``Graph.add_edges``
            
        * xtras(sequence of dictionaries) [default: ``None``]
        
            A sequence of xtra dictionaries corresponding to the added
            node objects. The topological nodes corresponding to the added
            dictionaries will have ``Node.xtra`` updated with the contents of 
            this sequence. Either all or no xtra dictionaries have to be 
            specified.
    """
    def __init__(self, nodes=(), edges=(), xtras=None):
        self.add_nodes(nodes, xtras)
        self.add_edges(edges)
        dict.__init__(self)

    def dfs(self, node, bucket=None, order='append'):
        """
        Recursive depth first search. By default (order = 'append') this returns
        the node objects in the reverse postorder. To change this into the 
        preorder use a ``collections.deque`` bucket and order 'appendleft'.
            
        Arguments:

            * bucket(list or queue) [default: ``None``]

                The user *must* provide the list or queue to store the nodes

            * order(str) [default: 'append']

                Method of the bucket which will be called with the node object 
                which has been examined. Other valid options might be 
                'appendleft' for a ``dequeue``.
        """
        if self[node].discovered:
            return bucket
        self[node].discovered = True
        for node_ in self[node].iternodes():
            self.dfs(node_, bucket, order)
        getattr(bucket, order)(node)
        self[node].examined = True
        return bucket

    def postorder(self, reverse=False):
        """
        Returns the postorder of node objects of the *Graph* if it is a directed
        acyclic graph.
        
        Arguments:
        
            * reverse(bool) [default: ``False``]
            
                Should the order be reversed?
        """
        nodes = []
        for node in self.nodes():
            self.dfs(node, nodes)
            if reverse:
                nodes.reverse()
        if reverse:
            nodes.reverse()
        self.clear_nodes()
        return nodes

    def preorder(self, reverse=False):
        """
        Returns the preorder of node objects if the *Graph* if it is a directed
        acyclic graph.
        
        
        """
        nodes = deque([])
        for node in self.nodes():
            self.dfs(node, nodes, order='appendleft')
            if not reverse:
                nodes.rotate()
        if not reverse:
            nodes.rotate()
        self.clear_nodes()
        return list(nodes)

    def node_rank(self):
        """
        Returns the maximum rank for each node in ther graph. The rank of a node
        is define as the number of edges between the node and a node which has
        rank 0. A node has rank 0 if it has no incoming edges.
        """
        nodes = self.postorder()
        node_rank = {}
        for node in nodes:
            max_rank = 0
            for child in self[node].nodes():
                some_rank = node_rank[child] + 1
                max_rank = max(max_rank, some_rank)
            node_rank[node] = max_rank
        return node_rank

    def node_width(self):
        nodes = self.postorder()
        node_width = {}
        for node in nodes:
            sum_width = 0
            for child in self[node].nodes():
                sum_width += node_width[child]
            node_width[node] = (sum_width or 1)
        return node_width

    def rank_width(self):
        """
        Returns the width of each rank in the graph.
        """
        rank_width = defaultdict(int)
        node_rank = self.node_rank()
        for rank in node_rank.values():
            rank_width[rank] += 1
        return dict(rank_width)


    def add_node(self, node, xtra=None):
        """
        Adds a node object to the *Graph*. Returns ``True`` if a new node object
        has been added. If the node object is already in the *Graph* returns 
        ``False``.
        
        Arguments:
        
            * node(object)
            
                Node to be added. Any hashable Python object.
                
            * xtra(dict) [default: ``None``]
            
                The newly created topological ``Node.xtra`` dictionary will be 
                updated with the contents of this dictionary. 
        """
        if not node in self:
            node_ = Node(node, xtra)
            self.update(node_)
            return True
        return False

    def del_node(self, node):
        """
        Removes a node object to the *Graph*. Returns ``True`` if a node object
        has been removed. If the node object was not in the *Graph* raises a
        ``KeyError``.
        
        Arguments:
            
            * node(object)
            
                Node to be removed. Any hashable Python object. 
        """
        for node_ in self.values():
            if node in node_:
                node_.pop(node)
        return bool(self.pop(node))

    def add_edge(self, edge, double=False):
        """
        Adds an edge to the *Graph*. An edge is just a pair of node objects. If 
        the node objects are not in the *Graph* they are added.
        
        Arguments:
        
            * edge(sequence)
            
                An ordered pair of node objects. The edge is assumed to have a 
                direction from the first to the second node object.
                
            * double(bool) [default: ``False```]
            
                If ``True`` the the reverse edge is also added.
        """
        (left_entity, right_entity) = edge
        self.add_node(left_entity)
        self.add_node(right_entity)
        self[left_entity].update({right_entity:self[right_entity]})
        if double:
            self.add_edge((edge[1], edge[0]))

    def del_edge(self, edge, double=False):
        """
        Removes an edge to the *Graph*. An edges is just a pair of node objects.
        But the node objects are not removed from the *Graph*.
        
        Arguments:
        
            * edge(sequence)
            
                An ordered pair of node objects. The edge is assumed to have 
                a direction from the first to the second node object.
                
            * double(bool) [default: ``False```]
            
                If ``True`` the the reverse edge is also remove.
        """
        (left_entity, right_entity) = edge
        self[left_entity].pop(right_entity)
        if double:
            self.del_edge((edge[1], edge[0]))

    def add_nodes(self, nodes, xtras=None):
        """
        Adds nodes to the graph.
        
        Arguments:
        
            * nodes(sequence of objects)
            
                Sequence of node objects to be added to the *Graph*
                
            * xtras(sequence of dictionaries) [default: ``None``]
            
                Sequence of ``Node.xtra`` dictionaries corresponding to the node
                objects being added. See: ``Graph.add_node``.
        """
        for node, xtra in izip(nodes, (xtras or repeat(None))):
            self.add_node(node, xtra)

    def del_nodes(self, nodes):
        """
        Removes nodes from the graph.
        
        Arguments:
        
            * nodes(sequence of objects)
            
                Sequence of node objects to be removed from the *Graph*. See:
                ``Graph.del_node``.
        """
        for node in nodes:
            self.del_node(node)

    def add_edges(self, edges, *args, **kwargs):
        """
        Adds edges to the graph. Takes optional arguments for 
        ``Graph.add_edge``.
        
        Arguments:
        
            * edges(sequence of edges)
            
                Sequence of edges to be added to the *Graph*
        """
        for edge in edges:
            self.add_edge(edge, *args, **kwargs)

    def del_edges(self, edges, *args, **kwargs):
        """
        Removes edges from the graph. Takes optional arguments for 
        ``Graph.del_edge``.
        
        Arguments:
        
            * edges(sequence of edges)
            
                Sequence of edges to be removed from the *Graph*
        """
        for edge in edges:
            self.del_edge(edge, *args, **kwargs)

    def nodes(self):
        """
        Returns a list of all node objects in the *Graph* 
        """
        return self.keys()

    def iternodes(self):
        """
        Returns an iterator of all node objects in the *Graph* 
        """
        return self.iterkeys()

    def clear_nodes(self):
        """
        Clears all nodes in the *Graph*. See ``Node.clear``.
        """
        for node in self.itervalues():
            node.clear()

    def deep_nodes(self, node):
        """
        Returns all reachable node objects from a node object. See: 
        ``Node.deep_nodes``
        """
        return self[node].deep_nodes()

    def edges(self, nodes=None):
        """
        Returns a tuple of all edges in the *Graph*.
        
        Arguments:
        
            * nodes(objects)
            
                If specified the edges will be limited to those originating
                from one of the specified nodes.
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
        """
        Returns a tuple of incoming edges for this node object.
        
        Arguments:
        
            * node(object)
            
                Node to be inspected for incoming edges.
        """
        edges = self.edges()
        in_edges = []
        for out_node, in_node in edges:
            if node is in_node:
                in_edges.append((out_node, in_node))
        return tuple(in_edges)

    def outgoing_edges(self, node):
        """
        Returns a tuple of outgoing edges for this node object.
        
        Arguments:
        
            * node(object)
            
                Node to be inspected for outgoing edges.
        """
        #TODO: pls make outgoig_edges less insane
        edges = self.edges()
        out_edges = []
        for out_node, in_node in edges:
            if node is out_node:
                out_edges.append((out_node, in_node))
        return tuple(out_edges)
