""" IO
"""
from itertools import izip

def pipeline_to_dot(digraph, file_name):
        """ Returns a string representing a dot-file.
        """
        file_name = "test.dot"
        handle = file(file_name, 'wb')
        dot_out = 'digraph \"%s\" {\r\n' % repr(digraph)
        dot_out += 'edge [penwidth =2.0];\r\n'
        dot_out += 'node [shape =box]'
        print digraph

        for node, Node in digraph.iteritems():
            attrs =  'subgraph \"cluster_%s\" {\r\n' % id(node)
            attrs += 'node [style=filled];\r\n'
            attrs += 'color=%s;\r\n' % (Node.xtra['color'] or 'blue')
            attrs += 'edge [penwidth =0.5 color =lightgray];\r\n'
            task_ids = [str(id(node)+i) for i in range(len(node.worker.task))]
            for id_, task in izip(task_ids, node.worker.task):
                attrs += '%s [label ="%s"]' % (id_, task.__name__)
            attrs += " -> ".join(task_ids) + ";\r\n"
            attrs += "}\r\n"
            dot_out += attrs

        for src, dst in digraph.edges():
            dot_out += '%s -> %s [color = %s];\r\n' % (id(dst) + len(dst.worker.task) - 1, id(src), (digraph[dst].xtra['color'] or 'blue'))

        start_pipers = [p for p in digraph if not digraph.outgoing_edges(p)]
        end_pipers = [p for p in digraph if not digraph.incoming_edges(p)]
        for start in start_pipers:
            print start.inbox
            start_id = id((start.inbox or start)) -1
            dot_out += "%s [label =input, shape=plaintext]\r\n" % start_id
            dot_out += '%s -> %s [style=dashed, color = blue];\r\n' % (start_id, id(start))
        for end in end_pipers:
            dot_out += "_%s [label =output, shape=plaintext]\r\n" % (id(end))
            dot_out += '%s -> _%s [style=dashed, color = %s];\r\n' % (id(end) + len(end.worker.task) - 1, id(end), (digraph[end].xtra['color'] or 'blue'))



        dot_out += "}"
        handle.write(dot_out)
        handle.close()