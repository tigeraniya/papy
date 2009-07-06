#!/usr/bin/env python
# Python
from threading import Thread
from Queue import Queue
from code import InteractiveConsole
from functools import partial

# Tkinter imports
from Tkinter import *
import Pmw
from tkMessageBox import *

# papy
from graph import Graph


g = Graph()
g.add_node('node_1')
g.add_node('node_2')
g.add_node('node_3')
g['node_1'].xtra['x'] = 25
g['node_1'].xtra['y'] = 25
g['node_1'].xtra['color'] = 'red'
g['node_1'].xtra['status'] = 'green'
g['node_1'].xtra['screen_name'] = 'node_1'
g['node_2'].xtra['x'] = 75
g['node_2'].xtra['y'] = 75
g['node_2'].xtra['color'] = 'blue'
g['node_2'].xtra['status'] = 'orange'
g['node_2'].xtra['screen_name'] = 'node_2'

g['node_3'].xtra['x'] = 75
g['node_3'].xtra['y'] = 25
g['node_3'].xtra['color'] = 'green'
g['node_3'].xtra['status'] = 'red'
g['node_3'].xtra['screen_name'] = 'node_3'

g.add_edge(('node_1', 'node_2'))
g.add_edge(('node_2', 'node_3'))



class Options(dict):

    defaults = (('app_name', 'PaPy'),
                ('node_color', 'blue'),
                ('node_status', 'green'),
                ('graph_background', 'aliceblue'),
                ('pipers_background', 'white'),
                ('resources_background', 'white')
                )

    def __init__(self, options =None):
        init = dict(self.defaults)
        init.update(options or {})
        dict.__init__(self, init)


class ScrolledText(Frame):

    def __init__(self, parent =None, text ='', file =None):
        Frame.__init__(self, parent)
        self.make_widgets()
        self.settext(text, file)

    def make_widgets(self):
        sbar = Scrollbar(self)
        text = Text(self, relief=SUNKEN)
        sbar.config(command =text.yview)                  # xlink sbar and text
        text.config(yscrollcommand =sbar.set)             # move one moves other
        sbar.pack(side=RIGHT, fill=Y)                     # pack first=clip last
        text.pack(side=LEFT, expand=YES, fill=BOTH)       # text clipped first
        self.text = text

    def settext(self, text ='', file =None):
        if file:
            text = open(file, 'r').read()
        self.text.delete('1.0', END)                     # delete current text
        self.text.insert('1.0', text)                    # add at line 1, col 0
        self.text.mark_set(INSERT, '1.0')                # set insert cursor
        self.text.focus()                                # save user a click

    def gettext(self):                                   # returns a string
        return self.text.get('1.0', END+'-1c')           # first through last



class MenuBar(Menu):

    def __init__(self, *args, **kwargs):
        Menu.__init__(self, *args, **kwargs)
        self.create_widgets()

    def not_impl(self):
        showerror(message ='Not implemented')

    def create_widgets(self):
        file_menu = Menu(self)
        file_menu.add_command(label ='Open', command =self.not_impl)
        file_menu.add_command(label ='Save', command =self.not_impl)
        self.add_cascade(label="File", underline=1, menu =file_menu)

class StatusBar(Frame):
    def __init__(self, parent):
        Frame.__init__(self, parent)
        self.label = Label(self, bd=1, anchor=W)
        self.label.pack(fill=X)

    def set(self, format, *args):
        self.label.config(text=format % args)
        self.label.update_idletasks()

    def clear(self):
        self.label.config(text="")
        self.label.update_idletasks()

class GraphCanvas(Pmw.ScrolledCanvas):

    def __init__(self, graph, parent =None, **kwargs):
        apply(Pmw.ScrolledCanvas.__init__, (self, parent), kwargs)
        
        # position of the last click
        self.lastx = 0
        self.lasty = 0
        self.lasttags = []

        self.tag_to_node = {}
        self.tag_to_Node = {}
        self.tag_to_edge = {}

        self.component('canvas').config(bg =O['graph_background'], width= 600, height =400, relief =SUNKEN)
        self.component('canvas').bind("<Button-1>",  self.mouse1_down)
        self.component('canvas').bind("<B1-Motion>", self.mouse1_drag)
        self.component('canvas').bind("<ButtonRelease-1>", self.mouse1_up)
        self.component('canvas').bind("<Button-3>", self.mouse3_down)
        self.component('canvas').bind("<ButtonRelease-3>", self.mouse3_up)
        # MacOSX compability
        self.component('canvas').bind("<Control-Button-1>", self.mouse3_down)
        self.component('canvas').bind("<Control-ButtonRelease-1>", self.mouse3_up)
        self.connect = False

        # left and right click canvas
        self.self_menu = Menu(root, tearoff =0)
        self.self_menu.add_command(label="add piper", command =self.create_piper)
        #self_menu.add_separator()
        #self_menu.add_command(label="cancel pipe", command =self._toggle_connect, state =DISABLED) 

        # right click node
        self.piper_menu = Menu(root, tearoff =0)
        self.piper_menu.add_command(label="edit piper", command =self.edit_piper)
        self.piper_menu.add_command(label="remove piper", command =self.remove_piper)
        self.piper_menu.add_separator()
        self.piper_menu.add_command(label="begin pipe", command =self._toggle_connect, state =NORMAL)
        self.piper_menu.add_command(label="end pipe", command =self._toggle_connect, state =DISABLED)

        # right click edge
        self.edge_menu = Menu(root, tearoff =0)
        self.edge_menu.add_command(label="remove pipe", command =self.remove_pipe)

        self.graph = graph
        self.create_node_tags()
        self.create_edge_tags()

    def _menu_popup(self, event, menu):
        menu.post(event.x_root, event.y_root)

    def _canvas_coords(self, x, y):
        return (int(self.canvasx(x)), int(self.canvasy(y)))

    def _toggle_connect(self):
        if self.connect:
            self.piper_menu.entryconfig(3, state =NORMAL)
            self.piper_menu.entryconfig(4, state =DISABLED)
            self.unbind("<Motion>")
            self.connect = False
        else:
            self.piper_menu.entryconfig(3, state =DISABLED)
            self.piper_menu.entryconfig(4, state =NORMAL)
            self.bind("<Motion>", self.draw_pipe)
            self.connect = True


    def draw_pipe(self, event):
        print event.x, event.y

    def create_node_tags(self):
        for node, Node in self.graph.iteritems():
            tag = self.create_node_tag(node, Node)
            Node.xtra['tag'] = tag[1]
            self.tag_to_node[tag[1]] = node
            self.tag_to_Node[tag[1]] = Node
        self.resizescrollregion()

    def move_node_tag(self, tag, dx, dy):
        # delete edges
        self.delete("%s&&%s" % ('edge', tag))
        # move node
        n1 = self.graph[self.tag_to_node[tag]]
        self.move("%s&&%s" % ('node', tag), dx, dy)
        # update position
        n1.xtra['x'] += dx
        n1.xtra['y'] += dy
        # re-draw affected edges
        edges = self.graph.incoming_edges(self.tag_to_node[tag]) +\
                self.graph.outgoing_edges(self.tag_to_node[tag])
        for edge in edges:
            self.create_edge_tag(edge)
        self.resizescrollregion()

        
    def create_edge_tags(self):
        for edge in self.graph.edges():
            tag = self.create_edge_tag(edge)
            self.tag_to_edge[tag[1]] = edge

    def move_edge_tag(self, tags, dx, dy):
        for tag in tags:
            self.move_node_tag(tag, dx, dy)
        
    def create_node_tag(self, node, Node):
        x = (Node.xtra.get('x') or self.lastx)
        y = (Node.xtra.get('y') or self.lasty)
        status = (Node.xtra.get('status') or O['node_status'])
        color  = (Node.xtra.get('color')  or O['node_color'])
        tag = ("node", "n%s" % id(node))
        self.create_oval(x-12, y-12, x+12, y+12, fill=status, tags =tag,\
                                            activewidth=3.0, activefill ="plum")
        self.create_oval(x-8,  y-8,  x+8,  y+8, fill=color,  tags =tag,\
                                            state ='disabled', width =0.0)
        self.create_text(x+12, y-17, text=tag, fill='black', anchor =NW,\
                                            tags =tag)
        return tag

    def create_edge_tag(self, edge):
        N1 = self.graph[edge[0]]
        N2 = self.graph[edge[1]]
        t1 = N1.xtra['tag']
        t2 = N2.xtra['tag']
        xy1 = N1.xtra['x'], N1.xtra['y']
        xy2 = N2.xtra['x'], N2.xtra['y']
        tag = ("edge", t1, t2)
        self.create_line(*xy1 + xy2, tags =tag)
        return tag

    def create_piper(self):
        pass

    def edit_piper(self):
        pass

    def remove_piper(self):
        t1 = self.lasttags[1]
        n1 = self.tag_to_node[t1]
        # remove piper from dagger
        self.graph.del_node(n1) # wrong!
        # remove pipers and pipes from canvas
        self.delete(self.lasttags[1])
        
    def remove_pipe(self):
        t1, t2 = self.lasttags[1:3]
        n1 = self.tag_to_node[t1]
        n2 = self.tag_to_node[t2]
        # remove pipe from dagger
        self.graph.del_edge((n1,n2)) # wrong!
        # remove pipes from canvas
        self.delete("%s&&%s&&%s" % ('edge', t1, t2))


    def mouse1_down(self, event):
        # canvas position of click
        self.lastx, self.lasty = self._canvas_coords(event.x, event.y)
        # what did we click
        self.lasttags = self.gettags(CURRENT)

    def mouse1_drag(self, event):
        old_x, old_y = self.lastx, self.lasty
        self.lastx, self.lasty = self._canvas_coords(event.x, event.y)
        dx, dy = self.lastx - old_x, self.lasty - old_y

        if self.lasttags and self.lasttags[0] == 'node':
            # node clicked
            self.move_node_tag(self.lasttags[1], dx, dy)
        
        elif self.lasttags and self.lasttags[0] == 'edge':
            # edge clicked
            self.move_edge_tag(self.lasttags[1:3], dx, dy)

        elif not self.lasttags:
            # canvas clicked
            pass

    def mouse1_up(self, event):
        self.lasttag = []
        root.frame.status_bar.set('canvas released at: %s-%s' % (event.x, event.y))

    def mouse3_down(self, event):
        lasttags = self.gettags(CURRENT)
        lastx, lasty = self._canvas_coords(event.x, event.y)

        if lasttags and lasttags[0] == 'node':    
            self._menu_popup(event, self.piper_menu)
        elif lasttags and lasttags[0] == 'edge':    
            self._menu_popup(event, self.edge_menu)
        elif not lasttags:
            self._menu_popup(event, self.self_menu)
        
        if not self.connect:
            self.lasttags = lasttags
            self.lastx = lastx
            self.lasty = lasty

    def mouse3_up(self, event):
        pass



class ToolBar(Frame):
    def __init__(self, parent =None):
        Frame.__init__(self, parent)
        self.make_widgets()

    def make_widgets(self):

        b1 = Button(master =self, text ='B1', command=None)
        b1.pack(side=LEFT)
        b2 = Button(master =self, text ='B2', command=None)
        b2.pack(side=LEFT)

class NoteBook(Pmw.NoteBook):
    def __init__(self, parent =None, **kwargs):
        Pmw.NoteBook.__init__(self, parent)
        self.make_widgets()
        self.setnaturalsize()

class LoggingShell(NoteBook):

    def make_widgets(self):
        self.add('Shell')
        self.add('Logging')
        self.tab('Shell').focus_set()

class Pipeline(NoteBook):

    def make_widgets(self):
        self.add('Pipeline')
        self.add('Functions')
        self.tab('Pipeline').focus_set()

        self.graph = GraphCanvas(graph =g, parent =self.page('Pipeline'))
        self.graph.pack(expand =YES, fill =BOTH)

class PapyMainFrame(Frame):
    def __init__(self, parent =None, title =None,**kwargs):
        Frame.__init__(self, parent)
        self.master.title(title or O['app_name'])
        self.make_widgets()
        self.pack(expand =YES, fill =BOTH)

    def make_widgets(self, title =None):
        #main menu
        self.menu_bar = MenuBar(self)
        self.master.config(menu =self.menu_bar)

        # toolbar
        self.tool_bar = ToolBar(self)
        self.tool_bar.pack(fill =X)

        # 4 panes
        self.lr = PanedWindow(self)
        self.lr.pack(fill=BOTH, expand=YES)

        self.l = PanedWindow(self.lr, orient=VERTICAL)
        self.r = PanedWindow(self.lr, orient=VERTICAL)
        self.lr.add(self.l)
        self.lr.add(self.r)
        
        # pipers
        self.pipers = Pmw.Group(self.l,
		                         tag_pyclass =Label,
		                         tag_text='Pipers',
		                         tag_foreground='blue')
        cw = Pmw.ScrolledCanvas(self.pipers.interior())
        cw.component('canvas').config(bg =O['pipers_background'], width= 200, height =400)
        cw.pack(fill =BOTH, expand =True)
        
        # resources
        self.resources = Pmw.Group(self.l,
		                         tag_pyclass =Label,
		                         tag_text='Resources',
		                         tag_foreground='red')
        cw = Pmw.ScrolledCanvas(self.resources.interior())
        cw.component('canvas').config(bg =O['resources_background'], width= 200, height =400)
        cw.pack(fill =BOTH, expand =YES)
 
        
        self.l.add(self.pipers)
        self.l.add(self.resources)
        self.l.paneconfigure(self.pipers, sticky =N+E+W+S) 
        self.l.paneconfigure(self.resources, sticky =N+E+W+S) 

        # pipeline & code
        self.pipeline = Pipeline(self.r)
        self.r.add(self.pipeline)

        # shell and logging
        self.io = LoggingShell(self.r)
        self.r.add(self.io)

        # statusbar
        self.status_bar = StatusBar(self)
        self.status_bar.pack(side =LEFT)


if __name__ == '__main__':

    def gui_start():
        global root, O
        O = Options()
        root = Tk()
        Pmw.initialise(root)
        root.protocol("WM_DELETE_WINDOW", gui_stop)
        root.frame = PapyMainFrame(root)
        root.mainloop()

    def gui_stop():
        root.destroy()

    gui_thread = Thread(target =gui_start)
    gui_thread.start()

    
