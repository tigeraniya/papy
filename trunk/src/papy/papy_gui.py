#!/usr/bin/env python
# Python
from threading import Thread
from Queue import Queue
import os

# Tkinter imports
import Pmw
from Tkinter import *
from tkMessageBox import *
#
from idlelib.TreeWidget import TreeItem, TreeNode

# papy
#from papy import Worker, Piper, Dagger, Plumber,\
#                 PiperError, WorkerError, DaggerError, PlumberError,\
#                 imports
#
from graph import Graph, Node
import workers
#import utils

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
                ('Pipers_background', 'white'),
                ('IMaps_background', 'white')
                )

    def __init__(self, options =None):
        init = dict(self.defaults)
        init.update(options or {})
        dict.__init__(self, init)


class _TreeItem(TreeItem):

    def __init__(self, item):
        self.item = item
    
    def GetText(self):
        return self.item.name

    def SetText(self, text):
        self.item.name = text

    def IsExpandable(self):
        return True

    def IsEditable(self):
        return True
    
    def GetSubList(self):
        return [AttributeTreeItem(self.item, attr) for attr in self.attrs]


class IMapTreeItem(_TreeItem):

    attrs = ('worker_type', 'worker_num', 'worker_remote',\
             'stride', 'buffer', 'ordered', 'skip')
    
class PiperTreeItem(TreeItem):
    
    attrs = ()





class AttributeTreeItem(TreeItem):
    
    def __init__(self, item, argument):
        self.item = item
        self.argument = argument

    def GetText(self):
        return "%s: %s" % (self.argument, getattr(self.item, self.argument))

    def SetText(self, text):
        text = text.split(':')[1]
        setattr(self.item, self.argument, text)

    def IsExpandable(self):
        return False

    def IsEditable(self):
        return self.editable


class Tree(object):

    def __init__(self, parent, **kwargs):
        self.parent = parent
        self.name = kwargs.get('name')
        self.label_text = kwargs.get('label_text') or self.name
        self.add_text = kwargs.get('add_text') or 'add %s' %\
                                                        self.name[:-1]
        self.del_text = kwargs.get('del_text') or 'del %s' %\
                                                        self.name[:-1]
        self.add_cmd = kwargs.get('add_cmd')
        self.del_cmd = kwargs.get('del_cmd')
        self.node_pyclass = kwargs.get('node_pyclass') or eval('TreeNode')
        self.item_pyclass = kwargs.get('item_pyclass') or eval(self.name[:-1] + 'TreeItem')
        self.make_widgets()

    def make_widgets(self):
        self.frame = Frame(self.parent)
        self.buttons = Pmw.ButtonBox(self.frame, padx =0, pady =0)
        self.buttons.add(self.add_text, command =self.add_cmd)
        self.buttons.add(self.del_text, command =self.del_cmd)

        self.group = Pmw.Group(self.frame, tag_pyclass =Label,
                                              tag_text
                                              =self.label_text)

        canvas = Pmw.ScrolledCanvas(self.group.interior())
        self.canvas = canvas.component('canvas') 
        self.canvas.config(bg =O[self.name + '_background'])#, width= 170, height =350)
        self.buttons.pack(side =BOTTOM, anchor =W)
        canvas.pack(fill =BOTH, expand =YES)
        self.group.pack(fill =BOTH, expand =YES)


    def add_item(self, item):
        node = self.node_pyclass(self.canvas, None, self.item_pyclass(item))
        node.update()

        

    



    
    


class MainMenuBar(Pmw.MainMenuBar):

    def __init__(self, parent, **kwargs):
        apply(Pmw.MainMenuBar.__init__, (self, parent), kwargs)
        self.create_widgets()

    def not_impl(self):
        showerror(message ='Not implemented')

    def create_widgets(self):
        self.addmenu('File', 'Load/Save/Exit')

        self.addmenuitem('File', 'command', 'Load',
		                 command = None,
		                   label = 'Load')
        
        self.addmenuitem('File', 'command', 'Save',
		                 command = None,
		                   label = 'Save')

        self.addmenuitem('File', 'command', 'Exit',
		                 command = None,
		                   label = 'Exit')

        self.addmenu('Options', 'Options/Setting')
        self.addmenuitem('Options', 'command', 'Gui',
		                 command = None,
		                   label = 'Gui Options')

        self.addmenu('Help', 'User manuals', name = 'help')
        self.addmenuitem('Help', 'command', 'About this application',
                command = None,
                  label = 'About')


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
        root.frame.status_bar.message('state', 'canvas released at: %s-%s' % (event.x, event.y))

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
        self.menu_bar = MainMenuBar(self)
        self.master.config(menu =self.menu_bar)

        # toolbar
        self.tool_bar = ToolBar(self)
        self.tool_bar.pack(fill =X)

        # 4 panes
        self.lr = PanedWindow(self)
        self.lr.pack(fill=BOTH, expand=YES)

        self.l = PanedWindow(self.lr, orient=VERTICAL, showhandle =YES, sashwidth =20)
        self.r = PanedWindow(self.lr, orient=VERTICAL, showhandle =YES, sashwidth =20)
        self.lr.add(self.l, stretch ='always')
        self.lr.add(self.r, stretch ='always')
        
        # pipers
        self.pipers = Tree(self.l, name ='Pipers')
        print papy.workers
        #piper = Piper(papy.workers.io.print_)
        #self.pipers.add_item(piper)

        # imaps
        self.imaps = Tree(self.l, name ='IMaps')

        imap = IMap()
        self.imaps.add_item(imap)

        
        self.l.add(self.pipers.frame, stretch ='always')
        self.l.add(self.imaps.frame, stretch ='always')
        self.l.paneconfigure(self.pipers.frame, sticky =N+E+W+S) 
        self.l.paneconfigure(self.imaps.frame, sticky =N+E+W+S) 

        # pipeline & code, shell & logging
        self.pipeline = Pipeline(self.r)
        self.io = LoggingShell(self.r)
        self.r.add(self.pipeline, stretch ='always')
        self.r.add(self.io, stretch ='always')
        self.r.paneconfigure(self.pipeline, sticky =N+E+W+S) 
        self.r.paneconfigure(self.io, sticky =N+E+W+S) 

        # statusbar
        self.status_bar = Pmw.MessageBar(self,
		   entry_relief = 'groove',
		       labelpos = W,
		     label_text = 'Status:')
        self.status_bar.pack(fill =BOTH, anchor =W)


if __name__ == '__main__':
        
    #import idlelib
    #from idlelib import PyShell
    #PyShellEditorWindow=PyShell.PyShellEditorWindow
    #PyShellFileList=PyShell.PyShellFileList
    #idlelib.PyShell.use_subprocess = False
    #root.flist = PyShellFileList(root)
    #root.firstidle = True
    #root.save_idle = None
    #flist = idlelib.PyShell.PyShellFileList(root)
    #root.withdraw()
    #flist.pyshell = PyShell.PyShell(root)
    #flist.pyshell.begin()
        
    def console_start():
        import code
        ic = code.InteractiveConsole()
        ic.interact()

    console_thread = Thread(target =console_start)
    console_thread.daemon = True
    console_thread.start()

    O = Options()
    root = Tk()
    Pmw.initialise(root)
    root.frame = PapyMainFrame(root)
    #root.minsize(root.winfo_reqwidth(), root.winfo_reqheight())
    root.mainloop()
    
