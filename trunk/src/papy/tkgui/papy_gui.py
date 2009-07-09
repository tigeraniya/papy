#!/usr/bin/env python
""" The PaPy gui written in Tkinter.
"""
# PaPy/IMap imports
from papy import Piper, Plumber, Graph, workers
from IMap import IMap

# Python imports
from threading import Thread
from Queue import Queue
import code
import os
# Tkinter/Pmw/idlelib imports
from Tkinter import *
from tkMessageBox import *
from idlelib.TreeWidget import TreeItem, TreeNode
import Pmw


class RootItem(TreeItem):
    
    def __init__(self, items, item_pyclass, icon_name =None):
        self.items = items
        self.icon_name = icon_name 
        self.item_pyclass = item_pyclass

    def GetLabelText(self):
        return "Name"

    def GetText(self):
        return "Value" 

    def IsExpandable(self):
        return True

    def GetSubList(self):
        return [self.item_pyclass(item) for item in self.items]

    def GetIconName(self):
        return self.icon_name


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
    
class PiperTreeItem(_TreeItem):
    
    attrs = ('worker', 'IMap', 'consume', 'produce', 'spawn', 'timeout',
             'cmp', 'ornament', 'debug', 'track')

class AttributeTreeItem(TreeItem):
    
    def __init__(self, item, attr):
        self.item = item
        self.attr = attr

    def GetLabelText(self):
        return self.attr

    def GetText(self):
        attr = getattr(self.item, self.attr.lower())
        try:
            return attr.__name__
        except AttributeError:
            return repr(attr)

    def SetText(self, text):
        text = text.split(':')[1]
        setattr(self.item, self.attr.lower(), text)

    def IsExpandable(self):
        return False

    def IsEditable(self):
        return False


class Tree(object):

    def __init__(self, parent, items, **kwargs):
        self.parent = parent
        self.items = items
        self.name = kwargs.get('name')
        self.label_text = kwargs.get('label_text') or self.name
        self.add_text = kwargs.get('add_text') or 'add %s' %\
                                                        self.name[:-1]
        self.del_text = kwargs.get('del_text') or 'del %s' %\
                                                        self.name[:-1]
        self.add_cmd = kwargs.get('add_cmd')
        self.del_cmd = kwargs.get('del_cmd')
        self.root_pyclass = kwargs.get('root_pyclass') or eval('RootItem')
        self.item_pyclass = kwargs.get('item_pyclass') or eval(self.name[:-1] + 'TreeItem')
        self.make_widgets()

    def update(self):
        self.root.children = []
        self.root.update()
        self.root.expand()

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
        self.canvas.config(bg =O[self.name + '_background'], width= 200)
        self.buttons.pack(side =BOTTOM, anchor =W)
        self.root = TreeNode(self.canvas, None,\
        self.root_pyclass(self.items, self.item_pyclass, O[self.name + '_root_icon']))
         
        # this patches TreeNode with icons for the specific tree.
        icondir = os.path.join(os.path.dirname(__file__), 'icons', self.name + 'Tree')
        icons = os.listdir(icondir)
        for icon in (i for i in icons if i.endswith('gif')):
            image = PhotoImage(master =self.canvas,\
                                file  =os.path.join(icondir, icon))
            self.root.iconimages[icon] = image
            
        canvas.pack(fill =BOTH, expand =YES)
        self.group.pack(fill =BOTH, expand =YES)
        
    def add_item(self, item):
        TreeNode(self.canvas, self.root, self.item_pyclass(item))
        self.update()

    def del_item(self, item):
        self.update()



        
class MainMenuBar(Pmw.MainMenuBar):

    def __init__(self, parent, **kwargs):
        apply(Pmw.MainMenuBar.__init__, (self, parent), kwargs)
        self.make_widgets()

    def not_impl(self):
        showerror(message ='Not implemented')

    def make_widgets(self):
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
        root.papy.status_bar.message('state', 'canvas released at: %s-%s' % (event.x, event.y))

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
    def __init__(self, parent, pages, **kwargs):
        apply(Pmw.NoteBook.__init__, (self, parent), kwargs)
        self.add_pages(pages)

    def add_pages(self, pages):
        for page in pages:
            self.add(page)

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


class PaPyGui(Pmw.MegaToplevel):

    def __init__(self, parent, **kwargs):
        kwargs['title'] = O['app_name']
        apply(Pmw.MegaToplevel.__init__, (self, parent), kwargs)      
        self.toplevel = self.interior()
        self.make_namespace()
        self.make_plumber()
        self.make_widgets()

    def make_widgets(self, title =None):
        #main menu
        self.menu_bar = MainMenuBar(self.toplevel)
        self.toplevel.config(menu =self.menu_bar)

        #toolbar
        self.tool_bar = ToolBar(self.toplevel)
        self.tool_bar.pack(fill =X)

        # 4 panes
        self.lr = PanedWindow(self.toplevel)
        self.lr.pack(fill=BOTH, expand=YES)

        self.l = PanedWindow(self.lr, orient=VERTICAL, showhandle =YES, sashwidth =20)
        self.r = PanedWindow(self.lr, orient=VERTICAL, showhandle =YES, sashwidth =20)
        self.lr.add(self.l, stretch ='always')
        self.lr.add(self.r, stretch ='always')
        
        # pipers
        self.pipers = Tree(self.l, self.namespace['pipers'], name ='Pipers')
        # imaps
        self.imaps = Tree(self.l, self.namespace['imaps'], name ='IMaps')

        self.l.add(self.pipers.frame, stretch ='always')
        self.l.add(self.imaps.frame, stretch ='always')
        self.l.paneconfigure(self.pipers.frame, sticky =N+E+W+S) 
        self.l.paneconfigure(self.imaps.frame, sticky =N+E+W+S) 

        # pipeline & code, shell & logging
        self.pipeline = NoteBook(self.r, ['Pipeline', 'Functions'])
        self.io = NoteBook(self.r, ['Shell', 'Logging'])

        self.r.add(self.pipeline, stretch ='always')
        self.r.add(self.io, stretch ='always')
        self.r.paneconfigure(self.pipeline, sticky =N+E+W+S) 
        self.r.paneconfigure(self.io, sticky =N+E+W+S) 

        # statusbar
        self.status_bar = Pmw.MessageBar(self.toplevel,
		   entry_relief = 'groove',
		       labelpos = W,
		     label_text = 'Status:')
        self.status_bar.pack(fill =BOTH, anchor =W)

    def make_namespace(self):
        self.namespace = {}
        self.namespace['pipeline'] = self
        self.namespace['functions'] = set([])
        self.namespace['imaps'] = set([])
        self.namespace['pipers'] = set([])

    def add_piper(self, worker, **kwargs):
        piper = Piper(worker, **kwargs)
        self.namespace['pipers'].add(piper)
        self.pipers.add_item(piper)

    def del_piper(self, **kwargs):
        self.namespace['pipers'].remove(piper)
        self.pipers.del_item(piper)

    def add_imap(self, **kwargs):
        imap = IMap(**kwargs)
        self.namespace['imaps'].add(imap)
        self.imaps.add_item(imap)

    def del_imap(self, imap, **kwargs):
        self.namespace['imaps'].remove(imap)
        self.imaps.del_item(imap)

    def make_plumber(self):
        if False: # some input file
            pass
        else:
            self.namespace['plumber'] = Plumber()

class Options(dict):
    """ Provide options throughout the PaPy Gui application.
    """

    defaults = (('app_name', 'PaPy'),
                ('default_font', ("tahoma", 8)),
                ('node_color', 'blue'),
                ('node_status', 'green'),
                ('graph_background', 'aliceblue'),
                ('Pipers_background', 'white'),
                ('Pipers_root_icon', 'pipe_16.gif'),
                ('IMaps_root_icon', 'gear_16.gif'),
                ('IMaps_background', 'white')
                )

    def __init__(self, config_options =None, command_options =None):
        init = dict(self.defaults)
        init.update(config_options or {})
        init.update(command_options or {})
        dict.__init__(self, init)


class ConfigOptions(dict):
    pass


class CommandOptions(dict):
    pass


if __name__ == '__main__':
        
    cfg_opts = ConfigOptions()
    cmd_opts = CommandOptions()
    O = Options(cfg_opts, cmd_opts)
    root = Tk()
    root.option_add("*font", O['default_font'])
    root.withdraw()
    Pmw.initialise(root)
    papy = PaPyGui(root)




    #g = Graph()
    #g.add_node('node_1')
    #g.add_node('node_2')
    #g.add_node('node_3')
    #g['node_1'].xtra['x'] = 25
    #g['node_1'].xtra['y'] = 25
    #g['node_1'].xtra['color'] = 'red'
    #g['node_1'].xtra['status'] = 'green'
    #g['node_1'].xtra['screen_name'] = 'node_1'
    #g['node_2'].xtra['x'] = 75
    #g['node_2'].xtra['y'] = 75
    #g['node_2'].xtra['color'] = 'blue'
    #g['node_2'].xtra['status'] = 'orange'
    #g['node_2'].xtra['screen_name'] = 'node_2'

    #g['node_3'].xtra['x'] = 75
    #g['node_3'].xtra['y'] = 25
    #g['node_3'].xtra['color'] = 'green'
    #g['node_3'].xtra['status'] = 'red'
    #g['node_3'].xtra['screen_name'] = 'node_3'

    #g.add_edge(('node_1', 'node_2'))
    #g.add_edge(('node_2', 'node_3'))



    papy.add_imap()
    papy.add_imap()
    papy.add_imap()
    papy.add_imap()

    papy.add_piper(workers.io.dump_item)
    papy.add_piper(workers.io.print_)

    #root.papy.graph = GraphCanvas(graph =g, parent =root.papy.pipeline.page('Pipeline'))
    #root.papy.graph.pack(expand =YES, fill =BOTH)

    # start python gui interpreter
    if O:
        pass
    # start python shell interpreter
    if O:
        def ic():
            ic = code.InteractiveConsole(papy.namespace)
            ic.interact()
        console_thread = Thread(target =ic)
        console_thread.daemon = True
        console_thread.start()
    
    papy.protocol("WM_DELETE_WINDOW", root.destroy)
    root.mainloop()

  


