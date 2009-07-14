#!/usr/bin/env python
""" The PaPy gui written in Tkinter.
"""
# PaPy/IMap imports
from papy import *
from IMap import *

# Python imports
import os

# Tkinter/Pmw/idlelib imports
from Tkinter import *
from tkMessageBox import *
from TreeWidget import TreeItem, TreeNode
from ShellWidget import PythonShell
import Pmw






class RootItem(TreeItem):
    
    def __init__(self, items, item_pyclass, tree, icon_name =None):
        self.items = items
        self.item_pyclass = item_pyclass
        self.tree = tree
        self.icon_name = icon_name 

    def GetLabelText(self):
        return "Name"

    def GetText(self):
        return "Value" 

    def IsExpandable(self):
        return True

    def GetSubList(self):
        return [self.item_pyclass(item, self) for item in self.items.values()]

    def GetIconName(self):
        return self.icon_name


class _TreeItem(TreeItem):

    def __init__(self, item, root):
        self.item = item
        self.root = root
    
    def GetText(self):
        try:
            return self.item.name
        except:
            return repr(self.item)
            
    def SetText(self, text):
        self.item.name = text

    def IsExpandable(self):
        return True

    def IsEditable(self):
        return True
    
    def OnSelect(self):
        self.root.tree.update_selected(self.item)
        
    def GetSubList(self):
        return [self.subclass(self.item, subi) for subi in self.GetSubItems()]
    
    def GetSubItems(self):
        return self.subitems

class AttributeTreeItem(TreeItem):
    
    def __init__(self, item, attr):
        self.item = item
        self.attr = attr

    def GetIconName(self):
        return 'attribute'

    def GetSelectedIconName(self):
        return 'attribute'

    def GetLabelText(self):
        return self.attr

    def GetText(self):
        attr = getattr(self.item, self.attr.lower())
        try:
            return attr.name
        except AttributeError:
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


class FuncArgsTreeItem(TreeItem):
    
    def __init__(self, item, func, args):
        self.item = item
        self.func = func
        self.args = args

    def GetIconName(self):
        return 'python'

    def GetSelectedIconName(self):
        return 'python'

    def GetLabelText(self):
        try:
            return self.func.name
        except AttributeError:
            return self.func.__name__

    def GetText(self):
        return repr(self.args)

    def SetText(self, text):
        self.args = eval(text)

    def IsExpandable(self):
        return False

    def IsEditable(self):
        return True


class IMapTreeItem(_TreeItem):

    subitems = ('worker_type', 'worker_num', 'worker_remote',\
             'stride', 'buffer', 'ordered', 'skip')
    subclass = AttributeTreeItem
    
class PiperTreeItem(_TreeItem):
    
    subitems = ('worker', 'IMap', 'consume', 'produce', 'spawn', 'timeout',\
             'cmp', 'ornament', 'debug', 'track')
    subclass = AttributeTreeItem

class WorkerTreeItem(_TreeItem):
    
    def GetSubList(self):
        return [FuncArgsTreeItem(self, f, a) for f, a in zip(self.item.task, self.item.args)]


class Tree(object):

    def __init__(self, parent, items, dialogs, **kwargs):
        self.parent = parent
        self.items = items
        self.dialogs = dialogs
        self.selected_item = None
        self.name = kwargs.get('name') or self.name
        self.label_text = kwargs.get('label_text') or self.name
        self.new_text = 'new %s' % self.name[:-1]
        self.del_text = 'remove %s' % self.name[:-1]
        self.root_pyclass = kwargs.get('root_pyclass') or eval('RootItem')
        self.item_pyclass = kwargs.get('item_pyclass') or eval(self.name[:-1] + 'TreeItem')
        self.make_widgets()        

    def update(self):
        self.root.children = []
        self.root.update()
        self.root.expand()

    def update_selected(self, item):
        self.selected_item = item
        
    def make_widgets(self):
        self.frame = Frame(self.parent)
        self.buttons = Pmw.ButtonBox(self.frame, padx =0, pady =0)
        self.buttons.add(self.new_text, command =self.new_cmd)
        self.buttons.add(self.del_text, command =self.del_cmd)

        self.group = Pmw.Group(self.frame, tag_pyclass =Label,
                                              tag_text
                                              =self.label_text)

        canvas = Pmw.ScrolledCanvas(self.group.interior())
        self.canvas = canvas.component('canvas') 
        self.canvas.config(bg =O[self.name + '_background'], width= 200)
        self.buttons.pack(side =BOTTOM, anchor =W)
        self.root = TreeNode(self.canvas, None,\
        self.root_pyclass(self.items, self.item_pyclass, self, O[self.name + '_root_icon']))
         
        # this patches TreeNode with icons for the specific tree.
        icondir = os.path.join(os.path.dirname(__file__), 'icons', self.name + 'Tree')
        icons = os.listdir(icondir)
        for icon in (i for i in icons if i.endswith('.gif')):
            image = PhotoImage(master =self.canvas,\
                                file  =os.path.join(icondir, icon))
            self.root.iconimages[icon.split('.')[0]] = image
            
        canvas.pack(fill =BOTH, expand =YES)
        self.group.pack(fill =BOTH, expand =YES)
        
    def add_item(self, item):
        #TreeNode(self.canvas, self.root, self.item_pyclass(item, root =self.root))
        self.update()

    def del_item(self, item):
        self.update()

class PipersTree(Tree):
    
    name = 'Pipers'

    def new_cmd(self):
        self.dialogs['new_piper'].activate()

    def del_cmd(self):
        self.dialogs['del_piper'].activate()


class WorkersTree(Tree):
    
    name = 'Workers'

    def new_cmd(self):
        self.dialogs['new_worker'].activate()

    def del_cmd(self):
        self.dialogs['del_worker'].activate()
        

class IMapsTree(Tree):
    
    name = 'IMaps'

    def new_cmd(self):
        self.dialogs['new_imap'].activate()


    def del_cmd(self):
        self.dialogs['del_imap'].activate()



        
        
class MainMenuBar(Pmw.MainMenuBar):

    def __init__(self, parent, dialogs, **kwargs):
        apply(Pmw.MainMenuBar.__init__, (self, parent), kwargs)
        self.dialogs = dialogs
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
                command = self.dialogs['about'].show,
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
        root.papyg.status_bar.message('state', 'canvas released at: %s-%s' % (event.x, event.y))

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


class ScrolledLog(Pmw.ScrolledText):

    def write(self, *args, **kwargs):
        self.appendtext(*args, **kwargs)

    

class RestrictedComboBox(Pmw.ComboBox):
    
    def __init__(self, parent, **kwargs):
        apply(Pmw.ComboBox.__init__, (self, parent), kwargs)  
        self.component('entryfield_entry').bind('<Button-1>',
                            lambda event: 'break')
        self.component('entryfield_entry').bind('<B1-Motion>',
                            lambda event: 'break')


class _CreationDialog(Pmw.Dialog):
    
    def __init__(self, parent, **kwargs):
        kwargs['buttons'] = ('Create', 'Cancel', 'Help')
        kwargs['title'] = 'Create PaPy %s' % self.name.capitalize()
        kwargs['command'] = self.create
        kwargs['defaultbutton'] = 'Cancel'
        apply(Pmw.Dialog.__init__, (self, parent), kwargs)
        self.withdraw() # no flash
        self.parent = parent
        self.create_widgets()

    def create_widgets(self):
        self.group = Pmw.Group(self.interior(),\
                               tag_pyclass =Label,\
                               tag_text ="%s Arguments" %\
                               self.name.capitalize())
        # provided by sub-class
        self.create_entries()
        self.named_entries = [(name, getattr(self, name[1])) for name\
                                     in sorted(self.defaults.keys())] 
        entries = [e for n, e in self.named_entries] 
        # align and pack
        Pmw.alignlabels(entries)
        for w in entries:
            w.pack(expand =YES, fill =BOTH)
        self.group.pack(expand =YES, fill =BOTH)
        self.wm_resizable(NO, NO)

    def update(self):
        # reset previous selections
        for (i, name), default in self.defaults.iteritems():
            e = getattr(self, name)
            if default is not None:
                try:
                    # combobox
                    e.setlist(list(default)) 
                    e.selectitem(0)
                except AttributeError:
                    # checkbuttons and counter
                    try:
                        # integer counter
                        e.setvalue(int(default))
                    except ValueError:
                        # float counter
                        e.setvalue(float(default))
                    except TypeError:
                        # checkbox
                        e.setvalue(list(default))
            else:
                try:
                    e.clear()      # clear selection
                except AttributeError:
                    e.setvalue([]) # deselect all checkboxes
        # update entries which change
        self.update_entries()

    def update_entries(self):
        pass
      
    def activate(self):
        # super does not work with classic classes
        self.update()
        Pmw.Dialog.activate(self)

    def create(self, result):
        if result == 'Create':
            self._create()                  
        elif result == 'Help':
            self.help()
        self.deactivate(result)

    def help(self):
        pass

class IMapDialog(_CreationDialog):
   
    name = 'imap'
    defaults = {(0, 'name'):None, 
                (1, 'worker_type'): ('process', 'thread'),
                (2, 'worker_num'):'0',
                (3, 'worker_remote'):None, 
                (4, 'stride'):'0',
                (5, 'buffer'):'0',
                (6, 'misc'):('ordered',)} # remember to copy

    def _create(self):
        kwargs = {}
        for (i, name), entry in self.named_entries:
            value = entry.getvalue()
            ns = papyg.namespace
            if value and value != self.defaults[(i, name)]:
                if name == 'name':
                    kwargs['name'] = value
                elif name == 'worker_type':
                    kwargs['worker_type'] = value[0]
                elif name == 'worker_remote':
                    kwargs['worker_remote'] = eval(value)
                elif name == 'misc':
                    for arg_true in name:
                        kwargs[arg_true] = True      
        papyg.add_imap(**kwargs)
       

    def create_entries(self):

        self.name = Pmw.EntryField(self.group.interior(),\
                                   labelpos ='w',\
		                           label_text ='Name:',\
		                           validate ={'validator':'alphabetic'})
        
        self.worker_type = RestrictedComboBox(self.group.interior(),\
                                 label_text ='Type:',\
                                 labelpos ='w')

        self.worker_remote = Pmw.EntryField(self.group.interior(),\
                                   labelpos ='w',\
		                           label_text ='Remote:')

        self.parallel = RestrictedComboBox(self.group.interior(),\
                                 label_text = 'IMap:',\
                                 labelpos = 'w')        

        for a in ('worker_num', 'stride', 'buffer'):
            label = a.capitalize() + ':' if a != 'worker_num' else 'Number:'
            setattr(self, a, Pmw.Counter(self.group.interior(),\
                                label_text = label,\
		                        labelpos = 'w'))

        self.misc = Pmw.RadioSelect(self.group.interior(),
                        buttontype = 'checkbutton',
                        orient = 'vertical',
                        labelpos = 'w',
                        label_text = 'Misc.:',
                        hull_borderwidth = 0)
        self.misc.add('ordered')
        self.misc.add('skip')


class PiperDialog(_CreationDialog):

    name = 'piper'
    defaults = {(0,'name'):None, 
                (1,'worker'):None, 
                (2,'parallel'):None,
                (3,'produce'):'1',
                (4,'spawn'):'1',
                (5,'consume'):'1',
                (6,'timeout'):'0.0', 
                (7,'cmp'):None,
                (8,'ornament'):None,
                (9,'runtime'):None}

    def create_entries(self):

        self.name = Pmw.EntryField(self.group.interior(),\
                                   labelpos ='w',\
		                           label_text ='Name:',\
		                           validate ={'validator':'alphabetic'})
        
        self.worker = RestrictedComboBox(self.group.interior(),\
                                 label_text = 'Worker:',\
                                 labelpos = 'w')

        self.parallel = RestrictedComboBox(self.group.interior(),\
                                 label_text = 'IMap:',\
                                 labelpos = 'w')        

        for a in (('produce'),('spawn'),('consume')):
	        setattr(self, a, Pmw.Counter(self.group.interior(),\
                                label_text = a.capitalize() + ':',\
		                        labelpos = 'w',\
		                        entryfield_value = '1',\
	                            entryfield_validate = {'validator' : 'integer',\
			                    'min' : '1'}))
        
        self.timeout = Pmw.Counter(self.group.interior(),\
                                label_text = 'Timeout:',\
		                        labelpos = 'w',\
		                        entryfield_value = '0.0',\
                                increment = 0.1,\
                                datatype = {'counter' : 'real'},\
	                            entryfield_validate = \
                                {'validator' : 'real', 'min' : 0.0})

        self.cmp = RestrictedComboBox(self.group.interior(),\
                                 label_text = 'Compare:',\
                                 labelpos = 'w')
        
        self.ornament = Pmw.EntryField(self.group.interior(),\
                                   labelpos ='w',\
		                           label_text ='Ornament:')

        self.runtime = Pmw.RadioSelect(self.group.interior(),
                        buttontype = 'checkbutton',
                        orient = 'vertical',
                        labelpos = 'w',
                        label_text = 'Runtime:',
                        hull_borderwidth = 0)
        self.runtime.add('debug')
        self.runtime.add('track')

    def update_entries(self):
        # HARD CODED locations
        self.parallel.setlist([i for i in papyg.namespace['imaps']])
        self.worker.setlist([w for w in papyg.namespace['workers']])
        self.cmp.setlist([o for o in papyg.namespace['objects']])
      
    def _create(self):
        named_entries = [(name, getattr(self, name[1])) for name\
                                     in sorted(self.defaults.keys())] 
        kwargs = {}
        for (i, name), entry in self.named_entries:
            value = entry.getvalue()
            ns = papyg.namespace
            if value and value != self.defaults[(i, name)]:
                if name == 'name':
                    kwargs['name'] = value
                elif name == 'worker':
                    kwargs['worker'] = ns['workers'][value[0]]
                elif name == 'parallel':
                    kwargs['parallel'] = ns['imaps'][value[0]]
                elif name in ('produce', 'spawn', 'consume'):
                    kwargs[name] = int(value)
                elif name == 'cmp':
                    kwargs['parallel'] = ns['objects'][value[0]]
                elif name == 'ornament':
                    kwargs['ornament'] = eval(value)
                elif name == 'runtime':
                    for arg_true in value:
                        kwargs[arg_true] = True
        papyg.add_piper(**kwargs)


class PaPyGui(Pmw.MegaToplevel):

    def __init__(self, parent, **kwargs):
        kwargs['title'] = O['app_name']
        apply(Pmw.MegaToplevel.__init__, (self, parent), kwargs)
        self.wm_withdraw() # hide
        self.make_namespace()
        self.toplevel = self.interior()
        self.make_dialogs()
        self.make_plumber()
        self.make_widgets()
        self.start()


    def start(self):
        utils.logger.start_logger(log_filename =O['log_filename'], log_stream =self.log)        

    def make_dialogs(self):
        # About
        self.dialogs = {}
        self.dialogs['about'] = Pmw.AboutDialog(self.toplevel, applicationname ='My Application')
        self.dialogs['about'].withdraw()
        self.dialogs['new_piper'] = PiperDialog(self.toplevel)
        self.dialogs['new_imap'] = IMapDialog(self.toplevel)
        

    def make_widgets(self, title =None):
        #main menu
        self.menu_bar = MainMenuBar(self.toplevel, self.dialogs)
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
        self.pipers = PipersTree(self.l, self.namespace['pipers'], self.dialogs)
        self.workers = WorkersTree(self.l, self.namespace['workers'], self.dialogs)

        # pipeline & code, shell & logging
        self.pipeline = NoteBook(self.r, ['Pipeline', 'Functions', 'IMaps'])
        self.io = NoteBook(self.r, ['Shell', 'Logging'])
        
        # imaps
        self.imaps = IMapsTree(self.pipeline.page('IMaps'),\
                               self.namespace['imaps'],\
                               self.dialogs)
        self.imaps.frame.pack(fill =BOTH, expand =YES)

        # logging
        self.log = ScrolledLog(self.io.page('Logging'),
                    borderframe = True,
                    text_padx = O['default_font'][1] // 2, # half-font
                    text_pady = O['default_font'][1] // 2,
                    text_wrap='none')
        self.log.configure(text_state = 'disabled')
        self.log.pack(fill=BOTH, expand=YES)
        
        # shell
        self.shell = PythonShell(self.io.page('Shell'),
                    text_padx = O['Shell_font'][1] // 2, # half-font
                    text_pady = O['Shell_font'][1] // 2)
        self.shell.text['background'] = O['Shell_background']
        self.shell.text['foreground'] = O['Shell_fontcolor']
        self.shell.text['font'] = O['Shell_font']

        self.shell.pack(fill=BOTH, expand=YES)

        # packing
        self.l.add(self.pipers.frame, stretch ='always')
        self.l.add(self.workers.frame, stretch ='always')
        self.l.paneconfigure(self.pipers.frame, sticky =N+E+W+S) 
        self.l.paneconfigure(self.workers.frame, sticky =N+E+W+S) 
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
        for n in ('functions', 'workers', 'pipers', 'imaps', 'objects'):
            self.namespace[n] = {}

    def add_piper(self, **kwargs):
        piper = Piper(**kwargs)
        self.namespace['pipers'][piper.name] = piper
        self.pipers.add_item(piper)

    def del_piper(self, **kwargs):
        self.namespace['pipers'].remove(piper)
        self.pipers.del_item(piper)

    def add_imap(self, **kwargs):
        imap = IMap(**kwargs)
        self.namespace['imaps'][imap.name] = imap
        self.imaps.add_item(imap)

    def del_imap(self, imap, **kwargs):
        self.namespace['imaps'].remove(imap)
        self.imaps.del_item(imap)

    def add_worker(self, **kwargs):
        worker = Worker(**kwargs)
        self.namespace['workers'][worker.name] = worker
        self.workers.add_item(worker)

    def del_worker(self):
        pass

    def make_plumber(self):
        if False: # some input file
            pass
        else:
            self.namespace['plumber'] = Plumber()

class Options(dict):
    """ Provide options throughout the PaPy Gui application.
    """

    defaults = (('app_name', 'PaPy'),
                ('log_filename', None),
                ('default_font', ("tahoma", 8)),
                ('node_color', 'blue'),
                ('node_status', 'green'),
                ('graph_background', 'aliceblue'),
                ('Pipers_background', 'pink'),
                ('Workers_background', 'LightSteelBlue2'),
                ('Shell_background', 'white'),
                ('Shell_history', 1000),
                ('Shell_fontcolor', 'black'),
                ('Shell_font', ("courier new", 9)),
                ('Pipers_root_icon', 'pipe_16'),
                ('IMaps_root_icon', 'gear_16'),
                ('Workers_root_icon', 'component_16'),
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
    root.withdraw()
    Pmw.initialise(root)
    root.option_add("*font", O['default_font'])
    papyg = PaPyGui(root)





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



    papyg.add_imap()
    papyg.add_imap()
    papyg.add_imap()
    papyg.add_imap()

    papyg.add_worker(functions =workers.io.dump_item)

    papyg.add_piper(worker =workers.io.dump_item)
    papyg.add_piper(worker =workers.io.print_)

    #root.papy.graph = GraphCanvas(graph =g, parent =root.papy.pipeline.page('Pipeline'))
    #root.papy.graph.pack(expand =YES, fill =BOTH)
    #import readline
    #readline.parse_and_bind('tab: complete')    
    papyg.protocol("WM_DELETE_WINDOW", root.destroy)
    papyg.wm_deiconify()
    root.mainloop()

  

# tkFileDialog.askopenfilename()
#        if '\x04' == key.char: #ctrl-D
#            self.stdin.put('exit()\n')
#            return

