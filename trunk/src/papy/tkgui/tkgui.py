#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" The PaPy gui written in Tkinter using Pmw.
"""
# PaPy/IMap imports
import papy
import IMap

# Python imports
import os
import inspect

# Tkinter/Pmw/idlelib imports
# watch for errors on multiprocessing/Tkinter/linux
import Pmw
import Tkinter as Tk
import tkMessageBox as tkm
from Tkconstants import *

# Boundled Generic widgets
from TreeWidget import TreeItem, TreeNode
from ShellWidget import PythonShell


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

class ModuleTreeItem(_TreeItem):
    
    def get_functions(self):
        """ Returns a list of function from a 
        """
        fs = [v for v in self.item.__dict__.values if inspect.isfunction(v)]
        return fs


    def GetSubList(self):
        return [FunctionTreeItem(self, f) for f in self.get_functions()]


class Tree(object):

    def __init__(self, parent, items, dialogs, **kwargs):
        self.parent = parent
        self.items = items
        self.dialogs = dialogs
        self.selected_item = None
        self.name = kwargs.get('name') or self.name
        self.label_text = kwargs.get('label_text') or self.name

        # make button labels
        for label in self._buttons:
            setattr(self, label+'_text', '%s %s' % (label, self.name[:-1]))

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
        self.frame = Tk.Frame(self.parent)



        self.buttons = Pmw.ButtonBox(self.frame, padx =0, pady =0)
        self.buttons.add(self.new_text, command =self.new_cmd)
        self.buttons.add(self.del_text, command =self.del_cmd)

        self.group = Pmw.Group(self.frame, tag_pyclass =Tk.Label,
                                              tag_text
                                              =self.label_text)

        canvas = Pmw.ScrolledCanvas(self.group.interior())
        self.canvas = canvas.component('canvas') 
        self.canvas.config(bg =O[self.name + '_background'], width= 200)
        self.buttons.pack(side =BOTTOM, anchor =W)
        self.root = TreeNode(self.canvas, None,\
        self.root_pyclass(self.items, self.item_pyclass, self, O[self.name + '_root_icon']))
         
        # this patches TreeNode with icons for the specific tree.
        # icondir = os.path.join(os.path.dirname(__file__), 'icons', self.name + 'Tree')
        # icons = os.listdir(icondir)
        # for icon in (i for i in icons if i.endswith('.gif')):
        #    image = Tk.PhotoImage(master =self.canvas,\
        #                            file =os.path.join(icondir, icon))
        #    self.root.iconimages[icon.split('.')[0]] = image
            
        canvas.pack(fill =BOTH, expand =YES)
        self.group.pack(fill =BOTH, expand =YES)
        
    def add_item(self, item):
        #TreeNode(self.canvas, self.root, self.item_pyclass(item, root =self.root))
        self.update()

    def del_item(self, item):
        self.update()


class PipersTree(Tree):
    
    name = 'Pipers'
    _buttons = ('new', 'del')

    def new_cmd(self):
        self.dialogs['new_piper'].activate()

    def del_cmd(self):
        self.dialogs['del_piper'].activate()


class WorkersTree(Tree):
    
    name = 'Workers'
    _buttons = ('new', 'del')

    def new_cmd(self):
        self.dialogs['new_worker'].activate()

    def del_cmd(self):
        self.dialogs['del_worker'].activate()
    

class IMapsTree(Tree):
    
    name = 'IMaps'
    _buttons = ('new', 'del')

    def new_cmd(self):
        self.dialogs['new_imap'].activate()


    def del_cmd(self):
        self.dialogs['del_imap'].activate()


class ModulesTree(Tree):
    
    name = 'Modules'
    _buttons = ('new', 'del', 'add')

    def add_cmd(self):
        self.dialogs['add_module'].activate()

    def new_cmd(self):
        self.dialogs['new_module'].activate()

    def del_cmd(self):
        self.dialogs['del_module'].activate()

        
        
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

    def _canvas_coords(self, x, y):
        return (int(self.canvasx(x)), int(self.canvasy(y)))

    def _last_click(self, event):
        self._update_canvas()
        # canvas position of click
        self.lastxy = self._canvas_coords(event.x, event.y)
        # what did we click
        self.lasttags = self.gettags(CURRENT)
        print self.lasttags

    def _update_canvas(self):
        # un-clutter
        self.canvas_menu.unpost()
        self.pipe_menu.unpost()
        self.resizescrollregion()
        self.canvas.focus_set()

    def _plumber_error_dialog(self, exception):

        # used to display an exception
        txt = exception[0]
        self._canvas_error_dialog(txt)

    def _canvas_error_dialog(self, txt):
        # used to display an error text
        error = Pmw.MessageDialog(root,
                                title = 'PaPy Panic!',
                                defaultbutton = 0,
                                message_text =txt)
        error.iconname('Simple message dialog')
        error.activate()

    def _center_canvas(self):
        pass

    def __init__(self, parent, **kwargs):
        # initialize the graph
        self.plumber = papy.Plumber()
        # initialize the canvas
        apply(Pmw.ScrolledCanvas.__init__, (self, parent), kwargs)
        self.make_widgets()
        self.canvas = self.component('canvas')
        # canvas options
        self.canvas['background'] = O['graph_background']
        self.canvas.background_select = O['graph_background_select']
        self.canvas['relief'] = SUNKEN
        # position of the last click
        self.lastxy = (0, 0)
        self.lasttags = []
        self.connecting = False
        self.dragging = False
        # canvas 'objects'
        self.tag_to_node = {}
        self.tag_to_Node = {}
        self.tag_to_pipe = {}
        # button bindings 
        self.canvas.bind("<Button-1>",         self.mouse1_down)
        self.canvas.bind("<B1-Motion>",        self.mouse1_drag)
        self.canvas.bind("<ButtonRelease-1>",  self.mouse1_up)
        self.canvas.bind("<Button-3>",         self.mouse3_down)
        self.canvas.bind("<B3-Motion>",        self.mouse3_drag)
        self.canvas.bind("<ButtonRelease-3>",  self.mouse3_up)
        # MacOSX compability
        self.canvas.bind("<Control-Button-1>",        self.mouse3_down)
        self.canvas.bind("<Control-B1-Motion>",       self.mouse3_drag)
        self.canvas.bind("<Control-ButtonRelease-1>", self.mouse3_up)
        # keyboard
        self.canvas.bind("<d>", lambda event: self.pop())
        self.canvas.focus_set()

    def make_widgets(self):
        # right click pipe
        self.pipe_menu = Tk.Menu(root, tearoff =0)
        self.pipe_menu.add_command(label="del pipe",    command =self.del_pipe)
        # right click canvas
        self.canvas_menu = Tk.Menu(root, tearoff =0)
        self.canvas_menu.add_command(label="add piper",    command =self.add_piper)
        self.canvas_menu.add_command(label="center graph", command=self._center_canvas)
        self.canvas_menu.add_command(label="align graph",  command =self.align_graph)

    def create_piper_graphics(self, node, Node):
        x = Node.xtra.get('x') or self.lastxy[0]
        y = Node.xtra.get('y') or self.lastxy[1]
        tag = Node.xtra['tag']
        status = (Node.xtra.get('status') or O['node_status'])
        color  = (Node.xtra.get('color')  or O['node_color'])
        self.create_oval(x-12, y-12, x+12, y+12, fill=status, tags =tag,\
                                            activewidth=3.0, activefill ="plum")
        self.create_oval(x-8,  y-8,  x+8,  y+8, fill=color,  tags =tag,\
                                            state ='disabled', width =0.0)
        self.create_text(x+12, y-17, text =node.name, fill='black', anchor =NW,\
                                            tags =tag)
        self._update_canvas()

    def move_piper_tag(self, tag, event):
        # get Node
        N = self.tag_to_Node[tag[:2]]
        # delete all pipes of this piper
        self.delete("%s&&%s" % ('pipe', tag[1]))
        # move piper
        currxy = self._canvas_coords(event.x, event.y)
        dx, dy = currxy[0] - N.xtra['x'],\
                 currxy[1] - N.xtra['y'],
        N.xtra['x'] += dx
        N.xtra['y'] += dy
        self.move("%s&&%s" % tag[:2], dx, dy)
        # re-draw affected pipes
        edges = self.plumber.incoming_edges(self.tag_to_node[tag[:2]]) +\
                self.plumber.outgoing_edges(self.tag_to_node[tag[:2]])
        for edge in edges:
            self.create_pipe_tag((edge[1], edge[0]))
        self._update_canvas()

    def create_piper_tag(self, node):
        # tag type: node
        # tags have to start with a letter why?
        Node = self.plumber[node]
        tag = ("piper", "n%s" % id(node))
        Node.xtra['tag'] = tag
        Node.xtra['x'] = self.lastxy[0]
        Node.xtra['y'] = self.lastxy[1]
        self.create_piper_graphics(node, Node)
        self.tag_to_node[tag] = node
        self.tag_to_Node[tag] = Node
        return tag

    def del_piper_tag(self, tag):
        # also delete edges
        self.delete(tag[1])
        # clean node dicts
        self.tag_to_node.pop(tag[:2])
        self.tag_to_Node.pop(tag[:2])
        # clean pipe dict
        [self.tag_to_pipe.pop(t) for t in self.tag_to_pipe.keys() if tag[1] in t]
        # clean last 
        self.lasttags = None

    def add_piper(self, piper):
        if piper:
            new, piper = self.plumber.add_piper(piper)
        else:
            self._canvas_error_dialog('No piper selected.')
            return
        if new:
            tag = self.create_piper_tag(piper)
            self.lasttags = tag
        else:
            self._canvas_error_dialog('Piper alread added.')

    def del_piper(self):
        self.plumber.del_piper(self.tag_to_node[self.lasttags[:2]], forced =True)
        self.del_piper_tag(self.lasttags)

    def create_pipe_graphics(self, xy1, xy2, tags):
        self.create_line(*xy1 + xy2, width =3, arrow =LAST, tags = tags)
        self._update_canvas()

    def create_pipe_tag(self, pipe):
        N1 = self.plumber[pipe[0]]
        N2 = self.plumber[pipe[1]]
        t1 = N1.xtra['tag'][1]
        t2 = N2.xtra['tag'][1]
        tag = ("pipe", t1, t2)
        xy1 = (N1.xtra['x'], N1.xtra['y'])
        xy2 = (N2.xtra['x'], N2.xtra['y'])
        self.create_pipe_graphics(xy1, xy2, tags =tag)
        self.tag_to_pipe[tag] = pipe
        return tag

    def del_pipe_tag(self, tag):
        self.tag_to_pipe.pop(tag[:3])
        self.delete("%s&&%s&&%s" % tag[:3])
        #
        self.lasttags = None

    def pop(self):
        if self.lasttags and self.lasttags[0] == 'pipe':
            self.del_pipe()
        elif self.lasttags and self.lasttags[0] == 'piper':
            self.del_piper()
        else:
            # error dialog
            self._canvas_error_dialog('No piper or pipe selected.')

    def add_pipe(self, pipe):
        try:
            self.plumber.add_pipe(pipe)
            tag = self.create_pipe_tag(pipe)
            self.lasttags = tag
        except papy.DaggerError, e:
            self._plumber_error_dialog(e)

    def del_pipe(self):
        n1 = self.tag_to_node[('piper', self.lasttags[1])]
        n2 = self.tag_to_node[('piper', self.lasttags[2])]
        self.plumber.del_edge((n2, n1))
        self.del_pipe_tag(tag =self.lasttags)

    def mouse1_down(self, event):
        self._last_click(event)
        if self.lasttags and self.lasttags[0] == 'piper':
            # right click on a piper
            self.dragging = True

    def mouse3_down(self, event):
        self._last_click(event)
        if self.lasttags:
            # right click on a pipe
            if self.lasttags[0] == 'pipe':    
                self.pipe_menu.post(event.x_root, event.y_root)
            # right click on a piper
            elif self.lasttags[0] == 'piper':
                self.connecting = True
        else:
            # right click on the canvas
            self.canvas_menu.post(event.x_root, event.y_root)

    def mouse1_up(self, event):
        self.canvas_menu.unpost()
        self.dragging = False

    def mouse3_up(self, event):
        self.delete("BROKEN_PIPE")
        if self.connecting:
            # down was on a piper
            x, y = self._canvas_coords(event.x, event.y)
            item = self.find_closest(x, y)
            currtags = self.gettags(item)
            if currtags and currtags[0] == 'piper':
                # up was on a piper
                if self.lasttags[:2] != currtags[:2]:
                    # have different piper
                    n1 = self.tag_to_node[self.lasttags[:2]]
                    n2 = self.tag_to_node[currtags[:2]]
                    self.add_pipe((n1, n2))
            self.connecting = False

    def mouse1_drag(self, event):
        if self.dragging:
            self.move_piper_tag(self.lasttags, event)

    def mouse3_drag(self, event):
        self.delete("BROKEN_PIPE")
        if self.connecting:
            self.create_pipe_graphics(self.lastxy, 
                                      self._canvas_coords(event.x, event.y),
                                      ('BROKEN_PIPE',))     

    def align_graph(self):
        pass


class NoteBook(Pmw.NoteBook):
    def __init__(self, parent, pages, **kwargs):
        apply(Pmw.NoteBook.__init__, (self, parent), kwargs)
        self.add_pages(pages)
        #self.setnaturalsize()

    def add_pages(self, pages):
        for page in pages:
            self.add(page)



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

class _DeleteDialog(Pmw.Dialog):
    
    def __init__(self, parent, **kwargs):
        kwargs['buttons'] = ('Delete', 'Cancel', 'Help')
        kwargs['title'] = 'Delete PaPy %s' % self.name.capitalize()
        kwargs['command'] = self.delete
        kwargs['defaultbutton'] = 'Cancel'
        apply(Pmw.Dialog.__init__, (self, parent), kwargs)
        self.create_widgets()

    def create_widgets(self):
        self.group = Pmw.Group(self.interior())
        self.group.pack(expand =YES, fill =BOTH)
        self.wm_resizable(NO, NO)

    def activate(self):
        # super does not work with classic classes
        self.update()
        Pmw.Dialog.activate(self)

    def update(self):
        pass

    def delete(self):
        pass


class _CreateDialog(Pmw.Dialog):
    
    def __init__(self, parent, **kwargs):
        kwargs['buttons'] = ('Create', 'Cancel', 'Help')
        kwargs['title'] = 'Create PaPy %s' % self.name.capitalize()
        kwargs['command'] = self.create
        kwargs['defaultbutton'] = 'Cancel'
        apply(Pmw.Dialog.__init__, (self, parent), kwargs)
        self.withdraw()
        self.parent = parent
        self.create_widgets()

    def create_widgets(self):
        self.group = Pmw.Group(self.interior(),\
                               tag_pyclass =Tk.Label,\
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
                    try:
                        e.setvalue([]) # deselect all checkboxes
                    except AttributeError:
                        # labeled widget
                        pass
        # update entries which change
        try:
            # not all subclasses provide this
            self.update_entries()
        except AttributeError:
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
        Pmw.Dialog.deactivate(self)
        #self.deactivate(result)

    def help(self):
        pass

class IMapDialog(_CreateDialog):
   
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


class PiperDialog(_CreateDialog):

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

class WorkerDialog(_CreateDialog):
   
    name = 'worker'
    defaults = {(0, 'name'):None, 
                (1, 'funcsargs'): None,
                (2, 'doc'): None}
    fargs = []

    def _create(self):
        objects = papyg.namespace['objects']
        functions = papyg.namespace['functions']
        funcs = []
        kwargs = []
        func_names = self.funcs.get()
        for index, fn in enumerate(func_names):
            f = functions[fn]
            funcs.append(f)
            fkwargs = {}
            for an in inspect.getargspec(f).args:
                if an == 'inbox':
                    continue
                entryfield = self.fargs[index][an].component('entryfield')
                raw_value = entryfield.getvalue()
                # this might be a string, number or object
                if raw_value != "":
                    try:
                        value = objects[raw_value]
                    except KeyError:
                        try:
                            value = int(raw_value)
                        except ValueError:
                            try:
                                value = float(raw_value)
                            except ValueError:
                                value = raw_value
                else:
                    raise ValueError('XXX')
                fkwargs[an] = value
            kwargs.append(fkwargs)
        papyg.add_worker(funcs, kwargs =kwargs)

               
    def add_func(self):
        value = self.cfunc.getvalue()[0] # value in combobox
        try:
            index = int(self.funcs.curselection()[0]) + 1
            self.fargs.insert(index, {}) # a dict for our precious widgets
        except IndexError:
            index = END
            self.fargs.append({})   # ...
        self.funcs.insert(index, value)   # always after selection

    def del_func(self):
        try:
            index = int(self.funcs.curselection()[0])
        except IndexError:
            return
        self.funcs.delete(index)
        self.fargs.pop(index)
        first_or_previous = 0 if not index else index - 1
        try:
            self.funcs.activate(first_or_previous)
            self.funcs.selection_set(first_or_previous)
        finally:
            self.sel_func()
        # should we destroy widgets they are still in children
        # widget.destroy()

    def sel_func(self):
        # remove whatever in args
        frame = self.args.interior()
        self.noargs.grid_remove()
        for child in frame.children.values():
            child.pack_forget()
        
        try:
            index = int(self.funcs.curselection()[0]) # current function index
        except IndexError:
            return
        func_name = self.funcs.getvalue()[0]      # current function name 
        func = papyg.namespace['functions'][func_name] # the real function

        # get arguments and defaults
        arg_names = list(inspect.getargspec(func).args) # argument names
        try:
            arg_defaults = list(inspect.getargspec(func).defaults) # argument defaults
        except TypeError:
            # None if no defaults
            arg_defaults = []
        arg_names.reverse()
        arg_defaults.reverse()
        # if this tuple has n elements, they correspond to the
        # last n elements listed in args. (WTF)

        # fill defaults
        wholder = self.fargs[index] # fargs list, wholder dict
        if not wholder:
            if arg_names and arg_defaults:
                # the map is like zip but fills None for missing.
                for name, default in map(None, arg_names, arg_defaults):
                    if name == 'inbox':
                        continue
                    wholder[name] = Pmw.ComboBox(frame,
                                                label_text = '%s:' % name,
                                                labelpos = 'w',
                                                sticky = N+E+W+S)
                    wholder[name].component('entryfield').setvalue(repr(default))
            else:
                self.noargs.grid(row =0, column =1)


        Pmw.alignlabels(frame.children.values()) # all widget labels
        for widget in wholder.values():
            widget.pack(expand =NO, fill =X)
            widget.setlist(papyg.namespace['objects'].keys())

        self.doc.delete('1.0', END)
        self.doc.insert(END, func.__doc__)

    def create_entries(self):
        # name
        self.name = Pmw.EntryField(self.group.interior(),
                                    labelpos ='w',
		                            label_text ='Name:',
                                    validate ={'validator':'alphabetic'})
        # funcsargs
        self.funcsargs = Pmw.LabeledWidget(self.group.interior(),
                                    labelpos ='w',
                                    label_text ='Definition:')
        # funcs
        self.funcs = Pmw.ScrolledListBox(self.funcsargs.interior(),
                                    labelpos='n',
                                    label_text ='Functions:',
                                    selectioncommand =self.sel_func)
        # cfunc 
        self.cfunc = RestrictedComboBox(self.funcsargs.interior(),
                                   #selectioncommand=self.selectionCommand,
                                   #dblclickcommand=self.defCmd
                                    )

        # bfunc
        self.bfunc = Pmw.ButtonBox(self.funcsargs.interior())
        self.bfunc.add('Add', command =self.add_func)
        self.bfunc.add('Remove', command =self.del_func)

        # args
        self.args = Pmw.LabeledWidget(self.funcsargs.interior(), 
                                        labelpos ='n',
                                        label_text ='Arguments:')
        self.noargs = Tk.Label(self.funcsargs.interior(), text ='no arguments')

        # barg
        self.barg = Pmw.ButtonBox(self.funcsargs.interior())
        self.barg.add('Add', command =None)
        self.barg.add('Remove', command =None)
        
        self.cfunc.grid(row =1, column =0, sticky =N+E+W+S) # function combobox
        self.funcs.grid(row =0, column =0, sticky =N+E+W+S) # function list
        self.bfunc.grid(row =2, column =0, sticky =N+E+W+S) # function buttons
        self.args.grid( row =0, column =1, sticky =N+E+W+S) # arguments group  
        self.barg.grid( row =2, column =1, sticky =N+E+W+S) # argument buttons
        
        # documentation
        self.doc = Pmw.ScrolledText(self.group.interior(),
                                        labelpos ='w',
                                        label_text ='Documentation:',
                                        #usehullsize =YES,
                                        vscrollmode ='static',
                                        hscrollmode =NONE,
                                        text_height =6,
                                        text_width =70,
                                        text_background =O['Function_doc_background'],
                                        text_foreground =O['Function_doc_foreground'],
                                        text_wrap =WORD,
                                        #text_state =DISABLED
                                        )

    def update_entries(self):
        # HARD CODED locations
        self.cfunc.setlist(papyg.namespace['functions'].keys())


     
class PaPyGui(object):

    def __init__(self, parent, **kwargs):   
        self.make_namespace()
        self.toplevel = parent
        # globally change fonts
        self.toplevel.option_add("*font", O['default_font'])
        self.toplevel.title(O['app_name'])
        self.make_dialogs()
        self.make_plumber()
        self.make_widgets()
        self.start()

    def start(self):
        papy.utils.logger.start_logger(log_filename =O['log_filename'], log_stream =self.log)        

    def make_dialogs(self):
        # About
        self.dialogs = {}
        self.dialogs['about'] = Pmw.AboutDialog(self.toplevel, applicationname ='My Application')
        self.dialogs['about'].withdraw()
        self.dialogs['new_piper'] = PiperDialog(self.toplevel)
        self.dialogs['new_imap'] = IMapDialog(self.toplevel)
        self.dialogs['new_worker'] = WorkerDialog(self.toplevel)

    def make_widgets(self, title =None):
        #main menu
        self.menu_bar = MainMenuBar(self.toplevel, self.dialogs)
        self.toplevel.config(menu =self.menu_bar)

        #toolbar
        # 4 panes
        self.lr = Tk.PanedWindow(self.toplevel)
        self.lr.pack(fill=BOTH, expand=YES)

        self.l = Tk.PanedWindow(self.lr, orient=VERTICAL, showhandle =YES, sashwidth =20)
        self.r = Tk.PanedWindow(self.lr, orient=VERTICAL, showhandle =YES, sashwidth =20)
        self.lr.add(self.l, stretch ='always')
        self.lr.add(self.r, stretch ='always')
        
        # pipeline & code, shell & logging
        self.pipeline = NoteBook(self.r, ['Pipeline', 'Functions', 'IMaps', 'Data'])
        self.io = NoteBook(self.r, ['Shell', 'Logging'])

        # pipers
        self.pipers = PipersTree(self.l, self.namespace['pipers'],
                                                        self.dialogs)
        self.workers = WorkersTree(self.l, self.namespace['workers'],
                                                        self.dialogs)
        # pipeline
        pipeline_ = self.pipeline.page('Pipeline')
        pipeline_.grid_rowconfigure(0, weight =1)
        pipeline_.grid_columnconfigure(1, weight =1)

        self.graph = GraphCanvas(pipeline_)
        self.pipeline_buttons = Pmw.ButtonBox(pipeline_,
                                                orient =VERTICAL,
                                                padx =0, pady =0)
        self.pipeline_buttons.add('Use\n->', command =self.use)
        self.pipeline_buttons.add('Pop\n<-', command =self.pop)
        self.pipeline_buttons.add('Run\n|>', command =None)
        self.pipeline_buttons.add('Stop\n[]', command =None)
            
        self.pipeline_buttons.grid(row =0, column =0, sticky =N)
        self.graph.grid(row =0, column =1, sticky =N+E+W+S)

        # fucntions
        functions_ = self.pipeline.page('Functions')
        self.modules = ModulesTree(functions_, self.namespace['modules'],
                                                            self.dialogs)
        self.function_text = Pmw.ScrolledText(functions_, 
                                            labelpos =N+W,
                                            text_padx = O['Code_font'][1] // 2,  # half-font
                                            text_pady = O['Code_font'][1] // 2,
                                            label_text ='Module code')
        functions_.grid_rowconfigure(0, weight =1)
        functions_.grid_columnconfigure(1, weight =1)
        self.modules.frame.grid(row =0, column =0, sticky =N+E+W+S)
        self.function_text.grid(row =0, column =1, sticky =N+E+W+S)
        

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
                    text_padx = O['Shell_font'][1] // 2,  # half-font
                    text_pady = O['Shell_font'][1] // 2)
        self.shell.text['background'] = O['Shell_background']
        self.shell.text['foreground'] = O['Shell_fontcolor']
        self.shell.text['font'] = O['Shell_font']
        self.io.tab('Shell').bind("<Button-3>", self.shell.kill_console)
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
        for n in ('modules','functions', 'workers', 'pipers', 'imaps', 'objects'):
            self.namespace[n] = {}

    def add_piper(self, **kwargs):
        # add to namespace
        piper = papy.Piper(**kwargs)
        self.namespace['pipers'][piper.name] = piper
        self.pipers.add_item(piper)

    def use(self):
        # add to Dagger
        self.graph.add_piper(self.pipers.selected_item)

    def del_piper(self, **kwargs):
        # remove from namespace
        self.namespace['pipers'].remove(piper)
        self.pipers.del_item(piper)

    def pop(self):
        # remove pipe or edge from Dagger
        self.graph.pop()

    def add_imap(self, **kwargs):
        imap = IMap.IMap(**kwargs)
        self.namespace['imaps'][imap.name] = imap
        self.imaps.add_item(imap)

    def del_imap(self, imap, **kwargs):
        self.namespace['imaps'].remove(imap)
        self.imaps.del_item(imap)

    def add_worker(self, functions, arguments =None, kwargs =None):
        worker = papy.Worker(functions, arguments, kwargs)
        self.namespace['workers'][worker.name] = worker
        self.workers.add_item(worker)

    def del_worker(self):
        pass

    def add_function(self, **kwargs):
        mod = __import__(kwargs['module'], fromlist =[''])
        function = getattr(mod, kwargs['function'])
        self.namespace['functions'][function.__name__] = function

    def add_object(self, name, object):
        self.namespace['objects'][name] = object

    def make_plumber(self):
        if False: # some input file
            pass
        else:
            self.namespace['plumber'] = papy.Plumber()

class Options(dict):
    """ Provide options throughout the PaPy Gui application.
    """

    defaults = (('app_name', 'PaPy'),
                ('log_filename', None),
                ('default_font', ("tahoma", 8)),
                ('node_color', 'blue'),
                ('node_status', 'green'),
                ('graph_background', 'white'),
                ('graph_background_select', 'gray'),
                ('Pipers_background', 'white'),
                ('Workers_background', 'white'),
                ('Modules_background', 'white'),
                ('Function_doc_background', 'white'),
                ('Function_doc_foreground', 'gray'),
                ('Shell_background', 'white'),
                ('Shell_history', 1000),
                ('Shell_fontcolor', 'black'),
                ('Shell_font', ("courier new", 9)),
                ('Code_font', ("courier new", 9)),
                ('Pipers_root_icon', 'pipe_16'),
                ('IMaps_root_icon', 'gear_16'),
                ('Workers_root_icon', 'component_16'),
                ('Modules_root_icon', 'python'),
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

cfg_opts = ConfigOptions()
cmd_opts = CommandOptions()
O = Options(cfg_opts, cmd_opts)

def main():
    global root
    root = Tk.Tk()
    Pmw.initialise(root)
    root.withdraw()

    # make gui
    papyg = PaPyGui(root)

    def make_junk(papyg):
        papyg.add_imap()
        papyg.add_imap()
        papyg.add_imap()
        papyg.add_imap()
        papyg.add_function(module ='papy.workers.io', function ='print_')
        papyg.add_function(module ='papy.workers.io', function ='dump_item')
        papyg.add_function(module ='papy.workers.io', function ='load_item')
        papyg.add_object('None', None)
        papyg.add_object('False', False)
        papyg.add_worker(papy.workers.io.dump_item)
        papyg.add_piper(worker =papy.workers.io.dump_item)
        papyg.add_piper(worker =papy.workers.io.load_item)
        papyg.add_piper(worker =papy.workers.io.print_)
        papyg.graph.add_piper(papyg.namespace['pipers'].values()[0])
        papyg.graph.add_piper(papyg.namespace['pipers'].values()[1])
        papyg.graph.add_piper(papyg.namespace['pipers'].values()[2])
        # temp add junk
    
    make_junk(papyg)

    # show gui
    root.deiconify()
    root.mainloop()

  

# tkFileDialog.askopenfilename()
#        if '\x04' == key.char: #ctrl-D
#            self.stdin.put('exit()\n')
#            return



if __name__ == '__main__':
    main()
