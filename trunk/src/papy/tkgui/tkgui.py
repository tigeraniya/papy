#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" The PaPy gui written in Tkinter using Pmw.
"""
# PaPy/IMap imports
import papy
import IMap

# Python imports
import inspect

# Tkinter/Pmw/idlelib imports
# watch for errors on multiprocessing/Tkinter/linux
import Pmw
import Tkinter as Tk
#import tkMessageBox as tkm
from Tkconstants import *

# Boundled Generic widgets
from TreeWidget import TreeItem, TreeNode
from ShellWidget import PythonShell


class RootItem(TreeItem):

    def __init__(self, items, item_pyclass, tree, icon_name=None):
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

    def __init__(self, item, root_):
        self.item = item
        self.root = root_

    def GetText(self):
        try:
            return self.item.name
        except AttributeError:
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

    subitems = ('worker_type', 'worker_num', 'worker_remote', \
             'stride', 'buffer', 'ordered', 'skip')
    subclass = AttributeTreeItem

class PiperTreeItem(_TreeItem):

    subitems = ('worker', 'IMap', 'consume', 'produce', 'spawn', 'timeout', \
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


#   def GetSubList(self):
#        return [FunctionTreeItem(self, f) for f in self.get_functions()]


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
            setattr(self, label + '_text', '%s %s' % (label, self.name[:-1]))

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



        self.buttons = Pmw.ButtonBox(self.frame, padx=0, pady=0)
        self.buttons.add(self.new_text, command=self.new_cmd)
        self.buttons.add(self.del_text, command=self.del_cmd)

        self.group = Pmw.Group(self.frame, tag_pyclass=Tk.Label,
                                              tag_text=self.label_text)

        canvas = Pmw.ScrolledCanvas(self.group.interior())
        self.canvas = canvas.component('canvas')
        self.canvas.config(bg=O[self.name + '_background'], width=200)
        self.buttons.pack(side=BOTTOM, anchor=W)
        self.root = TreeNode(self.canvas, None, \
        self.root_pyclass(self.items, self.item_pyclass, self, O[self.name + '_root_icon']))

        # this patches TreeNode with icons for the specific tree.
        # icondir = os.path.join(os.path.dirname(__file__), 'icons', self.name + 'Tree')
        # icons = os.listdir(icondir)
        # for icon in (i for i in icons if i.endswith('.gif')):
        #    image = Tk.PhotoImage(master =self.canvas,\
        #                            file =os.path.join(icondir, icon))
        #    self.root.iconimages[icon.split('.')[0]] = image

        canvas.pack(fill=BOTH, expand=YES)
        self.group.pack(fill=BOTH, expand=YES)

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
        showerror(message='Not implemented')

    def make_widgets(self):
        self.addmenu('File', 'Load/Save/Exit')

        self.addmenuitem('File', 'command', 'Load',
		                 command=None,
		                   label='Load')

        self.addmenuitem('File', 'command', 'Save',
		                 command=None,
		                   label='Save')

        self.addmenuitem('File', 'command', 'Exit',
		                 command=None,
		                   label='Exit')

        self.addmenu('Options', 'Options/Setting')
        self.addmenuitem('Options', 'command', 'Gui',
		                 command=None,
		                   label='Gui Options')

        self.addmenu('Help', 'User manuals', name='help')
        self.addmenuitem('Help', 'command', 'About this application',
                command=self.dialogs['about'].show,
                  label='About')


class GraphCanvas(Pmw.ScrolledCanvas):

    def _canvas_coords(self, x, y):
        return (int(self.canvasx(x)), int(self.canvasy(y)))

    def _last_click(self, event):
        self._update_canvas()
        # canvas position of click
        self.lastxy = self._canvas_coords(event.x, event.y)
        # what did we click
        self.lasttags = self.gettags(CURRENT)

    def _create_piper_graphics(self, Node):
        x = Node.xtra['x']
        y = Node.xtra['y']
        tag = Node.xtra['tag']
        self.delete("%s&&%s" % (tag[0], tag[1]))

        status = Node.xtra.get('status') or O['node_status']
        color = Node.xtra.get('color') or O['node_color']
        if Node is self.last_piper_Node[1]:
            self.create_oval(x - 16, y - 16, x + 16, y + 16, \
                             fill=O['node_select'], \
                             outline=O['node_select'], \
                             tags=tag)

        self.create_oval(x - 12, y - 12, x + 12, y + 12, fill=status, tags=tag, \
                                            activewidth=3.0, activefill="plum")
        self.create_oval(x - 8, y - 8, x + 8, y + 8, fill=color, tags=tag, \
                                            state='disabled', width=0.0)
        self.create_text(x + 12, y - 17, text=Node.xtra.get('name'), \
                                         fill='black', anchor=NW, tags=tag)
        self._update_canvas()

    def _create_pipe_graphics(self, Node1, Node2):
        xy1 = Node1.xtra['x'], Node1.xtra['y']
        xy2 = Node2.xtra['x'], Node2.xtra['y']
        tag = ('pipe', Node1.xtra['tag'][1], Node2.xtra['tag'][1])
        print tag
        self.delete("%s&&%s&&%s" % (tag[0], tag[1], tag[2]))

        if Node1 is self.last_pipe_Node_pipe[1][0] and \
           Node2 is self.last_pipe_Node_pipe[1][1]:
            self.create_line(*xy1 + xy2, fill='red', \
                                         width=3, \
                                         arrow=LAST,
                                         tags=tag)
        else:
            self.create_line(*xy1 + xy2, \
                                    width=3, \
                                    arrow=LAST, \
                                    tags=tag)
        self._update_canvas()

    def _create_broken_pipe_graphics(self, xy1, xy2):
        self.delete("BROKEN_PIPE")
        self.create_line(*xy1 + xy2, width=1, arrow=LAST, tags=("BROKEN_PIPE",))
        self._update_canvas()

    def _redraw_pipes(self, Node):
        # filter affected pipes
        Node_pipes = [No_p for No_p in self.tag_to_Node_pipe.values() if \
                      Node in No_p]
        # redraw affected pipes
        for Node_pipe in Node_pipes:
            self._create_pipe_graphics(*Node_pipe)

    def _update_canvas(self):
        # un-clutter
        self.canvas_menu.unpost()
        self.pipe_menu.unpost()
        self.resizescrollregion()
        self.canvas.focus_set()
        self.tag_raise('pipe')

    def _center_canvas(self):
        pass

    def __init__(self, parent, **kwargs):
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
        self.constructed_pipe = None
        # canvas 'objects'
        self.tag_to_piper = {}
        self.tag_to_Node = {}
        self.tag_to_Node_pipe = {}
        self.last_piper_Node = (None, None)
        self.last_pipe_Node_pipe = ((None, None), (None, None))
        # button bindings 
        self.canvas.bind("<Button-1>", self.mouse1_down)
        self.canvas.bind("<B1-Motion>", self.mouse1_drag)
        self.canvas.bind("<ButtonRelease-1>", self.mouse1_up)
        self.canvas.bind("<Button-3>", self.mouse3_down)
        self.canvas.bind("<B3-Motion>", self.mouse3_drag)
        self.canvas.bind("<ButtonRelease-3>", self.mouse3_up)
        # MacOSX compability
        self.canvas.bind("<Control-Button-1>", self.mouse3_down)
        self.canvas.bind("<Control-B1-Motion>", self.mouse3_drag)
        self.canvas.bind("<Control-ButtonRelease-1>", self.mouse3_up)
        # keyboard
        self.canvas.bind("<d>", lambda event: self.pop())
        self.canvas.focus_set()

    def make_widgets(self):
        # right click pipe
        self.pipe_menu = Tk.Menu(root, tearoff=0)
        self.pipe_menu.add_command(label="delete pipe", command=self.menu_del_pipe)
        self.pipe_menu.add_command(label="reverse pipe", command=self.menu_reverse_pipe)
        # right click canvas
        self.canvas_menu = Tk.Menu(root, tearoff=0)
        self.canvas_menu.add_command(label="add piper", command=self.menu_add_piper)
        self.canvas_menu.add_command(label="center graph", command=self._center_canvas)
        self.canvas_menu.add_command(label="align graph", command=self.align_graph)


    # methods modifying the contents of the canvas

    def move_piper(self, tag, event):
        # get Node
        Node = self.tag_to_Node[tag[:2]]
        # delete all pipes of this piper
        self.delete("%s&&%s" % ('pipe', tag[1]))
        # move piper
        currxy = self._canvas_coords(event.x, event.y)
        dx, dy = currxy[0] - Node.xtra['x'], \
                 currxy[1] - Node.xtra['y'],
        self.move("%s&&%s" % tag[:2], dx, dy)
        # update Node
        Node.xtra['x'] += dx
        Node.xtra['y'] += dy
        # re-draw affected pipes
        self._redraw_pipes(Node)

    def add_piper(self, piper, Node):
        # create tag
        tag = ("piper", "n%s" % id(piper))
        # update Node
        Node.xtra['tag'] = tag
        Node.xtra['name'] = Node.xtra.get('name') or piper.name
        Node.xtra['x'] = Node.xtra.get('x') or self.lastxy[0]
        Node.xtra['y'] = Node.xtra.get('y') or self.lastxy[1]
        # create graphics
        self._create_piper_graphics(Node)
        # update mappings, lasttag
        self.tag_to_piper[tag] = piper
        self.tag_to_Node[tag] = Node
        self.lasttags = tag

    def add_pipe(self, piper1, Node1, piper2, Node2):
        # create tag
        tag1 = Node1.xtra['tag']
        tag2 = Node2.xtra['tag']
        tag = ("pipe", tag1[1], tag2[1])
        # create graphics
        self._create_pipe_graphics(Node1, Node2)
        # update mappings, lasttag
        self.tag_to_Node_pipe[tag] = (Node1, Node2)
        self.lasttags = tag

    def del_piper(self, tag=None):
        tag = tag or self.lasttags
        if not tag:
            return None
        piper, Node = self.last_piper_Node
        if not piper:
            return None
        tag = tag[:2]
        # delete from canvas (pipers and pipes)
        self.delete(tag[1])
        # clean piper/pipe mappings
        self.tag_to_piper.pop(tag)
        self.tag_to_Node.pop(tag)
        for tag, Node_pipe in self.tag_to_Node_pipe.items():
            if Node in Node_pipe:
                self.tag_to_Node_pipe.pop(tag)
        # clean lasttag
        self.lasttags = None
        self.last_piper_Node = (None, None)

        return piper

    def del_pipe(self, tag=None):
        tag = tag or self.lasttags
        if not tag:
            return None
        tag = tag[:3]
        # delete from canvas (pipe)
        self.delete("%s&&%s&&%s" % tag)
        # delete from mapping
        self.tag_to_Node_pipe.pop(tag)
        piper1 = self.tag_to_piper[('piper', tag[1])]
        piper2 = self.tag_to_piper[('piper', tag[2])]
        self.lasttags = None
        return piper1, piper2

    # methods for mouse events

    def mouse1_down(self, event):
        self._last_click(event)
        if self.lasttags and self.lasttags[0] == 'piper':
            # left click on a piper
            # 1. get previous and current Node and Node pipe
            previous_Node = self.last_piper_Node[1]
            previous_Node_pipe = self.last_pipe_Node_pipe[1]
            current_tag = self.lasttags[:2]
            current_Node = self.tag_to_Node[current_tag]
            current_piper = self.tag_to_piper[current_tag]
            # 2. change last Node and Node pipe
            self.last_piper_Node = current_piper, current_Node
            self.last_pipe_Node_pipe = ((None, None), (None, None))
            # 3. deemphasize previous Node and Node pipe
            if previous_Node is not None:
                self._create_piper_graphics(previous_Node)
            if previous_Node_pipe != (None, None):
                self._create_pipe_graphics(*previous_Node_pipe)
            # emphasize current Node
            self._create_piper_graphics(current_Node) # emphasize
            self.dragging = True

        if self.lasttags and self.lasttags[0] == 'pipe':
            # left click on a pipe
            # 1. get previous and current Node and Node pipe
            previous_Node = self.last_piper_Node[1]
            previous_Node_pipe = self.last_pipe_Node_pipe[1]
            current_tag = self.lasttags[:3]
            current_pipe = self.tag_to_Node_pipe[current_tag]
            current_Node_pipe = self.tag_to_Node_pipe[current_tag]
            # 2. change last Node and Node pipe
            self.last_piper_Node = (None, None)
            self.last_pipe_Node_pipe = current_pipe, current_Node_pipe
            # 3. deemphasize previous Node and Node pipe
            if previous_Node is not None:
                self._create_piper_graphics(previous_Node)
            if previous_Node_pipe != (None, None):
                self._create_pipe_graphics(*previous_Node_pipe) # deemphasize
            # 3. emphasize current Node pipe
            self._create_pipe_graphics(*current_Node_pipe) # emphasize

    def mouse3_down(self, event):
        self.constructed_pipe = None
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
                    piper1 = self.tag_to_piper[self.lasttags[:2]]
                    piper2 = self.tag_to_piper[currtags[:2]]
                    self.constructed_pipe = (piper1, piper2)
                    # add to plumber
                    papyg.add_pipe_canvas() ## this is the call-back
            self.connecting = False

    def mouse1_drag(self, event):
        if self.dragging:
            self.move_piper(self.lasttags, event)

    def mouse3_drag(self, event):
        if self.connecting:
            xy1 = self.lastxy
            xy2 = self._canvas_coords(event.x, event.y)
            self._create_broken_pipe_graphics(xy1, xy2)

    # methods calling global papyg

    def menu_add_pipe(self):
        papyg.add_pipe_canvas()

    def menu_del_pipe(self):
        papyg.del_pipe_canvas()

    def menu_reverse_pipe(self):
        pipe = list(papyg.del_pipe_canvas())
        pipe.reverse()
        papyg.add_pipe_canvas(pipe)

    def menu_add_piper(self):
        papyg.add_piper_canvas()

    def menu_del_piper(self):
        papyg.del_piper_canvas()

    # layouts

    def align_graph(self):
        # 1. get node ranks
        # 2. get rank widths -> determine max width
        # 3. get preorder or reverse postorder?
        # 4. go from input -> output (decreasing rank order)
        # for each node determine the number of center

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
        self.group.pack(expand=YES, fill=BOTH)
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
        self.group = Pmw.Group(self.interior(), \
                               tag_pyclass=Tk.Label, \
                               tag_text="%s Arguments" % \
                               self.name.capitalize())
        # provided by sub-class
        self.create_entries()
        self.named_entries = [(name, getattr(self, name[1])) for name\
                                     in sorted(self.defaults.keys())]
        entries = [e for n, e in self.named_entries]
        # align and pack
        Pmw.alignlabels(entries)
        for w in entries:
            w.pack(expand=YES, fill=BOTH)
        self.group.pack(expand=YES, fill=BOTH)
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

        self.name = Pmw.EntryField(self.group.interior(), \
                                   labelpos='w', \
		                           label_text='Name:', \
		                           validate={'validator':'alphabetic'})

        self.worker_type = RestrictedComboBox(self.group.interior(), \
                                 label_text='Type:', \
                                 labelpos='w')

        self.worker_remote = Pmw.EntryField(self.group.interior(), \
                                   labelpos='w', \
		                           label_text='Remote:')

        self.parallel = RestrictedComboBox(self.group.interior(), \
                                 label_text='IMap:', \
                                 labelpos='w')

        for a in ('worker_num', 'stride', 'buffer'):
            label = a.capitalize() + ':' if a != 'worker_num' else 'Number:'
            setattr(self, a, Pmw.Counter(self.group.interior(), \
                                label_text=label, \
		                        labelpos='w'))

        self.misc = Pmw.RadioSelect(self.group.interior(),
                        buttontype='checkbutton',
                        orient='vertical',
                        labelpos='w',
                        label_text='Misc.:',
                        hull_borderwidth=0)
        self.misc.add('ordered')
        self.misc.add('skip')


class PiperDialog(_CreateDialog):

    name = 'piper'
    defaults = {(0, 'name'):None,
                (1, 'worker'):None,
                (2, 'parallel'):None,
                (3, 'produce'):'1',
                (4, 'spawn'):'1',
                (5, 'consume'):'1',
                (6, 'timeout'):'0.0',
                (7, 'cmp'):None,
                (8, 'ornament'):None,
                (9, 'runtime'):None}

    def create_entries(self):

        self.name = Pmw.EntryField(self.group.interior(), \
                                   labelpos='w', \
		                           label_text='Name:', \
		                           validate={'validator':'alphabetic'})

        self.worker = RestrictedComboBox(self.group.interior(), \
                                 label_text='Worker:', \
                                 labelpos='w')

        self.parallel = RestrictedComboBox(self.group.interior(), \
                                 label_text='IMap:', \
                                 labelpos='w')

        for a in (('produce'), ('spawn'), ('consume')):
            setattr(self, a, Pmw.Counter(self.group.interior(), \
                                label_text=a.capitalize() + ':', \
		                        labelpos='w', \
		                        entryfield_value='1', \
	                            entryfield_validate={'validator' : 'integer', \
			                    'min' : '1'}))

        self.timeout = Pmw.Counter(self.group.interior(), \
                                label_text='Timeout:', \
		                        labelpos='w', \
		                        entryfield_value='0.0', \
                                increment=0.1, \
                                datatype={'counter' : 'real'}, \
	                            entryfield_validate=\
                                {'validator' : 'real', 'min' : 0.0})

        self.cmp = RestrictedComboBox(self.group.interior(), \
                                 label_text='Compare:', \
                                 labelpos='w')

        self.ornament = Pmw.EntryField(self.group.interior(), \
                                   labelpos='w', \
		                           label_text='Ornament:')

        self.runtime = Pmw.RadioSelect(self.group.interior(),
                        buttontype='checkbutton',
                        orient='vertical',
                        labelpos='w',
                        label_text='Runtime:',
                        hull_borderwidth=0)
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
        for (i, name), entry in named_entries:
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
        papyg.add_worker(funcs, kwargs=kwargs)


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
                                                label_text='%s:' % name,
                                                labelpos='w',
                                                sticky=N + E + W + S)
                    wholder[name].component('entryfield').setvalue(repr(default))
            else:
                self.noargs.grid(row=0, column=1)


        Pmw.alignlabels(frame.children.values()) # all widget labels
        for widget in wholder.values():
            widget.pack(expand=NO, fill=X)
            widget.setlist(papyg.namespace['objects'].keys())

        self.doc.delete('1.0', END)
        self.doc.insert(END, func.__doc__)

    def create_entries(self):
        # name
        self.name = Pmw.EntryField(self.group.interior(),
                                    labelpos='w',
		                            label_text='Name:',
                                    validate={'validator':'alphabetic'})
        # funcsargs
        self.funcsargs = Pmw.LabeledWidget(self.group.interior(),
                                    labelpos='w',
                                    label_text='Definition:')
        # funcs
        self.funcs = Pmw.ScrolledListBox(self.funcsargs.interior(),
                                    labelpos='n',
                                    label_text='Functions:',
                                    selectioncommand=self.sel_func)
        # cfunc 
        self.cfunc = RestrictedComboBox(self.funcsargs.interior(),
                                   #selectioncommand=self.selectionCommand,
                                   #dblclickcommand=self.defCmd
                                    )

        # bfunc
        self.bfunc = Pmw.ButtonBox(self.funcsargs.interior())
        self.bfunc.add('Add', command=self.add_func)
        self.bfunc.add('Remove', command=self.del_func)

        # args
        self.args = Pmw.LabeledWidget(self.funcsargs.interior(),
                                        labelpos='n',
                                        label_text='Arguments:')
        self.noargs = Tk.Label(self.funcsargs.interior(), text='no arguments')

        # barg
        self.barg = Pmw.ButtonBox(self.funcsargs.interior())
        self.barg.add('Add', command=None)
        self.barg.add('Remove', command=None)

        self.cfunc.grid(row=1, column=0, sticky=N + E + W + S) # function combobox
        self.funcs.grid(row=0, column=0, sticky=N + E + W + S) # function list
        self.bfunc.grid(row=2, column=0, sticky=N + E + W + S) # function buttons
        self.args.grid(row=0, column=1, sticky=N + E + W + S) # arguments group  
        self.barg.grid(row=2, column=1, sticky=N + E + W + S) # argument buttons

        # documentation
        self.doc = Pmw.ScrolledText(self.group.interior(),
                                        labelpos='w',
                                        label_text='Documentation:',
                                        #usehullsize =YES,
                                        vscrollmode='static',
                                        hscrollmode=NONE,
                                        text_height=6,
                                        text_width=70,
                                        text_background=O['Function_doc_background'],
                                        text_foreground=O['Function_doc_foreground'],
                                        text_wrap=WORD,
                                        #text_state =DISABLED
                                        )

    def update_entries(self):
        # HARD CODED locations
        self.cfunc.setlist(papyg.namespace['functions'].keys())


class PaPyGui(object):

    def __init__(self, parent, **kwargs):
        self.namespace = {}
        self.dialogs = {}
        self.make_namespace()
        self.toplevel = parent
        # globally change fonts
        self.toplevel.option_add("*font", O['default_font'])
        self.toplevel.title(O['app_name'])
        self.make_dialogs()
        self.make_widgets()
        self.namespace['plumber'] = papy.Plumber()

    def make_namespace(self):
        self.namespace['pipeline'] = self
        for name in ('modules', 'functions', 'workers', \
                  'pipers', 'imaps', 'objects'):
            self.namespace[name] = {}
        self.namespace['papyg'] = self
        self.namespace['papy'] = papy
        self.namespace['IMap'] = IMap

    def load(self, filename):
        #1. add filename as module
        #2. add pipers to namespace
        #3. add workers to namespace
        #4. add worker-functions to namespace
        #5. add objects to namespace
        #9. update plumber with pipers, pipes and xtra
        pass

    def save(self, filename):
        self.namespace['plumber'].save(filename)

    def make_dialogs(self):
        # About is a Pmw, no auto withdraw
        self.dialogs['about'] = Pmw.AboutDialog(self.toplevel, applicationname='My Application')
        self.dialogs['about'].withdraw()
        self.dialogs['new_piper'] = PiperDialog(self.toplevel)
        self.dialogs['new_imap'] = IMapDialog(self.toplevel)
        self.dialogs['new_worker'] = WorkerDialog(self.toplevel)

    def _error_dialog(self, txt):
        # generic error dialog
        if isinstance(txt, Exception):
            txt = txt[0]
        # used to display an error text
        error = Pmw.MessageDialog(root, title='PaPy Panic!',
                                        defaultbutton=0,
                                        message_text=txt)
        error.iconname('Simple message dialog')
        error.activate()

    def make_widgets(self, title=None):
        #main menu
        self.menu_bar = MainMenuBar(self.toplevel, self.dialogs)
        self.toplevel.config(menu=self.menu_bar)

        #toolbar
        # 4 panes
        self.lr = Tk.PanedWindow(self.toplevel)
        self.lr.pack(fill=BOTH, expand=YES)

        self.l = Tk.PanedWindow(self.lr, orient=VERTICAL, showhandle=YES, sashwidth=20)
        self.r = Tk.PanedWindow(self.lr, orient=VERTICAL, showhandle=YES, sashwidth=20)
        self.lr.add(self.l, stretch='always')
        self.lr.add(self.r, stretch='always')

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
        pipeline_.grid_rowconfigure(0, weight=1)
        pipeline_.grid_columnconfigure(1, weight=1)

        self.graph = GraphCanvas(pipeline_)
        self.pipeline_buttons = Pmw.ButtonBox(pipeline_,
                                                orient=VERTICAL,
                                                padx=0, pady=0)
        self.pipeline_buttons.add('Use\n->', command=self.add_piper_canvas)
        self.pipeline_buttons.add('Pop\n<-', command=self.del_piper_canvas)
        self.pipeline_buttons.add('Run\n|>', command=None)
        self.pipeline_buttons.add('Stop\n[]', command=None)

        self.pipeline_buttons.grid(row=0, column=0, sticky=N)
        self.graph.grid(row=0, column=1, sticky=N + E + W + S)

        # fucntions
        functions_ = self.pipeline.page('Functions')
        self.modules = ModulesTree(functions_, self.namespace['modules'],
                                                            self.dialogs)
        self.function_text = Pmw.ScrolledText(functions_,
                                            labelpos=N + W,
                                            text_padx=O['Code_font'][1] // 2, # half-font
                                            text_pady=O['Code_font'][1] // 2,
                                            label_text='Module code')
        functions_.grid_rowconfigure(0, weight=1)
        functions_.grid_columnconfigure(1, weight=1)
        self.modules.frame.grid(row=0, column=0, sticky=N + E + W + S)
        self.function_text.grid(row=0, column=1, sticky=N + E + W + S)


        # imaps
        self.imaps = IMapsTree(self.pipeline.page('IMaps'), \
                               self.namespace['imaps'], \
                               self.dialogs)
        self.imaps.frame.pack(fill=BOTH, expand=YES)

        # logging
        self.log = ScrolledLog(self.io.page('Logging'),
                    borderframe=True,
                    text_padx=O['default_font'][1] // 2, # half-font
                    text_pady=O['default_font'][1] // 2,
                    text_wrap='none')
        self.log.configure(text_state='disabled')
        self.log.pack(fill=BOTH, expand=YES)

        # shell
        self.shell = PythonShell(self.io.page('Shell'),
                    namespace=self.namespace,
                    text_padx=O['Shell_font'][1] // 2, # half-font
                    text_pady=O['Shell_font'][1] // 2)
        self.shell.text['background'] = O['Shell_background']
        self.shell.text['foreground'] = O['Shell_fontcolor']
        self.shell.text['font'] = O['Shell_font']
        self.io.tab('Shell').bind("<Button-3>", self.shell.kill_console)
        self.shell.pack(fill=BOTH, expand=YES)

        # packing
        self.l.add(self.pipers.frame, stretch='always')
        self.l.add(self.workers.frame, stretch='always')
        self.l.paneconfigure(self.pipers.frame, sticky=N + E + W + S)
        self.l.paneconfigure(self.workers.frame, sticky=N + E + W + S)
        self.r.add(self.pipeline, stretch='always')
        self.r.add(self.io, stretch='always')
        self.r.paneconfigure(self.pipeline, sticky=N + E + W + S)
        self.r.paneconfigure(self.io, sticky=N + E + W + S)

        # statusbar
        self.status_bar = Pmw.MessageBar(self.toplevel,
		   entry_relief='groove',
		       labelpos=W,
		     label_text='Status:')
        self.status_bar.pack(fill=BOTH, anchor=W)

    def add_piper(self, piper=None, **kwargs):
        if not piper:
            # create from kwargs
            piper = papy.Piper(**kwargs)
        # add to namespace    
        self.namespace['pipers'][piper.name] = piper
        # add to tree
        self.pipers.add_item(piper)
        return piper

    def del_piper(self, piper):
        # remove from namespace
        self.namespace['pipers'].remove(piper.name)
        # remove from tree
        self.pipers.del_item(piper)
        return piper

    def add_worker(self, worker=None, **kwargs):
        if not worker:
            worker = papy.Worker(**kwargs)
        # add to namespace
        self.namespace['workers'][worker.name] = worker
        # add to tree
        self.workers.add_item(worker)
        return worker

    def del_worker(self, worker):
        # remove from namespace
        self.namespace['workers'].remove(worker.name)
        # remove from tree
        self.pipers.del_item(worker)
        return worker

    def add_imap(self, imap=None, **kwargs):
        if not imap:
            imap = IMap.IMap(**kwargs)
        # add to namespace
        self.namespace['imaps'][imap.name] = imap
        # add to tree
        self.imaps.add_item(imap)
        return imap

    def del_imap(self, imap):
        # remove from namespace
        self.namespace['imaps'].remove(imap.name)
        # remove from tree
        self.imaps.del_item(imap)
        return imap

    def add_function(self, function=None, **kwargs):
        #TODO: write add_function
        if not function:
            mod = __import__(kwargs['mod_name'], fromlist=[''])
            func_name = kwargs['func_name']
            function = getattr(mod, func_name)
        else:
            func_name = function.__name__
        # add to namespace
        self.namespace['functions'][func_name] = function
        # add to tree
        # self.functions.add_item(function)
        return function

    def del_function(self, function):
        #TODO: write del_function
        # remove from namespace
        self.namespace['functions'].remove(function.__name__)

    def add_object(self, name, object):
        #TODO: write add_object
        self.namespace['objects'][name] = object

        return object

    def del_object(self, name):
        #TODO: write del_object
        object = self.namespace['objects'].pop(name)
        return object

    def add_piper_canvas(self, piper=None, xtra=None):
        # add to GrapCanvas
        piper = piper or self.pipers.selected_item
        if not piper:
            self._error_dialog('No Piper selected.')
            return
        elif piper in self.namespace['plumber']:
            self._error_dialog('Piper alread added.')
            return
        self.namespace['plumber'].add_piper(piper, xtra)
        self.graph.add_piper(piper, self.namespace['plumber'][piper])
        return piper

    def del_piper_canvas(self, piper=None):
        if piper:
            try:
                Node = self.namespace['plumber'][piper]
            except KeyError:
                self._error_dialog('Unknown Piper.')
                return
            tag = Node.xtra['tag']
            self.graph.del_piper(tag)
        else:
            piper = self.graph.del_piper()
            if piper is None:
                self._error_dialog('No Piper selected.')
                return
        self.namespace['plumber'].del_piper(piper, forced=True)
        return piper

    def add_pipe_canvas(self, pipe=None):
        pipe = pipe or self.graph.constructed_pipe
        try:
            self.namespace['plumber'].add_pipe(pipe)
        except papy.DaggerError, excp:
            self._error_dialog(excp)
            return
        piper1 = pipe[0]
        piper2 = pipe[1]
        Node1 = self.namespace['plumber'][piper1]
        Node2 = self.namespace['plumber'][piper2]
        self.graph.add_pipe(piper1, Node1, piper2, Node2)
        return pipe

    def del_pipe_canvas(self, pipe=None):
        if pipe:
            try:
                Node1 = self.namespace['plumber'][pipe[0]]
                Node2 = self.namespace['plumber'][pipe[1]]
            except KeyError:
                self._error_dialog('Unknown Pipe.')
            tag = ('pipe', Node1.xtra['tag'], Node2.xtra['tag'])
            self.graph.del_pipe(tag)
        else:
            pipe = self.graph.del_pipe()
        self.namespace['plumber'].del_edge((pipe[1], pipe[0]))
        return pipe


class Options(dict):
    """ Provide options throughout the PaPy Gui application.
    """

    defaults = (('app_name', 'PaPy'),
                ('log_filename', None),
                ('default_font', ("tahoma", 8)),
                ('node_color', 'blue'),
                ('node_status', 'green'),
                ('node_select', 'red'),
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

    def __init__(self, config_options=None, command_options=None):
        init = dict(self.defaults)
        init.update(config_options or {})
        init.update(command_options or {})
        dict.__init__(self, init)


class ConfigOptions(dict):
    #TODO: write config option parser for tkgui
    pass


class CommandOptions(dict):
    #TODO: write command option parser for tkgui
    pass

cfg_opts = ConfigOptions()
cmd_opts = CommandOptions()
O = Options(cfg_opts, cmd_opts)

def main():
    global root
    global papyg
    root = Tk.Tk()
    Pmw.initialise(root)
    root.withdraw()

    # make gui
    papyg = PaPyGui(root)

    def make_junk(papyg):
        from papy.workers.io import dump_db_item
        papyg.add_function(dump_db_item)
        papyg.add_function(mod_name='papy.workers.io', func_name='dump_item')
        papyg.add_function(mod_name='papy.workers.io', func_name='load_item')
        imap = IMap.IMap(name='benek')
        papyg.add_imap(imap)
        papyg.add_imap(name='zenek', worker_num=3)
        papyg.add_imap(name='franek', worker_num=4, worker_type='thread')
        papyg.add_object('None', None)
        papyg.add_object('False', False)
        papyg.add_object('True', True)
        papyg.add_worker(functions=papyg.namespace['functions'].values()[0])
        papyg.add_worker(functions=papyg.namespace['functions'].values()[1:])
        w = papy.Worker(papy.workers.io.dump_pickle_stream, name='asferoth')
        papyg.add_worker(w)
        p = papy.Piper(w, name='juhas_zmarl')
        xtra = {'x':10, 'y':20}

        papyg.add_piper(p)
        papyg.add_piper(worker=papyg.namespace['workers'].values()[0], name='a')
        papyg.add_piper(worker=papyg.namespace['workers'].values()[1])
        papyg.add_piper_canvas(papyg.namespace['pipers'].values()[0], xtra)
        papyg.add_piper_canvas(papyg.namespace['pipers'].values()[1])
        papyg.add_piper_canvas(papyg.namespace['pipers'].values()[2])

    make_junk(papyg)

    # show gui
    root.deiconify()
    root.mainloop()


if __name__ == '__main__':
    main()
