#!/usr/bin/env python
# -*- coding: utf-8 -*-
#TODO: shell - restart thread
#TODO: status bar - make it useful or drop it';
#TODO: fix create IMap
#TODO: better icons
#TODO: write add object convenience function


"""
The PaPy gui written in Tkinter using Pmw.
"""
# PaPy/IMap imports
import papy
import IMap

# Python imports
from itertools import imap, izip
import inspect
import sys
import os

# Tkinter/Pmw/idlelib imports
# watch for errors on multiprocessing/Tkinter/linux
import Pmw
import Tkinter as Tk
import tkFileDialog
#import tkMessageBox as tkm
from Tkconstants import *

# Boundled Generic widgets
from TreeWidget import TreeItem, TreeNode
from ShellWidget import PythonShell

def get_name(object, namespaces):
    name = None
    try:
        name = object.name
    except AttributeError:
        try:
            name = object.__name__
        except AttributeError:
            for namespace in namespaces:
                for some_name, some_object in namespace.iteritems():
                    if some_object is object:
                        name = some_name
                        return name
    return name


class RootItem(TreeItem):

    def __init__(self, items, item_pyclass, tree, icon_name=None):
        self.items = items
        self.item_pyclass = item_pyclass
        self.tree = tree
        self.icon_name = icon_name

    def IsExpandable(self):
        return True

    def GetSubList(self):
        return [self.item_pyclass(item, self) for item in self.items.values()]

    def GetIconName(self):
        return self.icon_name

    def OnSelect(self):
        self.tree.update_selected(None)


class _TreeItem(TreeItem):

    def __init__(self, item, root_):
        self.item = item
        self.root = root_

    def GetText(self):
        return get_name(self.item, (self.root.items,))

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
        return get_name(attr, []) or repr(attr)

    def SetText(self, text):
        text = text.split(':')[1]
        setattr(self.item, self.attr.lower(), text)

    def IsExpandable(self):
        return False

    def IsEditable(self):
        return False


class FunctionTreeItem(TreeItem):

    def __init__(self, item, func):
        self.item = item
        self.func = func

    def GetIconName(self):
        return 'python'

    def GetSelectedIconName(self):
        return 'python'

    def GetText(self):
        return get_name(self.func, [])

    def OnSelect(self):
        self.item.root.tree.update_selected(self.func)
        papyg.function_text['text_state'] = NORMAL
        papyg.function_text.settext(inspect.getsource(self.func))
        papyg.function_text['text_state'] = DISABLED

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
        return get_name(self.func, [])

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
        return [FuncArgsTreeItem(self, f, a) for f, a in izip(self.item.task, self.item.args)]


class ModuleTreeItem(_TreeItem):

    def get_functions(self):
        """
        Returns a list of function defined in a given module.
        """
        fs = [v for v in self.item.__dict__.values() if inspect.isfunction(v) \
              and inspect.getmodule(v) is self.item]
        return fs

    def OnSelect(self):
        self.root.tree.update_selected(self.item)
        papyg.function_text['text_state'] = NORMAL
        papyg.function_text.clear()
        papyg.function_text['text_state'] = DISABLED

    def GetSubList(self):
        return [FunctionTreeItem(self, f) for f in self.get_functions()]


class ObjectTreeItem(_TreeItem):

    def IsExpandable(self):
        return False

    def IsEditable(self):
        return False

    def GetSubList(self):
        pass

    def GetSubItems(self):
        pass

    def GetText(self):
        return "type:%s, object:%s" % (repr(type(self.item)), repr(self.item))

    def GetLabelText(self):
        return "name:%s" % repr(get_name(self.item, (self.root.items,)))


class Tree(object):
    """
    Tree.
    """

    def __init__(self, parent, items, dialogs, name, **kwargs):
        self.parent = parent
        self.items = items
        self.dialogs = dialogs
        self.name = name
        self.selected_item = None
        self.name = kwargs.get('name') or self.name
        self.label_text = kwargs.get('label_text') or self.name

        # make button labels
        for label in ('add', 'del'):
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
        self.buttons.add(self.add_text, command=self.add_tree)
        self.buttons.add(self.del_text, command=self.del_tree)

        self.group = Pmw.Group(self.frame, tag_pyclass=Tk.Label,
                                              tag_text=self.label_text)

        canvas = Pmw.ScrolledCanvas(self.group.interior())
        self.canvas = canvas.component('canvas')
        self.canvas.config(bg=O[self.name + '_background'], width=200)
        self.buttons.pack(side=BOTTOM, anchor=W)
        self.root = TreeNode(self.canvas, None, \
        self.root_pyclass(self.items, self.item_pyclass, self, O[self.name + '_root_icon']))

        canvas.pack(fill=BOTH, expand=YES)
        self.group.pack(fill=BOTH, expand=YES)

    def add_tree(self):
        self.dialogs['add_%s' % self.name.lower()[:-1]].activate()

    def del_tree(self):
        if self.selected_item is not None:
            try:
                item_name = self.selected_item.name
            except AttributeError:
                try:
                    item_name = self.selected_item.__name__
                except AttributeError:
                    item_name = self.selected_item
            self.dialogs['del_tree'](self.name, item_name)
        else:
            self.dialogs['error']('No %s selected.' % self.name[:-1])


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
                         command=self.dialogs['new'].activate,
                           label='New')

        self.addmenuitem('File', 'command', 'Load',
		                 command=self.dialogs['load_file'].activate,
		                   label='Load')

        self.addmenuitem('File', 'command', 'Save',
		                 command=self.dialogs['save_file'].activate,
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

    def _enter(self, event):
        self.canvas['cursor'] = 'hand2'

    def _leave(self, event):
        self.canvas['cursor'] = 'arrow'

    def _canvas_coords(self, x, y):
        return (int(self.canvasx(x)), int(self.canvasy(y)))

    def _last_click(self, event):
        self._update_canvas()
        # canvas position of click
        self.lastxy = self._canvas_coords(event.x, event.y)
        # what did we click
        self.lasttags = self.gettags(CURRENT)

    def _create_object_graphics(self, obj_name, x=None, y=None):
        tag = ('object', obj_name)
        self.delete("%s&&%s" % (tag[0], tag[1]))

        width = O['graph_object_width']
        awidth = O['graph_active_object_width']
        border = O['graph_object_border']
        aborder = O['graph_active_object_border']
        fill = O['graph_object_fill']

        width = awidth if obj_name in self.current_obj_name_position else width
        border = aborder if obj_name in self.current_obj_name_position else border

        item = self.create_text(x, y, text=obj_name, tags=tag + ('text',), state='disabled')
        # create box
        box = self.bbox(item)
        box = (box[0] - 4, box[1] - 4, box[2] + 4, box[3] + 4)
        self.create_rectangle(*box,
                              activewidth=awidth,
                              width=width,
                              activeoutline=aborder,
                              outline=border,
                              fill=fill,
                              tags=tag)
        self._update_canvas()

    def _create_piper_graphics(self, Node):
        x = Node.xtra['x']
        y = Node.xtra['y']
        tag = Node.xtra['tag']
        self.delete("%s&&%s" % (tag[0], tag[1]))
        status = Node.xtra.get('status') or O['graph_node_status']
        connected = O['graph_node_external'] if Node.xtra.get('obj_name') else O['graph_node_internal']
        fill = Node.xtra.get('fill') or O['graph_node_fill']
        width = O['graph_node_width']
        awidth = O['graph_active_object_width']
        border = O['graph_object_border']
        aborder = O['graph_active_object_border']


        width = awidth if Node is self.current_piper_Node[1] else width
        border = aborder if Node is self.current_piper_Node[1] else border

        #pipe
        self.create_rectangle(x - 5, y - 25, x + 5, y + 25,
                        activewidth=awidth,
                        width=width,
                        activeoutline=aborder,
                        outline=border,
                        fill=fill,
                        tags=tag)
        #text
        nm_ = self.create_text(x + 9, y - 25,
                         text='Name: %s' % Node.xtra.get('name'),
                         fill='black',
                         font=O['default_font'],
                         anchor=NW, tags=tag + ('text',))
        nm_box = self.bbox(nm_)
        st_ = self.create_text(x + 9, nm_box[3] + 2,
                         text='Status:',
                         fill='black',
                         font=O['default_font'],
                         anchor=NW, tags=tag + ('text',))
        st_box = self.bbox(st_)
        cn_ = self.create_text(x + 9, st_box[3] + 2,
                         text='External input:',
                         fill='black',
                         font=O['default_font'],
                         anchor=NW, tags=tag + ('text',))
        cn_box = self.bbox(cn_)

        x1 = st_box[2] + 2 + 2
        y2 = st_box[3] - 2
        y1 = st_box[1] + 2
        x2 = y2 - y1 + x1
        self.create_oval(x1, y1, x2, y2,
                         fill=status,
                         tags=tag)
        x1 = cn_box[2] + 2 + 2
        y2 = cn_box[3] - 2
        y1 = cn_box[1] + 2
        x2 = y2 - y1 + x1
        self.create_oval(x1, y1, x2, y2,
                         fill=connected,
                         tags=tag)
        self._update_canvas()

    def _create_pipe_graphics(self, Node1, Node2):
        xy1 = Node1.xtra['x'], Node1.xtra['y'] + 25
        xy2 = Node2.xtra['x'], Node2.xtra['y'] - 25
        tag = ('pipe', Node1.xtra['tag'][1], Node2.xtra['tag'][1])
        self.delete("%s&&%s&&%s" % (tag[0], tag[1], tag[2]))


        width = O['graph_pipe_width']
        awidth = O['graph_active_pipe_width']
        fill = O['graph_pipe_fill']
        afill = O['graph_active_pipe_fill']

        if Node1 is self.current_pipe_Node_pipe[1][0] and \
           Node2 is self.current_pipe_Node_pipe[1][1]:
            width = awidth
            fill = afill

        self.create_line(*xy1 + xy2, \
                         activefill=afill, \
                         fill=fill, \
                         activewidth=awidth, \
                         width=width, \
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
        self._update_canvas()

    def _reselect(self, current_obj_name_position, \
                        current_piper_Node, \
                        current_pipe_Node_pipe):
        # old current -> previous
        self.previous_obj_name_position = self.current_obj_name_position
        self.previous_piper_Node = self.current_piper_Node
        self.previous_pipe_Node_pipe = self.current_pipe_Node_pipe
        # new current -> current
        self.current_obj_name_position = current_obj_name_position
        self.current_piper_Node = current_piper_Node
        self.current_pipe_Node_pipe = current_pipe_Node_pipe
        #
        current_Node = self.current_piper_Node[1]
        current_Node_pipe = self.current_pipe_Node_pipe[1]
        previous_Node = self.previous_piper_Node[1]
        previous_Node_pipe = self.previous_pipe_Node_pipe[1]
        # deemphasize previous object, Node or Node pipe
        if self.previous_obj_name_position != [None, None, None]:
            self._create_object_graphics(*self.previous_obj_name_position)
        if previous_Node is not None:
            self._create_piper_graphics(previous_Node)
        if previous_Node_pipe != (None, None):
            self._create_pipe_graphics(*previous_Node_pipe)
        # emphasize current object Node or Node pipe
        if self.current_obj_name_position != [None, None, None]:
            self._create_object_graphics(*self.current_obj_name_position)
        if current_Node is not None:
            self._create_piper_graphics(current_Node)
        if current_Node_pipe != (None, None):
            self._create_pipe_graphics(*current_Node_pipe)

    def _update_canvas(self):
        # un-clutter
        self.canvas_menu.unpost()
        self.pipe_menu.unpost()
        self.resizescrollregion()
        self.canvas.focus_set()
        self.tag_raise('pipe')
        self.tag_raise('text')

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
        # canvas 'objects'
        self.tag_to_obj_name_position = {}
        self.tag_to_piper = {}
        self.tag_to_Node = {}
        self.tag_to_pipe = {}
        self.tag_to_Node_pipe = {}
        self.current_obj_name_position = [None, None, None]
        self.current_piper_Node = (None, None)
        self.current_pipe_Node_pipe = ((None, None), (None, None))
        self.previous_obj_name_position = [None, None, None]
        self.previous_piper_Node = (None, None)
        self.previous_pipe_Node_pipe = ((None, None), (None, None))
        # button bindings 
        self.canvas.bind("<Button-1>", self.mouse_down)
        self.canvas.bind("<B1-Motion>", self.mouse1_drag)
        self.canvas.bind("<ButtonRelease-1>", self.mouse1_up)
        self.canvas.bind("<Button-3>", self.mouse_down)
        self.canvas.bind("<B3-Motion>", self.mouse3_drag)
        self.canvas.bind("<ButtonRelease-3>", self.mouse3_up)
        # MacOSX compability
        self.canvas.bind("<Control-Button-1>", self.mouse_down)
        self.canvas.bind("<Control-B1-Motion>", self.mouse3_drag)
        self.canvas.bind("<Control-ButtonRelease-1>", self.mouse3_up)
        self.canvas.tag_bind('piper', '<Enter>', self._enter)
        self.canvas.tag_bind('piper', '<Leave>', self._leave)
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
        # right click object

    # methods modifying the contents of the canvas

    def add_object(self, obj_name, x=None, y=None):
        tag = ('object', obj_name)
        obj_name_position = [obj_name, x or 50, y or 50]
        self.tag_to_obj_name_position[tag] = obj_name_position
        self._create_object_graphics(*obj_name_position)
        self.lasttags = tag

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
        self.tag_to_pipe[tag] = (piper1, piper2)
        self.tag_to_Node_pipe[tag] = (Node1, Node2)
        self.lasttags = tag

    def del_object(self, tag=None):
        tag = tag or self.lasttags
        if not tag:
            return None
        obj_name_position = self.current_obj_name_position
        if obj_name_position == [None, None, None]:
            return None
        tag = tag[:2]
        # delete object from canvas
        self.delete(tag[1])
        # clean object mappings
        self.tag_to_obj_name_position.pop(tag)
        # clean lasttag
        self.current_obj_name_position = [None, None, None]
        self.lasttags = None
        return obj_name_position[0]

    def del_piper(self, tag=None):
        tag = tag or self.lasttags
        if not tag:
            return None
        piper, Node = self.current_piper_Node
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
                self.tag_to_pipe.pop(tag)
                self.tag_to_Node_pipe.pop(tag)
        # clean lasttag
        self.lasttags = None
        self.current_piper_Node = (None, None)
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
        self.tag_to_pipe.pop(tag)
        piper1 = self.tag_to_piper[('piper', tag[1])]
        piper2 = self.tag_to_piper[('piper', tag[2])]
        self.lasttags = None
        return piper1, piper2

    # methods for mouse events

    def mouse_down(self, event):
        self._last_click(event)

        if self.lasttags and self.lasttags[0] == 'piper':
            # left click on a piper -> select piper
            current_tag = self.lasttags[:2]
            current_piper_Node = self.tag_to_piper[current_tag], \
                                 self.tag_to_Node[current_tag]
            self._reselect([None, None, None],
                           current_piper_Node,
                           ((None, None), (None, None)))
            if event.num == 3:
                pass
        elif self.lasttags and self.lasttags[0] == 'pipe':
            # left click on a pipe -> select pipe
            current_tag = self.lasttags[:3]
            current_pipe = self.tag_to_piper[('piper', current_tag[1])], \
                           self.tag_to_piper[('piper', current_tag[2])]
            current_Node_pipe = self.tag_to_Node_pipe[current_tag]
            current_pipe_Node_pipe = (current_pipe, current_Node_pipe)
            self._reselect([None, None, None],
                           (None, None),
                           current_pipe_Node_pipe)
            if event.num == 3:
                pipe = self.pipe_menu.post(event.x_root, event.y_root)
        elif self.lasttags and self.lasttags[0] == 'object':
            current_tag = self.lasttags[:2]
            current_obj_name_position = self.tag_to_obj_name_position[current_tag]
            for Node in self.tag_to_Node.itervalues():
                if Node.xtra.get('obj_name') == current_tag[1]:
                    Node.xtra.pop('obj_name')
                    self._create_piper_graphics(Node)
            self._reselect(current_obj_name_position,
                           (None, None),
                          ((None, None), (None, None)))
        else:
            # click on the canvas -> deselect pipe or piper
            self.canvas['cursor'] = 'hand2'
            self._reselect([None, None, None],
                           (None, None),
                          ((None, None), (None, None)))
            if event.num == 3:
                self.canvas_menu.post(event.x_root, event.y_root)
            self.canvas.scan_mark(event.x, event.y)

    def mouse1_up(self, event):
        if self.current_obj_name_position == [None, None, None] and \
           self.current_pipe_Node_pipe == ((None, None), (None, None)) and \
           self.current_piper_Node == (None, None):
            # reset cursor only if up was after a down on the canvas
            self.canvas['cursor'] = 'arrow'
        self.canvas_menu.unpost()

    def mouse3_up(self, event):
        self.delete("BROKEN_PIPE")
        x, y = self._canvas_coords(event.x, event.y)
        item = self.find_closest(x, y)
        currtags = self.gettags(item)
        if (self.current_piper_Node[0] is not None) and \
           (currtags and currtags[0] == 'piper'):
            # down and up was on a piper
            if self.lasttags[:2] != currtags[:2]:
                # have different piper - -> make pipe
                src_tag = self.lasttags[:2]
                dst_tag = currtags[:2]
                current_pipe_Node_pipe = ((self.tag_to_piper[src_tag], \
                                           self.tag_to_piper[dst_tag]), \
                                          (self.tag_to_Node[src_tag], \
                                          self.tag_to_Node[dst_tag]))
                if papyg.add_pipe_canvas(current_pipe_Node_pipe[0]):
                    # added succesfully
                    self._reselect([None, None, None],
                                   (None, None),
                                   current_pipe_Node_pipe)

        elif (self.current_obj_name_position[0] is not None) and \
             (currtags and currtags[0] == 'piper'):\
            # down on object up on a piper
            src_tag = self.lasttags[:2]
            dst_tag = currtags[:2]
            new_Node = self.tag_to_Node[dst_tag]
            self.current_obj_name_position[1] = new_Node.xtra['x']
            self.current_obj_name_position[2] = new_Node.xtra['y'] - 40
            new_Node.xtra['obj_name'] = self.current_obj_name_position[0]
            self.current_piper_Node = self.tag_to_piper[dst_tag], \
                                      self.tag_to_Node[dst_tag]
            self._reselect([None, None, None], \
                           self.current_piper_Node, \
                          ((None, None), (None, None)))


    def mouse1_drag(self, event):
        if self.current_piper_Node[0] is not None:
            # moving piper
            tag = self.lasttags[:2]
            # get Node
            Node = self.tag_to_Node[tag]
            # delete all pipes of this piper
            self.delete("%s&&%s" % ('pipe', tag[1]))
            # move piper
            currxy = self._canvas_coords(event.x, event.y)
            dx, dy = currxy[0] - Node.xtra['x'], \
                     currxy[1] - Node.xtra['y'],
            if Node.xtra.get('obj_name'):
                obj_name = Node.xtra['obj_name']
                tag = tag + (obj_name,)
                self.move("(%s&&%s)||%s" % tag , dx, dy)
                self.tag_to_obj_name_position[('object', obj_name)][1] = currxy[0]
                self.tag_to_obj_name_position[('object', obj_name)][2] = currxy[1] - 45
            else:
                self.move("%s&&%s" % tag , dx, dy)
            # update Node
            Node.xtra['x'] = currxy[0]
            Node.xtra['y'] = currxy[1]

            # re-draw affected pipes
            self._redraw_pipes(Node)
            self._update_canvas()

        elif self.current_obj_name_position[0] is not None:
            # moving object
            tag = self.lasttags[:2]
            currxy = self._canvas_coords(event.x, event.y)
            dx, dy = currxy[0] - self.current_obj_name_position[1], \
                     currxy[1] - self.current_obj_name_position[2]
            self.move("%s&&%s" % tag, dx, dy)
            self.tag_to_obj_name_position[tag][1] = currxy[0]
            self.tag_to_obj_name_position[tag][2] = currxy[1]
            self.current_obj_name_position = self.tag_to_obj_name_position[tag]
            self._update_canvas()

        else:
            self.canvas.scan_dragto(event.x, event.y, gain=1)

    def mouse3_drag(self, event):
        if self.current_piper_Node[0] is not None or \
           self.current_obj_name_position[0] is not None:
            # started on a Piper or Object
            xy1 = self.lastxy
            xy2 = self._canvas_coords(event.x, event.y)
            self._create_broken_pipe_graphics(xy1, xy2)

    # methods calling global papyg

    def menu_add_pipe(self):
        return papyg.add_pipe_canvas()

    def menu_del_pipe(self):
        if papyg.del_pipe_canvas():
            # unselect a just deleted pipe
            self.current_pipe_Node_pipe = ((None, None), (None, None))

    def menu_reverse_pipe(self):
        pipe = list(papyg.del_pipe_canvas())
        pipe.reverse()
        return papyg.add_pipe_canvas(pipe)

    def menu_add_piper(self):
        return papyg.add_piper_canvas()

    def menu_del_piper(self):
        return papyg.del_piper_canvas()

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
    # this is used as a stream for ShellWidget

    def write(self, *args, **kwargs):
        self.appendtext(*args, **kwargs)


class RestrictedComboBox(Pmw.ComboBox):

    def __init__(self, parent, **kwargs):
        apply(Pmw.ComboBox.__init__, (self, parent), kwargs)
        self.component('entryfield_entry').bind('<Button-1>',
                            lambda event: 'break')
        self.component('entryfield_entry').bind('<B1-Motion>',
                            lambda event: 'break')


class LabeledWidgetClear(Pmw.LabeledWidget):

    def clear(self):
        pass
        #TODO: clear worker dialog reappearance
        # self.interior().children.values()


class NewDialog(Pmw.Dialog):

    def __init__(self, parent, **kwargs):
        kwargs['buttons'] = ('New', 'Cancel', 'Help')
        kwargs['title'] = 'Create new PaPy pipeline.'
        kwargs['command'] = self.handler
        kwargs['defaultbutton'] = 'Cancel'
        apply(Pmw.Dialog.__init__, (self, parent), kwargs)

        self.message = Tk.Label(self.interior(), text=\
                                'Do you want to start a new PaPy pipeline?')
        self.message.pack(expand=YES, fill=BOTH)
        self.geometry("+%d+%d" % (parent.winfo_rootx() + 250,
                                  parent.winfo_rooty() + 250))
        self.wm_resizable(NO, NO)

    def handler(self, result):
        if result == 'Cancel':
            self.destroy()
        elif result == 'New':
            papyg.new()
            self.destroy()
        elif result == 'Help':
            #TODO: implement common help
            pass


class _DeleteDialog(Pmw.Dialog):

    def __init__(self, parent, name, item_name, **kwargs):
        self.name = name
        self.item_name = item_name
        kwargs['buttons'] = ('Delete', 'Cancel', 'Help')
        kwargs['title'] = 'Delete PaPy %s' % self.name[:-1]
        kwargs['command'] = self.handler
        kwargs['defaultbutton'] = 'Cancel'
        apply(Pmw.Dialog.__init__, (self, parent), kwargs)

        self.message = Tk.Label(self.interior(), text=\
                                'Do you want to delete %s: %s' % \
                                (name[:-1], item_name))
        self.message.pack(expand=YES, fill=BOTH)
        self.geometry("+%d+%d" % (parent.winfo_rootx() + 250,
                                  parent.winfo_rooty() + 250))
        self.wm_resizable(NO, NO)

    def handler(self, result):
        if result == 'Cancel':
            self.destroy()
        elif result == 'Delete':
            papyg.del_tree(self.name)
            self.destroy()
        elif result == 'Help':
            #TODO: implement common help
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
        self.geometry("+%d+%d" % (parent.winfo_rootx() + 50,
                                  parent.winfo_rooty() + 50))


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
                        try:
                            e.all_clear()
                        except:
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

    name = 'IMaps'

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
                elif name in ('worker_num', 'stride', 'buffer'):
                    kwargs[name] = int(value)
                elif name == 'misc':
                    for arg_true in value:
                        kwargs[arg_true] = True
        papyg.add_tree('IMaps', **kwargs)

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
            setattr(self, a, Pmw.Counter(self.group.interior(),
                                label_text=label,
		                        labelpos='w',
                                entryfield_validate={'validator' : 'integer',
                                                     'min' : 0,
                                                     'max' : 99,
                                                     'minstrict' : 0}))

        self.misc = Pmw.RadioSelect(self.group.interior(),
                        buttontype='checkbutton',
                        orient='vertical',
                        labelpos='w',
                        label_text='Misc.:',
                        hull_borderwidth=0)
        self.misc.add('ordered')
        self.misc.add('skip')


class PiperDialog(_CreateDialog):

    name = 'Pipers'

    defaults = {(0, 'pname'):None,
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

        self.pname = Pmw.EntryField(self.group.interior(), \
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
                if name == 'pname':
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
        papyg.add_tree(self.name, **kwargs)


class ModuleDialog(object):

    name = 'Modules'

    def activate(self):
        import sys
        abs_path = tkFileDialog.askopenfilename(filetypes=\
                                                [("Python Files", "*.py")])
        if abs_path:
            papyg.add_tree(self.name, path=abs_path)


class LoadDialog(object):

    def activate(self):
        abs_path = tkFileDialog.askopenfilename(filetypes=\
                                                [("Python Files", "*.py")])
        if abs_path:
            papyg.load(abs_path)


class SaveDialog(object):

    def activate(self):
        import sys
        abs_path = tkFileDialog.asksaveasfilename(filetypes=\
                                                [("Python Files", "*.py")])
        if abs_path:
            papyg.save(abs_path)



class WorkerDialog(_CreateDialog):

    name = 'Workers'
    defaults = {(0, 'wname'):None,
                (1, 'funcsargs'): None,
                (2, 'doc'): None}
    fargs = []

    def _create(self):
        objects = papyg.namespace['objects']
        modules = papyg.namespace['modules']
        funcs = []
        kwargs = []
        func_names = self.funcs.get()
        for index, fn in enumerate(func_names):
            parts = fn.split('.')
            func_name = parts.pop()
            module_name = '.'.join(parts)
            f = getattr(modules[module_name], func_name)
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
        papyg.add_tree(self.name, functions=funcs, kwargs=kwargs)

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
        parts = self.funcs.getvalue()[0].split('.')
        func_name = parts.pop()
        module_name = '.'.join(parts)
        func = getattr(papyg.namespace['modules'][module_name], func_name)

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
        try:
            doc_string = func.__doc__
            if doc_string:
                #TODO: why does an empty string raise an exception?
                self.doc['text_state'] = NORMAL
                self.doc.insert(END, doc_string)
        except AttributeError:
            pass
        self.doc['text_state'] = DISABLED

    def create_entries(self):
        # name
        self.wname = Pmw.EntryField(self.group.interior(),
                                    labelpos='w',
		                            label_text='Name:',
                                    validate={'validator':'alphabetic'})
        # funcsargs
        self.funcsargs = LabeledWidgetClear(self.group.interior(),
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
        self.args = LabeledWidgetClear(self.funcsargs.interior(),
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
                                        text_state=DISABLED
                                        )

    def update_entries(self):
        # HARD CODED locations
        func_names = []
        for mod_name, mod in papyg.namespace['modules'].items():
            for obj_name, obj in mod.__dict__.items():
                if (inspect.isfunction(obj) or inspect.isbuiltin(obj)) and \
                    inspect.getmodule(obj) is mod:
                    # obj is a function
                    func_name = ".".join([mod_name, obj_name])
                    func_names.append(func_name)
        self.cfunc.setlist(func_names)


class PaPyGui(object):

    def __init__(self, parent, **kwargs):
        # globally change fonts
        self.toplevel = parent
        self.toplevel.option_add("*font", O['default_font'])
        self.toplevel.title(O['app_name'])
        # initialize widgets
        self.make_namespace()
        self.make_dialogs()
        self.make_widgets()
        self.update_widgets()
        self.plumber = papy.Plumber({'log_to_stream': True, 'log_stream':self.log})

    def make_namespace(self):
        # make the plumber:
        self.namespace = {}
        self.namespace['pipeline'] = self
        # PaPy modules
        self.namespace['papy'] = papy
        self.namespace['IMap'] = IMap
        # empty mappings for objects
        for name in ('modules', 'functions', 'workers', \
                     'pipers', 'imaps', 'objects'):
            self.namespace[name] = {}
        # common objects
        self.namespace['objects']['False'] = False
        self.namespace['objects']['True'] = True
        self.namespace['objects']['None'] = None
        # built in worker-functions
        for obj in papy.workers.__dict__.values():
            if inspect.ismodule(obj):
                self.namespace['modules'][obj.__name__] = obj
        # built in IMaps
        self.namespace['imaps']['process_imap'] = IMap.IMap(name='process_imap')
        self.namespace['imaps']['thread_imap'] = IMap.IMap(worker_type='thread',
                                                           name='thread_imap')

    def update_widgets(self):
        self.pipers.update()
        self.workers.update()
        self.imaps.update()
        self.modules.update()
        self.objects.update()

    def new(self):
        # only if plumber is stopped
        # remove everything from canvas
        # remove pipers and pipes from plumber
        # 
        pass

    def load(self, filename):
        #1. add filename as module
        mod = self.add_tree('Modules', path=filename)
        #2. add pipers/workers/imaps to namespace
        pipers, xtras, pipes = mod.pipeline()
        for piper, xtra in izip(pipers, xtras):
            self.add_tree('Pipers', piper)
            self.add_tree('Workers', piper.worker)
            if piper.imap is not imap:
                self.add_tree('IMaps', piper.imap)
            # 3. add piper to graph
            self.add_piper_canvas(piper, xtra)
        # 4. add pipes to canvas
        for pipe in pipes:
            self.add_pipe_canvas(pipe)

    def save(self, filename):
        self.plumber.save(filename)

    def make_dialogs(self):
        self.dialogs = {}
        # About is a Pmw, no auto withdraw
        self.dialogs['about'] = Pmw.AboutDialog(self.toplevel, applicationname='My Application')
        self.dialogs['about'].withdraw()
        # this is generated on the fly
        self.dialogs['del_tree'] = self._delete_dialog
        self.dialogs['error'] = self._error_dialog
        # and these are one-instance
        self.dialogs['add_imap'] = IMapDialog(self.toplevel)
        self.dialogs['add_worker'] = WorkerDialog(self.toplevel)
        self.dialogs['add_piper'] = PiperDialog(self.toplevel)
        self.dialogs['add_module'] = ModuleDialog()
        # these are use by the main menu
        self.dialogs['new'] = NewDialog(self.toplevel)
        self.dialogs['new'].withdraw()
        self.dialogs['load_file'] = LoadDialog()
        self.dialogs['save_file'] = SaveDialog()

    def _new_dialog(self):
        error = Pmw.MessageDialog(self.toplevel, title='PaPy Panic!',
                                        defaultbutton=0,
                                        message_text=txt)
        error.iconname('Simple message dialog')
        error.activate()

    def _delete_dialog(self, name, item_name):
        _DeleteDialog(self.toplevel, name, item_name)

    def _error_dialog(self, txt):
        # generic error dialog
        if isinstance(txt, Exception):
            txt = txt[0]
        # used to display an error text
        error = Pmw.MessageDialog(self.toplevel, title='PaPy Panic!',
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
        self.pipeline = NoteBook(self.r, ['Pipeline', 'Modules', 'IMaps', \
                                          'Workers'])
        self.io = NoteBook(self.r, ['Shell', 'Logging'])

        # pipers
        self.pipers = Tree(self.l, self.namespace['pipers'], self.dialogs,
                           'Pipers')

        self.objects = Tree(self.l, self.namespace['objects'], self.dialogs,
                            'Objects')
        # pipeline
        pipeline_ = self.pipeline.page('Pipeline')
        pipeline_.grid_rowconfigure(0, weight=1)
        pipeline_.grid_columnconfigure(1, weight=1)

        self.graph = GraphCanvas(pipeline_)
        self.pipeline_buttons = Pmw.ButtonBox(pipeline_,
                                                orient=VERTICAL,
                                                padx=0, pady=0)
        self.pipeline_buttons.add('Start\nStop', command=self.start_stop_plumber)
        self.pipeline_buttons.add('Run\nPause', command=self.run_pause_plumber)
        self.pipeline_buttons.add('Add\nPiper', command=self.add_piper_canvas)
        self.pipeline_buttons.add('Delete\nPiper', command=self.del_piper_canvas)
        self.pipeline_buttons.add('Add\nObject', command=self.add_object_canvas)
        self.pipeline_buttons.add('Delete\nObject', command=self.del_object_canvas)
        self.pipeline_buttons.grid(row=0, column=0, sticky=N)
        self.graph.grid(row=0, column=1, sticky=N + E + W + S)

        # fucntions
        modules_ = self.pipeline.page('Modules')
        self.modules = Tree(modules_,
                            self.namespace['modules'],
                            self.dialogs,
                            'Modules')
        self.function_text = Pmw.ScrolledText(modules_,
                              labelpos=N + W,
                              text_padx=O['Code_font'][1] // 2, # half-font
                              text_pady=O['Code_font'][1] // 2,
                              text_background=O['Code_background'],
                              label_text='Function code',
                              text_wrap=NONE,
                              text_state=DISABLED)
        modules_.grid_rowconfigure(0, weight=1)
        modules_.grid_columnconfigure(1, weight=1)
        self.modules.frame.grid(row=0, column=0, sticky=N + E + W + S)
        self.function_text.grid(row=0, column=1, sticky=N + E + W + S)

        # imaps
        self.imaps = Tree(self.pipeline.page('IMaps'),
                               self.namespace['imaps'],
                               self.dialogs,
                               'IMaps')
        self.imaps.frame.pack(fill=BOTH, expand=YES)
        self.workers = Tree(self.pipeline.page('Workers'), \
                            self.namespace['workers'], \
                            self.dialogs,
                            'Workers')
        self.workers.frame.pack(fill=BOTH, expand=YES)

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
        self.l.add(self.objects.frame, stretch='always')
        self.l.paneconfigure(self.pipers.frame, sticky=N + E + W + S)
        self.l.paneconfigure(self.objects.frame, sticky=N + E + W + S)
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

    def add_tree(self, obj_type, obj=None, obj_name=None, **kwargs):
        if not obj:
            if obj_type == 'Pipers':
                obj = papy.Piper(**kwargs)
            elif obj_type == 'Workers':
                obj = papy.Worker(**kwargs)
            elif obj_type == 'IMaps':
                obj = IMap.IMap(**kwargs)
            elif obj_type == 'Modules':
                dir_name = os.path.dirname(kwargs['path'])
                mod_name = os.path.basename(kwargs['path']).split('.')[0]
                sys.path.insert(0, dir_name)
                obj = __import__(mod_name)
                #sys.path.remove(dir_name) # do not pollute the path.
            elif obj_type == 'Objects':
                pass
        try:
            obj_name = obj_name or obj.name
        except AttributeError:
            obj_name = obj.__name__
        self.namespace[obj_type.lower()][obj_name] = obj
        getattr(self, obj_type.lower()).update()
        return obj

    def del_tree(self, obj_type, obj=None, obj_name=None):
        obj_type_lower = obj_type.lower()
        obj_tree = getattr(self, obj_type_lower)
        obj_ns = self.namespace[obj_type_lower]
        obj = obj or obj_tree.selected_item
        if obj is None:
            self._error_dialog('No %s selected.' % obj_type)
            return
        try:
            obj_name = obj_name or obj.name
        except AttributeError:
            try:
                obj_name = obj.__name__
            except AttributeError:
                for name_, obj_ in self.namespace[obj_type_lower].iteritems():
                    if obj_ is obj:
                        obj_name = name_
        obj_ns.pop(obj_name)
        obj_tree.update_selected(None)
        obj_tree.update()
        return obj

    def add_object_canvas(self, obj=None, obj_name=None):
        obj = obj or self.objects.selected_item
        if obj is None:
            self._error_dialog('No Object selected.')
            return
        obj_name = get_name(obj, (self.namespace['objects'],))
        if ('object', obj_name) in self.graph.tag_to_obj_name_position:
            self._error_dialog('Object alread added.')
            return
        self.graph.add_object(obj_name)
        return obj_name

    def del_object_canvas(self, obj=None, obj_name=None):
        if obj is not None:
            obj_name = get_name(obj, (self.namespace['objects'],))
        obj_name = self.graph.del_object(obj_name)
        if obj_name is None:
            self._error_dialog('No Object selected.')
        return obj_name

    def add_piper_canvas(self, piper=None, xtra=None):
        # add to GrapCanvas
        piper = piper or self.pipers.selected_item
        if not piper:
            self._error_dialog('No Piper selected.')
            return
        elif piper in self.plumber:
            self._error_dialog('Piper alread added.')
            return
        self.plumber.add_piper(piper, xtra)
        self.graph.add_piper(piper, self.plumber[piper])
        return piper

    def del_piper_canvas(self, piper=None):
        if piper:
            try:
                Node = self.plumber[piper]
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
        self.plumber.del_piper(piper, forced=True)
        return piper

    def add_pipe_canvas(self, pipe=None):
        if pipe:
            piper1, piper2 = pipe
            Node1, Node2 = self.plumber[piper1], \
                           self.plumber[piper2],
        else:
            pipe, Node_pipe = self.graph.current_pipe_Node_pipe
            piper1 , piper2 = pipe
            Node1, Node2 = Node_pipe
        try:
            self.plumber.add_pipe(pipe)
        except papy.DaggerError, excp:
            self._error_dialog(excp)
            return
        self.graph.add_pipe(piper1, Node1, piper2, Node2)
        return pipe

    def del_pipe_canvas(self, pipe=None):
        if pipe:
            try:
                Node1 = self.plumber[pipe[0]]
                Node2 = self.plumber[pipe[1]]
            except KeyError:
                self._error_dialog('Unknown Pipe.')
            tag = ('pipe', Node1.xtra['tag'], Node2.xtra['tag'])
            self.graph.del_pipe(tag)
        else:
            pipe = self.graph.del_pipe()
        self.plumber.del_edge((pipe[1], pipe[0]))
        return pipe

    def start_stop_plumber(self):
        if self.plumber._started.isSet():
            self.plumber.stop()



        else:
            inputs = []
            for piper in self.plumber.get_inputs():
                Node = self.plumber[piper]
                input_name = Node.xtra.get('obj_name')
                if not input_name:
                    self._error_dialog('Not all inputs are connected.')
                    return
                inputs.append(self.namespace['objects'][input_name])
            self.plumber.start(inputs)

    def run_pause_plumber(self):
        if self.plumber._running.isSet():
            # if running pause
            self.plumber.pause()
        else:
            self.plumber.run()


class Options(dict):
    """ Provide options throughout the PaPy Gui application.
    """

    defaults = (('app_name', 'PaPy'),
                ('log_filename', None),
                ('default_font', ("tahoma", 8)),
                ('graph_background', 'white'),
                ('graph_background_select', 'gray'),
                ('graph_node_fill', 'blue'),
                ('graph_node_status', 'green'),
                ('graph_node_external', 'green'),
                ('graph_node_internal', 'white'),
                ('graph_node_width', 2.0),
                ('graph_active_object_fill', 'red'),
                ('graph_active_node_width', 4.0),
                ('graph_node_border', 'black'),
                ('graph_active_node_border', 'red'),
                ('graph_object_fill', 'white'),
                ('graph_object_width', 2.0),
                ('graph_active_object_fill', 'red'),
                ('graph_active_object_width', 4.0),
                ('graph_object_border', 'black'),
                ('graph_active_object_border', 'red'),
                ('graph_pipe_width', 2.0),
                ('graph_active_pipe_width', 4.0),
                ('graph_pipe_fill', 'black'),
                ('graph_active_pipe_fill', 'red'),
                ('Pipers_background', 'white'),
                ('Workers_background', 'white'),
                ('Modules_background', 'white'),
                ('Objects_background', 'white'),
                ('Function_doc_background', 'white'),
                ('Function_doc_foreground', 'gray'),
                ('Shell_background', 'white'),
                ('Shell_history', 1000),
                ('Shell_fontcolor', 'black'),
                ('Shell_font', ("courier new", 9)),
                ('Code_font', ("courier new", 9)),
                ('Code_background', 'white'),
                ('Pipers_root_icon', 'pipe_16'),
                ('IMaps_root_icon', 'gear_16'),
                ('Workers_root_icon', 'component_16'),
                ('Modules_root_icon', 'python'),
                ('Objects_root_icon', 'python'),
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
    # show gui
    root.deiconify()
    root.mainloop()

if __name__ == '__main__':
    main()
