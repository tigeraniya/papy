#!/usr/bin/env python
# Python
from threading import Thread
from Queue import Queue
from code import InteractiveConsole
from functools import partial

# Tkinter imports
from Tkinter import *
from tkMessageBox import *

# papy
from papy import *


g = Graph()
g.add_node('node_1')
g.add_node('node_2')
g['node_1'].xtra['x'] = 25
g['node_1'].xtra['y'] = 25
g['node_1'].xtra['color'] = 'red'
g['node_1'].xtra['status'] = 'green'
g['node_1'].xtra['screen_name'] = 'node_1'
g['node_2'].xtra['x'] = 75
g['node_2'].xtra['y'] = 75
g['node_2'].xtra['color'] = 'blue'
g['node_2'].xtra['status'] = 'orange'
g['node_1'].xtra['screen_name'] = 'node_2'
g.add_edge(('node_1', 'node_2'))


class Options(dict):

    defaults = (('app_name', 'PaPy'),
                ('node_color', 'blue'),
                ('node_status', 'green'),
                ('canvas_background', 'yellow'),
                )

    def __init__(self, options =None):
        init = dict(self.defaults)
        init.update(options or {})
        dict.__init__(self, init)





class ScrolledText(Frame):

    def __init__(self, parent =None, text ='', file =None):
        Frame.__init__(self, parent)
        self.pack(expand=YES, fill=BOTH)
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


class ScrolledCanvas(Frame):

    def __init__(self, parent):
        Frame.__init__(self, parent)
        self.pack(expand=YES, fill=BOTH)
        self.make_widgets()


    def make_widgets(self):
        canvas = Canvas(self, bg=O['canvas_background'], relief=SUNKEN)
        canvas.config(scrollregion=(0,0,1000,1000))
        canvas.config(highlightthickness=0)


        ybar = Scrollbar(self)
        ybar.config(command=canvas.yview)
        canvas.config(yscrollcommand=ybar.set)

        xbar = Scrollbar(self, orient=HORIZONTAL)
        xbar.config(command=canvas.xview)
        canvas.config(xscrollcommand=xbar.set)

        ybar.pack(side=RIGHT, fill=Y)
        xbar.pack(side=BOTTOM, fill=X)

        canvas.pack(side=LEFT, expand=YES, fill=BOTH)
        self.canvas = canvas


class Shell(ScrolledText):

    @staticmethod
    def iconsole():
        """
        """
        ic = InteractiveConsole()
        ic.interact()

    def kill_ic(self):
        self.lineque.put('exit()\n')
        self.quit()

    def keybuffer(self, key):
        """
        """
        if '\x04' == key.char: #ctrl-D
            put('exit()\n')
            return
        elif 'BackSpace' == key.keysym:
            try:
                self.charbuf.pop()
            except IndexError:
                pass
        elif 'Return' == key.keysym:
            line = "".join(self.charbuf) + "\n"
            self.lineque.put(line)
            self.charbuf = []
        elif len(key.char) == 1:
            self.charbuf.append(key.char)


    def __init__(self, parent =None):
        ScrolledText.__init__(self, parent =None)
        self.text.bind('<Key>', self.keybuffer)

        self.charbuf = []
        self.lineque = Queue()

        sys.stdout = self
        sys.stderr = self
        sys.stdin = self

        self.master.protocol("WM_DELETE_WINDOW", self.kill_ic)
        self.ic = Thread(target =self.iconsole)
        self.ic.deamon = True
        self.ic.start()

    def readline(self):
        return self.lineque.get()

    def write(self, stuff):
        self.text.insert("end", stuff)
        self.text.yview_pickplace("end")

    def writelines(self, lines):
        for line in lines:
            self.write(line)

    def flush(self):
        # typically stdout is buffered
        return None

    def fileno(self):
        # this is a work-around the file-like stdin assumptions
        # in multiprocessing/process.py
        raise OSError


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




class GraphCanvas(ScrolledCanvas):

    def _menu_popup(self, event, menu):
        menu.post(event.x_root, event.y_root)

    def _canvas_coords(self, x, y):
        return (int(self.canvas.canvasx(x)), int(self.canvas.canvasy(y)))


    def __init__(self, parent, graph, **kwargs):
        ScrolledCanvas.__init__(self, parent)

        # position of the last click
        self.lastx = 0
        self.lasty = 0
        self.tag_to_node = {}
        self.tag_to_Node = {}
        self.tag_to_edge = {}

        self.canvas.bind("<Button-1>",  self.mouse1_down)
        self.canvas.bind("<B1-Motion>", self.mouse1_drag)
        self.canvas.bind("<ButtonRelease-1>", self.mouse1_up)
        self.canvas.bind("<Button-3>", self.mouse3_down)
        #self.bind("<ButtonRelease-3>", self.mouse3_up)

        self.canvas_menu = Menu(root, tearoff=0)
        self.canvas_menu.add_command(label="create piper", command =self.create_piper)
        self.canvas_menu.add_separator()
        self.canvas_menu.add_command(label="Home")

        self.graph = graph
        self.create_node_tags()
        self.create_edge_tags()

    def create_node_tags(self):
        for node, Node in self.graph.iteritems():
            tag = self.create_node_tag(node, Node)
            Node.xtra['tag'] = tag[1]
            self.tag_to_node[tag[1]] = node
            self.tag_to_Node[tag[1]] = Node
    
    def create_edge_tags(self):
        for edge in self.graph.edges():
            tag = self.create_edge_tag(edge)
            self.tag_to_edge[tag[1]] = edge
    
    def create_node_tag(self, node, Node):
        x = (Node.xtra.get('x') or self.lastx)
        y = (Node.xtra.get('y') or self.lasty)
        status = (Node.xtra.get('status') or O['node_status'])
        color  = (Node.xtra.get('color')  or O['node_color'])
        tag = ("node", 'node_%s' % id(node))
        self.canvas.create_oval(x-12, y-12, x+12, y+12, fill=status, tags =tag, activewidth=3.0, activefill ="plum")
        self.canvas.create_oval(x-8,  y-8, x+8, y+8, fill=color, tags =tag, state ='disabled', width =0.0)
        self.canvas.create_text(x+12, y-17, text=tag, fill='black', anchor =NW, tags =tag)
        return tag

    def create_edge_tag(self, edge):
        N1 = self.graph[edge[0]]
        N2 = self.graph[edge[1]]
        t1 = N1.xtra['tag']
        t2 = N2.xtra['tag']
        xy1 = N1.xtra['x'], N1.xtra['y']
        xy2 = N2.xtra['x'], N2.xtra['y']
        tag = ("edge", t1, t2)
        self.canvas.create_line(*xy1 + xy2, tags =tag)


        print xy1, xy2
        print self.canvas.coords(t1)
        print self.canvas.coords(t2)


        
        return tag

    def create_piper(self):
        print 'c', self.lastx, self.lasty
        print dir(self)
        self.create_node_tag({},'90809809821')

    def flash_node_tag(self, tag):
        self.lower(tag)

    def mouse1_down(self, event):
        self.lastx, self.lasty = self._canvas_coords(event.x, event.y)
        print 'canvas clicked at: %s-%s' % (self.lastx, self.lasty)

        self.canvas.scan_mark(self.lastx, self.lasty)


    def mouse1_drag(self, event):
        old_x, old_y = self.lastx, self.lasty
        self.lastx, self.lasty = self._canvas_coords(event.x, event.y)
        dx, dy = self.lastx - old_x, self.lasty - old_y

        print 'mouse at: %s-%s' % (self.lastx, self.lasty)
        tags = self.canvas.gettags(CURRENT)
        if tags and tags[0] == 'node':
            self.canvas.delete("%s&&%s" % ('edge', tags[1]))
            n1 = self.graph[self.tag_to_node[tags[1]]]
            self.canvas.move("%s&&%s" % ('node', tags[1]), dx, dy)
            n1.xtra['x'] += dx
            n1.xtra['y'] += dy



   
            edges = self.graph.incoming_edges(self.tag_to_node[tags[1]]) +\
                    self.graph.outgoing_edges(self.tag_to_node[tags[1]])
            for edge in edges:
                self.create_edge_tag(edge)
        if tags and tags[0] == 'edge':
            pass

        if not tags:
            self.canvas.scan_dragto(dx, dy)



    def mouse1_up(self, event):
        print 'canvas released at: %s-%s' % (event.x, event.y)

    def mouse3_down(self, event):
        self.lastx, self.lasty = self._canvas_coords(event.x, event.y)
        print 'canvas right-clicked at: %s-%s' % (self.lastx, self.lasty)
        self._menu_popup(event, self.canvas_menu)



class PapyMainFrame(Frame):
    def __init__(self, parent =None, title =None,**kwargs):
        Frame.__init__(self, parent)
        self.pack(expand =YES, fill =BOTH)
        self.create_widgets(title)

    def create_widgets(self, title =None):
        self.menu_bar = MenuBar(self.master)
        self.master.config(menu =self.menu_bar)
        self.master.title(title or O['app_name'])

        self.graph = GraphCanvas(self, graph =g, background=O['canvas_background'])
        self.graph.pack(expand =YES, fill =BOTH)

        self.shell = Shell(self)
        self.shell.pack(expand =YES, fill =BOTH)

        self.status_bar = StatusBar(self)
        self.status_bar.pack(expand =YES, fill =BOTH)


if __name__ == '__main__':
    root = Tk()
    O = Options()
    frame = PapyMainFrame(root)
    root.mainloop()
