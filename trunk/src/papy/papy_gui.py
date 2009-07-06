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
        self.make_widgets()
        self.pack()

    def make_widgets(self):

        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)

        canvas = Canvas(self, bg=O['canvas_background'], relief=SUNKEN)
        canvas.config(scrollregion=(0,0,1000,1000))
        canvas.config(highlightthickness=0)

        ybar = Scrollbar(self)
        ybar.config(command=canvas.yview)
        canvas.config(yscrollcommand=ybar.set)

        xbar = Scrollbar(self, orient=HORIZONTAL)
        xbar.config(command=canvas.xview)
        canvas.config(xscrollcommand=xbar.set)

        xbar.grid(row=1, column=0, sticky=E+W)
        ybar.grid(row=0, column=1, sticky=N+S)
        canvas.grid(row=0, column=0, sticky=N+S+E+W)
        self.canvas = canvas

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

    def _toggle_connect(self):
        if self.connect:
            self.piper_menu.entryconfig(3, state =NORMAL)
            self.piper_menu.entryconfig(4, state =DISABLED)
            self.canvas.unbind("<Motion>")
            self.connect = False
        else:
            self.piper_menu.entryconfig(3, state =DISABLED)
            self.piper_menu.entryconfig(4, state =NORMAL)
            self.canvas.bind("<Motion>", self.draw_pipe)
            self.connect = True

    def __init__(self, parent, graph, **kwargs):
        ScrolledCanvas.__init__(self, parent)

        # position of the last click
        self.lastx = 0
        self.lasty = 0
        self.lasttags = []

        self.tag_to_node = {}
        self.tag_to_Node = {}
        self.tag_to_edge = {}

        self.canvas.bind("<Button-1>",  self.mouse1_down)
        self.canvas.bind("<B1-Motion>", self.mouse1_drag)
        self.canvas.bind("<ButtonRelease-1>", self.mouse1_up)
        self.canvas.bind("<Button-3>", self.mouse3_down)
        self.canvas.bind("<ButtonRelease-3>", self.mouse3_up)
        # MacOSX compability
        self.canvas.bind("<Control-Button-1>", self.mouse3_down)
        self.canvas.bind("<Control-ButtonRelease-1>", self.mouse3_up)
        self.connect = False

        # left and right click canvas
        self.canvas_menu = Menu(root, tearoff =0)
        self.canvas_menu.add_command(label="create piper", command =self.create_piper)
        self.canvas_menu.add_separator()
        self.canvas_menu.add_command(label="cancel pipe", command =self._toggle_connect, state =DISABLED) 

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


    def draw_pipe(self, event):
        print event.x, event.y


    def create_node_tags(self):
        for node, Node in self.graph.iteritems():
            tag = self.create_node_tag(node, Node)
            Node.xtra['tag'] = tag[1]
            self.tag_to_node[tag[1]] = node
            self.tag_to_Node[tag[1]] = Node

    def move_node_tag(self, tag, dx, dy):
        # delete edges
        self.canvas.delete("%s&&%s" % ('edge', tag))
        # move node
        n1 = self.graph[self.tag_to_node[tag]]
        self.canvas.move("%s&&%s" % ('node', tag), dx, dy)
        # update position
        n1.xtra['x'] += dx
        n1.xtra['y'] += dy
        # re-draw affected edges
        edges = self.graph.incoming_edges(self.tag_to_node[tag]) +\
                self.graph.outgoing_edges(self.tag_to_node[tag])
        for edge in edges:
            self.create_edge_tag(edge)
        
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
        self.canvas.create_oval(x-12, y-12, x+12, y+12, fill=status, tags =tag,\
                                            activewidth=3.0, activefill ="plum")
        self.canvas.create_oval(x-8,  y-8,  x+8,  y+8, fill=color,  tags =tag,\
                                            state ='disabled', width =0.0)
        self.canvas.create_text(x+12, y-17, text=tag, fill='black', anchor =NW,\
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
        self.canvas.create_line(*xy1 + xy2, tags =tag)
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
        self.canvas.delete(self.lasttags[1])
        
    def remove_pipe(self):
        t1, t2 = self.lasttags[1:3]
        n1 = self.tag_to_node[t1]
        n2 = self.tag_to_node[t2]
        # remove pipe from dagger
        self.graph.del_edge((n1,n2)) # wrong!
        # remove pipes from canvas
        self.canvas.delete("%s&&%s&&%s" % ('edge', t1, t2))

    def flash_node_tag(self, tag):
        self.lower(tag)

    def mouse1_down(self, event):
        # canvas position of click
        self.lastx, self.lasty = self._canvas_coords(event.x, event.y)
        # what did we click
        self.lasttags = self.canvas.gettags(CURRENT)

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
        lasttags = self.canvas.gettags(CURRENT)
        lastx, lasty = self._canvas_coords(event.x, event.y)

        if lasttags and lasttags[0] == 'node':    
            self._menu_popup(event, self.piper_menu)
        elif lasttags and lasttags[0] == 'edge':    
            self._menu_popup(event, self.edge_menu)
        elif not lasttags:
            self._menu_popup(event, self.canvas_menu)
        
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
        icon='''R0lGODdhFQAVAPMAAAQ2PESapISCBASCBMTCxPxmNCQiJJya/ISChGRmzPz+/PxmzDQyZDQyZDQy
        ZDQyZCwAAAAAFQAVAAAElJDISau9Vh2WMD0gqHHelJwnsXVloqDd2hrMm8pYYiSHYfMMRm53ULlQ
        HGFFx1MZCciUiVOsPmEkKNVp3UBhJ4Ohy1UxerSgJGZMMBbcBACQlVhRiHvaUsXHgywTdycLdxyB
        gm1vcTyIZW4MeU6NgQEBXEGRcQcIlwQIAwEHoioCAgWmCZ0Iq5+hA6wIpqislgGhthEAOw==
        '''

        ii = PhotoImage(data=icon)
        b1 = Button(master =self, data =ii, command=None)
        b1.pack(side=LEFT)
        b2 = Button(master =self, image =ii, command=None)
        b2.pack(side=LEFT)

class PapyMainFrame(Frame):
    def __init__(self, parent =None, title =None,**kwargs):
        Frame.__init__(self, parent)
        self.make_widgets(title)
        self.pack(expand =YES, fill =BOTH)

    def make_widgets(self, title =None):
        # top menu
        self.menu_bar = MenuBar(self.master)
        self.master.config(menu =self.menu_bar)

        # toolbar
        self.tool_bar = ToolBar(self.master)
        self.tool_bar.pack(side =TOP, fill=X)

        # canvas
        self.master.title(title or O['app_name'])
        self.graph = GraphCanvas(self, graph =g, background=O['canvas_background'])
        self.graph.pack(expand =YES, fill =BOTH)

        # statusbar
        self.status_bar = StatusBar(self)
        self.status_bar.pack(side =BOTTOM, fill =X)



if __name__ == '__main__':

    def gui_start():
        global root, O
        O = Options()
        root = Tk()
        root.protocol("WM_DELETE_WINDOW", gui_stop)
        root.frame = PapyMainFrame(root)
        root.mainloop()

    def gui_stop():
        root.destroy()

    gui_thread = Thread(target =gui_start)
    gui_thread.start()

