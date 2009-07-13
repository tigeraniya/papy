#!/usr/bin/env python
from Tkinter import *
import Pmw

from Queue import Queue, Empty
from threading import Thread
from code import InteractiveConsole

class StreamQueue(Queue):

    def read(self):
        return self.get()

    def readline(self):
        return self.get()

    def write(self, cargo):
        return self.put(cargo)

    def writeline(self):
        return self.put(carg)

    def writelines(self, lines):
        for line in lines:
            self.writeline(line)


class ShellWidget(Pmw.ScrolledText):

    def __init__(self, parent, **kwargs):
        Pmw.ScrolledText.__init__(self, parent, **kwargs)
        self.stdin = StreamQueue()
        self.stdout = StreamQueue()
        self.stderr = self.stdout
        self.charbuf = []
        self.text = self.component('text')
        self.text.bind('<Key>', self.keybuffer)
        self.poll_output()

    def poll_output(self):
        while self.stdout.qsize():
            try:
                output = self.stdout.get()
                self.appendtext(output)
            except Empty:
                pass
        self.after(100, self.poll_output)

    def keybuffer(self, key):
        """
        """
        #print key.char
        if '\x04' == key.char: #ctrl-D
            self.stdin.put('exit()\n')
            return
        elif 'BackSpace' == key.keysym:
            try:
                self.charbuf.pop()
            except IndexError:
                pass
        elif 'Return' == key.keysym:
            line = "".join(self.charbuf) + "\n"
            self.stdin.put(line)
            self.charbuf = []
        elif len(key.char) == 1:
            self.charbuf.append(key.char)

    def kill_ic(self):
        self.stdin.put('exit()\n')

if __name__ == '__main__':

    
    def ic(ns =None):
        sys.stdin = frame.stdin
        sys.stdout = frame.stdout
        sys.stderr = frame.stderr
        ic = InteractiveConsole(ns)
        ic.interact()

    root = Tk()
    frame = ShellWidget(root)
    frame.pack()
    namespace = globals()
    console_thread = Thread(target =ic)
    console_thread.daemon = True
    console_thread.start()
    root.mainloop()
