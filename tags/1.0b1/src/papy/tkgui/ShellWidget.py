#!/usr/bin/env python
"""
Tkinter ShellWidget
"""
try:
    from KillThread import KThread as Thread
except ImportError:
    from threading import Thread
from Queue import Queue, Empty
from code import InteractiveConsole
import rlcompleter
import string
from Tkconstants import YES, BOTH, INSERT, END
from Tkinter import Tk
import Pmw
import sys

PRINTABLE = string.letters + string.digits + string.punctuation + ' '
SHELL_HISTORY = 1000

class StreamQueue(Queue):

    def readline(self):
        return self.get()

    def write(self, cargo):
        self.put(cargo)

    def flush(self):
        pass

    def fileno(self):
        # this is a work-around the file-like stdin 
        # assumptions in multiprocessing/process.py
        raise OSError


class PythonShell(Pmw.ScrolledText):

    def __init__(self, parent, namespace=None, **kwargs):
        Pmw.ScrolledText.__init__(self, parent, **kwargs)
        self.namespace = namespace or globals()
        self.tabnum = -1
        self.offset = 1
        self.words = []
        self.charbuf = []
        self.charbuf_position = 0   # position in line
        self.linebuf = []
        self.linebuf_position = 0   # position 
        self.charbuf_history = None # lines entered
        self.text = self.component('text')
        self.text.bind('<Key>', self.keybuffer)
        self.completer = rlcompleter.Completer()
        self.start_console()
        self.poll_output()

    def start_console(self):
        self.stdin = StreamQueue()
        self.stdout = StreamQueue()
        self.stderr = self.stdout
        def ic():
            sys.stdin = self.stdin
            sys.stdout = self.stdout
            sys.stderr = self.stderr
            ic = InteractiveConsole(self.namespace)
            ic.interact()
        self.ic = Thread(target=ic)
        self.ic.daemon = True
        self.ic.start()

    def kill_console(self, *args):
        self.clear()
        self.stdin.put('exit()\n')
        self.start_console()

    def poll_output(self):
        while self.stdout.qsize():
            try:
                output = self.stdout.get(0)
                self.appendtext(output)
                last_char = self.index("%s-1c" % INSERT) # this is a _huge_ hack
                self.mark_set('LEFT_END', last_char)
            except Empty:
                pass
        self.after(100, self.poll_output)

    def replace(self, text):
        self.charbuf = list(text)
        self.charbuf_position = len(text)
        self.delete('LEFT_END+%sc' % 1, END)
        self.appendtext(text)

    def keybuffer(self, key):
        """
        """
        if key.keysym == 'Tab':
            self.tabnum += 1
            if not self.tabnum:
                # first-tab, last_word
                self.words = "".join(self.charbuf).split(' ')
            guess = self.completer.complete(self.words[-1], self.tabnum)
            if guess:
                words = self.words[:-1] + [guess]
                text = " ".join(words)
            else:
                # reached end
                self.tabnum = -1
                text = " ".join(self.words)
            self.replace(text)
            return 'break'
        else:
            self.tabnum = -1
            self.text['state'] = 'normal'
            if key.keysym == 'Return':
                self.mark_set(INSERT, END) # move to the END
                line = "".join(self.charbuf)
                self.stdin.put(line + '\n')
                self.linebuf.append(line)
                self.charbuf = []
                self.charbuf_position = 0
                self.linebuf_position = 0
                if len(self.linebuf) > SHELL_HISTORY:
                    del self.linebuf[0]
            elif self.compare('LEFT_END', '>', INSERT):
                self.text['state'] = 'disabled'
            elif key.keysym in ('Up', 'Down'):
                if key.keysym == 'Up':
                    self.linebuf_position -= 1
                elif key.keysym == 'Down':
                    self.linebuf_position += 1
                try:
                    text = self.linebuf[self.linebuf_position]
                    self.replace(text)
                except IndexError:
                    if key.keysym == 'Up':
                        self.linebuf_position = 0
                    elif key.keysym == 'Down':
                        self.linebuf_position = -1
                return 'break'
            elif 'Left' == key.keysym and self.charbuf_position:
                self.charbuf_position -= 1
            elif 'Right' == key.keysym and len(self.charbuf) - self.charbuf_position:
                self.charbuf_position += 1
            elif 'BackSpace' == key.keysym and self.charbuf_position:
                self.charbuf_position -= 1
                self.charbuf.pop(self.charbuf_position)
            elif 'Delete' == key.keysym and len(self.charbuf) - self.charbuf_position:
                self.charbuf.pop(self.charbuf_position)
            elif len(key.char) == 1 and key.char in PRINTABLE:
                self.charbuf.insert(self.charbuf_position, key.char)
                self.charbuf_position += 1
                if key.char == ' ':
                    self.offset = self.charbuf_position + 1
            else:
                return 'break'

if __name__ == '__main__':
    root = Tk()
    shell = ShellWidget(root)
    shell.pack(expand=YES, fill=BOTH)
    root.mainloop()

