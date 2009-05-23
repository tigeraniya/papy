from papy import *
from IMap import IMap


def filter_line(inputs):
    line = inputs[0]
    if len(line) > 50 and len(line) > 50:
        return line[:-2] # strip '\r\n'
    else:
        return False

def levendist(a, b):
    # distance written by Magnus Lie Hetland
    "Calculates the Levenshtein distance between a and b."
    n, m = len(a), len(b)
    if n > m:
        # Make sure n <= m, to use O(min(n,m)) space
        a,b = b,a
        n,m = m,n
    current = range(n+1)
    for i in range(1,m+1):
        previous, current = current, [i]+[0]*n
        for j in range(1,n+1):
            add, delete = previous[j]+1, current[j-1]+1
            change = previous[j-1]
            if a[j-1] != b[i-1]:
                change = change + 1
            current[j] = min(add, delete, change)
    return current[n]

@imports([['random',['shuffle']]])
def minpdist(inputs, p):
    a, b = inputs[0]
    if not a or not b:
        return None
    al, bl = list(a.split()), list(b.split())
    mindist = 'inf'
    for i in xrange(10):
        shuffle(al)
        shuffle(bl)
        mindist = min(dist("".join(al), "".join(bl)), mindist)
    return mindist

def pipeline():
    plumber = Plumber()
    ulysses = open('ulysses_book', 'rb')
    dracula = open('dracula_book', 'rb')



    draculysses = ((u, d) for u in ulysses for d in dracula)

    shared_imap = IMap(worker_num =2)

    pfilt = Piper(filt, parallel =shared_imap)

    wminpdist = Worker(minpdist, (100,))
    pminpdist = Piper(wminpdist, parallel =shared_imap)

    pfilt([draculysses])
    plumber.add_pipe((pfilt, pminpdist))


    plumber.connect()
    plumber.start()
    output = plumber.get_outputs()[0]
    return output

if __name__ == '__main__':
    from time import time
    start = time()
    d = pipeline()
    for i in d:
        pass
    stop = time()
    print stop - start
